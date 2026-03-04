#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use serde::Serialize;
use std::{
    env,
    fs::OpenOptions,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Mutex,
    time::{Duration, Instant},
};

#[derive(Default)]
struct ProcessState {
    api: Option<Child>,
    worker: Option<Child>,
}

#[derive(Serialize)]
struct ServiceStatus {
    api_running: bool,
    worker_running: bool,
}

const STARTUP_TIMEOUT: Duration = Duration::from_secs(90);

#[tauri::command]
fn start_services(state: tauri::State<'_, Mutex<ProcessState>>) -> Result<ServiceStatus, String> {
    let mut processes = state.lock().map_err(|_| "lock poisoned".to_string())?;

    clean_stale_processes_for_port(&mut processes.api, 8081)?;
    if !is_service_running(&mut processes.api, 8081) {
        processes.api =
            Some(spawn_service("api").map_err(|e| format!("failed to start api: {e}"))?);
    }
    if !wait_for_service(8081, &mut processes.api, STARTUP_TIMEOUT) {
        let hint = read_log_tail("api", 20);
        return Err(format!(
            "API failed to become ready on http://127.0.0.1:8081. {}",
            hint
        ));
    }

    clean_stale_processes_for_port(&mut processes.worker, 8082)?;
    if !is_service_running(&mut processes.worker, 8082) {
        processes.worker =
            Some(spawn_service("worker").map_err(|e| format!("failed to start worker: {e}"))?);
    }
    if !wait_for_service(8082, &mut processes.worker, STARTUP_TIMEOUT) {
        let hint = read_log_tail("worker", 20);
        return Err(format!(
            "Worker failed to become ready on http://127.0.0.1:8082. {}",
            hint
        ));
    }

    Ok(ServiceStatus {
        api_running: is_alive(&mut processes.api),
        worker_running: is_alive(&mut processes.worker),
    })
}

#[tauri::command]
fn stop_services(state: tauri::State<'_, Mutex<ProcessState>>) -> Result<ServiceStatus, String> {
    let mut processes = state.lock().map_err(|_| "lock poisoned".to_string())?;
    terminate(&mut processes.api);
    terminate(&mut processes.worker);

    Ok(ServiceStatus {
        api_running: false,
        worker_running: false,
    })
}

#[tauri::command]
fn service_status(state: tauri::State<'_, Mutex<ProcessState>>) -> Result<ServiceStatus, String> {
    let mut processes = state.lock().map_err(|_| "lock poisoned".to_string())?;
    Ok(ServiceStatus {
        api_running: is_service_running(&mut processes.api, 8081),
        worker_running: is_service_running(&mut processes.worker, 8082),
    })
}

fn main() {
    tauri::Builder::default()
        .manage(Mutex::new(ProcessState::default()))
        .invoke_handler(tauri::generate_handler![
            start_services,
            stop_services,
            service_status
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

fn spawn_service(package: &str) -> std::io::Result<Child> {
    let services_dir = services_root();
    if !services_dir.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("services directory not found: {}", services_dir.display()),
        ));
    }

    let app_data_dir = services_dir.join(".runtime");
    std::fs::create_dir_all(&app_data_dir)?;
    let logs_dir = app_data_dir.join("logs");
    std::fs::create_dir_all(&logs_dir)?;

    let log_path = service_log_path(package);
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    let log_file_err = log_file.try_clone()?;

    Command::new("cargo")
        .arg("run")
        .arg("-p")
        .arg(package)
        .env("EXPENSE_APP_DATA_DIR", app_data_dir)
        .current_dir(services_dir)
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file_err))
        .spawn()
}

fn services_root() -> PathBuf {
    if let Ok(explicit) = env::var("EXPENSE_RS_ROOT") {
        let explicit_path = PathBuf::from(explicit);
        let cwd = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        return resolve_services_root(
            Some(explicit_path),
            cwd,
            PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        );
    }

    let cwd = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    resolve_services_root(None, cwd, PathBuf::from(env!("CARGO_MANIFEST_DIR")))
}

fn is_alive(child: &mut Option<Child>) -> bool {
    if let Some(process) = child {
        match process.try_wait() {
            Ok(Some(_)) => {
                *child = None;
                false
            }
            Ok(None) => true,
            Err(_) => false,
        }
    } else {
        false
    }
}

fn terminate(child: &mut Option<Child>) {
    if let Some(process) = child {
        let _ = process.kill();
        let _ = process.wait();
    }
    *child = None;
}

fn wait_for_service(port: u16, child: &mut Option<Child>, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }

        if !is_alive(child) {
            return false;
        }

        if Instant::now() >= deadline {
            return false;
        }

        std::thread::sleep(Duration::from_millis(250));
    }
}

fn is_service_running(child: &mut Option<Child>, port: u16) -> bool {
    is_alive(child) || is_port_open(port)
}

fn is_port_open(port: u16) -> bool {
    std::net::TcpStream::connect(("127.0.0.1", port)).is_ok()
}

fn clean_stale_processes_for_port(child: &mut Option<Child>, port: u16) -> Result<(), String> {
    // If this Tauri instance owns a healthy child, do not touch the port owner.
    if is_alive(child) {
        return Ok(());
    }

    if !is_port_open(port) {
        return Ok(());
    }

    kill_port_owner(port).map_err(|e| format!("failed to clear stale process on port {port}: {e}"))
}

#[cfg(unix)]
fn kill_port_owner(port: u16) -> std::io::Result<()> {
    // macOS/Linux: get PIDs listening on tcp:<port>
    let output = Command::new("lsof")
        .arg("-ti")
        .arg(format!("tcp:{port}"))
        .output()?;
    if !output.status.success() {
        return Ok(());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    for pid in stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let _ = Command::new("kill").arg("-TERM").arg(pid).status();
    }

    // Give processes a moment to exit gracefully.
    std::thread::sleep(Duration::from_millis(300));

    // Force kill any stubborn listener.
    let output = Command::new("lsof")
        .arg("-ti")
        .arg(format!("tcp:{port}"))
        .output()?;
    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        for pid in stdout
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            let _ = Command::new("kill").arg("-KILL").arg(pid).status();
        }
    }

    Ok(())
}

#[cfg(not(unix))]
fn kill_port_owner(_port: u16) -> std::io::Result<()> {
    Ok(())
}

fn service_log_path(package: &str) -> PathBuf {
    services_root()
        .join(".runtime")
        .join("logs")
        .join(format!("{package}.log"))
}

fn read_log_tail(package: &str, max_lines: usize) -> String {
    let path = service_log_path(package);
    let file = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(_) => return format!("Check startup logs at {}", path.display()),
    };

    let reader = BufReader::new(file);
    let mut lines: Vec<String> = reader
        .lines()
        .map_while(Result::ok)
        .filter(|line| !line.trim().is_empty())
        .collect();
    if lines.len() > max_lines {
        lines = lines.split_off(lines.len() - max_lines);
    }
    if lines.is_empty() {
        return format!("Check startup logs at {}", path.display());
    }
    format!("Log tail ({}): {}", path.display(), lines.join(" | "))
}

fn resolve_services_root(
    explicit: Option<PathBuf>,
    cwd: PathBuf,
    manifest_dir: PathBuf,
) -> PathBuf {
    if let Some(path) = explicit {
        if path.exists() {
            return path;
        }
    }

    let manifest_candidate = manifest_dir.join("../../../services/expense-rs");
    if manifest_candidate.exists() {
        return manifest_candidate;
    }

    let candidates = [
        cwd.join("services/expense-rs"),
        cwd.join("../services/expense-rs"),
        cwd.join("../../services/expense-rs"),
        cwd.join("../../../services/expense-rs"),
    ];

    for candidate in candidates {
        if candidate.exists() {
            return candidate;
        }
    }

    Path::new("services/expense-rs").to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock moved backwards")
            .as_nanos();
        std::env::temp_dir().join(format!("expense-tauri-test-{name}-{nanos}"))
    }

    #[test]
    fn resolve_services_root_prefers_explicit_existing_path() {
        let explicit = temp_path("explicit");
        std::fs::create_dir_all(&explicit).expect("create explicit path");
        let resolved = resolve_services_root(
            Some(explicit.clone()),
            PathBuf::from("/tmp"),
            PathBuf::from("/tmp/manifest"),
        );
        assert_eq!(resolved, explicit);
        let _ = std::fs::remove_dir_all(explicit);
    }

    #[test]
    fn resolve_services_root_falls_back_to_default_when_missing() {
        let resolved = resolve_services_root(
            Some(PathBuf::from("/definitely/missing/path")),
            PathBuf::from("/also/missing"),
            PathBuf::from("/manifest/missing"),
        );
        assert_eq!(resolved, PathBuf::from("services/expense-rs"));
    }

    #[test]
    fn is_alive_returns_false_for_none_and_keeps_none() {
        let mut child: Option<Child> = None;
        let alive = is_alive(&mut child);
        assert!(!alive);
        assert!(child.is_none());
    }

    #[test]
    fn wait_for_service_returns_false_for_dead_or_missing_process() {
        let mut child: Option<Child> = None;
        let ready = wait_for_service(65534, &mut child, Duration::from_millis(100));
        assert!(!ready);
    }

}
