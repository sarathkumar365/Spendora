#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use serde::Serialize;
use std::{
    env,
    fs::{create_dir_all, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Mutex,
    time::{Duration, Instant},
};
use tauri::Manager;

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

#[derive(Clone, Serialize)]
struct StartupStatus {
    state: String,
    phase: String,
    attempt: u8,
    message: Option<String>,
}

impl Default for StartupStatus {
    fn default() -> Self {
        Self {
            state: "starting".to_string(),
            phase: "idle".to_string(),
            attempt: 0,
            message: Some("starting local services".to_string()),
        }
    }
}

const STARTUP_TIMEOUT: Duration = Duration::from_secs(90);
const AUTO_START_MAX_ATTEMPTS: u8 = 3;
const RETRY_DELAY_MS: [u64; 2] = [500, 1500];
const SHUTDOWN_GRACE_TIMEOUT: Duration = Duration::from_millis(1500);

#[tauri::command]
fn start_services(
    process_state: tauri::State<'_, Mutex<ProcessState>>,
    startup_state: tauri::State<'_, Mutex<StartupStatus>>,
) -> Result<ServiceStatus, String> {
    let attempt = startup_state
        .lock()
        .ok()
        .map(|status| status.attempt.saturating_add(1))
        .unwrap_or(1);
    set_startup_status(
        &startup_state,
        "starting",
        "retry",
        attempt,
        Some("Retrying local services startup...".to_string()),
    );

    let started = Instant::now();
    let result = start_services_internal(&process_state, true);
    match &result {
        Ok(_) => {
            set_startup_status(
                &startup_state,
                "healthy",
                "healthy",
                attempt,
                Some("Services are healthy.".to_string()),
            );
            log_startup_timing("manual", "healthy", attempt, started.elapsed());
        }
        Err(error) => {
            set_startup_status(
                &startup_state,
                "failed",
                "failed_terminal",
                attempt,
                Some(error.clone()),
            );
            log_startup_timing("manual", "failed", attempt, started.elapsed());
        }
    }
    result
}

#[tauri::command]
fn stop_services(process_state: tauri::State<'_, Mutex<ProcessState>>) -> Result<ServiceStatus, String> {
    stop_services_internal(&process_state)
}

#[tauri::command]
fn service_status(process_state: tauri::State<'_, Mutex<ProcessState>>) -> Result<ServiceStatus, String> {
    service_status_internal(&process_state)
}

#[tauri::command]
fn startup_status(startup_state: tauri::State<'_, Mutex<StartupStatus>>) -> Result<StartupStatus, String> {
    let status = startup_state
        .lock()
        .map_err(|_| "startup lock poisoned".to_string())?
        .clone();
    Ok(status)
}

fn main() {
    let app = tauri::Builder::default()
        .manage(Mutex::new(ProcessState::default()))
        .manage(Mutex::new(StartupStatus::default()))
        .setup(|app| {
            let handle = app.handle().clone();
            std::thread::spawn(move || {
                run_auto_start_pipeline(handle);
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            start_services,
            stop_services,
            service_status,
            startup_status
        ])
        .build(tauri::generate_context!())
        .expect("error while building tauri application");

    app.run(|app_handle, event| {
        if matches!(
            event,
            tauri::RunEvent::ExitRequested { .. } | tauri::RunEvent::Exit
        ) {
            let process_state = app_handle.state::<Mutex<ProcessState>>();
            let _ = stop_services_internal(&process_state);
        }
    });
}

fn run_auto_start_pipeline(app_handle: tauri::AppHandle) {
    let pipeline_started_at = Instant::now();
    let startup_state = app_handle.state::<Mutex<StartupStatus>>();
    set_startup_status(
        &startup_state,
        "starting",
        "idle",
        1,
        Some("Starting local services...".to_string()),
    );

    for attempt in 1..=AUTO_START_MAX_ATTEMPTS {
        let process_state = app_handle.state::<Mutex<ProcessState>>();
        set_startup_status(
            &startup_state,
            "starting",
            "api",
            attempt,
            Some(format!("Starting API (attempt {attempt}/{AUTO_START_MAX_ATTEMPTS})")),
        );

        let clear_logs = attempt == 1;
        match start_services_internal(&process_state, clear_logs) {
            Ok(_) => {
                set_startup_status(
                    &startup_state,
                    "healthy",
                    "healthy",
                    attempt,
                    Some("Services are healthy.".to_string()),
                );
                log_startup_timing("auto", "healthy", attempt, pipeline_started_at.elapsed());
                return;
            }
            Err(error) => {
                if attempt >= AUTO_START_MAX_ATTEMPTS {
                    set_startup_status(
                        &startup_state,
                        "failed",
                        "failed_terminal",
                        attempt,
                        Some(error),
                    );
                    log_startup_timing("auto", "failed", attempt, pipeline_started_at.elapsed());
                    return;
                }

                set_startup_status(
                    &startup_state,
                    "starting",
                    "retry",
                    attempt,
                    Some(format!(
                        "Retrying service startup ({}/{})...",
                        attempt + 1,
                        AUTO_START_MAX_ATTEMPTS
                    )),
                );
                std::thread::sleep(retry_delay_for_attempt(attempt));
            }
        }
    }
}

fn retry_delay_for_attempt(attempt: u8) -> Duration {
    let idx = (attempt as usize).saturating_sub(1);
    let delay = RETRY_DELAY_MS
        .get(idx)
        .copied()
        .unwrap_or_else(|| *RETRY_DELAY_MS.last().unwrap_or(&1500));
    Duration::from_millis(delay)
}

fn set_startup_status(
    startup_state: &Mutex<StartupStatus>,
    state: &str,
    phase: &str,
    attempt: u8,
    message: Option<String>,
) {
    if let Ok(mut status) = startup_state.lock() {
        status.state = state.to_string();
        status.phase = phase.to_string();
        status.attempt = attempt;
        status.message = message;
    }
}

fn start_services_internal(
    process_state: &Mutex<ProcessState>,
    clear_logs_for_dev: bool,
) -> Result<ServiceStatus, String> {
    let mut processes = process_state
        .lock()
        .map_err(|_| "process lock poisoned".to_string())?;
    let services_dir = services_root();
    if clear_logs_for_dev {
        clear_runtime_logs_for_dev(&services_dir)
            .map_err(|e| format!("failed to clear dev logs: {e}"))?;
    }

    clean_stale_processes_for_port(&mut processes.api, 8081)?;
    if !is_service_running(&mut processes.api, 8081) {
        processes.api =
            Some(spawn_service("api").map_err(|e| format!("failed to start api: {e}"))?);
    }
    if !wait_for_service_ready(8081, &mut processes.api, STARTUP_TIMEOUT) {
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
    if !wait_for_service_ready(8082, &mut processes.worker, STARTUP_TIMEOUT) {
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

fn stop_services_internal(process_state: &Mutex<ProcessState>) -> Result<ServiceStatus, String> {
    let mut processes = process_state
        .lock()
        .map_err(|_| "process lock poisoned".to_string())?;
    terminate(&mut processes.api);
    terminate(&mut processes.worker);
    Ok(ServiceStatus {
        api_running: false,
        worker_running: false,
    })
}

fn service_status_internal(process_state: &Mutex<ProcessState>) -> Result<ServiceStatus, String> {
    let mut processes = process_state
        .lock()
        .map_err(|_| "process lock poisoned".to_string())?;
    Ok(ServiceStatus {
        api_running: is_service_running(&mut processes.api, 8081),
        worker_running: is_service_running(&mut processes.worker, 8082),
    })
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
    let db_path = app_data_dir.join("expense.db");
    let logs_dir = app_data_dir.join("logs");
    std::fs::create_dir_all(&logs_dir)?;

    let log_path = service_log_path(package);
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    let log_file_err = log_file.try_clone()?;

    let mut cmd = Command::new("cargo");
    cmd.arg("run")
        .arg("-p")
        .arg(package)
        .arg("--")
        .arg("--db-path")
        .arg(&db_path)
        .arg("--migrate")
        .env("EXPENSE_APP_DATA_DIR", app_data_dir)
        .current_dir(services_dir.clone())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file_err));

    for (key, value) in load_env_file_vars(&services_dir) {
        cmd.env(key, value);
    }

    cmd.spawn()
}

fn load_env_file_vars(services_dir: &Path) -> Vec<(String, String)> {
    let mut candidates = Vec::new();
    if let Ok(explicit) = env::var("EXPENSE_ENV_FILE") {
        candidates.push(PathBuf::from(explicit));
    }
    if let Ok(cwd) = env::current_dir() {
        candidates.push(cwd.join(".env"));
    }
    if let Some(services_parent) = services_dir.parent() {
        candidates.push(services_parent.join(".env"));
        if let Some(repo_root) = services_parent.parent() {
            candidates.push(repo_root.join(".env"));
        }
    }

    for path in candidates {
        if let Ok(content) = std::fs::read_to_string(&path) {
            return parse_env_file(content.as_str());
        }
    }
    Vec::new()
}

fn parse_env_file(content: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for raw in content.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((key_raw, value_raw)) = line.split_once('=') else {
            continue;
        };
        let key = key_raw.trim();
        if key.is_empty() {
            continue;
        }
        let mut value = value_raw.trim().to_string();
        if ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
            && value.len() >= 2
        {
            value = value[1..value.len() - 1].to_string();
        }
        out.push((key.to_string(), value));
    }
    out
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
        #[cfg(unix)]
        {
            let _ = Command::new("kill")
                .arg("-TERM")
                .arg(process.id().to_string())
                .status();
            let deadline = Instant::now() + SHUTDOWN_GRACE_TIMEOUT;
            loop {
                match process.try_wait() {
                    Ok(Some(_)) => break,
                    Ok(None) => {
                        if Instant::now() >= deadline {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    Err(_) => break,
                }
            }
        }
        let _ = process.kill();
        let _ = process.wait();
    }
    *child = None;
}

fn wait_for_service_ready(port: u16, child: &mut Option<Child>, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() && is_health_ready(port) {
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

fn is_health_ready(port: u16) -> bool {
    http_status_code(port, "/health")
        .is_some_and(|code| code == 200)
        || http_status_code(port, "/api/v1/health").is_some_and(|code| code == 200)
}

fn http_status_code(port: u16, path: &str) -> Option<u16> {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let mut stream =
        std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(400)).ok()?;
    let _ = stream.set_read_timeout(Some(Duration::from_millis(400)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(400)));

    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).ok()?;
    stream.flush().ok()?;

    let mut reader = BufReader::new(stream);
    let mut first_line = String::new();
    reader.read_line(&mut first_line).ok()?;
    let mut parts = first_line.split_whitespace();
    let _http = parts.next()?;
    let status = parts.next()?.parse::<u16>().ok()?;
    Some(status)
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

fn clear_runtime_logs_for_dev(services_dir: &Path) -> std::io::Result<()> {
    if !cfg!(debug_assertions) {
        return Ok(());
    }
    let logs_dir = services_dir.join(".runtime").join("logs");
    std::fs::create_dir_all(&logs_dir)?;
    for entry in std::fs::read_dir(&logs_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file()
            && path
                .extension()
                .and_then(|v| v.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("log"))
        {
            // Truncate all runtime .log files for a fresh dev run
            // (api/worker/bootstrap/provider/external-api-raw).
            let _ = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(path)?;
        }
    }
    Ok(())
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

fn startup_metrics_log_path() -> PathBuf {
    services_root()
        .join(".runtime")
        .join("logs")
        .join("startup-metrics.log")
}

fn log_startup_timing(mode: &str, result: &str, attempts: u8, elapsed: Duration) {
    let path = startup_metrics_log_path();
    if let Some(parent) = path.parent() {
        let _ = create_dir_all(parent);
    }
    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) else {
        return;
    };

    let elapsed_ms = elapsed.as_millis();
    let elapsed_secs = elapsed.as_secs_f64();
    let ts = chrono::Utc::now().to_rfc3339();
    let _ = writeln!(
        file,
        "[{ts}] total startup time took = {:.3}s ({}ms) | mode={} | result={} | attempts={}",
        elapsed_secs,
        elapsed_ms,
        mode,
        result,
        attempts
    );
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
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread::JoinHandle;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct MockHealthServer {
        port: u16,
        stop: Arc<AtomicBool>,
        handle: Option<JoinHandle<()>>,
    }

    impl MockHealthServer {
        fn start(health_status: u16, versioned_status: u16) -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind mock server");
            let port = listener.local_addr().expect("local addr").port();
            listener
                .set_nonblocking(true)
                .expect("set nonblocking listener");
            let stop = Arc::new(AtomicBool::new(false));
            let stop_flag = stop.clone();

            let handle = std::thread::spawn(move || {
                while !stop_flag.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            let mut buffer = [0u8; 1024];
                            let read = stream.read(&mut buffer).unwrap_or(0);
                            if read == 0 {
                                continue;
                            }
                            let request = String::from_utf8_lossy(&buffer[..read]);
                            let path = request
                                .lines()
                                .next()
                                .and_then(|line| line.split_whitespace().nth(1))
                                .unwrap_or("/");
                            let status = if path == "/health" {
                                health_status
                            } else if path == "/api/v1/health" {
                                versioned_status
                            } else {
                                404
                            };
                            let reason = if status == 200 { "OK" } else { "ERR" };
                            let body = if status == 200 { "ok" } else { "error" };
                            let response = format!(
                                "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                status,
                                reason,
                                body.len(),
                                body
                            );
                            let _ = stream.write_all(response.as_bytes());
                            let _ = stream.flush();
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(10));
                        }
                        Err(_) => break,
                    }
                }
            });

            Self {
                port,
                stop,
                handle: Some(handle),
            }
        }
    }

    impl Drop for MockHealthServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            let _ = std::net::TcpStream::connect(("127.0.0.1", self.port));
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

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
        let ready = wait_for_service_ready(65534, &mut child, Duration::from_millis(100));
        assert!(!ready);
    }

    #[test]
    fn retry_delay_for_attempt_uses_expected_backoff() {
        assert_eq!(retry_delay_for_attempt(1), Duration::from_millis(500));
        assert_eq!(retry_delay_for_attempt(2), Duration::from_millis(1500));
        assert_eq!(retry_delay_for_attempt(3), Duration::from_millis(1500));
        assert_eq!(retry_delay_for_attempt(9), Duration::from_millis(1500));
    }

    #[test]
    fn set_startup_status_updates_state_payload() {
        let state = Mutex::new(StartupStatus::default());
        set_startup_status(
            &state,
            "failed",
            "failed_terminal",
            3,
            Some("terminal startup error".to_string()),
        );
        let snapshot = state.lock().expect("status lock").clone();
        assert_eq!(snapshot.state, "failed");
        assert_eq!(snapshot.phase, "failed_terminal");
        assert_eq!(snapshot.attempt, 3);
        assert_eq!(snapshot.message.as_deref(), Some("terminal startup error"));
    }

    #[test]
    fn http_status_code_reads_server_response() {
        let server = MockHealthServer::start(200, 200);
        let status = http_status_code(server.port, "/health");
        assert_eq!(status, Some(200));
    }

    #[test]
    fn is_health_ready_accepts_primary_health_endpoint() {
        let server = MockHealthServer::start(200, 500);
        assert!(is_health_ready(server.port));
    }

    #[test]
    fn is_health_ready_falls_back_to_versioned_endpoint() {
        let server = MockHealthServer::start(500, 200);
        assert!(is_health_ready(server.port));
    }

    #[test]
    fn is_health_ready_rejects_when_no_200_health() {
        let server = MockHealthServer::start(500, 500);
        assert!(!is_health_ready(server.port));
    }

    #[test]
    fn is_health_ready_unreachable_returns_fast() {
        let start = Instant::now();
        assert!(!is_health_ready(65534));
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "unreachable health probe took too long: {:?}",
            start.elapsed()
        );
    }

    #[cfg(unix)]
    fn spawn_sleep_child(seconds: &str) -> Child {
        Command::new("sleep")
            .arg(seconds)
            .spawn()
            .expect("spawn sleep child")
    }

    #[cfg(unix)]
    #[test]
    fn wait_for_service_ready_requires_http_health_not_just_tcp() {
        let server = MockHealthServer::start(500, 500);
        let mut child = Some(spawn_sleep_child("2"));
        let ready = wait_for_service_ready(server.port, &mut child, Duration::from_millis(600));
        assert!(!ready);
        terminate(&mut child);
    }

    #[cfg(unix)]
    #[test]
    fn wait_for_service_ready_succeeds_when_health_endpoint_is_200() {
        let server = MockHealthServer::start(200, 500);
        let mut child = Some(spawn_sleep_child("2"));
        let ready = wait_for_service_ready(server.port, &mut child, Duration::from_secs(2));
        assert!(ready);
        terminate(&mut child);
    }

    #[cfg(unix)]
    #[test]
    fn terminate_stops_running_child_process() {
        let mut child = Some(spawn_sleep_child("5"));
        terminate(&mut child);
        assert!(child.is_none());
    }

}
