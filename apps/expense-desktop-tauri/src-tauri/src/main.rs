#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use serde::Serialize;
use std::{
    env,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Mutex,
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

#[tauri::command]
fn start_services(state: tauri::State<'_, Mutex<ProcessState>>) -> Result<ServiceStatus, String> {
    let mut processes = state.lock().map_err(|_| "lock poisoned".to_string())?;

    if !is_alive(&mut processes.api) {
        processes.api =
            Some(spawn_service("api").map_err(|e| format!("failed to start api: {e}"))?);
    }

    if !is_alive(&mut processes.worker) {
        processes.worker =
            Some(spawn_service("worker").map_err(|e| format!("failed to start worker: {e}"))?);
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
        api_running: is_alive(&mut processes.api),
        worker_running: is_alive(&mut processes.worker),
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

    Command::new("cargo")
        .arg("run")
        .arg("-p")
        .arg(package)
        .current_dir(services_dir)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
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
}
