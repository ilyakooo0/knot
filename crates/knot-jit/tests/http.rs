//! HTTP server e2e tests: build a real server binary, start it on a port,
//! hit it with curl, assert responses, kill it.

mod e2e;
use e2e::build_program;
use std::process::{Command, Stdio};
use std::time::Duration;

/// Start `bin` (cwd `dir`), wait for it to listen on `port`, then run `f`
/// (which curls endpoints), then kill the server. `f` receives the base URL.
fn with_server(name: &str, src: &str, port: u16, f: impl FnOnce(&str)) {
    let (bin, dir) = build_program(name, src);
    let mut child = Command::new(&bin)
        .current_dir(dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to start server");
    let base = format!("http://127.0.0.1:{port}");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let up = Command::new("curl")
            .args(["-s", "-o", "/dev/null", "-m", "1", &base])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if up {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "server {name} did not start listening on {port}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&base)));
    let _ = child.kill();
    let _ = child.wait();
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

fn get(base: &str, path: &str) -> String {
    let out = Command::new("curl")
        .args(["-s", &format!("{base}{path}")])
        .output()
        .expect("curl failed");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn send(base: &str, method: &str, path: &str, json: &str) -> String {
    let out = Command::new("curl")
        .args([
            "-s",
            "-X",
            method,
            "-H",
            "Content-Type: application/json",
            "-d",
            json,
            &format!("{base}{path}"),
        ])
        .output()
        .expect("curl failed");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn status(base: &str, path: &str) -> String {
    let out = Command::new("curl")
        .args([
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            &format!("{base}{path}"),
        ])
        .output()
        .expect("curl failed");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

#[test]
fn http_hello_world() {
    with_server(
        "http_hello",
        r#"with {
api Api where
  Root  get / -> Text
}
(base.listen 18081 (serve Api where
  Root = \{} -> yield (Result.Ok {value "hi there"})))"#,
        18081,
        |base| assert_eq!(get(base, "/"), "\"hi there\""),
    );
}

#[test]
fn http_path_param() {
    with_server(
        "http_param",
        r#"with {
api Api where
  GetUser  get /users/{id (Int 1)} -> Text
}
(base.listen 18082 (serve Api where
  GetUser = \{id id} -> yield (Result.Ok {value ("user " ++ base.show id)})))"#,
        18082,
        |base| assert_eq!(get(base, "/users/42"), "\"user 42\""),
    );
}

#[test]
fn http_unmatched_route_404() {
    with_server(
        "http_404",
        r#"with {
api Api where
  Root  get / -> Text
}
(base.listen 18083 (serve Api where
  Root = \{} -> yield (Result.Ok {value "root"})))"#,
        18083,
        |base| assert_eq!(status(base, "/nonexistent"), "404"),
    );
}

#[test]
fn http_json_body() {
    with_server(
        "http_json",
        r#"with {
api Api where
  CreateUser  post /users ={name Text  age (Int 1)} -> Text
}
(base.listen 18084 (serve Api where
  CreateUser = \{name name  age age} -> yield (Result.Ok {value ("created " ++ name)})))"#,
        18084,
        |base| {
            assert_eq!(
                send(base, "POST", "/users", "{\"name\":\"ada\",\"age\":36}"),
                "\"created ada\""
            );
        },
    );
}

#[test]
fn http_result_err_sets_status() {
    with_server(
        "http_err",
        r#"with {
api Api where
  Find  get /thing -> Text
}
(base.listen 18086 (serve Api where
  Find = \{} -> yield (Result.Err {error {status 404  message "no such thing"}})))"#,
        18086,
        |base| {
            assert_eq!(status(base, "/thing"), "404");
            // Error responses serialize as a JSON envelope.
            assert_eq!(get(base, "/thing"), "{\"error\":\"no such thing\"}");
        },
    );
}
