// Copyright 2024 ScopeDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use tokio::sync::Notify;

use crate::Client;

#[derive(Debug, Default)]
pub(crate) struct ResponseGate {
    released: Mutex<bool>,
    wake: Condvar,
    blocked: Notify,
}

impl ResponseGate {
    pub(crate) fn block(&self) {
        self.blocked.notify_one();
        let (released, _) = self
            .wake
            .wait_timeout_while(lock(&self.released), Duration::from_secs(5), |released| {
                !*released
            })
            .unwrap();
        assert!(*released, "test did not release the response gate");
    }

    pub(crate) async fn wait_until_blocked(&self) {
        self.blocked.notified().await;
    }

    pub(crate) fn release(&self) {
        *lock(&self.released) = true;
        self.wake.notify_all();
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RecordedRequest {
    pub(crate) target: String,
    pub(crate) headers: HashMap<String, String>,
    pub(crate) body: String,
}

#[derive(Debug, Clone)]
pub(crate) struct MockResponse {
    status: u16,
    pub(crate) body: Vec<u8>,
    delay: Duration,
    headers: Vec<(&'static str, String)>,
    body_delay: Duration,
    disconnect: Option<MockDisconnect>,
    body_gate: Option<Arc<ResponseGate>>,
    sent: Option<Arc<Notify>>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum MockDisconnect {
    BeforeHeaders,
    DuringBody,
}

impl MockResponse {
    pub(crate) fn json(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            body: body.into().into_bytes(),
            delay: Duration::ZERO,
            headers: Vec::new(),
            body_delay: Duration::ZERO,
            disconnect: None,
            body_gate: None,
            sent: None,
        }
    }

    pub(crate) fn committed(request: &RecordedRequest) -> Self {
        let rows = request
            .body
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count();
        Self::json(
            200,
            format!(r#"{{"status":"committed","num_rows_inserted":{rows}}}"#),
        )
    }

    pub(crate) fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    pub(crate) fn with_header(mut self, name: &'static str, value: &str) -> Self {
        self.headers.push((name, value.to_string()));
        self
    }

    pub(crate) fn without_ack(mut self) -> Self {
        self.disconnect = Some(MockDisconnect::BeforeHeaders);
        self
    }

    pub(crate) fn with_body_delay(mut self, delay: Duration) -> Self {
        self.body_delay = delay;
        self
    }

    pub(crate) fn with_disconnect(mut self, disconnect: MockDisconnect) -> Self {
        self.disconnect = Some(disconnect);
        self
    }

    pub(crate) fn with_body_gate(mut self, gate: Arc<ResponseGate>) -> Self {
        self.body_gate = Some(gate);
        self
    }

    pub(crate) fn notify_when_sent(mut self, sent: Arc<Notify>) -> Self {
        self.sent = Some(sent);
        self
    }
}

type Responder = dyn Fn(usize, &RecordedRequest) -> MockResponse + Send + Sync;

pub(crate) struct MockServer {
    pub(crate) endpoint: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    max_active: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    accept_task: Option<thread::JoinHandle<()>>,
}

impl MockServer {
    pub(crate) fn start<F>(responder: F) -> Self
    where
        F: Fn(usize, &RecordedRequest) -> MockResponse + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = format!("http://{address}");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let responder: Arc<Responder> = Arc::new(responder);

        let accept_task = thread::spawn({
            let requests = requests.clone();
            let active = active.clone();
            let max_active = max_active.clone();
            let stop = stop.clone();
            move || {
                while !stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            if stop.load(Ordering::Acquire) {
                                break;
                            }
                            let requests = requests.clone();
                            let active = active.clone();
                            let max_active = max_active.clone();
                            let responder = responder.clone();
                            thread::spawn(move || {
                                handle_connection(stream, requests, active, max_active, responder);
                            });
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(1));
                        }
                        Err(_) => break,
                    }
                }
            }
        });

        Self {
            endpoint,
            requests,
            max_active,
            stop,
            accept_task: Some(accept_task),
        }
    }

    pub(crate) fn client(&self) -> Client {
        let http_client = reqwest::Client::builder().no_proxy().build().unwrap();
        Client::new(&self.endpoint, http_client).unwrap()
    }

    pub(crate) fn client_with_write_timeout(&self, timeout: Duration) -> Client {
        Client::builder(&self.endpoint)
            .http_client(reqwest::Client::builder().no_proxy().build().unwrap())
            .write_timeout(timeout)
            .build()
            .unwrap()
    }

    pub(crate) fn requests(&self) -> Vec<RecordedRequest> {
        lock(&self.requests).clone()
    }

    pub(crate) fn max_active(&self) -> usize {
        self.max_active.load(Ordering::Acquire)
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(self.endpoint.trim_start_matches("http://"));
        if let Some(task) = self.accept_task.take() {
            let _ = task.join();
        }
    }
}

fn handle_connection(
    mut stream: TcpStream,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
    responder: Arc<Responder>,
) {
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let Some(request) = read_request(&mut stream) else {
        return;
    };
    let index = {
        let mut recorded = lock(&requests);
        let index = recorded.len();
        recorded.push(request.clone());
        index
    };
    let now_active = active.fetch_add(1, Ordering::AcqRel) + 1;
    max_active.fetch_max(now_active, Ordering::AcqRel);
    let response = responder(index, &request);
    if !response.delay.is_zero() {
        thread::sleep(response.delay);
    }
    if matches!(response.disconnect, Some(MockDisconnect::BeforeHeaders)) {
        active.fetch_sub(1, Ordering::AcqRel);
        return;
    }
    let reason = match response.status {
        200 => "OK",
        400 => "Bad Request",
        422 => "Unprocessable Entity",
        503 => "Service Unavailable",
        _ => "Test Response",
    };
    let headers = response
        .headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect::<String>();
    let wire = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n",
        response.status,
        reason,
        response.body.len(),
        headers,
    );
    let _ = stream.write_all(wire.as_bytes());
    let _ = stream.flush();
    if let Some(gate) = &response.body_gate {
        gate.block();
    }
    if !response.body_delay.is_zero() {
        thread::sleep(response.body_delay);
    }
    let body = response.body.as_slice();
    let body = if matches!(response.disconnect, Some(MockDisconnect::DuringBody)) {
        &body[..body.len().min(1)]
    } else {
        body
    };
    let _ = stream.write_all(body);
    let _ = stream.flush();
    active.fetch_sub(1, Ordering::AcqRel);
    if let Some(sent) = response.sent {
        sent.notify_one();
    }
}

fn read_request(stream: &mut TcpStream) -> Option<RecordedRequest> {
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut buffer).ok()?;
        if read == 0 {
            return None;
        }
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let headers_text = String::from_utf8(bytes[..header_end].to_vec()).ok()?;
    let mut lines = headers_text.split("\r\n");
    let target = lines.next()?.split_whitespace().nth(1)?.to_string();
    let mut headers = HashMap::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':')?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }
    let content_length = headers
        .get("content-length")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    while bytes.len().saturating_sub(header_end) < content_length {
        let read = stream.read(&mut buffer).ok()?;
        if read == 0 {
            return None;
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
    let wire_body = &bytes[header_end..header_end.saturating_add(content_length)];
    let decoded_body = match headers.get("content-encoding").map(String::as_str) {
        Some("zstd") => zstd::stream::decode_all(wire_body).ok()?,
        None | Some("identity") => wire_body.to_vec(),
        Some(_) => return None,
    };
    let body = String::from_utf8(decoded_body).ok()?;
    Some(RecordedRequest {
        target,
        headers,
        body,
    })
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
