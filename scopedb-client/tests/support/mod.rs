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

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use scopedb_client::Client;
use serde_json::Value;
use serde_json::json;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;

#[derive(Clone, Debug)]
pub struct Request {
    pub method: String,
    pub target: String,
    pub headers: BTreeMap<String, String>,
    pub body: String,
}

impl Request {
    pub fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap()
    }
}

pub struct Reply {
    status: u16,
    body: String,
    headers: Vec<(&'static str, String)>,
    body_gate: Option<watch::Receiver<bool>>,
}

impl Reply {
    pub fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            body: body.to_string(),
            headers: Vec::new(),
            body_gate: None,
        }
    }

    pub fn committed(request: &Request) -> Self {
        Self::json(
            200,
            json!({"status": "committed", "num_rows_inserted": request.body.lines().count()}),
        )
    }

    pub fn header(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.push((name, value.into()));
        self
    }

    pub fn hold_body(mut self, gate: watch::Receiver<bool>) -> Self {
        self.body_gate = Some(gate);
        self
    }
}

pub struct Server {
    pub endpoint: String,
    requests: mpsc::UnboundedReceiver<Request>,
    task: JoinHandle<()>,
}

impl Server {
    pub async fn start<F, Fut>(handler: F) -> Self
    where
        F: Fn(Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<Reply>> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/proxy/", listener.local_addr().unwrap());
        let (record, requests) = mpsc::unbounded_channel();
        let handler = Arc::new(handler);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    connection = listener.accept() => {
                        let (socket, _) = connection.unwrap();
                        let handler = handler.clone();
                        let record = record.clone();
                        connections.spawn(async move {
                            let mut socket = BufReader::new(socket);
                            let mut line = String::new();
                            socket.read_line(&mut line).await.unwrap();
                            let mut request_line = line.split_whitespace();
                            let method = request_line.next().unwrap().to_string();
                            let target = request_line.next().unwrap().to_string();
                            let mut headers = BTreeMap::new();
                            loop {
                                line.clear();
                                socket.read_line(&mut line).await.unwrap();
                                if line == "\r\n" { break; }
                                let (name, value) = line.split_once(':').unwrap();
                                headers.insert(name.to_ascii_lowercase(), value.trim().to_string());
                            }
                            let length = headers.get("content-length").map(|v| v.parse().unwrap()).unwrap_or(0);
                            let mut body = vec![0; length];
                            socket.read_exact(&mut body).await.unwrap();
                            if headers.get("content-encoding").is_some_and(|v| v == "zstd") {
                                body = zstd::stream::decode_all(body.as_slice()).unwrap();
                            }
                            let request = Request { method, target, headers, body: String::from_utf8(body).unwrap() };
                            record.send(request.clone()).unwrap();
                            let Some(mut reply) = handler(request).await else { return; };
                            let mut socket = socket.into_inner();
                            let mut head = format!("HTTP/1.1 {} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n", reply.status, reply.body.len());
                            for (name, value) in reply.headers { head.push_str(&format!("{name}: {value}\r\n")); }
                            head.push_str("\r\n");
                            if socket.write_all(head.as_bytes()).await.is_err() { return; }
                            if let Some(gate) = &mut reply.body_gate { gate.wait_for(|open| *open).await.unwrap(); }
                            let _ = socket.write_all(reply.body.as_bytes()).await;
                        });
                    }
                    result = connections.join_next(), if !connections.is_empty() => { result.unwrap().unwrap(); }
                }
            }
        });
        Self {
            endpoint,
            requests,
            task,
        }
    }

    pub fn client(&self) -> Client {
        Client::builder(&self.endpoint)
            .api_key("test-key")
            .http_client(
                scopedb_client::reqwest::Client::builder()
                    .no_proxy()
                    .redirect(scopedb_client::reqwest::redirect::Policy::none())
                    .retry(scopedb_client::reqwest::retry::never())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap()
    }

    pub async fn next(&mut self) -> Request {
        tokio::time::timeout(Duration::from_secs(3), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }

    pub fn received(&mut self) -> Vec<Request> {
        let mut requests = Vec::new();
        while let Ok(request) = self.requests.try_recv() {
            requests.push(request);
        }
        requests
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub fn statement(id: Value, status: &str) -> Value {
    json!({"status": status, "statement_id": id, "created_at": "2026-10-31T00:00:00Z", "progress": scopedb_client::StatementEstimatedProgress::default()})
}

pub fn finished(id: Value) -> Value {
    let mut result = statement(id, "finished");
    result["result_set"] = json!({"metadata": {"fields": [{"name": "id", "data_type": "uint"}, {"name": "label", "data_type": "string"}], "num_rows": 2}, "format": "json", "rows": [["9007199254740993", "first"], ["2", null]]});
    result
}
