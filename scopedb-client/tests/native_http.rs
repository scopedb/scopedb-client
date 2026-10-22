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

use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use scopedb_client::CatalogOptions;
use scopedb_client::Client;
use scopedb_client::ErrorKind;
use scopedb_client::StatementEstimatedProgress;
use scopedb_client::StatementStatus;
use serde_json::Value;
use serde_json::json;

fn server(responses: Vec<(u16, String)>) -> (Client, thread::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/proxy/", listener.local_addr().unwrap());
    let task = thread::spawn(move || {
        let mut requests = Vec::new();
        for (status, body) in responses {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut bytes = Vec::new();
            let mut byte = [0u8; 1];
            while !bytes.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).unwrap();
                bytes.push(byte[0]);
            }
            let headers = String::from_utf8(bytes).unwrap();
            let len = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            let mut request_body = vec![0; len];
            socket.read_exact(&mut request_body).unwrap();
            requests.push(headers + &String::from_utf8(request_body).unwrap());
            write!(socket, "HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\nETag: \"opaque\"\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        }
        requests
    });
    let client = Client::builder(endpoint)
        .api_key("test-secret")
        .http_client(
            scopedb_client::reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    (client, task)
}

#[tokio::test]
async fn catalog_round_trip_preserves_unknown_fields_and_preconditions() {
    let definition = json!({"name":"events/2026","columns":[],"future_option":{"enabled":true}});
    let result = json!({"before":definition,"after":definition,"destructive":false});
    let (client, task) = server(vec![(200, definition.to_string()), (200, result.to_string()), (412, json!({"code":"catalog_version_mismatch","message":"changed","details":{"current":"new"}}).to_string())]);
    let resource = client
        .catalog()
        .table("scopedb", "public", "events/2026")
        .raw();
    let snapshot = resource.get().await.unwrap();
    let preview = resource
        .apply(
            &snapshot.definition,
            CatalogOptions {
                dry_run: true,
                if_match: Some(snapshot.etag),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        preview.after.as_ref().unwrap()["future_option"]["enabled"],
        true
    );
    let stale = resource
        .apply(
            &snapshot.definition,
            CatalogOptions {
                if_match: Some(preview.etag),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Some("catalog_version_mismatch"));
    assert_eq!(stale.details().unwrap()["current"], "new");
    assert!(!stale.is_retryable());
    let requests = task.join().unwrap();
    assert!(requests[0].starts_with(
        "GET /proxy/v1/catalog/databases/scopedb/schemas/public/tables/events%2F2026 "
    ));
    assert!(requests[1].starts_with(
        "PUT /proxy/v1/catalog/databases/scopedb/schemas/public/tables/events%2F2026?dry_run=true "
    ));
    assert!(
        requests[1]
            .to_ascii_lowercase()
            .contains("if-match: \"opaque\"")
    );
    let sent: Value = serde_json::from_str(requests[1].split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(sent, definition);
    assert!(
        requests[1]
            .to_ascii_lowercase()
            .contains("x-scopedb-client: scopedb-client")
    );
}

#[tokio::test]
async fn malformed_mutation_responses_keep_the_commit_outcome_unknown() {
    let (client, task) = server(vec![
        (200, "invalid json".to_string()),
        (200, "{}".to_string()),
    ]);
    let resource = client.catalog().table("scopedb", "public", "events");
    for _ in 0..2 {
        let error = resource
            .delete(CatalogOptions {
                allow_destructive: true,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some("catalog_commit_outcome_unknown"));
        assert!(!error.is_retryable());
    }
    assert_eq!(task.join().unwrap().len(), 2);
}

#[tokio::test]
async fn cancellation_retains_full_terminal_snapshots() {
    for status in ["finished", "failed", "cancelled"] {
        let mut progress = StatementEstimatedProgress::default();
        progress.details.compute_seconds = 0.125;
        let mut response = json!({"status":status,"statement_id":uuid::Uuid::nil(),"created_at":"2026-10-23T00:00:00Z","progress":progress});
        if status == "finished" {
            response["result_set"] = json!({"metadata":{"fields":[{"name":"id","data_type":"int"}],"num_rows":1},"format":"json","rows":[["7"]]});
        }
        if status == "failed" {
            response["error"] =
                json!({"code":"future_failure","message":"precise failure","details":{"limit":7}});
        }
        let (client, task) = server(vec![(200, response.to_string())]);
        let mut handle = client.statement_handle(uuid::Uuid::nil());
        let cancelled = handle.cancel().await.unwrap();
        assert_eq!(handle.progress().unwrap().details.compute_seconds, 0.125);
        match cancelled {
            StatementStatus::Finished(_) => assert_eq!(handle.wait().await.unwrap().num_rows(), 1),
            StatementStatus::Failed(_) => {
                let error = handle.wait().await.unwrap_err();
                assert_eq!(error.code(), Some("future_failure"));
                assert_eq!(error.message(), "precise failure");
                assert_eq!(error.details().unwrap()["limit"], 7);
            }
            StatementStatus::Cancelled(_) => assert_eq!(
                handle.wait().await.unwrap_err().kind(),
                ErrorKind::StatementFailed
            ),
            _ => panic!("expected terminal state"),
        }
        let requests = task.join().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with(
            "POST /proxy/v1/statements/00000000-0000-0000-0000-000000000000/cancel?format=json "
        ));
    }
}

#[tokio::test]
async fn dot_segments_cannot_turn_a_table_delete_into_a_parent_delete() {
    let client = Client::builder("https://example.invalid").build().unwrap();
    for name in ["", ".", ".."] {
        let error = client
            .catalog()
            .table("scopedb", "public", name)
            .delete(CatalogOptions {
                allow_destructive: true,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
    }
}
