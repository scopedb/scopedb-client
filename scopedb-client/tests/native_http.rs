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
    server_with_etag(responses, Some("\"opaque\""))
}

fn server_with_etag(
    responses: Vec<(u16, String)>,
    etag: Option<&str>,
) -> (Client, thread::JoinHandle<Vec<String>>) {
    serve(responses, etag, |_| {})
}

fn serve(
    responses: Vec<(u16, String)>,
    etag: Option<&str>,
    before_response: impl FnMut(usize) + Send + 'static,
) -> (Client, thread::JoinHandle<Vec<String>>) {
    serve_responses(responses, etag, before_response, Vec::new())
}

fn truncated_server(responses: Vec<(u16, String)>) -> (Client, thread::JoinHandle<Vec<String>>) {
    let truncated = (0..responses.len()).collect();
    serve_responses(responses, Some("\"opaque\""), |_| {}, truncated)
}

fn serve_responses(
    responses: Vec<(u16, String)>,
    etag: Option<&str>,
    mut before_response: impl FnMut(usize) + Send + 'static,
    truncated_responses: Vec<usize>,
) -> (Client, thread::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/proxy/", listener.local_addr().unwrap());
    let etag = etag
        .map(|value| format!("ETag: {value}\r\n"))
        .unwrap_or_default();
    let task = thread::spawn(move || {
        let mut requests = Vec::new();
        for (index, (status, body)) in responses.into_iter().enumerate() {
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
            if headers
                .to_ascii_lowercase()
                .contains("content-encoding: zstd")
            {
                request_body = zstd::stream::decode_all(request_body.as_slice()).unwrap();
            }
            requests.push(headers + &String::from_utf8(request_body).unwrap());
            before_response(index);
            let content_length = if truncated_responses.contains(&index) {
                body.len() + 1
            } else {
                body.len()
            };
            let _ = write!(
                socket,
                "HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\n{etag}Content-Length: {content_length}\r\nConnection: close\r\n\r\n{body}"
            );
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
async fn query_poll_failures_resume_the_existing_statement() {
    for truncated in [false, true] {
        let id = uuid::Uuid::now_v7();
        let running = json!({"status":"running","statement_id":id,"created_at":"2026-10-23T00:00:00Z","progress":StatementEstimatedProgress::default()});
        let mut finished = running.clone();
        finished["status"] = json!("finished");
        finished["result_set"] = json!({"metadata":{"fields":[{"name":"id","data_type":"int"}],"num_rows":1},"format":"json","rows":[["7"]]});
        let failure = if truncated {
            (200, finished.to_string())
        } else {
            (503, json!({"code":"unavailable","message":"status unavailable","request_id":"poll-request","retryable":true,"details":{"reason":"busy"}}).to_string())
        };
        let (client, task) = serve_responses(
            vec![
                (200, running.to_string()),
                failure,
                (200, finished.to_string()),
            ],
            None,
            |_| {},
            if truncated { vec![1] } else { Vec::new() },
        );

        let error = client
            .query("SELECT 7 AS id INSERT INTO events")
            .await
            .unwrap_err();
        assert!(!error.is_retryable());
        assert_eq!(error.statement_id(), Some(id));
        assert_eq!(
            error.http_status().unwrap().as_u16(),
            if truncated { 200 } else { 503 }
        );
        if truncated {
            assert!(std::error::Error::source(&error).is_some());
        } else {
            assert_eq!(error.code(), Some("unavailable"));
            assert_eq!(error.request_id(), Some("poll-request"));
            assert_eq!(error.details().unwrap()["reason"], "busy");
        }
        let result = client
            .statement_handle(error.statement_id().unwrap())
            .wait()
            .await
            .unwrap();
        assert_eq!(result.raw_rows()[0][0].as_deref(), Some("7"));
        let requests = task.join().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].starts_with("POST /proxy/v1/statements "));
        for request in &requests[1..] {
            assert!(request.starts_with(&format!("GET /proxy/v1/statements/{id}?format=json ")));
        }
    }
}

#[tokio::test]
async fn ambiguous_statement_submissions_are_not_retryable() {
    for id in [None, Some(uuid::Uuid::now_v7())] {
        for (status, body) in [
            (502, "gateway lost upstream response".to_string()),
            (
                503,
                json!({"code":"unavailable","message":"gateway failure","retryable":true})
                    .to_string(),
            ),
            (408, "request timed out".to_string()),
            (200, "invalid response".to_string()),
        ] {
            let (client, task) = server(vec![(status, body)]);
            let mut statement = client.statement("SELECT 1 AS id INSERT INTO events");
            if let Some(id) = id {
                statement = statement.with_statement_id(id);
            }
            let error = statement.submit().await.unwrap_err();
            assert!(error.is_persistent(), "{error:?}");
            assert!(!error.is_retryable());
            assert_eq!(error.statement_id(), id);
            assert_eq!(error.http_status().unwrap().as_u16(), status);
            if status == 503 {
                assert_eq!(error.code(), Some("unavailable"));
            }
            assert_eq!(task.join().unwrap().len(), 1);
        }
    }
}

#[tokio::test]
async fn statement_rejections_and_handle_polling_keep_safe_retries() {
    let id = uuid::Uuid::now_v7();
    let (client, task) = server(vec![
        (
            429,
            json!({"code":"rate_limited","message":"not admitted","retryable":true}).to_string(),
        ),
        (
            503,
            json!({"code":"unavailable","message":"status unavailable"}).to_string(),
        ),
    ]);
    let rejection = client.statement("SELECT 1").submit().await.unwrap_err();
    assert!(rejection.is_retryable());
    assert_eq!(rejection.code(), Some("rate_limited"));
    let error = client.statement_handle(id).wait().await.unwrap_err();
    assert!(error.is_retryable());
    assert_eq!(error.statement_id(), Some(id));
    assert_eq!(task.join().unwrap().len(), 2);
}

#[tokio::test]
async fn catalog_round_trip_preserves_unknown_fields_and_preconditions() {
    let definition: Value = serde_json::from_str(r#"{"name":"events/2026","columns":[],"future_option":{"enabled":true,"counter":184467440737095516160,"fraction":0.12345678901234567890123456789}}"#).unwrap();
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
    assert_eq!(
        sent["future_option"]["counter"].to_string(),
        "184467440737095516160"
    );
    assert_eq!(
        sent["future_option"]["fraction"].to_string(),
        "0.12345678901234567890123456789"
    );
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
        assert_eq!(
            handle
                .snapshot()
                .unwrap()
                .progress()
                .details
                .compute_seconds,
            0.125
        );
        match cancelled {
            StatementStatus::Finished(_) => assert_eq!(handle.wait().await.unwrap().num_rows(), 1),
            StatementStatus::Failed(_) => {
                let error = handle.wait().await.unwrap_err();
                assert_eq!(error.statement_id(), Some(handle.statement_id()));
                assert_eq!(error.code(), Some("future_failure"));
                assert_eq!(error.message(), "precise failure");
                assert_eq!(error.details().unwrap()["limit"], 7);
            }
            StatementStatus::Cancelled(_) => {
                let error = handle.wait().await.unwrap_err();
                assert_eq!(error.kind(), ErrorKind::StatementCancelled);
                assert_eq!(error.statement_id(), Some(handle.statement_id()));
            }
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

#[tokio::test]
async fn typed_catalog_rejects_unknown_fields_at_every_definition_level() {
    let definition = json!({"databases":[{"name":"scopedb","schemas":[{"name":"public",
        "tables":[{"name":"events","columns":[{"name":"id","data_type":"int",
            "indexes":[{"type":"search","analyzer":"log"}]}],"distinct":{"on":[],"by":[]}}],
        "views":[{"name":"recent","statement":"FROM events"}]}]}]});
    for pointer in [
        "",
        "/databases/0",
        "/databases/0/schemas/0",
        "/databases/0/schemas/0/tables/0",
        "/databases/0/schemas/0/tables/0/columns/0",
        "/databases/0/schemas/0/tables/0/columns/0/indexes/0",
        "/databases/0/schemas/0/tables/0/distinct",
        "/databases/0/schemas/0/views/0",
    ] {
        let mut future = definition.clone();
        future
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("future_option".into(), json!({"enabled":true}));
        let (client, task) = server(vec![(200, future.to_string())]);
        let error = client.catalog().get().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::CatalogFailed);
        assert!(error.message().contains("raw()"));
        assert!(format!("{error:?}").contains("future_option"));
        assert_eq!(task.join().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn catalog_requires_etag_and_preserves_ambiguous_mutations() {
    let result = json!({"before":null,"after":{"name":"events"},"destructive":false});
    let (client, task) = server_with_etag(vec![(200, result.to_string())], None);
    let error = client
        .table("events")
        .catalog()
        .raw()
        .apply(&json!({"name":"events"}), CatalogOptions::default())
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some("catalog_commit_outcome_unknown"));
    assert!(!error.is_retryable());
    task.join().unwrap();

    let (client, task) = server(vec![
        (502, "gateway lost response".into()),
        (502, "gateway lost response".into()),
    ]);
    let resource = client.table("events").catalog();
    for dry_run in [false, true] {
        let error = resource
            .delete(CatalogOptions {
                dry_run,
                if_match: Some("\"version-1\"".into()),
                allow_destructive: true,
            })
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            if dry_run {
                None
            } else {
                Some("catalog_commit_outcome_unknown")
            }
        );
        if !dry_run {
            assert!(!error.is_retryable());
        }
    }
    let requests = task.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|r| r.to_ascii_lowercase().contains("if-match: \"version-1\""))
    );
}

#[tokio::test]
async fn cancellation_can_remain_running_then_fail_without_losing_details() {
    let id = uuid::Uuid::now_v7();
    let progress = StatementEstimatedProgress::default();
    let running = json!({"status":"running","statement_id":id,"created_at":"2026-10-23T00:00:00Z","progress":progress});
    let mut failed = running.clone();
    failed["status"] = json!("failed");
    failed["error"] = serde_json::from_str(r#"{"code":"future_failure","message":"original failure","details":{"counter":184467440737095516160}}"#).unwrap();
    let (client, task) = server(vec![
        (200, running.to_string()),
        (200, running.to_string()),
        (200, failed.to_string()),
    ]);
    let mut handle = client
        .statement("SELECT 1")
        .with_statement_id(id)
        .submit()
        .await
        .unwrap();
    assert!(matches!(
        handle.snapshot(),
        Some(StatementStatus::Running(_))
    ));
    assert!(matches!(
        handle.cancel().await.unwrap(),
        StatementStatus::Running(_)
    ));
    let error = handle.wait().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::StatementFailed);
    assert_eq!(error.code(), Some("future_failure"));
    assert_eq!(error.message(), "original failure");
    assert_eq!(
        error.details().unwrap()["counter"].to_string(),
        "184467440737095516160"
    );
    assert!(matches!(
        handle.refresh().await.unwrap(),
        StatementStatus::Failed(_)
    ));
    assert!(matches!(
        handle.cancel().await.unwrap(),
        StatementStatus::Failed(_)
    ));
    let requests = task.join().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests[1].contains("/cancel?format=json"));
    assert!(requests[2].starts_with("GET "));
}

#[tokio::test]
async fn dropping_refresh_does_not_publish_a_late_nonterminal_response() {
    let progress = StatementEstimatedProgress::default();
    let running = json!({"status":"running","statement_id":uuid::Uuid::nil(),"created_at":"2026-10-23T00:00:00Z","progress":progress});
    let mut finished = running.clone();
    finished["status"] = json!("finished");
    finished["result_set"] =
        json!({"metadata":{"fields":[],"num_rows":0},"format":"json","rows":[]});
    let (arrived_tx, arrived_rx) = tokio::sync::oneshot::channel();
    let mut arrived_tx = Some(arrived_tx);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (client, task) = serve(
        vec![(200, running.to_string()), (200, finished.to_string())],
        Some("\"opaque\""),
        move |index| {
            if index == 0 {
                arrived_tx.take().unwrap().send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            }
        },
    );
    let mut handle = client.statement_handle(uuid::Uuid::nil());
    tokio::select! {
        _ = handle.refresh() => panic!("response must remain gated"),
        arrived = arrived_rx => arrived.unwrap(),
    }
    assert!(handle.snapshot().is_none());
    release_tx.send(()).unwrap();
    assert!(matches!(
        handle.cancel().await.unwrap(),
        StatementStatus::Finished(_)
    ));
    assert_eq!(handle.wait().await.unwrap().num_rows(), 0);
    assert!(matches!(
        handle.refresh().await.unwrap(),
        StatementStatus::Finished(_)
    ));
    assert_eq!(task.join().unwrap().len(), 2);
}

#[tokio::test]
async fn catalog_write_budget_preserves_a_shorter_custom_http_timeout() {
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (client, task) = serve(vec![(200, "{}".into())], Some("\"opaque\""), move |_| {
        release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    });
    // The helper's custom HTTP client times out in 3 seconds; the SDK write budget is 30.
    let error = tokio::time::timeout(
        Duration::from_secs(8),
        client
            .table("events")
            .catalog()
            .delete(CatalogOptions::default()),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.code(), Some("catalog_commit_outcome_unknown"));
    assert!(!error.is_retryable());
    release_tx.send(()).unwrap();
    assert_eq!(task.join().unwrap().len(), 1);
}

#[tokio::test]
async fn response_body_interruptions_follow_operation_retry_safety() {
    let running = json!({
        "status":"running",
        "statement_id":uuid::Uuid::nil(),
        "created_at":"2026-10-23T00:00:00Z",
        "progress":StatementEstimatedProgress::default()
    });
    let (client, task) = truncated_server(vec![
        (200, json!({"name":"events"}).to_string()),
        (
            200,
            json!({"before":null,"after":null,"destructive":false}).to_string(),
        ),
        (
            200,
            json!({"before":null,"after":null,"destructive":false}).to_string(),
        ),
        (200, running.to_string()),
        (200, running.to_string()),
        (200, running.to_string()),
    ]);
    let resource = client.table("events").catalog();

    let catalog_get = resource.get().await.unwrap_err();
    let catalog_dry_run = resource
        .delete(CatalogOptions {
            dry_run: true,
            ..Default::default()
        })
        .await
        .unwrap_err();
    let catalog_write = resource
        .delete(CatalogOptions::default())
        .await
        .unwrap_err();
    let mut handle = client.statement_handle(uuid::Uuid::nil());
    let refresh = handle.refresh().await.unwrap_err();
    let cancel = handle.cancel().await.unwrap_err();
    let submit = client.statement("SELECT 1").submit().await.unwrap_err();

    assert_eq!(refresh.statement_id(), Some(handle.statement_id()));
    assert_eq!(cancel.statement_id(), Some(handle.statement_id()));
    assert!(std::error::Error::source(&submit).is_some());
    assert!(submit.is_persistent());
    for error in [catalog_get, catalog_dry_run, refresh, cancel] {
        assert!(error.is_retryable());
        assert_eq!(error.http_status().map(|status| status.as_u16()), Some(200));
    }
    assert!(!catalog_write.is_retryable());
    assert_eq!(catalog_write.code(), Some("catalog_commit_outcome_unknown"));
    assert!(!submit.is_retryable());
    assert_eq!(task.join().unwrap().len(), 6);
}
