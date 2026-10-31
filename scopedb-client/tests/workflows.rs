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

mod support;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use scopedb_client::AppendDeliveryOutcome;
use scopedb_client::AppendFailurePolicy;
use scopedb_client::AppendState;
use scopedb_client::AppendWriter;
use scopedb_client::CatalogMutationOutcome;
use scopedb_client::CatalogOptions;
use scopedb_client::Error;
use scopedb_client::ErrorKind;
use scopedb_client::ErrorOperation;
use scopedb_client::StatementStatus;
use scopedb_client::Value;
use scopedb_client::WriteErrorReason;
use serde_json::json;
use tokio::sync::Notify;
use tokio::sync::watch;

use self::support::Reply;
use self::support::Server;
use self::support::finished;
use self::support::statement;

#[tokio::test]
async fn selected_table_is_shared_by_all_write_modes() {
    let mut server = Server::start(|request| async move { Some(Reply::committed(&request)) }).await;
    let table = server
        .client()
        .table("events/2026")
        .with_database("analytics")
        .with_schema("logs");
    table.append(r#"{"id":1}"#).await.unwrap();
    let report = table
        .append_writer()
        .build()
        .unwrap()
        .write(&[json!({"id": 2})])
        .await
        .unwrap();
    assert_eq!(report.committed_rows(), 1);
    let stream = table.append_stream().build().unwrap();
    stream.send(&json!({"id": 3})).await.unwrap();
    assert_eq!(stream.shutdown().await.unwrap().committed_rows, 1);
    let requests = server.received();
    assert_eq!(requests.len(), 3);
    for request in &requests {
        assert_eq!(
            request.target,
            "/proxy/v1/catalog/databases/analytics/schemas/logs/tables/events%2F2026/ingest"
        );
        assert_eq!(request.headers["authorization"], "Bearer test-key");
        assert_eq!(request.headers["content-encoding"], "zstd");
    }
}

#[tokio::test]
async fn submitted_query_returns_typed_rows() {
    let mut server = Server::start(|request| async move {
        let body = if request.method == "POST" {
            statement(request.json()["statement_id"].clone(), "running")
        } else {
            finished(json!(
                request
                    .target
                    .split("/statements/")
                    .nth(1)
                    .unwrap()
                    .split('?')
                    .next()
                    .unwrap()
            ))
        };
        Some(Reply::json(200, body))
    })
    .await;
    let rows = server
        .client()
        .query("SELECT id, label FROM events")
        .await
        .unwrap()
        .into_objects()
        .unwrap();
    assert_eq!(rows[0]["id"], Value::UInt(9_007_199_254_740_993));
    assert_eq!(rows[1]["label"], Value::Null);
    let requests = server.received();
    assert_eq!(requests.len(), 2);
    let id = requests[0].json()["statement_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(requests[1].target.contains(&id));
}

#[tokio::test]
async fn query_recovery_reads_the_original_execution() {
    let reads = Arc::new(AtomicUsize::new(0));
    let mut server = Server::start(move |request| {
        let reads = reads.clone();
        async move {
            let body = if request.method == "POST" {
                statement(request.json()["statement_id"].clone(), "running")
            } else if reads.fetch_add(1, Ordering::SeqCst) == 0 {
                return Some(
                    Reply::json(
                        429,
                        json!({"code": "too_many_requests", "message": "busy", "retryable": true}),
                    )
                    .header("Retry-After", "1"),
                );
            } else {
                finished(json!(
                    request
                        .target
                        .split("/statements/")
                        .nth(1)
                        .unwrap()
                        .split('?')
                        .next()
                        .unwrap()
                ))
            };
            Some(Reply::json(200, body))
        }
    })
    .await;
    let client = server.client();
    let error = client
        .query("SELECT id, label FROM events")
        .await
        .unwrap_err();
    assert_eq!(error.operation(), ErrorOperation::ExecuteStatement);
    assert!(!error.is_retryable());
    assert_eq!(error.retry_after(), Some(Duration::from_secs(1)));
    let id = error.statement_id().unwrap();
    assert_eq!(
        client.statement_handle(id).wait().await.unwrap().num_rows(),
        2
    );
    let requests = server.received();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "POST")
            .count(),
        1
    );
    assert!(
        requests[1..]
            .iter()
            .all(|request| request.target.contains(&id.to_string()))
    );
}

#[tokio::test]
async fn submitted_execution_can_be_cancelled_and_observed_again() {
    let mut server = Server::start(|request| async move {
        let body = if request.target.contains("/cancel") {
            let id = request
                .target
                .split("/statements/")
                .nth(1)
                .unwrap()
                .split('/')
                .next()
                .unwrap();
            statement(json!(id), "cancelled")
        } else {
            statement(request.json()["statement_id"].clone(), "running")
        };
        Some(Reply::json(200, body))
    })
    .await;
    let mut handle = server
        .client()
        .statement("SELECT id FROM events")
        .submit()
        .await
        .unwrap();
    assert!(matches!(
        handle.cancel().await.unwrap(),
        StatementStatus::Cancelled(_)
    ));
    assert_eq!(
        handle.wait().await.unwrap_err().kind(),
        ErrorKind::Cancelled
    );
    assert!(matches!(
        handle.refresh().await.unwrap(),
        StatementStatus::Cancelled(_)
    ));
    assert_eq!(server.received().len(), 2);
}

#[tokio::test]
async fn lost_submission_response_retains_a_recoverable_identity() {
    let mut server = Server::start(|_| async { None }).await;
    let client = server.client();
    let pending = client.statement("SELECT 1");
    let id = pending.statement_id();
    let error = pending.submit().await.unwrap_err();
    assert_eq!(error.statement_id(), Some(id));
    assert!(!error.is_retryable());
    assert!(
        std::error::Error::source(&error)
            .unwrap()
            .is::<scopedb_client::reqwest::Error>()
    );
    let request = server.next().await;
    assert_eq!(request.json()["statement_id"], id.to_string());
    assert!(server.received().is_empty());
}

#[tokio::test]
async fn catalog_updates_preserve_complete_definitions_and_versions() {
    let changed = Arc::new(AtomicBool::new(false));
    let definition = json!({"name": "events", "columns": [{"name": "id", "data_type": "int"}], "future_option": {"enabled": true}});
    let mut server = Server::start(move |request| {
        let changed = changed.clone();
        let definition = definition.clone();
        async move {
            if request.method == "GET" {
                return Some(Reply::json(200, definition).header("ETag", "\"v1\""));
            }
            if changed.swap(true, Ordering::SeqCst) {
                return Some(Reply::json(
                    412,
                    json!({"code": "precondition_failed", "message": "catalog changed"}),
                ));
            }
            Some(
                Reply::json(
                    200,
                    json!({"before": definition, "after": request.json(), "destructive": false}),
                )
                .header("ETag", "\"v2\""),
            )
        }
    })
    .await;
    let table = server.client().table("events");
    assert_eq!(
        table.catalog().get().await.unwrap_err().kind(),
        ErrorKind::InvalidResponse
    );
    let resource = table.catalog().raw();
    let mut snapshot = resource.get().await.unwrap();
    snapshot.definition["comment"] = json!("Application events");
    let options = CatalogOptions {
        if_match: Some(snapshot.etag),
        ..Default::default()
    };
    let applied = resource
        .apply(&snapshot.definition, options.clone())
        .await
        .unwrap();
    assert_eq!(applied.after.unwrap()["future_option"]["enabled"], true);
    let conflict = resource
        .apply(&snapshot.definition, options)
        .await
        .unwrap_err();
    assert_eq!(conflict.kind(), ErrorKind::Conflict);
    assert_eq!(
        conflict.catalog_outcome(),
        Some(CatalogMutationOutcome::Rejected)
    );
    let requests = server.received();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[2].headers["if-match"], "\"v1\"");
}

#[tokio::test]
async fn catalog_transport_failures_distinguish_reads_from_writes() {
    let mut server = Server::start(|_| async { None }).await;
    let table = server.client().table("events");
    let read = table.catalog().get().await.unwrap_err();
    assert!(read.is_retryable());
    let write = table
        .catalog()
        .delete(CatalogOptions::default())
        .await
        .unwrap_err();
    assert_eq!(
        write.catalog_outcome(),
        Some(CatalogMutationOutcome::Unknown)
    );
    assert!(!write.is_retryable());
    assert!(write.code().is_none());
    assert_eq!(server.received().len(), 2);
}

#[tokio::test]
async fn finite_write_reports_partial_delivery_and_original_row_indexes() {
    for policy in [AppendFailurePolicy::Stop, AppendFailurePolicy::Continue] {
        let mut server = Server::start(|request| async move {
            if request.body.starts_with(r#"{"id":2}"#) {
                Some(Reply::json(422, json!({"code": "invalid_schema", "message": "bad id", "details": {"status": "rejected", "row_errors": [{"row_index": 1, "column": "id", "message": "invalid integer"}]}})))
            } else { Some(Reply::committed(&request)) }
        }).await;
        let writer = server
            .client()
            .table("events")
            .append_writer()
            .max_batch_rows(2)
            .max_concurrent_batches(1)
            .failure_policy(policy)
            .build()
            .unwrap();
        let rows: Vec<_> = (0..6).map(|id| json!({"id": id})).collect();
        let error = writer.write(&rows).await.unwrap_err();
        assert!(!error.is_retryable());
        let report = error.append_write_report().unwrap();
        assert_eq!(report.outcome(), AppendDeliveryOutcome::Partial);
        let batch = &report.batches()[1];
        let rejected = batch.result().as_ref().unwrap_err();
        let row_error = &rejected.append_details().unwrap().row_errors[0];
        assert_eq!(batch.input_index(row_error.row_index), Some(3));
        if policy == AppendFailurePolicy::Stop {
            assert_eq!(report.committed_rows(), 2);
            assert_eq!(report.unsubmitted(), 4..6);
        } else {
            assert_eq!(report.committed_rows(), 4);
            assert!(report.unsubmitted().is_empty());
        }
        assert_eq!(server.received().len(), report.batches().len());
    }
}

#[tokio::test]
async fn finite_write_local_failure_keeps_buffered_input_unsubmitted() {
    let mut server = Server::start(|request| async move { Some(Reply::committed(&request)) }).await;
    let writer = server
        .client()
        .table("events")
        .append_writer()
        .max_batch_rows(2)
        .max_concurrent_batches(1)
        .build()
        .unwrap();
    async fn write(writer: &AppendWriter) -> Result<(), Error> {
        let _report = writer
            .write(&[
                json!({"id": 0}),
                json!({"id": 1}),
                json!({"id": 2}),
                json!([3]),
            ])
            .await?;
        Ok(())
    }
    let error = write(&writer).await.unwrap_err();
    assert_eq!(error.append_input_index(), Some(3));
    let report = error.append_write_report().unwrap();
    assert_eq!(report.committed_rows(), 2);
    assert_eq!(report.unsubmitted(), 2..4);
    assert_eq!(server.received().len(), 1);
}

#[tokio::test]
async fn replay_policy_preserves_earlier_unknown_outcomes() {
    for replay in [false, true] {
        let attempts = Arc::new(AtomicUsize::new(0));
        let mut server = Server::start(move |_| {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt == 0 { None } else {
                    Some(Reply::json(422, json!({"code": "invalid_schema", "message": "last rejection", "details": {"status": "rejected"}})))
                }
            }
        }).await;
        let writer = server
            .client()
            .table("events")
            .append_writer()
            .max_retries(1)
            .initial_backoff(Duration::ZERO)
            .retry_unknown(replay)
            .build()
            .unwrap();
        let error = writer.write(&[json!({"id": 1})]).await.unwrap_err();
        let report = error.append_write_report().unwrap();
        assert_eq!(report.outcome(), AppendDeliveryOutcome::Unknown);
        assert_eq!(report.batches()[0].retries(), usize::from(replay));
        assert!(!error.is_retryable());
        if replay {
            assert_eq!(error.details().unwrap()["status"], "rejected");
        }
        assert_eq!(server.received().len(), 1 + usize::from(replay));
    }
}

#[tokio::test]
async fn bulk_admission_failure_leaves_its_prefix_owned_by_the_stream() {
    let server = Server::start(|request| async move { Some(Reply::committed(&request)) }).await;
    let stream = server
        .client()
        .table("events")
        .append_stream()
        .build()
        .unwrap();
    let error = stream
        .send_all([json!({"id": 1}), json!([2]), json!({"id": 3})])
        .await
        .unwrap_err();
    assert_eq!(error.append_admission().unwrap().accepted_rows, 1);
    stream.try_send(&json!({"id": 4})).unwrap();
    assert_eq!(stream.shutdown().await.unwrap().committed_rows, 2);
}

#[tokio::test]
async fn continue_stream_retries_transient_rejections_and_reports_losses() {
    let first = Arc::new(AtomicBool::new(true));
    let mut server = Server::start(move |request| {
        let first = first.clone();
        async move {
            if request.json()["id"] == 1 {
                Some(Reply::json(422, json!({"message": "bad row", "details": {"status": "rejected"}})))
            } else if first.swap(false, Ordering::SeqCst) {
                Some(Reply::json(503, json!({"message": "busy", "retryable": true, "details": {"status": "rejected"}})))
            } else { Some(Reply::committed(&request)) }
        }
    }).await;
    let stream = server
        .client()
        .table("events")
        .append_stream()
        .max_batch_rows(1)
        .max_concurrent_batches(1)
        .max_retries(1)
        .initial_backoff(Duration::ZERO)
        .failure_policy(AppendFailurePolicy::Continue)
        .build()
        .unwrap();
    stream
        .send_all((0..3).map(|id| json!({"id": id})))
        .await
        .unwrap();
    let report = stream.shutdown().await.unwrap();
    assert_eq!(report.outcome, AppendDeliveryOutcome::Partial);
    assert_eq!(report.committed_rows, 2);
    assert_eq!(report.failed_rows, 1);
    assert_eq!(report.retries, 1);
    assert_eq!(server.received().len(), 4);
}

#[tokio::test]
async fn stopped_stream_settles_concurrent_requests_for_all_waiters() {
    let (reject, rejected) = watch::channel(false);
    let (commit, committed) = watch::channel(false);
    let failure = Arc::new(Notify::new());
    let mut server = Server::start(move |request| {
        let mut rejected = rejected.clone();
        let mut committed = committed.clone();
        async move {
            if request.json()["id"] == 0 {
                rejected.wait_for(|ready| *ready).await.unwrap();
                Some(Reply::json(
                    422,
                    json!({"message": "bad row", "details": {"status": "rejected"}}),
                ))
            } else {
                committed.wait_for(|ready| *ready).await.unwrap();
                Some(Reply::committed(&request))
            }
        }
    })
    .await;
    let stream = server
        .client()
        .table("events")
        .append_stream()
        .max_batch_rows(1)
        .max_concurrent_batches(2)
        .on_batch_failure({
            let failure = failure.clone();
            move |_| failure.notify_one()
        })
        .build()
        .unwrap();
    stream
        .send_all([json!({"id": 0}), json!({"id": 1})])
        .await
        .unwrap();
    let producer = stream.clone();
    let shutdown = tokio::spawn(async move { producer.shutdown().await });
    server.next().await;
    server.next().await;
    reject.send(true).unwrap();
    failure.notified().await;
    assert_eq!(
        stream.send(&json!({"id": 2})).await.unwrap_err().kind(),
        ErrorKind::Closed
    );
    commit.send(true).unwrap();
    let first = shutdown.await.unwrap().unwrap_err();
    let second = stream.flush().await.unwrap_err();
    let report = first.append_delivery_report().unwrap();
    assert_eq!(report.outcome, AppendDeliveryOutcome::Partial);
    assert_eq!(report.committed_rows, 1);
    assert_eq!(report.failed_rows, 1);
    assert_eq!(second.append_delivery_report().unwrap(), report);
}

#[tokio::test]
async fn cancellation_while_waiting_for_http_response_stops_finite_delivery() {
    let (release, blocked) = watch::channel(false);
    let mut server = Server::start(move |request| {
        let mut blocked = blocked.clone();
        async move {
            if request.json()["id"] == 0 {
                blocked.wait_for(|ready| *ready).await.unwrap();
                Some(Reply::json(503, json!({"message": "busy", "retryable": true, "details": {"status": "rejected"}})))
            } else { Some(Reply::committed(&request)) }
        }
    }).await;
    let writer = server
        .client()
        .table("events")
        .append_writer()
        .max_batch_rows(1)
        .max_concurrent_batches(1)
        .initial_backoff(Duration::ZERO)
        .build()
        .unwrap();
    let task = tokio::spawn({
        let writer = writer.clone();
        async move { writer.write(&[json!({"id": 0}), json!({"id": 1})]).await }
    });
    assert_eq!(server.next().await.json()["id"], 0);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    release.send(true).unwrap();
    assert_eq!(
        writer
            .write(&[json!({"id": 9})])
            .await
            .unwrap()
            .committed_rows(),
        1
    );
    assert_eq!(server.next().await.json()["id"], 9);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), server.next())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn response_body_timeout_is_unknown_and_is_not_replayed() {
    let (release, blocked) = watch::channel(false);
    let mut server = Server::start(move |request| {
        let blocked = blocked.clone();
        async move { Some(Reply::committed(&request).hold_body(blocked)) }
    })
    .await;
    let writer = server
        .client()
        .table("events")
        .append_writer()
        .attempt_timeout(Duration::from_millis(100))
        .retry_unknown(true)
        .build()
        .unwrap();
    let error = writer.write(&[json!({"id": 1})]).await.unwrap_err();
    let detail = error.write_details().unwrap();
    assert_eq!(detail.reason, WriteErrorReason::Timeout);
    assert_eq!(detail.outcome, AppendState::Unknown);
    assert_eq!(
        error.http_status(),
        Some(scopedb_client::reqwest::StatusCode::OK)
    );
    assert_eq!(
        error.append_write_report().unwrap().batches()[0].retries(),
        0
    );
    assert!(!error.is_retryable());
    release.send(true).unwrap();
    assert_eq!(server.received().len(), 1);
}
