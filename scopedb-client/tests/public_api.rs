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

#[test]
fn reexported_http_client_matches_constructor() {
    scopedb_client::Client::new(
        "http://127.0.0.1:6543",
        scopedb_client::reqwest::Client::new(),
    )
    .unwrap();
}

#[test]
fn ergonomic_client_builder_is_public() {
    scopedb_client::Client::builder("http://127.0.0.1:6543")
        .api_key("test-key")
        .http_client(scopedb_client::reqwest::Client::new())
        .write_timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap();
}

#[test]
fn structured_write_error_contract_is_public() {
    let error = scopedb_client::Client::builder("invalid endpoint")
        .build()
        .unwrap_err();
    let _details: Option<&scopedb_client::WriteErrorDetails> = error.write_details();
    assert_eq!(
        scopedb_client::WriteErrorReason::Timeout.to_string(),
        "timeout"
    );
}

#[allow(dead_code)]
fn application_api_surface_compiles(client: &scopedb_client::Client) {
    let _query = client.query("SELECT 1");
    let mut handle = client.statement_handle(uuid::Uuid::nil());
    let _last_status: Option<&scopedb_client::StatementStatus> = handle.snapshot();
    let status = handle.refresh();
    drop(status);
    let wait = handle.wait();
    drop(wait);
    let catalog = client.catalog();
    let _snapshot = catalog.get();
    let _schema = catalog.schema("scopedb", "public");
    let table = client.table("events").with_schema("public");
    let resource = table.catalog();
    let _description = resource.get();
    let writer: scopedb_client::AppendWriter = table
        .append_writer()
        .target_batch_bytes(1024)
        .max_batch_rows(100)
        .max_concurrent_batches(2)
        .max_retries(3)
        .failure_policy(scopedb_client::AppendFailurePolicy::Continue)
        .build()
        .unwrap();
    let rows = [serde_json::json!({"id": 1})];
    let _write = writer.write(&rows);
    let _stream = table
        .append_stream()
        .target_batch_bytes(1024)
        .max_batch_rows(100)
        .max_buffered_bytes(4096)
        .max_concurrent_batches(2);
}

#[tokio::test]
async fn sdk_operations_compose_without_erasing_partial_progress() {
    async fn write(client: &scopedb_client::Client) -> Result<(), scopedb_client::Error> {
        let table = client.table("events");
        let writer = table.append_writer().build()?;
        let _report = writer.write(&[serde_json::json!([1])]).await?;
        let stream = table.append_stream().build()?;
        stream.send(&serde_json::json!({"id": 1})).await?;
        stream.try_send(&serde_json::json!({"id": 3}))?;
        stream.send_all([serde_json::json!({"id": 2})]).await?;
        stream.flush().await?;
        stream.shutdown().await?;
        Ok(())
    }

    let client = scopedb_client::Client::builder("http://127.0.0.1:0")
        .build()
        .unwrap();
    let error = write(&client).await.unwrap_err();
    let report = error.append_write_report().unwrap();
    assert_eq!(report.unsubmitted(), 0..1);
    assert!(report.batches().is_empty());
    assert_eq!(error.append_input_index(), Some(0));
}

#[test]
fn local_admission_conversion_preserves_details_and_native_sources() {
    let error: scopedb_client::Error = scopedb_client::AppendTrySendError::RecordTooLarge {
        bytes: 1025,
        limit: 1024,
    }
    .into();
    let cloned = error.clone();
    assert!(matches!(
        cloned.try_send_error(),
        Some(scopedb_client::AppendTrySendError::RecordTooLarge {
            bytes: 1025,
            limit: 1024,
        })
    ));
    assert_eq!(cloned.kind(), scopedb_client::ErrorKind::InvalidInput);
    assert!(!cloned.is_retryable());
    let source = serde_json::from_str::<serde_json::Value>("invalid JSON").unwrap_err();
    let error: scopedb_client::Error =
        scopedb_client::AppendTrySendError::Serialization(source).into();
    assert!(matches!(
        error.try_send_error(),
        Some(scopedb_client::AppendTrySendError::Serialization(_))
    ));
    assert!(
        std::error::Error::source(&error)
            .unwrap()
            .is::<serde_json::Error>()
    );
}
