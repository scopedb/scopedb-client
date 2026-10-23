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

use std::time::Duration;

use anyhow::Result;
use anyhow::anyhow;
use anyhow::ensure;
use scopedb_client::AppendState;
use scopedb_client::CatalogOptions;
use scopedb_client::Client;
use scopedb_client::StatementStatus;
use serde_json::json;

/// Uses an isolated schema and always attempts cleanup before returning errors.
#[tokio::test]
#[ignore = "requires ScopeDB 0.6; set SCOPEDB_ENDPOINT and API key or local SCOPEDB_TENANT_ID"]
async fn native_06_lifecycle() -> Result<()> {
    let mut headers = scopedb_client::reqwest::header::HeaderMap::new();
    if let Ok(tenant) = std::env::var("SCOPEDB_TENANT_ID") {
        headers.insert("X-ScopeDB-Tenant-Id", tenant.parse()?);
    }
    let mut builder = Client::builder(std::env::var("SCOPEDB_ENDPOINT")?).http_client(
        scopedb_client::reqwest::Client::builder()
            .default_headers(headers)
            .redirect(scopedb_client::reqwest::redirect::Policy::none())
            .retry(scopedb_client::reqwest::retry::never())
            .timeout(Duration::from_secs(30))
            .build()?,
    );
    if let Ok(api_key) = std::env::var("SCOPEDB_API_KEY") {
        builder = builder.api_key(api_key);
    }
    let client = builder.build()?;
    let name = format!("sdk06_rust_{}", uuid::Uuid::now_v7().simple());
    let resource = client.catalog().schema("scopedb", &name);
    let definition: scopedb_client::CatalogSchema = serde_json::from_value(
        json!({ "name": name, "tables": [{ "name": "events", "columns": [
        { "name": "id", "data_type": "int" }, { "name": "source", "data_type": "string" }
    ] }] }),
    )?;
    let preview = resource
        .apply(
            &definition,
            CatalogOptions {
                dry_run: true,
                ..Default::default()
            },
        )
        .await?;
    ensure!(preview.before.is_none() && preview.after.is_some() && !preview.etag.is_empty());
    let missing = resource
        .get()
        .await
        .err()
        .ok_or_else(|| anyhow!("dry-run changed the catalog"))?;
    ensure!(missing.http_status().map(|s| s.as_u16()) == Some(404));
    let result: Result<()> = async {
        let applied = resource
            .apply(
                &definition,
                CatalogOptions {
                    if_match: Some(preview.etag.clone()),
                    ..Default::default()
                },
            )
            .await?;
        ensure!(resource.get().await?.etag == applied.etag);
        let stale = resource
            .apply(
                &definition,
                CatalogOptions {
                    if_match: Some(preview.etag),
                    ..Default::default()
                },
            )
            .await
            .err()
            .ok_or_else(|| anyhow!("accepted stale ETag"))?;
        ensure!(stale.http_status().map(|s| s.as_u16()) == Some(412));
        ensure!(stale.code() == Some("catalog_version_mismatch"));
        println!("STALE_ETAG code={}", stale.code().unwrap());
        let snapshot = resource.get().await?;
        let mut replacement = snapshot.definition;
        replacement.comment = Some("conditional round trip".into());
        resource.apply(&replacement, CatalogOptions {
            if_match: Some(snapshot.etag.clone()), ..Default::default()
        }).await?;
        let conflict = resource.delete(CatalogOptions {
            if_match: Some(snapshot.etag), allow_destructive: true, ..Default::default()
        }).await.unwrap_err();
        ensure!(conflict.code() == Some("catalog_version_mismatch"));
        ensure!(resource.get().await?.definition == replacement);
        println!("CATALOG full definition round trip and conditional delete conflict");
        let table = client.table("events").with_schema(&name);
        ensure!(table.catalog().get().await?.definition.columns.len() == 2);
        let inserted = table
            .append("{\"id\":1,\"source\":\"rust\"}\n{\"id\":2,\"source\":\"rust\"}")
            .await?;
        ensure!(inserted.num_rows_inserted == 2 && inserted.status == AppendState::Committed);
        let stream = table
            .append_stream()
            .max_batch_rows(1)
            .max_concurrent_batches(2)
            .build()?;
        stream
            .send_all((3..=6).map(|id| json!({"id":id,"source":"stream"})))
            .await?;
        let report = stream.shutdown().await?;
        ensure!(report.committed_rows == 4 && report.failed_rows == 0 && report.unknown_rows == 0);
        ensure!(stream.stats().pending_bytes == 0);
        println!("STREAM committed=4 pending_bytes=0");
        let rejected = table
            .append("{\"id\":\"bad\",\"source\":\"rust\"}")
            .await
            .err()
            .ok_or_else(|| anyhow!("accepted invalid row"))?;
        ensure!(rejected.code() == Some("invalid_schema"));
        let details = rejected
            .append_details()
            .ok_or_else(|| anyhow!("missing ingest details"))?;
        ensure!(details.status == AppendState::Rejected && details.row_errors.len() == 1);
        ensure!(
            rejected
                .details()
                .is_some_and(|d| d["status"] == "rejected")
        );
        println!(
            "REJECTED code={} row_errors={}",
            rejected.code().unwrap(),
            details.row_errors.len()
        );
        let mut handle = client
            .statement(format!("FROM {}", table.identifier()))
            .submit()
            .await?;
        ensure!(handle.wait().await?.num_rows() == 6);
        println!(
            "QUERY rows=6 compute_seconds={}",
            handle
                .snapshot()
                .unwrap()
                .progress()
                .details
                .compute_seconds
        );
        let mut late = client.statement_handle(handle.statement_id());
        ensure!(matches!(late.cancel().await?, StatementStatus::Finished(_)));
        ensure!(late.wait().await?.num_rows() == 6);
        println!("CANCEL completed statement retained result");
        let boundaries = client.query("SELECT -9223372036854775808 AS lo, 9223372036854775807 AS hi, 18446744073709551615 AS unsigned").await?;
        let row = boundaries.first()?.ok_or_else(|| anyhow!("missing numeric result"))?;
        ensure!(matches!(row["lo"], scopedb_client::Value::Int(i64::MIN)));
        ensure!(matches!(row["hi"], scopedb_client::Value::Int(i64::MAX)));
        ensure!(matches!(row["unsigned"], scopedb_client::Value::UInt(u64::MAX)));
        println!("NUMBERS i64 min/max and u64 max retained");
        let failed = client
            .query("SELECT UNKNOWN_FUNCTION()")
            .await
            .err()
            .ok_or_else(|| anyhow!("expected failed statement"))?;
        ensure!(failed.code() == Some("statement_failed") && !failed.message().is_empty());
        println!("FAILED code={}", failed.code().unwrap());
        let timeout = client
            .statement(format!("FROM {}", table.identifier()))
            .with_max_elapsed(Duration::from_nanos(1))
            .execute()
            .await
            .err()
            .ok_or_else(|| anyhow!("expected server timeout"))?;
        ensure!(timeout.code().is_some_and(|code| code.contains("timeout")));
        println!("TIMEOUT code={}", timeout.code().unwrap());
        Ok(())
    }
    .await;
    let cleanup = resource
        .delete(CatalogOptions {
            allow_destructive: true,
            ..Default::default()
        })
        .await;
    if cleanup.is_ok() {
        println!("CLEANUP schema removed");
    }
    result?;
    cleanup?;
    println!("PASS native 0.6 lifecycle");
    Ok(())
}
