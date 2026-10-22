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
#[ignore = "requires SCOPEDB_ENDPOINT and SCOPEDB_API_KEY for ScopeDB 0.6"]
async fn native_06_lifecycle() -> Result<()> {
    let client = Client::builder(std::env::var("SCOPEDB_ENDPOINT")?)
        .api_key(std::env::var("SCOPEDB_API_KEY")?)
        .http_client(
            scopedb_client::reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?,
        )
        .build()?;
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
        let table = client.table("events").with_schema(&name);
        ensure!(table.describe().await?.columns.len() == 2);
        let inserted = table
            .append("{\"id\":1,\"source\":\"rust\"}\n{\"id\":2,\"source\":\"rust\"}")
            .await?;
        ensure!(inserted.num_rows_inserted == 2);
        let rejected = table
            .append("{\"id\":\"bad\",\"source\":\"rust\"}")
            .await
            .err()
            .ok_or_else(|| anyhow!("accepted invalid row"))?;
        ensure!(rejected.code() == Some("invalid_schema"));
        let details = rejected
            .append_details()
            .ok_or_else(|| anyhow!("missing ingest details"))?;
        ensure!(details.append_state == AppendState::Rejected && details.row_errors.len() == 1);
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
        ensure!(handle.wait().await?.num_rows() == 2);
        println!(
            "QUERY rows=2 compute_seconds={}",
            handle.progress().unwrap().details.compute_seconds
        );
        let mut late = client.statement_handle(handle.statement_id());
        ensure!(matches!(late.cancel().await?, StatementStatus::Finished(_)));
        ensure!(late.result_set().is_some_and(|r| r.num_rows() == 2));
        println!("CANCEL completed statement retained result");
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
