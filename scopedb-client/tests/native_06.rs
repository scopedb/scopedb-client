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
    if std::env::var("SCOPEDB_API_KEY").is_ok() {
        let unauthorized = Client::builder(std::env::var("SCOPEDB_ENDPOINT")?)
            .api_key("deliberately-invalid-sdk-test-key")
            .build()?;
        let errors = [
            unauthorized.query("SELECT 1").await.unwrap_err(),
            unauthorized.catalog().get().await.unwrap_err(),
            unauthorized
                .table("unreachable")
                .append("{\"id\":1}")
                .await
                .unwrap_err(),
        ];
        for error in errors {
            ensure!(error.http_status().is_some_and(|s| s.as_u16() == 401));
            ensure!(error.request_id().is_some() && !error.is_retryable());
            ensure!(
                !error
                    .to_string()
                    .contains("deliberately-invalid-sdk-test-key")
            );
        }
        println!(
            "AUTH rejected invalid credentials on query, Catalog, and Append with request IDs"
        );
    }
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
        ensure!(failed.statement_details().is_some() && !failed.message().is_empty());
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
        exercise_data_paths(&client, &name).await?;
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

async fn row_count(client: &Client, table: &scopedb_client::Table) -> Result<u64> {
    let result = client
        .query(format!(
            "FROM {} AGGREGATE count() AS n",
            table.identifier()
        ))
        .await?;
    Ok(
        result.first()?.ok_or_else(|| anyhow!("missing count"))?["n"]
            .to_string()
            .parse()?,
    )
}

async fn exercise_data_paths(client: &Client, schema: &str) -> Result<()> {
    use scopedb_client::AppendFailurePolicy;
    use scopedb_client::AppendTrySendError;
    use scopedb_client::Value;

    let table = client.table("events / ` 验证").with_schema(schema);
    let definition: scopedb_client::CatalogTable = serde_json::from_value(json!({
        "name": "events / ` 验证", "retention_days": 7, "columns": [
            {"name":"id", "data_type":"int"},
            {"name":"message", "data_type":"string", "indexes":[{"type":"search","analyzer":"log"}]},
            {"name":"attributes", "data_type":"object"},
            {"name":"tags", "data_type":"array"},
            {"name":"unsigned", "data_type":"uint"},
            {"name":"doubled", "data_type":"int", "expression":"id * 2"}
        ]
    }))?;
    table
        .catalog()
        .apply(&definition, CatalogOptions::default())
        .await?;
    table.append("{\"id\":1,\"message\":\"hello 世界\",\"attributes\":{\"n\":9007199254740993},\"tags\":[1,\"a\"],\"unsigned\":9223372036854775807}\n{\"id\":2}").await?;
    let rows = client
        .query(format!("FROM {} ORDER BY id", table.identifier()))
        .await?
        .into_objects()?;
    ensure!(rows[0]["doubled"] == Value::Int(2));
    ensure!(rows[0]["unsigned"] == Value::UInt(i64::MAX as u64));
    ensure!(
        rows[0]["attributes"]
            .to_string()
            .contains("9007199254740993")
    );
    ensure!(rows[1]["message"] == Value::Null);
    let oversized_uint = table
        .append("{\"id\":3,\"unsigned\":18446744073709551615}")
        .await
        .unwrap_err();
    ensure!(
        oversized_uint
            .append_details()
            .is_some_and(|d| d.status == AppendState::Rejected)
    );
    let mixed = table
        .append("{\"id\":3}\n{\"id\":\"bad\"}")
        .await
        .unwrap_err();
    ensure!(mixed.code() == Some("invalid_schema"));
    ensure!(mixed.append_details().unwrap().row_errors[0].row_index == 1);
    ensure!(row_count(client, &table).await? == 2);

    let view = client.catalog().view("scopedb", schema, "computed_view");
    view.apply(
        &scopedb_client::CatalogView {
            name: "computed_view".into(),
            statement: format!("FROM {} SELECT id, doubled", table.identifier()),
            comment: None,
        },
        CatalogOptions::default(),
    )
    .await?;
    ensure!(
        client
            .query(format!(
                "FROM {}",
                client
                    .table("computed_view")
                    .with_schema(schema)
                    .identifier()
            ))
            .await?
            .num_rows()
            == 2
    );
    let snapshot = table.catalog().get().await?;
    let mut changed = snapshot.definition;
    changed.comment = Some("retained computed columns and indexes".into());
    table
        .catalog()
        .apply(
            &changed,
            CatalogOptions {
                if_match: Some(snapshot.etag),
                ..Default::default()
            },
        )
        .await?;
    ensure!(table.catalog().get().await?.definition == changed);
    let dependent = table
        .catalog()
        .delete(CatalogOptions {
            dry_run: true,
            ..Default::default()
        })
        .await
        .unwrap_err();
    ensure!(dependent.code() == Some("invalid_request"));
    ensure!(
        dependent
            .http_status()
            .is_some_and(|status| status.as_u16() == 422)
    );
    view.delete(CatalogOptions::default()).await?;
    let preview = table
        .catalog()
        .delete(CatalogOptions {
            dry_run: true,
            ..Default::default()
        })
        .await?;
    ensure!(preview.destructive && preview.before.is_some() && preview.after.is_none());
    let denied = table
        .catalog()
        .delete(CatalogOptions::default())
        .await
        .unwrap_err();
    ensure!(denied.code() == Some("catalog_destructive_change"));
    ensure!(row_count(client, &table).await? == 2);
    println!(
        "CATALOG encoded names, views, computed columns, indexes, retention, and destructive guard"
    );

    let stream = table
        .append_stream()
        .failure_policy(AppendFailurePolicy::Continue)
        .max_batch_rows(1)
        .max_concurrent_batches(2)
        .build()?;
    ensure!(matches!(
        stream.try_send(&7),
        Err(AppendTrySendError::InvalidRecord)
    ));
    stream
        .send_all([json!({"id":3}), json!({"id":"bad"}), json!({"id":4})])
        .await?;
    let report = stream.shutdown().await?;
    ensure!(
        (
            report.accepted_rows,
            report.committed_rows,
            report.failed_rows,
            report.dropped_rows
        ) == (3, 2, 1, 1)
    );
    ensure!(report.unknown_rows == 0 && report.retries == 0);
    let stats = stream.stats();
    ensure!(stats.pending_rows == 0 && stats.pending_bytes == 0 && stats.in_flight_batches == 0);
    ensure!(stats.last_failure.unwrap().error.code() == Some("invalid_schema"));
    ensure!(row_count(client, &table).await? == 4);
    println!(
        "APPEND atomic rejection, uint input boundary, continue-mode partial report, and local drops"
    );

    let bulk = client.table("bulk").with_schema(schema);
    bulk.catalog()
        .raw()
        .apply(
            &json!({"name":"bulk","columns":[
                {"name":"id","data_type":"int"},{"name":"payload","data_type":"string"}
            ]}),
            CatalogOptions::default(),
        )
        .await?;
    let stream = bulk
        .append_stream()
        .max_buffered_bytes(8 * 1024 * 1024)
        .max_concurrent_batches(3)
        .build()?;
    let payload = "event data 世界 ".repeat(40);
    let admitted = stream
        .send_all((0..25_000).map(|id| json!({"id":id,"payload":payload})))
        .await?;
    ensure!(admitted.accepted_rows == 25_000);
    let first = stream.flush().await?;
    ensure!(first.committed_rows == 25_000 && first.committed_batches > 1);
    stream
        .send(&json!({"id":25_000,"payload":"after flush"}))
        .await?;
    let final_report = stream.shutdown().await?;
    ensure!(final_report.committed_rows == 1);
    ensure!(stream.stats().committed_rows == 25_001 && stream.stats().pending_bytes == 0);
    ensure!(row_count(client, &bulk).await? == 25_001);
    let distinct = client
        .query(format!(
            "FROM {} AGGREGATE count_distinct(id) AS n",
            bulk.identifier()
        ))
        .await?;
    ensure!(distinct.first()?.unwrap()["n"].to_string() == "25001");
    println!("BULK 25001 unique rows, bounded concurrent batches, separate flush intervals");

    // Bound both join inputs independently; cancellation timing must not turn
    // this integration check into a load test on a shared server.
    let input = format!("(FROM {} SELECT id LIMIT 128)", bulk.identifier());
    let mut active = client
        .statement(format!(
            "FROM {input} AS a JOIN {input} AS b ON a.id != b.id AGGREGATE sum(a.id + b.id) AS total"
        ))
        .with_max_elapsed(Duration::from_secs(3))
        .submit()
        .await?;
    let initial_terminal = active.snapshot().unwrap().is_terminated();
    let cancelled = active.cancel().await?;
    let settled = active.wait().await;
    match (active.snapshot().unwrap(), settled) {
        (StatementStatus::Finished(_), Ok(_)) => {}
        (StatementStatus::Failed(_), Err(error)) => {
            ensure!(error.operation() == scopedb_client::ErrorOperation::WaitStatement);
            ensure!(error.statement_details().is_some());
        }
        (StatementStatus::Cancelled(_), Err(error)) => {
            ensure!(error.kind() == scopedb_client::ErrorKind::Cancelled);
        }
        (StatementStatus::Pending(_) | StatementStatus::Running(_), _) => {
            return Err(anyhow!(
                "wait returned with a nonterminal statement snapshot"
            ));
        }
        (_, Ok(_)) => {
            return Err(anyhow!("wait returned rows for a non-finished statement"));
        }
        (_, Err(error)) => {
            return Err(anyhow!(
                "wait error did not match the terminal snapshot: {error}"
            ));
        }
    }
    println!(
        "CANCEL response={cancelled:?}, terminal={:?}, initially_terminal={initial_terminal}",
        active.snapshot()
    );
    Ok(())
}
