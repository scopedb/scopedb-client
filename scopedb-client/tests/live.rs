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

//! Opt-in protocol checks against a test environment with schema creation and write permissions.
//!
//! Run with `cargo test -p scopedb-client --test live -- --ignored --nocapture` after configuring
//! the endpoint and test credentials. The test creates a temporary schema and attempts cleanup
//! even after assertion failures. CI and release workflows leave this test ignored.

use scopedb_client::AppendOutcome;
use scopedb_client::CatalogColumn;
use scopedb_client::CatalogMutationOutcome;
use scopedb_client::CatalogOptions;
use scopedb_client::CatalogSchema;
use scopedb_client::CatalogTable;
use scopedb_client::Client;
use scopedb_client::DataType;
use scopedb_client::ErrorKind;
use scopedb_client::Value;
use scopedb_client::reqwest;
use uuid::Uuid;

#[tokio::test]
#[ignore = "set SCOPEDB_ENDPOINT and test credentials to run against ScopeDB"]
async fn live_protocol() {
    let client = client();
    let rows = client
        .query("SELECT 1 AS value")
        .await
        .unwrap()
        .into_objects()
        .unwrap();
    assert_eq!(rows[0]["value"], Value::Int(1));

    let database = std::env::var("SCOPEDB_DATABASE").unwrap_or_else(|_| "scopedb".into());
    let schema = format!("sdk_smoke_{}", Uuid::now_v7().simple());
    eprintln!("Temporary schema: {database}.{schema}");
    let resource = client.catalog().schema(&database, &schema);
    // Join the assertions before cleanup so a failed assertion also releases the test schema.
    let result = tokio::spawn(check_protocol(client, database, schema)).await;
    let cleanup = resource
        .delete(CatalogOptions {
            allow_destructive: true,
            ..Default::default()
        })
        .await;
    if let Err(error) = &cleanup {
        eprintln!("Temporary schema cleanup failed: {error}");
    }
    result.unwrap();
    cleanup.unwrap();
}

async fn check_protocol(client: Client, database: String, schema: String) {
    client
        .catalog()
        .schema(&database, &schema)
        .apply(
            &CatalogSchema {
                name: schema.clone(),
                tables: vec![CatalogTable {
                    name: "events".into(),
                    columns: vec![CatalogColumn {
                        name: "id".into(),
                        data_type: DataType::Int,
                        expression: None,
                        comment: None,
                        indexes: Vec::new(),
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            CatalogOptions::default(),
        )
        .await
        .unwrap();
    let table = client
        .table("events")
        .with_database(&database)
        .with_schema(&schema);
    let resource = table.catalog();
    let snapshot = resource.get().await.unwrap();
    let mut definition = snapshot.definition.clone();
    definition.comment = Some("SDK protocol smoke test".into());
    let preview = resource
        .apply(
            &definition,
            CatalogOptions {
                dry_run: true,
                expected_version: Some(snapshot.version.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(preview.version, snapshot.version);
    assert_eq!(
        resource.get().await.unwrap().definition,
        snapshot.definition
    );
    let applied = resource
        .apply(
            &definition,
            CatalogOptions {
                expected_version: Some(preview.version),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_ne!(applied.version, snapshot.version);
    assert_eq!(resource.get().await.unwrap().definition, definition);
    let conflict = resource
        .delete(CatalogOptions {
            expected_version: Some(snapshot.version),
            allow_destructive: true,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(
        conflict.http_status(),
        Some(reqwest::StatusCode::PRECONDITION_FAILED)
    );
    assert_eq!(conflict.code(), Some("precondition_failed"));
    assert_eq!(conflict.kind(), ErrorKind::Conflict);
    assert_eq!(
        conflict.catalog_outcome(),
        Some(CatalogMutationOutcome::Rejected)
    );
    assert!(!conflict.is_retryable());

    let appended = table.append("{\"id\":1}\n{\"id\":2}").await.unwrap();
    assert_eq!(appended.status, AppendOutcome::Succeeded);
    assert_eq!(appended.num_rows_inserted, 2);
    let failed = table
        .append("{\"id\":3}\n{\"id\":\"invalid integer\"}")
        .await
        .unwrap_err();
    assert_eq!(
        failed.append_details().unwrap().status,
        AppendOutcome::Failed
    );
    assert!(!failed.is_retryable());
    let rows = client
        .query(format!("from {} order by id select id", table.identifier()))
        .await
        .unwrap()
        .into_objects()
        .unwrap();
    let ids: Vec<_> = rows.iter().map(|row| &row["id"]).collect();
    assert_eq!(ids, [&Value::Int(1), &Value::Int(2)]);
    assert_eq!(resource.get().await.unwrap().version, applied.version);
}

fn client() -> Client {
    let mut builder = Client::builder(std::env::var("SCOPEDB_ENDPOINT").unwrap());
    if let Ok(api_key) = std::env::var("SCOPEDB_API_KEY") {
        builder = builder.api_key(api_key);
    }
    if let Ok(tenant_id) = std::env::var("SCOPEDB_TENANT_ID") {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("X-ScopeDB-Tenant-Id", tenant_id.parse().unwrap());
        builder = builder.http_client(
            reqwest::Client::builder()
                .default_headers(headers)
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .build()
                .unwrap(),
        );
    }
    builder.build().unwrap()
}
