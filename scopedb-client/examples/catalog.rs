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

mod common;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::client()?;
    let database = common::database();
    let schema = common::schema();

    let catalog = client.catalog().get().await?;
    for item in catalog.definition.databases {
        println!(
            "database {}: {}",
            item.name,
            item.comment.unwrap_or_default()
        );
    }
    let snapshot = client.catalog().schema(&database, &schema).get().await?;
    println!("schema ETag: {}", snapshot.etag);
    for table in snapshot.definition.tables {
        println!("table {}: {:?}", table.name, table.columns);
    }
    Ok(())
}
