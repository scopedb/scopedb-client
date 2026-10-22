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
    let target = common::write_table(&client)?;

    let stream = target
        .append_stream()
        .max_concurrent_batches(1)
        .max_retries(0)
        .build()?;

    let source = serde_json::json!({
        "ts": "2026-03-13T12:00:00Z",
        "name": "ScopeDB",
    });
    let sent = stream
        .send(&serde_json::json!({
            "occurred_at": source["ts"],
            "name": source["name"],
        }))
        .await;
    if let Err(error) = sent {
        let _ = stream.shutdown().await;
        return Err(error.into());
    }

    let report = stream.shutdown().await?;
    println!("Committed rows: {}", report.committed_rows);

    Ok(())
}
