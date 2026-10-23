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

use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: import_ndjson <events.ndjson>")?;
    let mut input = BufReader::new(File::open(path)?);
    let client = common::client()?;
    let stream = common::write_table(&client)?.append_stream().build()?;
    let admitted: Result<(), Box<dyn std::error::Error>> = async {
        let mut line_number = 0;
        loop {
            // Bound a single input record as well as the stream's queued bytes.
            let mut line = String::new();
            let bytes = input
                .by_ref()
                .take(8 * 1024 * 1024 + 1)
                .read_line(&mut line)?;
            if bytes == 0 {
                break;
            }
            line_number += 1;
            if bytes > 8 * 1024 * 1024 {
                return Err(format!("line {line_number} exceeds 8 MiB").into());
            }
            if line.trim().is_empty() {
                continue;
            }
            let row: serde_json::Value = serde_json::from_str(&line)
                .map_err(|error| format!("line {line_number}: {error}"))?;
            stream.send(&row).await?;
        }
        Ok(())
    }
    .await;
    // Settle the accepted prefix even if parsing or admission failed midway.
    let settled = stream.shutdown().await;
    println!("delivery: {:?}", stream.stats());
    settled?;
    admitted?;
    Ok(())
}
