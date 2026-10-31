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

use scopedb_client::Client;
use scopedb_client::Value;

#[tokio::test]
#[ignore = "set SCOPEDB_ENDPOINT and SCOPEDB_API_KEY to run against ScopeDB"]
async fn query_live_service() {
    let client = Client::builder(std::env::var("SCOPEDB_ENDPOINT").unwrap())
        .api_key(std::env::var("SCOPEDB_API_KEY").unwrap())
        .build()
        .unwrap();
    let rows = client
        .query("SELECT 1 AS value")
        .await
        .unwrap()
        .into_objects()
        .unwrap();
    assert_eq!(rows[0]["value"], Value::Int(1));
}
