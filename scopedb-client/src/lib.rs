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

#![doc = include_str!("../README.md")]

#[doc = include_str!("../docs/migration-0.4.md")]
pub mod migration {}

mod append;
mod append_stream;
mod append_writer;
mod catalog;
mod client;
mod error;
mod protocol;
mod result;
mod statement;
mod table;
#[cfg(test)]
mod test_support;

pub use append::AppendDeliveryOutcome;
pub use append::AppendFailurePolicy;
pub use append_stream::AppendAdmissionError;
pub use append_stream::AppendAdmissionResult;
pub use append_stream::AppendBatchFailureAction;
pub use append_stream::AppendBatchFailureEvent;
pub use append_stream::AppendCircuitBreakerOptions;
pub use append_stream::AppendCircuitState;
pub use append_stream::AppendDeliveryReport;
pub use append_stream::AppendDroppedRows;
pub use append_stream::AppendLastFailure;
pub use append_stream::AppendStream;
pub use append_stream::AppendStreamBuilder;
pub use append_stream::AppendStreamState;
pub use append_stream::AppendStreamStats;
pub use append_stream::AppendTrySendError;
pub use append_writer::AppendRequestResult;
pub use append_writer::AppendWriteError;
pub use append_writer::AppendWriteReport;
pub use append_writer::AppendWriter;
pub use append_writer::AppendWriterBuilder;
pub use catalog::Catalog;
pub use catalog::CatalogColumn;
pub use catalog::CatalogColumnIndex;
pub use catalog::CatalogDatabase;
pub use catalog::CatalogDistinct;
pub use catalog::CatalogDocument;
pub use catalog::CatalogOptions;
pub use catalog::CatalogResource;
pub use catalog::CatalogResult;
pub use catalog::CatalogSchema;
pub use catalog::CatalogSnapshot;
pub use catalog::CatalogTable;
pub use catalog::CatalogView;
pub use client::Client;
pub use client::ClientBuilder;
pub use error::Error;
pub use error::ErrorKind;
pub use error::WriteErrorDetails;
pub use error::WriteErrorReason;
pub use protocol::AppendErrorDetails;
pub use protocol::AppendRowError;
pub use protocol::AppendRowsResult;
pub use protocol::AppendState;
pub use protocol::DataType;
pub use protocol::StatementErrorDetails;
pub use protocol::StatementEstimatedProgress;
pub use protocol::StatementProgress;
pub use protocol::StatementStatus;
pub use protocol::StatementStatusCancelled;
pub use protocol::StatementStatusFailed;
pub use protocol::StatementStatusFinished;
pub use protocol::StatementStatusPending;
pub use protocol::StatementStatusRunning;
/// The HTTP client version used by this SDK.
pub use reqwest;
pub use result::FieldSchema;
pub use result::ResultObject;
pub use result::ResultSet;
pub use result::Schema;
pub use result::Value;
pub use statement::Statement;
pub use statement::StatementHandle;
pub use table::Table;
