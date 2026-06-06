// Copyright 2025 The AgoraDB Authors
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

use agoradb_catalog::AgoraCatalogProvider;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::DataFrame;
use std::sync::Arc;

pub struct AgoraSessionContext {
    df_ctx: SessionContext,
}

impl AgoraSessionContext {
    pub fn new(catalog_provider: Arc<AgoraCatalogProvider>) -> Self {
        let df_ctx = SessionContext::new();
        df_ctx.register_catalog("agora", catalog_provider);
        Self { df_ctx }
    }

    pub async fn sql(
        &self,
        sql: &str,
    ) -> Result<DataFrame, agoradb_core::ExecutionError> {
        self.df_ctx
            .sql(sql)
            .await
            .map_err(|e| agoradb_core::ExecutionError::OperatorError(e.to_string()))
    }
}
