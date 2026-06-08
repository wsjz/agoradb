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
use iceberg::Catalog;
use std::sync::Arc;

use crate::sql_parser::{AgoraSQLParser, AgoraStatement};

pub struct AgoraSessionContext {
    df_ctx: SessionContext,
    catalog_provider: Arc<AgoraCatalogProvider>,
    sql_parser: AgoraSQLParser,
}

impl AgoraSessionContext {
    pub fn new(catalog_provider: Arc<AgoraCatalogProvider>) -> Self {
        let df_ctx = SessionContext::new();
        let provider_clone = Arc::clone(&catalog_provider);
        df_ctx.register_catalog("agora", provider_clone);
        Self {
            df_ctx,
            catalog_provider,
            sql_parser: AgoraSQLParser::new(),
        }
    }

    pub async fn sql(
        &self,
        sql: &str,
    ) -> Result<DataFrame, agoradb_core::ExecutionError> {
        let statements = self.sql_parser.parse(sql)
            .map_err(|e| agoradb_core::ExecutionError::OperatorError(
                format!("Parse error: {e}")))?;

        if statements.len() != 1 {
            return Err(agoradb_core::ExecutionError::OperatorError(
                "Only single statements supported".to_string()));
        }

        match &statements[0] {
            AgoraStatement::CreateSpace { name, properties: _ } => {
                // Create namespace via catalog
                self.create_space(name).await?;
                // Return empty DataFrame for DDL
                self.df_ctx.sql("SELECT 1 WHERE FALSE").await
                    .map_err(|e| agoradb_core::ExecutionError::OperatorError(e.to_string()))
            }
            AgoraStatement::Sql(_df_statements) => {
                // TODO: Convert sqlparser Statement to DataFusion Statement
                // For now, delegate directly to DataFusion's SQL parser
                self.df_ctx.sql(sql).await
                    .map_err(|e| agoradb_core::ExecutionError::OperatorError(e.to_string()))
            }
        }
    }

    async fn create_space(
        &self,
        name: &str,
    ) -> Result<(), agoradb_core::ExecutionError> {
        let catalog = self.catalog_provider.inner_catalog();
        let ns = iceberg::NamespaceIdent::new(name.to_string());
        catalog.create_namespace(&ns, std::collections::HashMap::new())
            .await
            .map_err(|e| agoradb_core::ExecutionError::OperatorError(
                format!("Failed to create space: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agoradb_catalog::AgoraCatalog;
    use iceberg::io::FileIO;

    #[tokio::test]
    async fn test_create_space_execution() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_io = FileIO::new_with_fs();
        let catalog = Arc::new(AgoraCatalog::new(file_io, temp_dir.path().to_str().unwrap()));
        let provider = Arc::new(AgoraCatalogProvider::new(catalog));
        let ctx = AgoraSessionContext::new(provider);

        let result = ctx.sql("CREATE SPACE testspace WITH STORAGE = 'disk'").await;
        assert!(result.is_ok(), "CREATE SPACE should succeed: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_session_standard_sql_delegate() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_io = FileIO::new_with_fs();
        let catalog = Arc::new(AgoraCatalog::new(file_io, temp_dir.path().to_str().unwrap()));

        // Create namespace and table
        let ns = iceberg::NamespaceIdent::new("default".to_string());
        catalog.create_namespace(&ns, std::collections::HashMap::new()).await.unwrap();

        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                iceberg::spec::NestedField::required(1, "id", iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long)).into(),
                iceberg::spec::NestedField::required(2, "name", iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let creation = iceberg::TableCreation::builder()
            .name("users".to_string())
            .schema(schema)
            .build();
        catalog.create_table(&ns, creation).await.unwrap();

        // Write data via StorageEngine
        let arrow_schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
        ]));
        let mut engine = agoradb_storage::StorageEngine::new(
            catalog.clone(),
            arrow_schema.clone(),
            temp_dir.path().to_path_buf(),
            "users".to_string(),
        );
        let batch = arrow_array::RecordBatch::try_new(
            arrow_schema,
            vec![
                std::sync::Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef,
                std::sync::Arc::new(arrow_array::StringArray::from(vec!["alice", "bob", "charlie"])) as arrow_array::ArrayRef,
            ],
        ).unwrap();
        engine.append(batch).await.unwrap();
        engine.flush().await.unwrap();

        // Query via AgoraSessionContext (standard SQL delegated to DataFusion)
        let provider = Arc::new(AgoraCatalogProvider::new(catalog));
        let ctx = AgoraSessionContext::new(provider);

        let df = ctx.sql("SELECT id, name FROM agora.default.users ORDER BY id").await;
        assert!(df.is_ok(), "Standard SQL should succeed: {:?}", df.err());

        let batches = df.unwrap().collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "Should return 3 rows");

        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_session_multiple_statements_error() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_io = FileIO::new_with_fs();
        let catalog = Arc::new(AgoraCatalog::new(file_io, temp_dir.path().to_str().unwrap()));
        let provider = Arc::new(AgoraCatalogProvider::new(catalog));
        let ctx = AgoraSessionContext::new(provider);

        let result = ctx.sql("SELECT 1; SELECT 2").await;
        assert!(result.is_err(), "Multiple statements should be rejected");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Only single statements supported"), "Error should mention single statements: {}", err);
    }
}
