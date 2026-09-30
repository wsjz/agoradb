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

use std::sync::Arc;

use agoradb_catalog::AgoraCatalog;
use agoradb_storage::StorageEngine;
use arrow_array::{
    ArrayRef, Date32Array, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use iceberg::io::FileIO;
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};

fn tpch_region_schema() -> Schema {
    Schema::new(vec![
        Field::new("r_regionkey", DataType::Int64, false),
        Field::new("r_name", DataType::Utf8, false),
        Field::new("r_comment", DataType::Utf8, false),
    ])
}

fn tpch_nation_schema() -> Schema {
    Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, false),
        Field::new("n_regionkey", DataType::Int64, false),
        Field::new("n_comment", DataType::Utf8, false),
    ])
}

fn tpch_customer_schema() -> Schema {
    Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int64, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
        Field::new("c_mktsegment", DataType::Utf8, false),
        Field::new("c_comment", DataType::Utf8, false),
    ])
}

fn tpch_orders_schema() -> Schema {
    Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
        Field::new("o_clerk", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8, false),
    ])
}

fn tpch_lineitem_schema() -> Schema {
    Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Decimal128(15, 2), false),
        Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
        Field::new("l_discount", DataType::Decimal128(15, 2), false),
        Field::new("l_tax", DataType::Decimal128(15, 2), false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_comment", DataType::Utf8, false),
    ])
}

/// Convert an Arrow [`DataType`] to an Iceberg [`PrimitiveType`].
fn arrow_to_iceberg_type(data_type: &DataType) -> PrimitiveType {
    match data_type {
        DataType::Int64 => PrimitiveType::Long,
        DataType::Int32 => PrimitiveType::Int,
        DataType::Utf8 => PrimitiveType::String,
        DataType::Date32 => PrimitiveType::Date,
        DataType::Decimal128(precision, scale) => PrimitiveType::Decimal {
            precision: *precision as u32,
            scale: *scale as u32,
        },
        _ => panic!("Unsupported Arrow type for Iceberg conversion: {data_type:?}"),
    }
}

/// Convert an Arrow [`Schema`] to an Iceberg [`IcebergSchema`].
fn arrow_to_iceberg_schema(arrow_schema: &Schema) -> IcebergSchema {
    let fields: Vec<iceberg::spec::NestedFieldRef> = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let primitive = arrow_to_iceberg_type(field.data_type());
            NestedField::required((idx + 1) as i32, field.name(), Type::Primitive(primitive)).into()
        })
        .collect();

    IcebergSchema::builder()
        .with_fields(fields)
        .build()
        .unwrap()
}

/// Helper: create a table, write a RecordBatch via StorageEngine, read back via TableScan, and
/// return the row count.
async fn create_and_load_table(
    catalog: Arc<AgoraCatalog>,
    file_io: FileIO,
    namespace: &NamespaceIdent,
    name: &str,
    arrow_schema: Arc<Schema>,
    batch: RecordBatch,
    temp_dir: &std::path::Path,
) -> usize {
    // 1. Convert Arrow schema to Iceberg schema and create table.
    let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema);
    let table_creation = TableCreation::builder()
        .name(name.to_string())
        .schema(iceberg_schema)
        .build();
    catalog
        .create_table(namespace, table_creation)
        .await
        .unwrap();

    // 2. Write batch via StorageEngine.
    let table_ident = TableIdent::new(namespace.clone(), name.to_string());
    let mut engine = StorageEngine::new_with_catalog(
        catalog.clone(),
        file_io.clone(),
        table_ident.clone(),
        arrow_schema,
        temp_dir.to_path_buf(),
    );
    engine.append(batch).await.unwrap();
    engine.flush().await.unwrap();

    // 3. Read back via TableScan, collecting all batches.
    let table = catalog.load_table(&table_ident).await.unwrap();
    let scan = table.scan().build().unwrap();
    let mut stream = scan.to_arrow().await.unwrap();

    let mut total_rows = 0;
    while let Some(result) = stream.next().await {
        let read_batch = result.unwrap();
        total_rows += read_batch.num_rows();
    }

    total_rows
}

#[tokio::test]
async fn test_tpch_region_table() {
    let temp_dir = tempfile::tempdir().unwrap();
    let root_path = temp_dir.path().to_str().unwrap().to_string();
    let file_io = iceberg::io::FileIO::new_with_fs();
    let catalog = Arc::new(AgoraCatalog::new(file_io.clone(), &root_path));

    let namespace = NamespaceIdent::new("default".to_string());
    catalog
        .create_namespace(&namespace, std::collections::HashMap::new())
        .await
        .unwrap();

    let arrow_schema = Arc::new(tpch_region_schema());
    let count = 5;

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from((1..=count as i64).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(vec!["region name"; count])) as ArrayRef,
            Arc::new(StringArray::from(vec!["region comment"; count])) as ArrayRef,
        ],
    )
    .unwrap();

    let rows = create_and_load_table(
        catalog,
        file_io.clone(),
        &namespace,
        "region",
        arrow_schema,
        batch,
        temp_dir.path(),
    )
    .await;
    assert_eq!(rows, count);
}

#[tokio::test]
async fn test_tpch_nation_table() {
    let temp_dir = tempfile::tempdir().unwrap();
    let root_path = temp_dir.path().to_str().unwrap().to_string();
    let file_io = iceberg::io::FileIO::new_with_fs();
    let catalog = Arc::new(AgoraCatalog::new(file_io.clone(), &root_path));

    let namespace = NamespaceIdent::new("default".to_string());
    catalog
        .create_namespace(&namespace, std::collections::HashMap::new())
        .await
        .unwrap();

    let arrow_schema = Arc::new(tpch_nation_schema());
    let count = 25;

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from((1..=count as i64).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(vec!["nation name"; count])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_i64; count])) as ArrayRef,
            Arc::new(StringArray::from(vec!["nation comment"; count])) as ArrayRef,
        ],
    )
    .unwrap();

    let rows = create_and_load_table(
        catalog,
        file_io.clone(),
        &namespace,
        "nation",
        arrow_schema,
        batch,
        temp_dir.path(),
    )
    .await;
    assert_eq!(rows, count);
}

#[tokio::test]
async fn test_tpch_customer_table() {
    let temp_dir = tempfile::tempdir().unwrap();
    let root_path = temp_dir.path().to_str().unwrap().to_string();
    let file_io = iceberg::io::FileIO::new_with_fs();
    let catalog = Arc::new(AgoraCatalog::new(file_io.clone(), &root_path));

    let namespace = NamespaceIdent::new("default".to_string());
    catalog
        .create_namespace(&namespace, std::collections::HashMap::new())
        .await
        .unwrap();

    let arrow_schema = Arc::new(tpch_customer_schema());
    let count = 1500;

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from((1..=count as i64).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(vec!["customer name"; count])) as ArrayRef,
            Arc::new(StringArray::from(vec!["customer address"; count])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_i64; count])) as ArrayRef,
            Arc::new(StringArray::from(vec!["customer phone"; count])) as ArrayRef,
            Arc::new(
                Decimal128Array::from(vec![10000_i128; count])
                    .with_precision_and_scale(15, 2)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(StringArray::from(vec!["BUILDING"; count])) as ArrayRef,
            Arc::new(StringArray::from(vec!["customer comment"; count])) as ArrayRef,
        ],
    )
    .unwrap();

    let rows = create_and_load_table(
        catalog,
        file_io.clone(),
        &namespace,
        "customer",
        arrow_schema,
        batch,
        temp_dir.path(),
    )
    .await;
    assert_eq!(rows, count);
}

#[tokio::test]
async fn test_tpch_orders_table() {
    let temp_dir = tempfile::tempdir().unwrap();
    let root_path = temp_dir.path().to_str().unwrap().to_string();
    let file_io = iceberg::io::FileIO::new_with_fs();
    let catalog = Arc::new(AgoraCatalog::new(file_io.clone(), &root_path));

    let namespace = NamespaceIdent::new("default".to_string());
    catalog
        .create_namespace(&namespace, std::collections::HashMap::new())
        .await
        .unwrap();

    let arrow_schema = Arc::new(tpch_orders_schema());
    let count = 15000;
    // 1995-01-01 = 9131 days since epoch.
    let order_date = 9131_i32;

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from((1..=count as i64).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_i64; count])) as ArrayRef,
            Arc::new(StringArray::from(vec!["O"; count])) as ArrayRef,
            Arc::new(
                Decimal128Array::from(vec![150000_i128; count])
                    .with_precision_and_scale(15, 2)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(Date32Array::from(vec![order_date; count])) as ArrayRef,
            Arc::new(StringArray::from(vec!["5-LOW"; count])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Clerk#000000001"; count])) as ArrayRef,
            Arc::new(Int32Array::from(vec![0_i32; count])) as ArrayRef,
            Arc::new(StringArray::from(vec!["orders comment"; count])) as ArrayRef,
        ],
    )
    .unwrap();

    let rows = create_and_load_table(
        catalog,
        file_io.clone(),
        &namespace,
        "orders",
        arrow_schema,
        batch,
        temp_dir.path(),
    )
    .await;
    assert_eq!(rows, count);
}

#[tokio::test]
async fn test_tpch_lineitem_table() {
    let temp_dir = tempfile::tempdir().unwrap();
    let root_path = temp_dir.path().to_str().unwrap().to_string();
    let file_io = iceberg::io::FileIO::new_with_fs();
    let catalog = Arc::new(AgoraCatalog::new(file_io.clone(), &root_path));

    let namespace = NamespaceIdent::new("default".to_string());
    catalog
        .create_namespace(&namespace, std::collections::HashMap::new())
        .await
        .unwrap();

    let arrow_schema = Arc::new(tpch_lineitem_schema());
    let count = 60000;
    // 1995-01-01 = 9131 days since epoch.
    let ship_date = 9131_i32;
    let commit_date = 9141_i32;
    let receipt_date = 9151_i32;

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64; count])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_i64; count])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_i64; count])) as ArrayRef,
            Arc::new(Int32Array::from(vec![1_i32; count])) as ArrayRef,
            Arc::new(
                Decimal128Array::from(vec![10_i128; count])
                    .with_precision_and_scale(15, 2)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(
                Decimal128Array::from(vec![10000_i128; count])
                    .with_precision_and_scale(15, 2)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(
                Decimal128Array::from(vec![5_i128; count])
                    .with_precision_and_scale(15, 2)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(
                Decimal128Array::from(vec![2_i128; count])
                    .with_precision_and_scale(15, 2)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(StringArray::from(vec!["N"; count])) as ArrayRef,
            Arc::new(StringArray::from(vec!["O"; count])) as ArrayRef,
            Arc::new(Date32Array::from(vec![ship_date; count])) as ArrayRef,
            Arc::new(Date32Array::from(vec![commit_date; count])) as ArrayRef,
            Arc::new(Date32Array::from(vec![receipt_date; count])) as ArrayRef,
            Arc::new(StringArray::from(vec!["DELIVER IN PERSON"; count])) as ArrayRef,
            Arc::new(StringArray::from(vec!["SHIP"; count])) as ArrayRef,
            Arc::new(StringArray::from(vec!["lineitem comment"; count])) as ArrayRef,
        ],
    )
    .unwrap();

    let rows = create_and_load_table(
        catalog,
        file_io.clone(),
        &namespace,
        "lineitem",
        arrow_schema,
        batch,
        temp_dir.path(),
    )
    .await;
    assert_eq!(rows, count);
}
