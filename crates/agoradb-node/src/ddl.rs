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

//! Table DDL for analytical Spaces: SQL column definitions → Iceberg schema.

use std::sync::Arc;

use agoradb_catalog::{AgoraCatalog, Space};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::{Catalog, TableCreation, TableIdent};
use sqlparser::ast::{
    ColumnDef, ColumnOption, CreateTable, DataType as SqlType, ExactNumberInfo, ObjectName,
    ObjectNamePart,
};

use crate::error::SessionError;

/// Map a SQL column type to the Arrow type the table is stored with.
pub fn sql_type_to_arrow(ty: &SqlType) -> Result<DataType, SessionError> {
    Ok(match ty {
        SqlType::TinyInt(_) => DataType::Int8,
        SqlType::SmallInt(_) | SqlType::Int16 => DataType::Int16,
        SqlType::Int(_) | SqlType::Integer(_) | SqlType::Int32 => DataType::Int32,
        SqlType::BigInt(_) | SqlType::Int64 => DataType::Int64,
        SqlType::Real | SqlType::Float32 | SqlType::Float4 => DataType::Float32,
        SqlType::Float(info) => match info {
            ExactNumberInfo::Precision(p) if *p <= 24 => DataType::Float32,
            _ => DataType::Float64,
        },
        SqlType::Double(_) | SqlType::DoublePrecision | SqlType::Float64 | SqlType::Float8 => {
            DataType::Float64
        }
        SqlType::Varchar(_)
        | SqlType::Text
        | SqlType::String(_)
        | SqlType::Char(_)
        | SqlType::Character(_)
        | SqlType::CharacterVarying(_)
        | SqlType::CharVarying(_)
        | SqlType::Nvarchar(_) => DataType::Utf8,
        SqlType::Boolean | SqlType::Bool => DataType::Boolean,
        SqlType::Date | SqlType::Date32 => DataType::Date32,
        SqlType::Timestamp(_, _) | SqlType::Datetime(_) => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        SqlType::Decimal(info) | SqlType::Numeric(info) | SqlType::BigDecimal(info) => match info {
            ExactNumberInfo::PrecisionAndScale(p, s) => DataType::Decimal128(*p as u8, *s as i8),
            ExactNumberInfo::Precision(p) => DataType::Decimal128(*p as u8, 0),
            ExactNumberInfo::None => DataType::Decimal128(38, 10),
        },
        SqlType::Blob(_) | SqlType::Bytea | SqlType::Binary(_) | SqlType::Varbinary(_) => {
            DataType::Binary
        }
        other => {
            return Err(SessionError::Unsupported(format!(
                "column type {other} in CREATE TABLE for an analytical space"
            )))
        }
    })
}

fn is_not_null(column: &ColumnDef) -> bool {
    column.options.iter().any(|opt| {
        matches!(
            opt.option,
            ColumnOption::NotNull | ColumnOption::PrimaryKey(_)
        )
    })
}

/// The Arrow schema described by a `CREATE TABLE` statement.
pub fn arrow_schema_from_create(create: &CreateTable) -> Result<Schema, SessionError> {
    if create.columns.is_empty() {
        return Err(SessionError::Unsupported(
            "CREATE TABLE without columns".to_string(),
        ));
    }
    let fields = create
        .columns
        .iter()
        .map(|c| {
            Ok(Field::new(
                c.name.value.clone(),
                sql_type_to_arrow(&c.data_type)?,
                !is_not_null(c),
            ))
        })
        .collect::<Result<Vec<_>, SessionError>>()?;
    Ok(Schema::new(fields))
}

/// Last part of a (qualified) object name.
pub fn table_name(name: &ObjectName) -> String {
    match name.0.last() {
        Some(ObjectNamePart::Identifier(ident)) => ident.value.clone(),
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

/// Create an Iceberg table in an analytical Space.
pub async fn create_analytical_table(
    catalog: &AgoraCatalog,
    space: &Space,
    create: &CreateTable,
) -> Result<(), SessionError> {
    let ns = catalog.space_namespace(space)?;
    let name = table_name(&create.name);
    let ident = TableIdent::new(ns.clone(), name.clone());
    if catalog
        .table_exists(&ident)
        .await
        .map_err(|e| SessionError::Iceberg(e.to_string()))?
    {
        if create.if_not_exists {
            return Ok(());
        }
        return Err(SessionError::Iceberg(format!(
            "table {}.{} already exists",
            space.name, name
        )));
    }
    let arrow = Arc::new(arrow_schema_from_create(create)?);
    let schema = arrow_schema_to_schema_auto_assign_ids(&arrow)
        .map_err(|e| SessionError::Iceberg(e.to_string()))?;
    catalog
        .create_table(
            &ns,
            TableCreation::builder().name(name).schema(schema).build(),
        )
        .await
        .map_err(|e| SessionError::Iceberg(e.to_string()))?;
    Ok(())
}

/// Drop tables from an analytical Space.
pub async fn drop_analytical_tables(
    catalog: &AgoraCatalog,
    space: &Space,
    names: &[ObjectName],
    if_exists: bool,
) -> Result<(), SessionError> {
    let ns = catalog.space_namespace(space)?;
    for name in names {
        let ident = TableIdent::new(ns.clone(), table_name(name));
        let exists = catalog
            .table_exists(&ident)
            .await
            .map_err(|e| SessionError::Iceberg(e.to_string()))?;
        if !exists {
            if if_exists {
                continue;
            }
            return Err(SessionError::Catalog(
                agoradb_core::CatalogError::TableNotFound(format!(
                    "{}.{}",
                    space.name,
                    ident.name()
                )),
            ));
        }
        catalog
            .drop_table(&ident)
            .await
            .map_err(|e| SessionError::Iceberg(e.to_string()))?;
    }
    Ok(())
}
