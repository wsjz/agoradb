// Copyright 2025 The Oikome Authors
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

//! Lance Spaces (architecture v3 §7.6): where a table's dataset lives, and
//! the index statements of DuckDB's `lance` extension, which name the
//! dataset by path rather than by table.

use std::path::{Path, PathBuf};

use oikome_catalog::{LocationFormat, OikomeCatalog, Space};
use oikome_engine::name::quote_literal;
use oikome_engine::EngineError;
use sqlparser::ast::{CreateIndex, Expr, Ident, ObjectName, Statement};

use crate::error::SessionError;
use crate::subscribe::replica_lance_root;

/// Index types of the `lance` extension: vector, scalar and full-text.
const INDEX_TYPES: [&str; 14] = [
    "IVF_FLAT",
    "IVF_PQ",
    "IVF_SQ",
    "IVF_RQ",
    "IVF_HNSW_FLAT",
    "IVF_HNSW_PQ",
    "IVF_HNSW_SQ",
    "BTREE",
    "BITMAP",
    "ZONEMAP",
    "BLOOMFILTER",
    "INVERTED",
    "NGRAM",
    "LABEL_LIST",
];

/// The directory of `space`'s datasets, if it is stored as Lance: its own
/// Location, or a subscribed copy of a Lance Space.
pub(crate) fn lance_root(
    catalog: &OikomeCatalog,
    space: &Space,
) -> Result<Option<PathBuf>, SessionError> {
    let location = catalog.get_location(&space.location)?;
    Ok(match &location.format {
        LocationFormat::Lance { .. } => Some(catalog.lance_root(&location)?),
        LocationFormat::Replica { .. } => replica_lance_root(catalog, &location)?,
        _ => None,
    })
}

/// The dataset of `space.table`, which must exist.
pub(crate) fn dataset_path(root: &Path, space: &str, table: &str) -> Result<PathBuf, SessionError> {
    let path = root.join(format!("{table}.lance"));
    if path.is_dir() {
        Ok(path)
    } else {
        Err(EngineError::TableNotFound(format!("{space}.{table}")).into())
    }
}

/// The extension's parser takes index and column names unquoted, so only
/// plain identifiers are accepted.
fn plain(ident: &Ident) -> Option<&str> {
    let name = ident.value.as_str();
    let mut chars = name.chars();
    let first = chars.next()?;
    ((first.is_ascii_alphabetic() || first == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_'))
    .then_some(name)
}

fn single_ident(name: &ObjectName) -> Option<&str> {
    match name.0.as_slice() {
        [part] => part.as_ident().and_then(plain),
        _ => None,
    }
}

fn unsupported_index(what: &str) -> SessionError {
    SessionError::Unsupported(format!(
        "{what}; a Lance index is CREATE INDEX <name> ON <space>.<table> USING <type> (<column>) \
         [WITH (<option> = <value>, ...)]"
    ))
}

/// `CREATE INDEX` / `DROP INDEX` on the dataset at `dataset`, in the
/// extension's syntax. Only the parts the extension takes are accepted.
pub(crate) fn index_sql(stmt: &Statement, dataset: &Path) -> Result<String, SessionError> {
    let path = quote_literal(&dataset.to_string_lossy());
    match stmt {
        Statement::CreateIndex(create) => create_index_sql(create, &path),
        Statement::Drop {
            names,
            if_exists: false,
            ..
        } => match names.as_slice() {
            [name] => {
                let name = single_ident(name).ok_or_else(|| {
                    unsupported_index("an index name that is not a plain identifier")
                })?;
                Ok(format!("DROP INDEX {name} ON {path}"))
            }
            _ => Err(unsupported_index("dropping several indexes at once")),
        },
        Statement::Drop { .. } => Err(unsupported_index("DROP INDEX IF EXISTS")),
        _ => Err(unsupported_index("this statement")),
    }
}

fn create_index_sql(create: &CreateIndex, path: &str) -> Result<String, SessionError> {
    if create.unique
        || create.concurrently
        || create.if_not_exists
        || !create.include.is_empty()
        || create.nulls_distinct.is_some()
        || create.predicate.is_some()
        || !create.index_options.is_empty()
        || !create.alter_options.is_empty()
    {
        return Err(unsupported_index(
            "UNIQUE, CONCURRENTLY, IF NOT EXISTS, INCLUDE or WHERE",
        ));
    }
    let name = create
        .name
        .as_ref()
        .and_then(single_ident)
        .ok_or_else(|| unsupported_index("an index name that is not a plain identifier"))?;
    let using = create
        .using
        .as_ref()
        .map(|t| t.to_string().to_ascii_uppercase())
        .ok_or_else(|| unsupported_index("an index needs USING <type>"))?;
    if !INDEX_TYPES.contains(&using.as_str()) {
        return Err(SessionError::Unsupported(format!(
            "index type {using} (expected one of {})",
            INDEX_TYPES.join(", ")
        )));
    }
    let column = match create.columns.as_slice() {
        [column]
            if column.operator_class.is_none()
                && column.column.options.asc.is_none()
                && column.column.options.nulls_first.is_none() =>
        {
            match &column.column.expr {
                Expr::Identifier(ident) => plain(ident).ok_or_else(|| {
                    unsupported_index("a column name that is not a plain identifier")
                })?,
                _ => return Err(unsupported_index("an index covers one column, by name")),
            }
        }
        _ => return Err(unsupported_index("an index covers exactly one column")),
    };
    let with = if create.with.is_empty() {
        String::new()
    } else {
        let options = create
            .with
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        format!(" WITH ({options})")
    };
    Ok(format!(
        "CREATE INDEX {name} ON {path} ({column}) USING {using}{with}"
    ))
}

#[cfg(test)]
mod tests {
    use oikome_semantic::{parse_single, OikomeStatement};

    use super::*;

    fn index(sql: &str) -> Result<String, SessionError> {
        let OikomeStatement::Sql(stmt) = parse_single(sql).unwrap() else {
            panic!("{sql}");
        };
        index_sql(&stmt, Path::new("/data/notes/items.lance"))
    }

    #[test]
    fn index_statements_name_the_dataset() {
        assert_eq!(
            index("CREATE INDEX emb ON notes.items USING ivf_flat (embedding) WITH (num_partitions = 2, metric_type = 'cosine')")
                .unwrap(),
            "CREATE INDEX emb ON '/data/notes/items.lance' (embedding) USING IVF_FLAT \
             WITH (num_partitions = 2, metric_type = 'cosine')"
        );
        assert_eq!(
            index("CREATE INDEX body ON notes.items USING INVERTED (body)").unwrap(),
            "CREATE INDEX body ON '/data/notes/items.lance' (body) USING INVERTED"
        );
        assert_eq!(
            index("DROP INDEX emb ON notes.items").unwrap(),
            "DROP INDEX emb ON '/data/notes/items.lance'"
        );
        for sql in [
            "CREATE INDEX emb ON notes.items (embedding)",
            "CREATE INDEX emb ON notes.items USING HASH (embedding)",
            "CREATE INDEX emb ON notes.items USING IVF_FLAT (a, b)",
            "CREATE UNIQUE INDEX emb ON notes.items USING BTREE (id)",
            "CREATE INDEX emb ON notes.items USING BTREE (id DESC)",
            "DROP INDEX IF EXISTS emb ON notes.items",
            "CREATE INDEX \"my index\" ON notes.items USING BTREE (id)",
        ] {
            assert!(
                matches!(index(sql), Err(SessionError::Unsupported(_))),
                "{sql}"
            );
        }
    }
}
