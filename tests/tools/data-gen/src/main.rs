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

//! Deterministic TPC-H test data generator for AgoraDB.
//!
//! Produces bit-identical Parquet files and Iceberg metadata for
//! local integration tests. Run before executing tests that depend
//! on pre-generated data:
//!
//! ```bash
//! cargo run -p test-data-gen --bin generate-test-data
//! ```

use agoradb_catalog::AgoraCatalog;
use agoradb_storage::StorageEngine;
use arrow::array::{Date32Array, Decimal128Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use clap::Parser;
use iceberg::io::FileIO;
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const BATCH_SIZE: usize = 1024;

#[derive(Parser, Debug)]
#[command(name = "generate-test-data")]
#[command(about = "Generate deterministic TPC-H test data for AgoraDB integration tests")]
struct Args {
    /// Scale factor (0.001, 0.01, 0.1, 1.0)
    #[arg(short, long, default_value = "0.1")]
    scale_factor: f64,

    /// Output directory for generated data
    #[arg(short, long, default_value = "tests/agora-local")]
    output_dir: PathBuf,

    /// Force regeneration even if output directory exists
    #[arg(long)]
    force: bool,
}

fn main() {
    let args = Args::parse();
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    rt.block_on(async { generate(args).await });
}

async fn generate(args: Args) {
    let output_dir = if args.output_dir.is_absolute() {
        args.output_dir.clone()
    } else {
        let cwd = std::env::current_dir().expect("Failed to get current dir");
        cwd.join(&args.output_dir)
    };

    let default_ns_dir = output_dir.join("default");
    if default_ns_dir.exists() {
        if args.force {
            eprintln!("Removing existing data at {}", default_ns_dir.display());
            let _ = std::fs::remove_dir_all(&default_ns_dir);
        } else {
            eprintln!(
                "Output directory already exists: {}. Use --force to overwrite.",
                default_ns_dir.display()
            );
            std::process::exit(1);
        }
    }

    let sf = args.scale_factor;
    let region_count = 5usize;
    let nation_count = 25usize;
    let customer_count = (sf * 15000.0).round() as usize;
    let orders_count = (sf * 150000.0).round() as usize;
    let supplier_count = (sf * 10000.0).round() as usize;
    let part_count = (sf * 200000.0).round() as usize;
    let partsupp_count = part_count * 4;

    eprintln!("Generating TPC-H test data (SF={}):", sf);
    eprintln!("  region:     {}", region_count);
    eprintln!("  nation:     {}", nation_count);
    eprintln!("  customer:   {}", customer_count);
    eprintln!("  orders:     {}", orders_count);
    eprintln!("  supplier:   {}", supplier_count);
    eprintln!("  part:       {}", part_count);
    eprintln!("  partsupp:   {}", partsupp_count);

    // Set up catalog
    let root_path = output_dir.to_str().unwrap().to_string();
    let file_io = FileIO::new_with_fs();
    let catalog = Arc::new(AgoraCatalog::new(file_io.clone(), &root_path));

    catalog
        .create_namespace(&NamespaceIdent::new("default".to_string()), HashMap::new())
        .await
        .expect("Failed to create namespace");

    // Create tables and generate data
    generate_region(&catalog, file_io.clone(), &root_path, &output_dir, region_count).await;
    generate_nation(&catalog, file_io.clone(), &root_path, &output_dir, nation_count).await;
    generate_customer(&catalog, file_io.clone(), &root_path, &output_dir, customer_count).await;
    generate_orders(&catalog, file_io.clone(), &root_path, &output_dir, orders_count, customer_count).await;
    generate_lineitem(&catalog, file_io.clone(), &root_path, &output_dir, orders_count, sf).await;
    generate_supplier(&catalog, file_io.clone(), &root_path, &output_dir, supplier_count).await;
    generate_part(&catalog, file_io.clone(), &root_path, &output_dir, part_count).await;
    generate_partsupp(&catalog, file_io.clone(), &root_path, &output_dir, part_count, supplier_count).await;

    eprintln!("Done. Data written to {}", output_dir.display());
}

// ============================================================================
// Helpers
// ============================================================================

fn seed_rng(table_name: &str) -> StdRng {
    let seed = match table_name {
        "region" => 1u64,
        "nation" => 2u64,
        "customer" => 3u64,
        "orders" => 4u64,
        "lineitem" => 5u64,
        "supplier" => 6u64,
        "part" => 7u64,
        "partsupp" => 8u64,
        _ => 42u64,
    };
    StdRng::seed_from_u64(seed)
}

async fn create_table(
    catalog: &Arc<AgoraCatalog>,
    file_io: FileIO,
    root_path: &str,
    output_dir: &Path,
    name: &str,
    arrow_schema: SchemaRef,
) -> StorageEngine {
    // Build Iceberg schema from Arrow schema
    let fields: Vec<iceberg::spec::NestedFieldRef> = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let primitive = arrow_to_iceberg_type(field.data_type());
            iceberg::spec::NestedField::required(
                (idx + 1) as i32,
                field.name(),
                iceberg::spec::Type::Primitive(primitive),
            )
            .into()
        })
        .collect();

    let iceberg_schema = iceberg::spec::Schema::builder()
        .with_fields(fields)
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name(name.to_string())
        .schema(iceberg_schema)
        .build();

    catalog
        .create_table(&NamespaceIdent::new("default".to_string()), table_creation)
        .await
        .unwrap_or_else(|_| panic!("Failed to create table {}", name));

    let temp_dir = output_dir.join(".tmp");
    let _ = std::fs::create_dir_all(&temp_dir);

    StorageEngine::new_with_catalog(
        catalog.clone(),
        file_io,
        root_path,
        arrow_schema,
        temp_dir,
        name.to_string(),
    )
}

fn arrow_to_iceberg_type(dt: &DataType) -> iceberg::spec::PrimitiveType {
    match dt {
        DataType::Int64 => iceberg::spec::PrimitiveType::Long,
        DataType::Int32 => iceberg::spec::PrimitiveType::Int,
        DataType::Utf8 => iceberg::spec::PrimitiveType::String,
        DataType::Decimal128(p, s) => iceberg::spec::PrimitiveType::Decimal {
            precision: *p as u32,
            scale: *s as u32,
        },
        DataType::Date32 => iceberg::spec::PrimitiveType::Date,
        _ => panic!("Unsupported Arrow type: {:?}", dt),
    }
}

async fn flush_batch(engine: &mut StorageEngine, batch: RecordBatch) {
    engine.append(batch).await.expect("Failed to append batch");
    engine.flush().await.expect("Failed to flush batch");
}

// ============================================================================
// Region
// ============================================================================

async fn generate_region(
    catalog: &Arc<AgoraCatalog>,
    file_io: FileIO,
    root_path: &str,
    output_dir: &Path,
    count: usize,
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("r_regionkey", DataType::Int64, false),
        Field::new("r_name", DataType::Utf8, false),
        Field::new("r_comment", DataType::Utf8, false),
    ]));

    let mut engine = create_table(catalog, file_io, root_path, output_dir, "region", schema.clone()).await;

    let names = vec!["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];
    let comments = vec![
        "lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to ",
        "hs use ironic, even requests. s",
        "ges. thinly even pinto beans ca",
        "ly final courts cajole furiously final excuse",
        "uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl",
    ];

    let keys: Vec<i64> = (0..count as i64).collect();
    let name_arr: Vec<&str> = names.into_iter().take(count).collect();
    let comment_arr: Vec<&str> = comments.into_iter().take(count).collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(keys)) as _,
            Arc::new(StringArray::from(name_arr)) as _,
            Arc::new(StringArray::from(comment_arr)) as _,
        ],
    )
    .unwrap();

    flush_batch(&mut engine, batch).await;
}

// ============================================================================
// Nation
// ============================================================================

async fn generate_nation(
    catalog: &Arc<AgoraCatalog>,
    file_io: FileIO,
    root_path: &str,
    output_dir: &Path,
    count: usize,
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, false),
        Field::new("n_regionkey", DataType::Int64, false),
        Field::new("n_comment", DataType::Utf8, false),
    ]));

    let mut engine = create_table(catalog, file_io, root_path, output_dir, "nation", schema.clone()).await;

    let names = vec![
        "ALGERIA",
        "ARGENTINA",
        "BRAZIL",
        "CANADA",
        "EGYPT",
        "ETHIOPIA",
        "FRANCE",
        "GERMANY",
        "INDIA",
        "INDONESIA",
        "IRAN",
        "IRAQ",
        "JAPAN",
        "JORDAN",
        "KENYA",
        "MOROCCO",
        "MOZAMBIQUE",
        "PERU",
        "CHINA",
        "ROMANIA",
        "SAUDI ARABIA",
        "VIETNAM",
        "RUSSIA",
        "UNITED KINGDOM",
        "UNITED STATES",
    ];

    let keys: Vec<i64> = (0..count as i64).collect();
    let region_keys: Vec<i64> = (0..count).map(|i| (i % 5) as i64).collect();
    let name_arr: Vec<&str> = names.into_iter().take(count).collect();
    let comment_arr: Vec<String> = (0..count)
        .map(|i| format!("Comment for nation {}", i))
        .collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(keys)) as _,
            Arc::new(StringArray::from(name_arr)) as _,
            Arc::new(Int64Array::from(region_keys)) as _,
            Arc::new(StringArray::from(comment_arr)) as _,
        ],
    )
    .unwrap();

    flush_batch(&mut engine, batch).await;
}

// ============================================================================
// Customer
// ============================================================================

async fn generate_customer(
    catalog: &Arc<AgoraCatalog>,
    file_io: FileIO,
    root_path: &str,
    output_dir: &Path,
    count: usize,
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int64, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
        Field::new("c_mktsegment", DataType::Utf8, false),
        Field::new("c_comment", DataType::Utf8, false),
    ]));

    let mut engine = create_table(catalog, file_io, root_path, output_dir, "customer", schema.clone()).await;
    let mut rng = seed_rng("customer");

    let mkt_segments = [
        "AUTOMOBILE",
        "BUILDING",
        "FURNITURE",
        "HOUSEHOLD",
        "MACHINERY",
    ];

    let mut keys = Vec::with_capacity(count);
    let mut names = Vec::with_capacity(count);
    let mut addresses = Vec::with_capacity(count);
    let mut nation_keys = Vec::with_capacity(count);
    let mut phones = Vec::with_capacity(count);
    let mut acctbals = Vec::with_capacity(count);
    let mut segments = Vec::with_capacity(count);
    let mut comments = Vec::with_capacity(count);

    for i in 1..=count {
        keys.push(i as i64);
        names.push(format!("Customer#{:09}", i));
        addresses.push(format!("Address#{}", i));
        nation_keys.push(((i - 1) % 25) as i64);
        phones.push(format!(
            "{:02}-{:03}-{:03}-{:04}",
            rng.gen_range(10..100),
            rng.gen_range(100..1000),
            rng.gen_range(100..1000),
            rng.gen_range(1000..10000)
        ));
        acctbals.push(
            (rng.gen_range(-99999..999999) as i128) * 100i128 + rng.gen_range(0..100) as i128,
        );
        segments.push(mkt_segments[rng.gen_range(0..mkt_segments.len())].to_string());
        comments.push(format!("Comment for customer {}", i));
    }

    // Write in batches
    let mut offset = 0;
    while offset < count {
        let end = (offset + BATCH_SIZE).min(count);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(names[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(addresses[offset..end].to_vec())) as _,
                Arc::new(Int64Array::from(nation_keys[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(phones[offset..end].to_vec())) as _,
                Arc::new(
                    Decimal128Array::from(acctbals[offset..end].to_vec())
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ) as _,
                Arc::new(StringArray::from(segments[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(comments[offset..end].to_vec())) as _,
            ],
        )
        .unwrap();
        flush_batch(&mut engine, batch).await;
        offset = end;
    }
}

// ============================================================================
// Orders
// ============================================================================

async fn generate_orders(
    catalog: &Arc<AgoraCatalog>,
    file_io: FileIO,
    root_path: &str,
    output_dir: &Path,
    count: usize,
    customer_count: usize,
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
        Field::new("o_clerk", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8, false),
    ]));

    let mut engine = create_table(catalog, file_io, root_path, output_dir, "orders", schema.clone()).await;
    let mut rng = seed_rng("orders");

    let priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
    let _order_statuses = [('F', 0.45), ('O', 0.45), ('P', 0.10)];

    // Pick 375 distinct customers from the available pool
    let mut customer_pool: Vec<i64> = (1..=customer_count as i64).collect();
    // Shuffle deterministically
    for i in (1..customer_pool.len()).rev() {
        let j = rng.gen_range(0..=i);
        customer_pool.swap(i, j);
    }
    let distinct_customers: Vec<i64> = customer_pool.into_iter().take(375).collect();

    let mut keys = Vec::with_capacity(count);
    let mut cust_keys = Vec::with_capacity(count);
    let mut statuses = Vec::with_capacity(count);
    let mut total_prices = Vec::with_capacity(count);
    let mut order_dates = Vec::with_capacity(count);
    let mut priorities_v = Vec::with_capacity(count);
    let mut clerks = Vec::with_capacity(count);
    let mut ship_priorities = Vec::with_capacity(count);
    let mut comments = Vec::with_capacity(count);

    // Base date: 1992-01-01 = day 8035 since 1970-01-01
    let base_date = 8035i32;

    for i in 1..=count {
        keys.push(i as i64);
        // Each of the 375 customers gets roughly equal number of orders
        let cust_idx = (i - 1) % distinct_customers.len();
        cust_keys.push(distinct_customers[cust_idx]);

        // Order status: F ~45%, O ~45%, P ~10%
        let r: f64 = rng.gen();
        let status = if r < 0.45 {
            "F"
        } else if r < 0.90 {
            "O"
        } else {
            "P"
        };
        statuses.push(status);

        // Total price: 1000.00 to 300000.00 (some > 100000)
        let price_dollars = rng.gen_range(1000..300001);
        let price_cents = rng.gen_range(0..100);
        total_prices.push((price_dollars as i128) * 100i128 + (price_cents as i128));

        // Order date: spread across ~2400 days (about 6.5 years)
        order_dates.push(base_date + rng.gen_range(0..2400));

        priorities_v.push(priorities[rng.gen_range(0..priorities.len())].to_string());
        clerks.push(format!("Clerk#{:09}", rng.gen_range(1..1000)));
        ship_priorities.push(0i32);
        comments.push(format!("Order comment {}", i));
    }

    // Write in batches
    let mut offset = 0;
    while offset < count {
        let end = (offset + BATCH_SIZE).min(count);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys[offset..end].to_vec())) as _,
                Arc::new(Int64Array::from(cust_keys[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(statuses[offset..end].to_vec())) as _,
                Arc::new(
                    Decimal128Array::from(total_prices[offset..end].to_vec())
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ) as _,
                Arc::new(Date32Array::from(order_dates[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(priorities_v[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(clerks[offset..end].to_vec())) as _,
                Arc::new(Int32Array::from(ship_priorities[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(comments[offset..end].to_vec())) as _,
            ],
        )
        .unwrap();
        flush_batch(&mut engine, batch).await;
        offset = end;
    }
}

// ============================================================================
// Lineitem
// ============================================================================

async fn generate_lineitem(
    catalog: &Arc<AgoraCatalog>,
    file_io: FileIO,
    root_path: &str,
    output_dir: &Path,
    orders_count: usize,
    sf: f64,
) {
    let schema = Arc::new(Schema::new(vec![
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
    ]));

    let mut engine = create_table(catalog, file_io, root_path, output_dir, "lineitem", schema.clone()).await;
    let mut rng = seed_rng("lineitem");

    let return_flags = ["N", "R", "A"];
    let line_statuses = ["O", "F"];
    let ship_instructs = [
        "DELIVER IN PERSON",
        "COLLECT COD",
        "NONE",
        "TAKE BACK RETURN",
    ];
    let ship_modes = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];

    let base_date = 8035i32;

    let mut order_keys = Vec::new();
    let mut part_keys = Vec::new();
    let mut supp_keys = Vec::new();
    let mut line_numbers = Vec::new();
    let mut quantities = Vec::new();
    let mut extended_prices = Vec::new();
    let mut discounts = Vec::new();
    let mut taxes = Vec::new();
    let mut return_flags_v = Vec::new();
    let mut line_statuses_v = Vec::new();
    let mut ship_dates = Vec::new();
    let mut commit_dates = Vec::new();
    let mut receipt_dates = Vec::new();
    let mut ship_instructs_v = Vec::new();
    let mut ship_modes_v = Vec::new();
    let mut comments = Vec::new();

    for order_key in 1..=orders_count as i64 {
        // 1-7 line items per order (deterministic: varies by order)
        let line_count = rng.gen_range(1..=7);
        for line_num in 1..=line_count {
            order_keys.push(order_key);
            part_keys.push(rng.gen_range(1..=((200000.0 * sf) as i64).max(1)));
            supp_keys.push(rng.gen_range(1..=((10000.0 * sf) as i64).max(1)));
            line_numbers.push(line_num);

            let qty = rng.gen_range(1..=50);
            quantities.push((qty as i128) * 100i128);

            let price = rng.gen_range(100..=10000);
            extended_prices.push((price as i128) * 100i128);

            let disc = rng.gen_range(0..=10);
            discounts.push((disc as i128) * 100i128);

            let tax = rng.gen_range(0..=8);
            taxes.push((tax as i128) * 100i128);

            return_flags_v.push(return_flags[rng.gen_range(0..return_flags.len())].to_string());
            line_statuses_v.push(line_statuses[rng.gen_range(0..line_statuses.len())].to_string());

            let order_date = base_date + rng.gen_range(0..2400);
            ship_dates.push(order_date + rng.gen_range(1..60));
            commit_dates.push(order_date + rng.gen_range(1..30));
            receipt_dates.push(order_date + rng.gen_range(1..90));

            ship_instructs_v
                .push(ship_instructs[rng.gen_range(0..ship_instructs.len())].to_string());
            ship_modes_v.push(ship_modes[rng.gen_range(0..ship_modes.len())].to_string());
            comments.push(format!("Line comment {}-{}", order_key, line_num));
        }
    }

    let total_rows = order_keys.len();
    eprintln!("  lineitem:   {}", total_rows);

    // Write in batches
    let mut offset = 0;
    while offset < total_rows {
        let end = (offset + BATCH_SIZE).min(total_rows);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(order_keys[offset..end].to_vec())) as _,
                Arc::new(Int64Array::from(part_keys[offset..end].to_vec())) as _,
                Arc::new(Int64Array::from(supp_keys[offset..end].to_vec())) as _,
                Arc::new(Int32Array::from(line_numbers[offset..end].to_vec())) as _,
                Arc::new(
                    Decimal128Array::from(quantities[offset..end].to_vec())
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ) as _,
                Arc::new(
                    Decimal128Array::from(extended_prices[offset..end].to_vec())
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ) as _,
                Arc::new(
                    Decimal128Array::from(discounts[offset..end].to_vec())
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ) as _,
                Arc::new(
                    Decimal128Array::from(taxes[offset..end].to_vec())
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ) as _,
                Arc::new(StringArray::from(return_flags_v[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(line_statuses_v[offset..end].to_vec())) as _,
                Arc::new(Date32Array::from(ship_dates[offset..end].to_vec())) as _,
                Arc::new(Date32Array::from(commit_dates[offset..end].to_vec())) as _,
                Arc::new(Date32Array::from(receipt_dates[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(ship_instructs_v[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(ship_modes_v[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(comments[offset..end].to_vec())) as _,
            ],
        )
        .unwrap();
        flush_batch(&mut engine, batch).await;
        offset = end;
    }
}

// ============================================================================
// Supplier
// ============================================================================

async fn generate_supplier(
    catalog: &Arc<AgoraCatalog>,
    file_io: FileIO,
    root_path: &str,
    output_dir: &Path,
    count: usize,
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_address", DataType::Utf8, false),
        Field::new("s_nationkey", DataType::Int64, false),
        Field::new("s_phone", DataType::Utf8, false),
        Field::new("s_acctbal", DataType::Decimal128(15, 2), false),
        Field::new("s_comment", DataType::Utf8, false),
    ]));

    let mut engine = create_table(catalog, file_io, root_path, output_dir, "supplier", schema.clone()).await;
    let mut rng = seed_rng("supplier");

    let mut keys = Vec::with_capacity(count);
    let mut names = Vec::with_capacity(count);
    let mut addresses = Vec::with_capacity(count);
    let mut nation_keys = Vec::with_capacity(count);
    let mut phones = Vec::with_capacity(count);
    let mut acctbals = Vec::with_capacity(count);
    let mut comments = Vec::with_capacity(count);

    for i in 1..=count {
        keys.push(i as i64);
        names.push(format!("Supplier#{:09}", i));
        addresses.push(format!("Supplier Address #{}", i));
        nation_keys.push(((i - 1) % 25) as i64);
        phones.push(format!(
            "{:02}-{:03}-{:03}-{:04}",
            rng.gen_range(10..100),
            rng.gen_range(100..1000),
            rng.gen_range(100..1000),
            rng.gen_range(1000..10000)
        ));
        acctbals.push(
            (rng.gen_range(-999..10000) as i128) * 100i128 + rng.gen_range(0..100) as i128,
        );
        comments.push(format!("Supplier comment {}", i));
    }

    let mut offset = 0;
    while offset < count {
        let end = (offset + BATCH_SIZE).min(count);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(names[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(addresses[offset..end].to_vec())) as _,
                Arc::new(Int64Array::from(nation_keys[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(phones[offset..end].to_vec())) as _,
                Arc::new(
                    Decimal128Array::from(acctbals[offset..end].to_vec())
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ) as _,
                Arc::new(StringArray::from(comments[offset..end].to_vec())) as _,
            ],
        )
        .unwrap();
        flush_batch(&mut engine, batch).await;
        offset = end;
    }
}

// ============================================================================
// Part
// ============================================================================

async fn generate_part(
    catalog: &Arc<AgoraCatalog>,
    file_io: FileIO,
    root_path: &str,
    output_dir: &Path,
    count: usize,
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
        Field::new("p_size", DataType::Int32, false),
        Field::new("p_container", DataType::Utf8, false),
        Field::new("p_retailprice", DataType::Decimal128(15, 2), false),
        Field::new("p_comment", DataType::Utf8, false),
    ]));

    let mut engine = create_table(catalog, file_io, root_path, output_dir, "part", schema.clone()).await;
    let mut rng = seed_rng("part");

    let colors = [
        "almond", "antique", "aquamarine", "azure", "beige",
        "bisque", "black", "blanched", "blue", "blush",
        "brown", "burlywood", "burnished", "chartreuse", "chocolate",
        "coral", "cornflower", "cornsilk", "cream", "cyan",
        "dark", "deep", "dim", "dodger", "drab",
        "firebrick", "floral", "forest", "frosted", "gainsboro",
        "ghost", "goldenrod", "green", "grey", "honeydew",
        "hot", "indian", "ivory", "khaki", "lace",
        "lavender", "lawn", "lemon", "light", "lime",
        "linen", "magenta", "maroon", "medium", "metallic",
        "midnight", "mint", "misty", "moccasin", "navajo",
        "navy", "olive", "orange", "orchid", "pale",
        "papaya", "peach", "peru", "pink", "plum",
        "powder", "puff", "purple", "red", "rose",
        "rosy", "royal", "saddle", "salmon", "sandy",
        "seashell", "sienna", "sky", "slate", "smoke",
        "snow", "spring", "steel", "tan", "thistle",
        "tomato", "turquoise", "violet", "wheat", "white",
        "yellow", "yellowgreen",
    ];

    let materials = [
        "brass", "bronze", "copper", "gold", "lead",
        "nickel", "plated", "steel", "tin", "titanium",
    ];

    let types = [
        "ANODIZED", "BURNISHED", "BRUSHED", "PLATED", "POLISHED",
    ];

    let containers = [
        "SM CASE", "SM BOX", "SM BAG", "SM JAR", "SM PKG",
        "SM PACK", "SM CAN", "SM DRUM", "LG CASE", "LG BOX",
        "LG BAG", "LG JAR", "LG PKG", "LG PACK", "LG CAN",
        "LG DRUM", "MED CASE", "MED BOX", "MED BAG", "MED JAR",
        "MED PKG", "MED PACK", "MED CAN", "MED DRUM", "JUMBO CASE",
        "JUMBO BOX", "JUMBO BAG", "JUMBO JAR", "JUMBO PKG", "JUMBO PACK",
        "JUMBO CAN", "JUMBO DRUM", "WRAP CASE", "WRAP BOX", "WRAP BAG",
        "WRAP JAR", "WRAP PKG", "WRAP PACK", "WRAP CAN", "WRAP DRUM",
    ];

    let mut keys = Vec::with_capacity(count);
    let mut names = Vec::with_capacity(count);
    let mut mfgrs = Vec::with_capacity(count);
    let mut brands = Vec::with_capacity(count);
    let mut types_v = Vec::with_capacity(count);
    let mut sizes = Vec::with_capacity(count);
    let mut containers_v = Vec::with_capacity(count);
    let mut retail_prices = Vec::with_capacity(count);
    let mut comments = Vec::with_capacity(count);

    for i in 1..=count {
        keys.push(i as i64);

        // Build p_name: color + material + type (e.g., "almond brass ANODIZED")
        let color = colors[rng.gen_range(0..colors.len())];
        let material = materials[rng.gen_range(0..materials.len())];
        let type_name = types[rng.gen_range(0..types.len())];
        names.push(format!("{} {} {}", color, material, type_name));

        // p_mfgr: Manufacturer#1 to Manufacturer#5
        mfgrs.push(format!("Manufacturer#{}", ((i - 1) % 5) + 1));

        // p_brand: Brand#11 to Brand#55
        brands.push(format!("Brand#{}{}", ((i - 1) % 5) + 1, ((i - 1) % 5) + 1));

        // p_type: from fixed list with some having "%BRASS" suffix for Q2
        // Use a deterministic set of types that includes BRASS variants
        let type_pool = [
            "PROMO BRUSHED STEEL",
            "STANDARD BRUSHED BRASS",
            "ECONOMY BRUSHED BRASS",
            "SMALL BRUSHED BRASS",
            "MEDIUM BRUSHED BRASS",
            "LARGE BRUSHED BRASS",
            "PROMO ANODIZED STEEL",
            "STANDARD POLISHED STEEL",
            "ECONOMY PLATED STEEL",
            "SMALL BURNISHED STEEL",
            "MEDIUM BRUSHED STEEL",
            "LARGE POLISHED STEEL",
            "PROMO PLATED BRASS",
            "STANDARD BURNISHED BRASS",
            "ECONOMY ANODIZED BRASS",
            "SMALL POLISHED BRASS",
            "MEDIUM PLATED BRASS",
            "LARGE BURNISHED BRASS",
            "PROMO BURNISHED STEEL",
            "STANDARD BRUSHED STEEL",
            "ECONOMY POLISHED STEEL",
            "SMALL ANODIZED STEEL",
            "MEDIUM BURNISHED STEEL",
            "LARGE PLATED STEEL",
            "PROMO POLISHED BRASS",
            "STANDARD ANODIZED BRASS",
            "ECONOMY BRUSHED STEEL",
            "SMALL BRUSHED STEEL",
            "MEDIUM ANODIZED BRASS",
            "LARGE BRUSHED STEEL",
            "PROMO BRUSHED BRASS",
            "STANDARD PLATED STEEL",
            "ECONOMY BURNISHED BRASS",
            "SMALL PLATED BRASS",
            "MEDIUM POLISHED STEEL",
            "LARGE ANODIZED STEEL",
            "PROMO ANODIZED BRASS",
            "STANDARD BURNISHED STEEL",
            "ECONOMY ANODIZED STEEL",
            "SMALL BURNISHED BRASS",
            "MEDIUM PLATED STEEL",
            "LARGE BURNISHED STEEL",
            "PROMO PLATED STEEL",
            "STANDARD POLISHED BRASS",
            "ECONOMY POLISHED BRASS",
            "SMALL ANODIZED BRASS",
            "MEDIUM BURNISHED BRASS",
            "LARGE POLISHED BRASS",
            "PROMO BURNISHED BRASS",
            "STANDARD ANODIZED STEEL",
            "ECONOMY PLATED BRASS",
            "SMALL POLISHED STEEL",
            "MEDIUM ANODIZED STEEL",
            "LARGE PLATED BRASS",
            "PROMO POLISHED STEEL",
            "STANDARD BRUSHED BRASS",
            "ECONOMY BRUSHED BRASS",
            "SMALL BRUSHED BRASS",
            "MEDIUM BRUSHED BRASS",
            "LARGE BRUSHED BRASS",
        ];
        types_v.push(type_pool[rng.gen_range(0..type_pool.len())].to_string());

        // p_size: 1 to 50
        sizes.push(rng.gen_range(1..=50));

        // p_container: from fixed list
        containers_v.push(containers[rng.gen_range(0..containers.len())].to_string());

        // p_retailprice: 901.00 to 2099.99
        let dollars = rng.gen_range(901..2100);
        let cents = rng.gen_range(0..100);
        retail_prices.push((dollars as i128) * 100i128 + (cents as i128));

        comments.push(format!("Part comment {}", i));
    }

    let mut offset = 0;
    while offset < count {
        let end = (offset + BATCH_SIZE).min(count);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(names[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(mfgrs[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(brands[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(types_v[offset..end].to_vec())) as _,
                Arc::new(Int32Array::from(sizes[offset..end].to_vec())) as _,
                Arc::new(StringArray::from(containers_v[offset..end].to_vec())) as _,
                Arc::new(
                    Decimal128Array::from(retail_prices[offset..end].to_vec())
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ) as _,
                Arc::new(StringArray::from(comments[offset..end].to_vec())) as _,
            ],
        )
        .unwrap();
        flush_batch(&mut engine, batch).await;
        offset = end;
    }
}

// ============================================================================
// Partsupp
// ============================================================================

async fn generate_partsupp(
    catalog: &Arc<AgoraCatalog>,
    file_io: FileIO,
    root_path: &str,
    output_dir: &Path,
    part_count: usize,
    supplier_count: usize,
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ps_partkey", DataType::Int64, false),
        Field::new("ps_suppkey", DataType::Int64, false),
        Field::new("ps_availqty", DataType::Int32, false),
        Field::new("ps_supplycost", DataType::Decimal128(15, 2), false),
        Field::new("ps_comment", DataType::Utf8, false),
    ]));

    let mut engine = create_table(catalog, file_io, root_path, output_dir, "partsupp", schema.clone()).await;
    let mut rng = seed_rng("partsupp");

    let total_rows = part_count * 4;

    let mut part_keys = Vec::with_capacity(total_rows);
    let mut supp_keys = Vec::with_capacity(total_rows);
    let mut avail_qtys = Vec::with_capacity(total_rows);
    let mut supply_costs = Vec::with_capacity(total_rows);
    let mut comments = Vec::with_capacity(total_rows);

    for part_key in 1..=part_count as i64 {
        for offset in 0..4 {
            part_keys.push(part_key);
            let supp_key = ((part_key + offset as i64 - 1) % supplier_count.max(1) as i64) + 1;
            supp_keys.push(supp_key);
            avail_qtys.push(rng.gen_range(1..=9999));
            let dollars = rng.gen_range(1..=1000);
            let cents = rng.gen_range(0..100);
            supply_costs.push((dollars as i128) * 100i128 + (cents as i128));
            comments.push(format!("Partsupp comment {}-{}", part_key, offset));
        }
    }

    eprintln!("  partsupp:   {}", total_rows);

    let mut offset = 0;
    while offset < total_rows {
        let end = (offset + BATCH_SIZE).min(total_rows);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(part_keys[offset..end].to_vec())) as _,
                Arc::new(Int64Array::from(supp_keys[offset..end].to_vec())) as _,
                Arc::new(Int32Array::from(avail_qtys[offset..end].to_vec())) as _,
                Arc::new(
                    Decimal128Array::from(supply_costs[offset..end].to_vec())
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ) as _,
                Arc::new(StringArray::from(comments[offset..end].to_vec())) as _,
            ],
        )
        .unwrap();
        flush_batch(&mut engine, batch).await;
        offset = end;
    }
}
