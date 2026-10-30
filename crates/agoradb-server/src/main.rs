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

//! `agoradb`: run an AgoraDB node.
//!
//! ```text
//! agoradb [--root DIR] identity          print this node's DID
//! agoradb [--root DIR] sql "<SQL>"       run statements as the node owner
//! agoradb [--root DIR] serve             serve this node's Spaces to peers
//! ```
//!
//! The node key is read from `AGORADB_NODE_KEY` (base64url seed) when set,
//! otherwise from `<root>/.agora/identity/node_ed25519`, created on first use.

use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agoradb_catalog::AgoraCatalog;
use agoradb_node::{AgoraSession, EngineRegistry, NodeConfig, QueryResult, SessionConfig};
use agoradb_server::{serve, AgoraFlightService};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_cast::pretty::pretty_format_batches;
use clap::{Parser, Subcommand};
use iceberg::io::FileIO;
use tokio::net::TcpListener;

#[derive(Parser)]
#[command(
    name = "agoradb",
    version,
    about = "A decentralized, local-first federated query layer"
)]
struct Cli {
    /// Data directory of this node.
    #[arg(
        long,
        env = "AGORADB_ROOT",
        default_value = "./agora-data",
        global = true
    )]
    root: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print this node's DID.
    Identity,
    /// Run SQL statements (separated by `;`) as the node owner.
    Sql {
        /// The statements; `-` reads them from standard input.
        sql: String,
        /// Space that bare table names resolve against.
        #[arg(long)]
        space: Option<String>,
        /// Print rows as tab-separated values without a header.
        #[arg(long)]
        raw: bool,
    },
    /// Serve this node's Spaces to peers over Arrow Flight.
    Serve {
        /// Address to listen on.
        #[arg(long, env = "AGORADB_LISTEN", default_value = "0.0.0.0:50051")]
        listen: SocketAddr,
    },
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn open_node(root: &Path) -> Result<(Arc<AgoraCatalog>, Arc<EngineRegistry>), BoxError> {
    std::fs::create_dir_all(root)?;
    let root = root.canonicalize()?;
    let root = root
        .to_str()
        .ok_or("the data directory path must be valid UTF-8")?;
    let catalog = Arc::new(AgoraCatalog::open(FileIO::new_with_fs(), root)?);
    let engines = Arc::new(EngineRegistry::new(catalog.clone(), NodeConfig::default())?);
    Ok((catalog, engines))
}

fn print_result(result: &QueryResult, raw: bool) -> Result<(), BoxError> {
    match result {
        QueryResult::Batches { batches, .. } if raw => {
            let options = FormatOptions::default().with_null("");
            for batch in batches {
                let formatters = batch
                    .columns()
                    .iter()
                    .map(|c| ArrayFormatter::try_new(c.as_ref(), &options))
                    .collect::<Result<Vec<_>, _>>()?;
                for row in 0..batch.num_rows() {
                    let line: Vec<String> = formatters
                        .iter()
                        .map(|f| f.value(row).to_string())
                        .collect();
                    println!("{}", line.join("\t"));
                }
            }
        }
        QueryResult::Batches { batches, .. } => println!("{}", pretty_format_batches(batches)?),
        QueryResult::RowsAffected(n) => println!("{n} row(s) affected"),
        QueryResult::Empty => println!("OK"),
    }
    Ok(())
}

async fn run(cli: Cli) -> Result<(), BoxError> {
    match cli.command {
        Command::Identity => {
            let (_, engines) = open_node(&cli.root)?;
            println!("{}", engines.identity()?.did());
        }
        Command::Sql { sql, space, raw } => {
            let sql = if sql == "-" {
                let mut buf = String::new();
                std::io::stdin().read_to_string(&mut buf)?;
                buf
            } else {
                sql
            };
            let (catalog, engines) = open_node(&cli.root)?;
            let session = AgoraSession::new(
                catalog,
                engines,
                SessionConfig {
                    default_space: space,
                    principal: None,
                },
            );
            for statement in agoradb_semantic::split_statements(&sql) {
                let result = session.sql(&statement).await?;
                print_result(&result, raw)?;
            }
        }
        Command::Serve { listen } => {
            let (catalog, engines) = open_node(&cli.root)?;
            let service = AgoraFlightService::new(catalog, engines)?;
            let listener = TcpListener::bind(listen).await?;
            tracing::info!(did = %service.did(), addr = %listener.local_addr()?, "serving Arrow Flight");
            serve(listener, service, async {
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!("shutting down");
            })
            .await?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    if let Err(e) = run(Cli::parse()).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
