use std::{collections::BTreeSet, pin::Pin, sync::Arc};

use clap::{Parser, Subcommand};
use ergo_lib::{
    chain::{
        ergo_state_context::ErgoStateContext,
        parameters::Parameters,
        transaction::{Transaction, TxId},
    },
    ergo_chain_types::{Header, PreHeader},
};
use ergovalidation::node::{Error, Node};
use futures::{pin_mut, StreamExt};
use tokio::{
    fs::OpenOptions,
    io::{AsyncWriteExt, BufWriter},
    sync::Mutex,
};
use tokio::{signal, sync::Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

async fn load_block_height() -> std::io::Result<usize> {
    let index = String::from_utf8(tokio::fs::read("./blockindex").await?)
        .unwrap()
        .parse()
        .unwrap();
    info!("Resuming indexing from {index}");
    Ok(index)
}

// Validate a list of transactions. Headers should be in reverse order (latest is at index 0)
async fn validate_transactions<'a, W: tokio::io::AsyncWriteExt + std::marker::Unpin>(
    node: &'a Node<'a>,
    headers: Vec<Header>,
    header: Header,
    transactions: Vec<Transaction>,
    failure_logger: Pin<Arc<Mutex<W>>>,
) -> Result<(), Error> {
    use std::sync::atomic::AtomicU64;
    let pre_header = PreHeader::from(header.clone());
    info!(
        "Starting validation of block #{} id: {}",
        pre_header.height, header.id
    );
    let parameters = Parameters::default();
    let state_context = ErgoStateContext::new(pre_header, headers.try_into().unwrap(), parameters);
    static TOTAL: AtomicU64 = AtomicU64::new(0);
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    let _ = START.set(std::time::Instant::now());
    for transaction in transactions {
        let tx_id = transaction.id();
        if tx_id.to_string() == "e179f12156061c04d375f599bd8aea7ea5e704fab2d95300efb2d87460d60b83" {
            warn!("Skipping e179f12156061c04d375f599bd8aea7ea5e704fab2d95300efb2d87460d60b83 due to wacky behavior");
            continue;
        }
        let tx_context_time = std::time::Instant::now();
        let tx_context = node.load_tx_context(transaction).await?;
        info!(
            "Loaded tx context for {tx_id} in {}μs",
            tx_context_time.elapsed().as_micros()
        );
        let start = std::time::Instant::now();
        match tx_context.validate(&state_context) {
            Ok(()) => {}
            Err(e) => {
                error!("Tx {tx_id} validation failed, reason: {e:?}");
                failure_logger
                    .lock()
                    .await
                    .write_all(format!("{tx_id}\n").as_bytes())
                    .await?;
            }
        }
        let duration = std::time::Instant::now() - start;
        info!("Validated tx {tx_id} in {}μs", duration.as_micros());
        TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    info!(
        "TPS: {}",
        TOTAL
            .load(std::sync::atomic::Ordering::Relaxed)
            .checked_div(START.get().unwrap().elapsed().as_secs())
            .unwrap_or(0)
    );
    Ok(())
}

async fn validation_loop(
    cancellation_token: CancellationToken,
    start_height: usize,
    client: Arc<Node<'static>>,
) -> Result<(), Error> {
    let header_stream = client.header_stream(start_height - 10).await;
    pin_mut!(header_stream);

    let mut missing: BTreeSet<u32> = BTreeSet::new();
    let mut joinset = tokio_util::task::JoinMap::new();

    let mut headers: Vec<Header> = vec![];
    // Limit the amount of tasks to prevent fd exhaustion. TODO: figure out a way to get ergo node to support HTTP/2
    let semaphore = Arc::new(Semaphore::new(8));

    let file = OpenOptions::new()
        .append(true)
        .write(true)
        .create(true)
        .open("failures.txt")
        .await?;
    let failure_logger = Arc::pin(Mutex::new(BufWriter::new(file)));
    loop {
        tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => break,
            Some((id, res)) = joinset.join_next() => match res {
                Ok(height) => { missing.remove(&height); },
                Err(e) => { error!("{e:?}, block id: {id}"); break },
            },
            new_header = header_stream.next() => match new_header {
                Some(Ok(new_header)) => {
                    info!("Received header {}", new_header.id);
                    headers.push(new_header);
                    if headers.len() > 10 { // Only validate transactions with 10 blocks preceding them. This is a limitation of sigma-rust atm
                        missing.insert(headers.last().unwrap().height);
                        let permit = semaphore.clone().acquire_owned().await; // Acquire permit outside of task. This helps bound memory usage since we won't spawn any tasks until there's another slot available
                        let failure_logger = failure_logger.clone();
                        let client = client.clone();
                        let header_idx = headers.len() - 1;
                        let header = headers[header_idx].clone();
                        let headers: Vec<Header> = headers[header_idx - 10..header_idx].iter().cloned().rev().collect();
                        joinset.spawn(header.id, async move {
                            let _permit = permit;
                            let height = header.height;
                            let transactions = client.get_block_transactions(header.id).await.unwrap();
                            validate_transactions(&client, headers, header, transactions, failure_logger).await.unwrap();
                            height
                        });
                    }
                }
                Some(Err(e)) => { error!("Header recv error: {e:?}"); break }
                None => unreachable!()
            },

        }
    }
    // Find minimum excluded integer in set of blocks and store it to resume validation from
    failure_logger.lock().await.flush().await?;
    let saved_height = missing
        .iter()
        .next()
        .unwrap_or(&headers.last().unwrap().height);
    info!("Saving index at {saved_height}");
    tokio::fs::write("./blockindex", format!("{}", saved_height)).await?;
    Ok(())
}

async fn validate_transaction<'a, 'b>(node: &'a Node<'a>, tx_id: &'b TxId) -> Result<(), Error> {
    let (height, tx) = node.load_tx_by_id(tx_id).await?;
    let mut headers = node
        .get_headers(
            height
                .checked_sub(11)
                .expect("Can't validate transactions in first 10 blocks yet")..height,
            1,
        )
        .await?;
    let pre_header: PreHeader = headers.pop().unwrap().into();
    let headers: [Header; 10] = headers.try_into().unwrap();
    let state_context = ErgoStateContext::new(pre_header, headers, Parameters::default());
    let tx_context = node.load_tx_context(tx).await?;
    let start = std::time::Instant::now();
    match tx_context.validate(&state_context) {
        Ok(()) => {}
        Err(e) => error!("Tx {tx_id} validation failed, reason: {e:?}"),
    }
    let duration = std::time::Instant::now() - start;
    info!("Validated tx {tx_id} in {}us", duration.as_micros());
    Ok(())
}

#[derive(Subcommand)]
enum Command {
    Run,
    ValidateTransaction { id: String },
}

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let node = Arc::new(Node::new("http://127.0.0.1:9054"));
    match args.command {
        Some(Command::Run) | None => {
            let appender = tracing_appender::rolling::never("./", "validation.log");
            let (file_writer, _guard) = tracing_appender::non_blocking(appender);
            let filter = tracing_subscriber::filter::LevelFilter::INFO;
            // console_subscriber::init();
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer().with_writer(file_writer))
                .with(tracing_subscriber::fmt::layer())
                .init();
            let start_height = match load_block_height().await {
                Ok(height) => height,
                Err(e) => match e.kind() {
                    tokio::io::ErrorKind::NotFound => {
                        info!("TX index not found, starting from 1");
                        11
                    }
                    _ => Err(e)?,
                },
            };
            let token = CancellationToken::new();
            let join_handle =
                tokio::spawn(validation_loop(token.clone(), start_height, node.clone()));
            signal::ctrl_c().await?;
            token.cancel();
            join_handle.await?.unwrap();
            info!("Exiting");
        }
        Some(Command::ValidateTransaction { id }) => {
            let filter = tracing_subscriber::filter::LevelFilter::INFO;
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer())
                .init();

            let tx_id = TxId(id.try_into().expect("Failed to parse transaction id"));
            validate_transaction(&node, &tx_id)
                .await
                .expect("TX validation failed");
        }
    }

    Ok(())
}
