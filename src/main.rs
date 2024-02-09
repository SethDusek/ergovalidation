use std::{collections::BTreeSet, ops::Range, sync::Arc, time::Duration};

use ergo_lib::{
    chain::{
        ergo_state_context::ErgoStateContext, parameters::Parameters, transaction::Transaction,
    },
    ergo_chain_types::{BlockId, Header, PreHeader},
    ergotree_ir::chain::ergo_box::{BoxId, ErgoBox},
    wallet::tx_context::TransactionContext,
};
use futures::{pin_mut, Stream, TryStreamExt};
use futures::{stream::FuturesOrdered, StreamExt};
use reqwest::Client;
use tokio::{signal, sync::RwLock};
use tokio::{fs::File, task::JoinSet};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

type Error = Box<dyn std::error::Error>;

async fn load_block_height() -> std::io::Result<usize> {
    let index = String::from_utf8(tokio::fs::read("./blockindex").await?)
        .unwrap()
        .parse()
        .unwrap();
    info!("Resuming indexing from {index}");
    Ok(index)
}

struct Node<'s> {
    node_url: &'s str,
    client: Client,
}

impl<'s> Node<'s> {
    async fn get_block_height(&'s self) -> Result<u32, Error> {
        Ok(self
            .client
            .get(format!("{}/info", self.node_url))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?["fullHeight"]
            .as_u64()
            .unwrap() as u32)
    }
    async fn get_header_id_by_height(&'s self, height: usize) -> Result<Option<BlockId>, Error> {
        let block_id: Vec<BlockId> = self
            .client
            .get(format!("{}/blocks/at/{height}", self.node_url))
            .send()
            .await?
            .json()
            .await?;
        Ok(block_id.get(0).copied())
    }

    async fn get_header_by_id(&self, id: BlockId) -> Result<Header, Error> {
        Ok(self
            .client
            .get(format!("{}/blocks/{id}/header", self.node_url))
            .send()
            .await?
            .json()
            .await?)
    }
    async fn get_box_by_id<'a>(&self, box_id: BoxId) -> Result<ErgoBox, Error> {
        Ok(self
            .client
            .get(format!("{}/blockchain/box/byId/{box_id}", self.node_url))
            .send()
            .await?
            .json()
            .await?)
    }
    async fn chain_slice(&self, start: usize, end: usize) -> Result<Vec<Header>, Error> {
        Ok(self
            .client
            .get(format!(
                "{}/blocks/chainSlice?fromHeight={start}&toHeight={end}",
                self.node_url
            ))
            .send()
            .await?
            .json()
            .await?)
    }

    async fn get_headers(
        &self,
        range: Range<usize>,
        max_simul: usize,
    ) -> Result<Vec<Header>, Error> {
        let mut res = vec![];
        for start in range.step_by(max_simul) {
            let chain_slice = self.chain_slice(start, start + max_simul).await?;
            res.extend_from_slice(&chain_slice);
            info!("Total headers: {}", res.len());
            if chain_slice.len() != max_simul {
                break;
            }
        }
        Ok(res)
    }
    async fn header_stream<'a>(
        &'a self,
        start: usize,
    ) -> impl Stream<Item = Result<Header, Error>> + 'a {
        async_stream::stream! {
            let mut last_yielded = start;
            'outer: loop {
                let headers = self.chain_slice(last_yielded, last_yielded + 256).await?;
                let new_height = headers.last().unwrap().height as usize;

                if new_height == last_yielded {
                    loop {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        let height = self.get_block_height().await?;
                        if last_yielded != height as usize {
                            info!("New block at height {height} prev {last_yielded} found.");
                            continue 'outer;
                        }
                    }
                }
                for header in headers {
                    if last_yielded == header.height as usize {
                        continue;
                    }
                    last_yielded = header.height as usize;
                    yield Ok(header);
                }
            }
        }
    }
    async fn get_block_transactions(&self, header_id: BlockId) -> Result<Vec<Transaction>, Error> {
        // TODO: probably a better way to do this
        #[derive(serde::Serialize, serde::Deserialize)]
        struct BlockTransactions {
            transactions: Vec<Transaction>,
        }
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Block {
            #[serde(rename = "blockTransactions")]
            transactions: BlockTransactions,
        }

        let res: Block = self
            .client
            .get(format!("{}/blocks/{header_id}", self.node_url))
            .send()
            .await?
            .json()
            .await?;
        Ok(res.transactions.transactions)
    }
    async fn load_tx_context(
        &self,
        transaction: Transaction,
    ) -> Result<TransactionContext<Transaction>, Error> {
        let mut inputs = vec![];
        let mut data_inputs = vec![];
        for input in &transaction.inputs {
            let b = self.get_box_by_id(input.box_id).await?;
            inputs.push(b);
        }
        for data_input in transaction.data_inputs.as_ref().into_iter().flatten() {
            let b = self.get_box_by_id(data_input.box_id).await?;
            data_inputs.push(b);
        }
        Ok(TransactionContext::new(transaction, inputs, data_inputs)?)
    }
}

// Validate a transaction. Headers should be in reverse order (latest is at index 0)
async fn validate_transactions<'a>(
    node: &'a Node<'a>,
    headers: &[Header],
    idx: usize,
    transactions: Vec<Transaction>,
) -> Result<(), Error> {
    info!(
        "Starting validation of block #{} id: {}",
        headers[idx].height, headers[idx].id
    );
    let pre_header = PreHeader::from(headers[idx].clone());
    // for some reason rustc doesn't believe [Header; 10] implements TryFrom<&[Header]>
    let mut headers: Vec<Header> = headers[idx - 10..idx].try_into().unwrap();
    headers.reverse();
    let parameters = Parameters::default();
    let state_context = ErgoStateContext::new(pre_header, headers.try_into().unwrap(), parameters);
    for transaction in transactions {
        let tx_id = transaction.id();
        let tx_context = node.load_tx_context(transaction).await?;
        let start = std::time::Instant::now();
        match tx_context.validate(&state_context) {
            Ok(()) => {}
            Err(e) => error!("Tx {tx_id} validation failed, reason: {e:?}"),
        }
        info!(
            "Validated tx {tx_id} in {:?}",
            std::time::Instant::now() - start
        );
    }
    Ok(())
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let appender = tracing_appender::rolling::never("./", "validation.log");
    let (file_writer, _guard) = tracing_appender::non_blocking(appender);
    let filter = tracing_subscriber::filter::LevelFilter::INFO;
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_writer(file_writer))
        .with(tracing_subscriber::fmt::layer())
        .init();
    let client = Arc::new(Node {
        node_url: "http://127.0.0.1:9052",
        client: reqwest::Client::builder()
            .http2_keep_alive_interval(Duration::from_secs(1))
            .build()?,
    });
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
    let header_stream = client.header_stream(979250).await;
    pin_mut!(header_stream);

    // let headers: Arc<[Header]> = client
    //     .get_headers(start_height - 11..1_000_000, 256)
    //     .await?
    //     .into();
    //headers.reverse();

    let mut missing: BTreeSet<usize> =
         BTreeSet::new();
    let mut joinset = JoinSet::new();

    let headers = Arc::new(RwLock::new(vec![]));
    let mut cursor = None; // Index current block to be validated. Gets updated whenever a new task is spawned
    loop {
        tokio::select! {
            new_header = header_stream.next() => match new_header {
                Some(Ok(new_header)) => {
                    info!("Received header {}", new_header.id);
                    headers.write().await.push(new_header);
                    let reader = headers.read().await;
                    if reader.len() > 10 { // Only validate transactions with 10 blocks preceding them. This is a limitation of sigma-rust atm
                        missing.insert(reader.last().unwrap().height as usize);
                        cursor = cursor.or(Some(reader.len() - 1));
                    }
                }
                Some(Err(e)) => { error!("Header recv error: {e:?}"); break }
                None => unreachable!()
            },
            Some(res) = joinset.join_next() => match res {
                Ok(new_height) => { missing.remove(&new_height); info!("Removing from missing"); },
                Err(e) => { error!("{e:?}"); break },
            },
            _ = futures::future::ready(()), if joinset.len() < 100 && !missing.is_empty() => {
                let header_idx = if let Some(next) = cursor.as_mut() {
                    if *next == headers.read().await.len() {
                        continue;
                    }
                    let tmp = *next;
                    *next+=1;
                    tmp

                }
                else {
                   continue;
                };
                let headers = headers.clone();
                let client = client.clone();
                joinset.spawn(async move {
                    let headers = headers.read().await;
                    let transactions = client.get_block_transactions(headers[header_idx].id).await.unwrap();
                    validate_transactions(&client, &headers, header_idx, transactions).await.unwrap();
                    header_idx
                });
            },
            _ = signal::ctrl_c() => break
        }
    }
    // Find minimum excluded integer in set of blocks
    //dbg!(&missing);
    tokio::fs::write(
        "./blockindex",
        format!("{}", missing.iter().next().unwrap()),
    )
    .await?;
    info!("Exiting");

    Ok(())
}
