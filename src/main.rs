use std::{ops::Range, sync::Arc, time::Duration};

use ergo_lib::{
    chain::{ergo_state_context::ErgoStateContext, parameters::Parameters, transaction::{Transaction, TxIoVec}}, ergo_chain_types::{BlockId, Header, PreHeader}, ergotree_ir::chain::ergo_box::{BoxId, ErgoBox}, wallet::tx_context::TransactionContext
};
use futures::TryStreamExt;
use futures::{stream::FuturesOrdered, StreamExt};
use reqwest::Client;
use tokio::fs::File;
use tokio::signal;
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
    async fn get_header_id_by_height(&self, height: usize) -> Result<Option<BlockId>, Error> {
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

    async fn get_headers(
        &self,
        range: Range<usize>,
        max_simul: usize,
    ) -> Result<Vec<Header>, Error> {
        let mut res = vec![];
        for start in range.step_by(max_simul) {
            let mut ordered = FuturesOrdered::new();
            for height in start..start + max_simul {
                let fut = async move {
                    let id = self.get_header_id_by_height(start).await.ok()?;
                    match id {
                        Some(id) => Some(self.get_header_by_id(id).await),
                        None => None,
                    }
                };
                ordered.push(fut);
            }
            let collected: Vec<Header> = ordered
                .take_while(|f| std::future::ready(f.is_some()))
                .map(|f| f.unwrap())
                .try_collect()
                .await?;
            res.extend_from_slice(&collected);
            info!("Total headers: {}", res.len());
            if collected.len() != max_simul {
                break;
            }
        }

        Ok(res)
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
    async fn load_tx_context(&self, transaction: Transaction) -> Result<TransactionContext<Transaction>, Error> {
        let mut inputs = vec![];
        let mut data_inputs = vec![];
        info!("Starting tx context loading for {}", transaction.id());
        for input in &transaction.inputs {
            info!("Loading input {}", input.box_id);
            let b = self.get_box_by_id(input.box_id).await?;
            inputs.push(b);
        }
        for data_input in transaction.data_inputs.as_ref().into_iter().flatten() {
            info!("Loading data input {}", data_input.box_id);
            let b = self.get_box_by_id(data_input.box_id).await?;
            data_inputs.push(b);
        }
        info!("Loaded tx context");
        Ok(TransactionContext::new(transaction, inputs, data_inputs)?)
    }
}

// Validate a transaction. Headers should be in reverse order (latest is at index 0)
async fn validate_transactions<'a>(node: &'a Node<'a>, headers: &[Header], height: usize, transactions: Vec<Transaction>) -> Result<(), Error> {
    info!("Starting validation of block #{height} id: {}", headers[height].id);
    let pre_header = PreHeader::from(headers[height].clone());
    // for some reason rustc doesn't believe [Header; 10] implements TryFrom<&[Header]>
    let mut headers: Vec<Header> = headers[height - 10..height].try_into().unwrap();
    headers.reverse();
    let parameters = Parameters::default();
    let state_context = ErgoStateContext::new(pre_header, headers.try_into().unwrap(), parameters);
    for transaction in transactions {
        let tx_id = transaction.id();
        let tx_context = node.load_tx_context(transaction).await?;
        let start = std::time::Instant::now();
        match tx_context.validate(&state_context) {
            Ok(()) => {},
            Err(e) => error!("Tx {tx_id} validation failed, reason: {e:?}"),
        }
        info!("Validated tx {tx_id} in {:?}", std::time::Instant::now() - start);
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let appender = tracing_appender::rolling::never("./", "validation.log");
    let (file_writer, _guard) = tracing_appender::non_blocking(appender);
    let filter = tracing_subscriber::filter::LevelFilter::INFO;
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_writer(file_writer))
        .with(tracing_subscriber::fmt::layer())
        .init();
    let client = Node {
        node_url: "http://127.0.0.1:9052",
        client: reqwest::Client::builder()
            .http2_keep_alive_interval(Duration::from_secs(1))
            .build()?,
    };
    let mut height = match load_block_height().await {
        Ok(height) => height,
        Err(e) => match e.kind() {
            tokio::io::ErrorKind::NotFound => {
                info!("TX index not found, starting from 1");
                11
            }
            _ => Err(e)?,
        },
    };

    let mut headers = client.get_headers(1..1_000_000, 100).await?;
    println!("{}", headers.len());
    //headers.reverse();


    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<usize>();
    tokio::spawn(async move {
        for block in height.. {
            let transactions = client.get_block_transactions(headers[block].id).await.unwrap();
            validate_transactions(&client, &headers, block, transactions).await.unwrap();
        }
        ()
    });
    loop {
    tokio::select! {
        new_height = rx.recv() => match new_height {
            Some(new_height) => height = std::cmp::max(height, new_height),
            None => break
        },
        _ = signal::ctrl_c() => break
    }
    }
    tokio::fs::write("./blockindex", format!("{}", height)).await?;
    info!("Exiting");

    Ok(())
}
