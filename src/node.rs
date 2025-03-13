use std::{
    ops::Range,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use ergo_lib::{
    chain::transaction::{ergo_transaction::ErgoTransaction, Transaction, TxId},
    ergo_chain_types::{BlockId, Header},
    ergotree_ir::chain::ergo_box::{BoxId, ErgoBox},
    wallet::tx_context::TransactionContext,
};
use fjall::PartitionHandle;
use futures::Stream;
use moka::policy::EvictionPolicy;
use reqwest::Client;
use tracing::info;

use crate::disk_index::DiskIndex;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub struct Node<'s> {
    node_url: &'s str,
    client: Client,
    box_cache: moka::future::Cache<BoxId, ErgoBox>,
    box_cache_disk: DiskIndex,
}

impl<'s> Node<'s> {
    pub fn new(node_url: &'s str) -> Node<'s> {
        Node {
            node_url,
            client: reqwest::Client::builder()
                .tcp_keepalive(Some(Duration::from_secs(1)))
                .http2_keep_alive_interval(Duration::from_secs(1))
                .build()
                .unwrap(),
            box_cache: moka::future::CacheBuilder::new(1_000_000)
                .eviction_policy(EvictionPolicy::lru())
                .build(),
            box_cache_disk: DiskIndex::new().unwrap(),
        }
    }
    pub async fn get_block_height(&'s self) -> Result<u32, Error> {
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
    pub async fn get_header_id_by_height(
        &'s self,
        height: usize,
    ) -> Result<Option<BlockId>, Error> {
        let block_id: Vec<BlockId> = self
            .client
            .get(format!("{}/blocks/at/{height}", self.node_url))
            .send()
            .await?
            .json()
            .await?;
        Ok(block_id.get(0).copied())
    }

    pub async fn get_header_by_id(&self, id: BlockId) -> Result<Header, Error> {
        Ok(self
            .client
            .get(format!("{}/blocks/{id}/header", self.node_url))
            .send()
            .await?
            .json()
            .await?)
    }
    pub async fn get_box_by_id<'a>(&self, box_id: BoxId) -> Result<ErgoBox, Error> {
        Ok(self
            .client
            .get(format!("{}/blockchain/box/byId/{box_id}", self.node_url))
            .send()
            .await?
            .json()
            .await?)
    }
    pub async fn get_boxes_by_id(
        &self,
        box_ids: impl Iterator<Item = BoxId>,
        delete: bool,
    ) -> Result<Vec<ErgoBox>, Error> {
        static TOTAL: AtomicUsize = AtomicUsize::new(0);
        static HITS: AtomicUsize = AtomicUsize::new(0);
        // info!(
        //     "Hit rate: {}%",
        //     (HITS.load(Ordering::Relaxed) as f64 / TOTAL.load(Ordering::Relaxed) as f64) * 100.0,
        // );
        // let mut lock = self.block_cache.lock().await;
        let mut boxes = Vec::with_capacity(box_ids.size_hint().0);
        for id in box_ids {
            TOTAL.fetch_add(1, Ordering::Relaxed);
            if let Some(ergo_box) = self.box_cache.get(&id).await {
                HITS.fetch_add(1, Ordering::Relaxed);
                if delete {
                    // inputs that are spent won't be needed again so demote them
                    self.box_cache.invalidate(&id).await;
                }
                boxes.push(ergo_box)
            } else {
                if let Some(ergo_box) = self.box_cache_disk.get(&id)? {
                    boxes.push(ergo_box);
                } else {
                    let ergo_box = self.get_box_by_id(id).await?;
                    self.box_cache_disk.insert(&ergo_box).await?;
                    boxes.push(ergo_box)
                }
            }
        }
        Ok(boxes)
    }

    async fn cache(&self, ergo_boxes: impl Iterator<Item = ErgoBox>) -> Result<(), Error> {
        let now = std::time::Instant::now();
        let cache = &self.box_cache;
        for ergo_box in ergo_boxes {
            self.box_cache_disk.insert(&ergo_box).await?;
            cache.insert(ergo_box.box_id(), ergo_box).await;
        }
        println!("Cached in {:?}", now.elapsed());
        Ok(())
    }

    pub async fn chain_slice(&self, start: usize, end: usize) -> Result<Vec<Header>, Error> {
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

    pub async fn get_headers(
        // TODO: possibly buggy behavior
        &self,
        range: Range<usize>,
        max_simul: usize,
    ) -> Result<Vec<Header>, Error> {
        let mut res = vec![];
        for start in range.step_by(max_simul) {
            let chain_slice = self.chain_slice(start, start + max_simul).await?;
            res.extend_from_slice(&chain_slice);
            if chain_slice.len() != max_simul {
                break;
            }
        }
        Ok(res)
    }
    pub async fn header_stream<'a>(
        &'a self,
        start: usize,
    ) -> impl Stream<Item = Result<Header, Error>> + 'a {
        async_stream::stream! {
            let mut last_yielded = start;
            'outer: loop {
                let now = std::time::Instant::now();
                let headers = self.chain_slice(last_yielded, last_yielded + 256).await?;
                println!("Got {:?} headers in {:?}", headers.len(), now.elapsed());
                let new_height = headers.last().unwrap().height as usize;
                if new_height == last_yielded {
                    loop {
                        tracing::warn!("Sleeping");
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
    pub async fn get_block_transactions(
        &self,
        header_id: BlockId,
    ) -> Result<Vec<Transaction>, Error> {
        // TODO: probably a better way to do this
        #[derive(serde::Serialize, serde::Deserialize, Debug)]
        struct BlockTransactions {
            transactions: Vec<Transaction>,
        }
        #[derive(serde::Serialize, serde::Deserialize, Debug)]
        struct Block {
            #[serde(rename = "blockTransactions")]
            transactions: BlockTransactions,
        }

        let now = std::time::Instant::now();
        let res: Block = self
            .client
            .get(format!("{}/blocks/{header_id}", self.node_url))
            .send()
            .await?
            .json()
            .await?;
        info!("Fetched transactions in {:?}", now.elapsed());

        Ok(res.transactions.transactions)
    }
    pub async fn load_tx_context(
        &self,
        transaction: Transaction,
    ) -> Result<TransactionContext<Transaction>, Error> {
        let inputs = self.get_boxes_by_id(transaction.inputs_ids(), true).await?;

        let data_inputs = self
            .get_boxes_by_id(
                transaction
                    .data_inputs()
                    .into_iter()
                    .flatten()
                    .map(|di| di.box_id),
                false,
            )
            .await?;
        self.cache(transaction.outputs().into_iter().cloned())
            .await?;
        Ok(TransactionContext::new(transaction, inputs, data_inputs)?)
    }
    // Load a transaction by ID and return the block height the transaction was included in
    pub async fn load_tx_by_id(&self, tx_id: &TxId) -> Result<(usize, Transaction), Error> {
        let inclusion_height =
            self.client
                .get(format!(
                    "{}/blockchain/transaction/byId/{tx_id}",
                    self.node_url
                ))
                .send()
                .await?
                .json::<serde_json::Value>()
                .await?["inclusionHeight"]
                .as_u64()
                .ok_or_else(|| "Inclusion height not found".to_string())? as usize;

        let header_id = self
            .get_header_id_by_height(inclusion_height)
            .await?
            .ok_or_else(|| format!("Block not found at height {inclusion_height}"))?;
        let transaction = self
            .get_block_transactions(header_id)
            .await?
            .into_iter()
            .find(|tx| tx.id() == *tx_id)
            .ok_or_else(|| format!("TX {tx_id} not found in block {header_id}"))?;
        Ok((inclusion_height, transaction))
    }
}
