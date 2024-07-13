use std::{ops::Range, time::Duration};

use ergo_lib::{
    chain::transaction::{unsigned::UnsignedTransaction, Transaction, TxId},
    ergo_chain_types::{BlockId, Header},
    ergotree_interpreter::sigma_protocol::prover::ProofBytes,
    ergotree_ir::{
        chain::{
            address::{AddressEncoder, NetworkPrefix},
            ergo_box::{BoxId, ErgoBox},
        },
        serialization::SigmaSerializable,
    },
    wallet::tx_context::TransactionContext,
};
use futures::Stream;
use reqwest::Client;
use tracing::info;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub struct Node<'s> {
    node_url: &'s str,
    client: Client,
}

impl<'s> Node<'s> {
    pub fn new(node_url: &'s str) -> Node<'s> {
        Node {
            node_url,
            client: reqwest::Client::builder()
                .http2_keep_alive_interval(Duration::from_secs(1))
                .build()
                .unwrap(),
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
                let headers = self.chain_slice(last_yielded, last_yielded + 256).await?;
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

        let res: Block = self
            .client
            .get(format!("{}/blocks/{header_id}", self.node_url))
            .send()
            .await?
            .json()
            .await?;
        Ok(res.transactions.transactions)
    }
    pub async fn load_tx_context(
        &self,
        mut transaction: Transaction,
    ) -> Result<TransactionContext<Transaction>, Error> {
        let mut inputs = vec![];
        let mut data_inputs = vec![];
        for (idx, input) in transaction.inputs.iter().enumerate() {
            let mut b = self.get_box_by_id(input.box_id).await?;
            // if transaction.id().to_string()
            //     == "3b325efe3e1902e057d9b4bf5927c6be36a868fc97c93c055527db22a1b6667b"
            //     && idx == 15
            if idx == 0 {
                info!("Manually replacing input box");
                b.ergo_tree = AddressEncoder::new(NetworkPrefix::Mainnet)
                    .parse_address_from_str("FxyP1tYPbxZVwjCTB")?
                    .script()?;
            }
            if idx == 1 {
                info!("Manually replacing input box");
                b.ergo_tree = AddressEncoder::new(NetworkPrefix::Testnet)
                    .parse_address_from_str("dFhcqgPyauxrm8NgeMLPJTK8MksbKaapxRVHpmGxQ3F2q6fWLLYgwn4sJWuqu94YyNXy3xockxPwHfWtv2ATuiZNpw9p4zb5RFkKzysfHhvPeJjz2hFsqfRd5rearFbvoFet9UiJFRpi6uoSGob1jxQ44F8DMgZsEz4NfQTKBRVJgJqkMMhBqs1rmF2Ti4hpVeXEHVY7H2TzM64n39X44L1dYzf33tsdwBVJrBqRRnERCtUX3PaCkD8XKiv7giGnEwCDSXZRCk4zU5QDCp33obCx3kgZZVUnWmKThYWWGu2YvT5VNANCES7yAZGPTvL1Xhx6SpTdD62SrnJmKrHkU6SiBp2gfGieb86maJfs2bcXStiT1oxdVUKqmxSjh46pVcthHEbi2NFjr662z8zMM6QPipsrRms6HpvENHXZ1StjmUhneXDogKkBsgUfDzsXGistbP8rskNraKoP2FZdL9aeYcNG2i3fXQAEnGfGgvK5Asc3DT8VNyfkA9ue9sz4sL7ptcsuzgT6hZ32MDkCwp9TWNiVdSgsgQhVnEZFdxHGzg4TEKJtTFh5vkN9nb8f1XWPYdoUJGoMoJrBzadpufN")?
                    .script()?;
            }
            inputs.push(b);
        }

        // TODO
        transaction
            .inputs
            .iter_mut()
            .enumerate()
            .for_each(|(i, b)| b.box_id = inputs[i].box_id());
        // let vec = transaction.sigma_serialize_bytes().unwrap();
        // panic!();
        // println!("vec: {vec:?}");
        // transaction = Transaction::sigma_parse_bytes(&vec).unwrap();
        for data_input in transaction.data_inputs.as_ref().into_iter().flatten() {
            let b = self.get_box_by_id(data_input.box_id).await?;
            data_inputs.push(b);
        }
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
        //Ok((inclusion_height, transaction))
        // TODO
        let unsigned_transaction: UnsignedTransaction = serde_json::from_str(r#"
            {"inputs":[{"boxId":"bfe6f0691dae9802c7ca932fb63817034cb6e4c527ccb6ccdcd28978fbdc7abe","extension":{}},{"boxId":"9bf8875d42a51621788baca6a0d4a1cf8b3d1a529d690bd4874f746a61248456",
            "extension":{"0":"0200","1":"07038dccc261ce57d55e766eea7122ed9af0bac1482af714c142c05fc54e87c0fea9","2":"0e206f8e02a1da99efbfa9bcc083afd868618b84cb9ac55cdce877743f4f67a13464","3":"0e46020000000000000000000000000000000000000000000000000000000000000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0000000004"}}],"dataInputs":[{"boxId":"0ee66816fb2a401e3e93fb5eb40d3fd5ed4e2d4ae7df12b69ad2ae62e2882c53"}],
            "outputs":[{"value":1000000,
            "ergoTree":"100e040004000400050204000400040004000402040204020e20c7bf34a3764070f1229165cabf4fe896f5f23e1e254557a397c28ea8ed39ae9e04020e204884f5d56b9e54192a4667fd1d5d9b51f5649982ee4d2a1b7077dc6f78e918cbd805d6017ee4e3000204d602b2db6308a7730000d6038c720202d604e4c6a70507d605b2db6501fe730100959272017302d806d606e4e3020ed607e4e30107d608e4c6a70605d609e30402d60ab2a5720100d60bd9010b63edededed93e4c6720b06059a7208730393db6401e4c6720b0464db6401e4dc640ce4c6a704640283013c0e0e86028cb2db6308720573040001b3db070272077206e4e3030e93c2720bc2a7e6c6720b050792c1720bc1a7d1eded939fdb6a01dd7b7206a072079f72047bcbb3b37a72087a72038c720201937204e4c67205040795e67209d801d60cb2a57ee472090400eded939a8cb2db6308720a730500028cb2db6308720c730600027203da720b01720ada720b01720ced93b2db6308720a7307007202da720b01720ad802d606c2b2a4730800d607c2b2a5730900ea02cd7204d1ed93cbb47206730ab17206730b93cbb47207730cb17207730d",
            "assets":[{"tokenId":"a1da89dd7a85bd7567cd63043171f398aca0f67d319733628170b180914e0d25","amount":10}],
            "additionalRegisters":{"R4":"64c1f0bbd4ad22d43793ca52bb9673dcc062b8b8643ccd6fe021cf70988331b6bb01012000","R5":"0702deb02f8bada290f2c931756cecea112132773fc91102a49a2ba5ac4928deda13","R6":"0502"},
            "creationHeight":1258100},{"value":19997703100000,"ergoTree":"0008cd02978f626f3d84d9b7245fb10082c8ef47a60b10178b1626a34232217bf85a26db","assets":[{"tokenId":"5f1ebb2026cb74d9433d7619b5b9b24e560ac2dd2c5b45ea6e9458693fa69a07","amount":912086102},{"tokenId":"28c7ba72039512afeb10c4abb411fc0db7b2bf523b8ab61830ab20dcc2e5344b","amount":16462},{"tokenId":"4cac82c7901199025bb1feca97eefa3165ba09a760d0eb3055794d511a0d080e","amount":1},{"tokenId":"7ea1dca1abdddc4e4b0bd0af36eeac370db1966f206d26c4c713d348f50658d0","amount":100},{"tokenId":"4e5bc370178b33f1722033a6e42bd2ed56fc9db4a01716ee939eb88a29709e59","amount":12321321},{"tokenId":"6626afae0f512a1482b13d32b27baf5c3f76456d5470f786cc02fdf74acf7e29","amount":1},{"tokenId":"a38fceaddf7f87cdb6756eca02c1702ca75beaaceeadfcc26111bdcd84b7cb7a","amount":34332},{"tokenId":"ee4e681179db95ae666d6997e927d0520655243778eb9871085d5c915cffcc3e","amount":9223372036854775807},{"tokenId":"9811d091a9b2d86ac6a3a8948e03abdcaef84d8c9e6bb9e8b5845cd3ed1f2c89","amount":9223371258075996819}],"additionalRegisters":{},"creationHeight":1258100},{"value":1100000,"ergoTree":"1005040004000e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a701730073011001020402d19683030193a38cc7b2a57300000193c2b2a57301007473027303830108cdeeac93b1a57304","assets":[],"additionalRegisters":{},"creationHeight":1258100}]}
            "#).unwrap();

        let unsigned_transaction: UnsignedTransaction = serde_json::from_str(r#"
            {"inputs":[{"boxId":"bfe6f0691dae9802c7ca932fb63817034cb6e4c527ccb6ccdcd28978fbdc7abe","extension":{}},{"boxId":"9bf8875d42a51621788baca6a0d4a1cf8b3d1a529d690bd4874f746a61248456","extension":{"0":"0200","1":"0702e6ce1fee3710007476138c356c25d6a2b41bca973334c23d65c7827c934c4ca4","2":"0e206b68deeda3923f5d130f31b1d70f4c2470aa842ed10e8fa4ca0c1e295cf65ee5","3":"0e46020000000000000000000000000000000000000000000000000000000000000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0000000004"}}],"dataInputs":[{"boxId":"0ee66816fb2a401e3e93fb5eb40d3fd5ed4e2d4ae7df12b69ad2ae62e2882c53"}],"outputs":[{"value":1000000,"ergoTree":"100e040004000400050204000400040004000402040204020e20c7bf34a3764070f1229165cabf4fe896f5f23e1e254557a397c28ea8ed39ae9e04020e204884f5d56b9e54192a4667fd1d5d9b51f5649982ee4d2a1b7077dc6f78e918cbd805d6017ee4e3000204d602b2db6308a7730000d6038c720202d604e4c6a70507d605b2db6501fe730100959272017302d806d606e4e3020ed607e4e30107d608e4c6a70605d609e30402d60ab2a5720100d60bd9010b63edededed93e4c6720b06059a7208730393db6401e4c6720b0464db6401e4dc640ce4c6a704640283013c0e0e86028cb2db6308720573040001b3db070272077206e4e3030e93c2720bc2a7e6c6720b050792c1720bc1a7d1eded939fdb6a01dd7b7206a072079f72047bcbb3b37a72087a72038c720201937204e4c67205040795e67209d801d60cb2a57ee472090400eded939a8cb2db6308720a730500028cb2db6308720c730600027203da720b01720ada720b01720ced93b2db6308720a7307007202da720b01720ad802d606c2b2a4730800d607c2b2a5730900ea02cd7204d1ed93cbb47206730ab17206730b93cbb47207730cb17207730d","assets":[{"tokenId":"a1da89dd7a85bd7567cd63043171f398aca0f67d319733628170b180914e0d25","amount":10}],"additionalRegisters":{"R4":"6445ed83dcab06e6c5dae8d70569a15f43e2b0147446347a2824aedef4e8a20f6e01012000","R5":"0702deb02f8bada290f2c931756cecea112132773fc91102a49a2ba5ac4928deda13","R6":"0502"},"creationHeight":1258314},{"value":19997703100000,"ergoTree":"0008cd02978f626f3d84d9b7245fb10082c8ef47a60b10178b1626a34232217bf85a26db","assets":[{"tokenId":"a38fceaddf7f87cdb6756eca02c1702ca75beaaceeadfcc26111bdcd84b7cb7a","amount":34332},{"tokenId":"4cac82c7901199025bb1feca97eefa3165ba09a760d0eb3055794d511a0d080e","amount":1},{"tokenId":"6626afae0f512a1482b13d32b27baf5c3f76456d5470f786cc02fdf74acf7e29","amount":1},{"tokenId":"5f1ebb2026cb74d9433d7619b5b9b24e560ac2dd2c5b45ea6e9458693fa69a07","amount":912086102},{"tokenId":"4e5bc370178b33f1722033a6e42bd2ed56fc9db4a01716ee939eb88a29709e59","amount":12321321},{"tokenId":"ee4e681179db95ae666d6997e927d0520655243778eb9871085d5c915cffcc3e","amount":9223372036854775807},{"tokenId":"7ea1dca1abdddc4e4b0bd0af36eeac370db1966f206d26c4c713d348f50658d0","amount":100},{"tokenId":"9811d091a9b2d86ac6a3a8948e03abdcaef84d8c9e6bb9e8b5845cd3ed1f2c89","amount":9223371258075996819},{"tokenId":"28c7ba72039512afeb10c4abb411fc0db7b2bf523b8ab61830ab20dcc2e5344b","amount":16462}],"additionalRegisters":{},"creationHeight":1258314},{"value":1100000,"ergoTree":"1005040004000e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a701730073011001020402d19683030193a38cc7b2a57300000193c2b2a57301007473027303830108cdeeac93b1a57304","assets":[],"additionalRegisters":{},"creationHeight":1258314}]}
                   "#).unwrap();
        dbg!(
            &unsigned_transaction
                .output_candidates
                .get(0)
                .unwrap()
                .additional_registers
        );
        let transaction = Transaction::from_unsigned_tx(
            unsigned_transaction,
            vec![ProofBytes::Empty, ProofBytes::Empty],
        )
        .unwrap();
        Ok((1258314, transaction))
    }
}
