use std::sync::Arc;

use ergo_lib::ergotree_ir::{
    chain::ergo_box::{BoxId, ErgoBox},
    serialization::SigmaSerializable,
};
use rocksdb::{WriteBatch, WriteOptions, DB};

pub struct DiskIndex {
    pub db: Arc<DB>,
}

impl DiskIndex {
    pub fn new() -> anyhow::Result<Self> {
        let mut options = rocksdb::Options::default();
        options.optimize_for_point_lookup(64);
        options.create_if_missing(true);
        let db = DB::open(&options, "rocksdb")?;
        Ok(Self { db: db.into() })
    }
    pub async fn insert(&self, ergo_box: &ErgoBox) -> anyhow::Result<()> {
        let db = self.db.clone();
        let box_id = ergo_box.box_id();
        let serialized = ergo_box.sigma_serialize_bytes()?;
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let mut opts = WriteOptions::new();
            opts.disable_wal(true);
            db.put_opt(box_id.as_ref(), serialized, &opts)?;
            Ok(())
        })
        .await??;
        Ok(())
    }
    pub async fn insert_many(&self, boxes: impl Iterator<Item = &ErgoBox>) -> anyhow::Result<()> {
        let mut batch = WriteBatch::new();
        for ergo_box in boxes {
            batch.put(ergo_box.box_id(), ergo_box.sigma_serialize_bytes()?);
        }
        self.db.write_without_wal(batch)?;
        Ok(())
    }
    pub fn get(&self, box_id: &BoxId) -> anyhow::Result<Option<ErgoBox>> {
        self.db
            .get_pinned(box_id.as_ref())?
            .map(|bytes| ErgoBox::sigma_parse_bytes(&*bytes))
            .transpose()
            .map_err(Into::into)
    }
}
