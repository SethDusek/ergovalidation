use ergo_lib::ergotree_ir::{
    chain::ergo_box::{BoxId, ErgoBox},
    serialization::SigmaSerializable,
};
use fjall::{Config, Keyspace, PartitionHandle};

pub struct DiskIndex {
    pub keyspace: Keyspace,
    pub db: PartitionHandle,
}

impl DiskIndex {
    pub fn new() -> anyhow::Result<Self> {
        let keyspace = Config::default().open()?;
        let db = keyspace.open_partition("box_index", Default::default())?;
        Ok(Self { keyspace, db })
    }
    pub async fn insert(&self, ergo_box: &ErgoBox) -> anyhow::Result<()> {
        let box_id = ergo_box.box_id();
        let serialized = ergo_box.sigma_serialize_bytes()?;
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            if db.contains_key(box_id.as_ref())? {
                return Ok(());
            }
            db.insert(box_id.as_ref(), serialized)?;
            Ok(())
        })
        .await??;
        self.keyspace.persist(fjall::PersistMode::Buffer)?;
        Ok(())
    }
    pub fn get(&self, box_id: &BoxId) -> anyhow::Result<Option<ErgoBox>> {
        Ok(self
            .db
            .get(box_id.as_ref())?
            .map(|b| ErgoBox::sigma_parse_bytes(&b))
            .transpose()?)
    }
}
