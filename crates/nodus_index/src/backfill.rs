use anyhow::Result;
use bytes::Bytes;
use nodus_catalog::{CatalogReader, CatalogWriter, IndexState};
use nodus_storage_api::{IndexKvCodec, KeyRange, KvEngine};
use nodus_txn::TxnManager;
use std::sync::Arc;

pub struct IndexBackfiller {
    catalog_reader: Arc<dyn CatalogReader>,
    catalog_writer: Arc<dyn CatalogWriter>,
    kv: Arc<dyn KvEngine>,
    txn: Arc<dyn TxnManager>,
    codec: Arc<dyn IndexKvCodec>,
}

/// A row cell as the backfill encodes it into an index key.
enum ParsedValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
}

impl ParsedValue {
    /// A JSON scalar as a cell; `None` for an array or object. (Parsed from
    /// `serde_json::Value` rather than as an untagged enum, which cannot
    /// take numbers when serde_json keeps their exact text.)
    fn from_json(value: serde_json::Value) -> Option<Self> {
        Some(match value {
            serde_json::Value::Null => ParsedValue::Null,
            serde_json::Value::Bool(b) => ParsedValue::Bool(b),
            serde_json::Value::Number(n) => ParsedValue::Number(n.as_f64()?),
            serde_json::Value::String(s) => ParsedValue::String(s),
            _ => return None,
        })
    }

    fn to_bytes(&self) -> Bytes {
        match self {
            ParsedValue::Null => Bytes::new(),
            ParsedValue::Bool(b) => Bytes::from(if *b { vec![1] } else { vec![0] }),
            ParsedValue::Number(n) => Bytes::from(n.to_string()),
            ParsedValue::String(s) => Bytes::from(s.clone()),
        }
    }
}

impl IndexBackfiller {
    pub fn new(
        catalog_reader: Arc<dyn CatalogReader>,
        catalog_writer: Arc<dyn CatalogWriter>,
        kv: Arc<dyn KvEngine>,
        txn: Arc<dyn TxnManager>,
        codec: Arc<dyn IndexKvCodec>,
    ) -> Self {
        Self {
            catalog_reader,
            catalog_writer,
            kv,
            txn,
            codec,
        }
    }

    pub async fn run_backfill(
        &self,
        db_name: &str,
        schema_name: &str,
        table_name: &str,
        index_name: &str,
    ) -> Result<()> {
        let table = self
            .catalog_reader
            .get_table(db_name, schema_name, table_name)?;

        let index_desc = table
            .indexes
            .iter()
            .find(|idx| idx.name == index_name)
            .ok_or_else(|| anyhow::anyhow!("Index {} not found", index_name))?;

        if index_desc.index_state == IndexState::Ready {
            return Ok(());
        }

        // Set state to Backfilling
        self.catalog_writer
            .update_index_state(table.id, index_desc.id, IndexState::Backfilling)?;

        // Scan the entire table
        // Start key is `table_id:`
        let prefix = format!("{}:", table.id);
        let prefix_bytes = Bytes::from(prefix.clone());
        let end_prefix = format!("{};", table.id); // Assuming ';' is next char after ':' in ascii
        let end_bytes = Bytes::from(end_prefix);

        // Start a read-only transaction implicitly by picking a read timestamp
        let read_ts = {
            // For MVP, just get a tick
            let temp_txn = self.txn.begin_txn()?;
            let ts = temp_txn.read_ts;
            self.txn.abort_txn(temp_txn.txn_id)?;
            ts
        };

        // We process the scan and build batch intents
        let mut index_intents = Vec::new();

        {
            let scan_iter = self.kv.scan(
                KeyRange {
                    start: prefix_bytes.clone(),
                    end: end_bytes,
                },
                read_ts,
            )?;

            for res in scan_iter {
                let pair = res?;

                // Parse base row JSON
                let parsed = serde_json::from_slice::<Vec<serde_json::Value>>(&pair.value)
                    .ok()
                    .and_then(|cells| {
                        cells
                            .into_iter()
                            .map(ParsedValue::from_json)
                            .collect::<Option<Vec<_>>>()
                    });
                if let Some(row_values) = parsed {
                    // Map index keys
                    let mut datum_values = Vec::new();
                    for key_col in &index_desc.key_columns {
                        let col_idx = table.columns.iter().position(|c| c.id == key_col.column_id);
                        if let Some(idx) = col_idx {
                            if let Some(val) = row_values.get(idx) {
                                datum_values.push(val.to_bytes());
                            } else {
                                datum_values.push(Bytes::new());
                            }
                        } else {
                            datum_values.push(Bytes::new());
                        }
                    }

                    // Extract the primary key (the part after the prefix)
                    // The kv key is `table_id:pk`
                    let key_str = String::from_utf8_lossy(&pair.key);
                    let pk_str = key_str.strip_prefix(&prefix).unwrap_or(&key_str);
                    let pk_bytes = Bytes::from(pk_str.to_string());

                    // Encode secondary index key
                    let sec_key = self.codec.encode_secondary_key(
                        index_desc.id,
                        &datum_values,
                        &pk_bytes,
                        0,
                    )?;
                    index_intents.push((sec_key, Bytes::new()));
                }
            }
        }

        // Write batch
        if !index_intents.is_empty() {
            let write_txn = self.txn.begin_txn()?;
            for (key, val) in index_intents {
                self.kv.write_intent(write_txn.txn_id, key, val)?;
            }
            let commit_ts = self.txn.commit_txn(write_txn.txn_id)?;
            self.kv.commit(write_txn.txn_id, commit_ts)?;
        }

        // Set state to Ready (skipping Validating for MVP)
        self.catalog_writer
            .update_index_state(table.id, index_desc.id, IndexState::Ready)?;

        Ok(())
    }
}
