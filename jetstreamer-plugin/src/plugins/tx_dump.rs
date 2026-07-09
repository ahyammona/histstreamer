use std::sync::Arc;

use clickhouse::Client;
use futures_util::FutureExt;
use solana_message::VersionedMessage;

use crate::{Plugin, PluginFuture};
use jetstreamer_firehose::firehose::TransactionData;

/// Debug plugin: prints full transaction details (signature, slot, accounts,
/// instructions) to stdout instead of writing aggregates to ClickHouse.
#[derive(Debug, Clone)]
pub struct TxDumpPlugin;

impl TxDumpPlugin {
    /// Creates a new instance.
    pub const fn new() -> Self {
        Self
    }
}

impl Plugin for TxDumpPlugin {
    fn name(&self) -> &'static str {
        "tx-dump"
    }

    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        async move {
            let (account_keys, instructions) = match &transaction.transaction.message {
                VersionedMessage::Legacy(msg) => (&msg.account_keys, &msg.instructions),
                VersionedMessage::V0(msg) => (&msg.account_keys, &msg.instructions),
            };

            println!(
                "=== tx {} (slot {}, vote={}) ===",
                transaction.signature, transaction.slot, transaction.is_vote
            );
            println!("  fee: {}", transaction.transaction_status_meta.fee);
            println!("  accounts ({}):", account_keys.len());
            for key in account_keys.iter() {
                println!("    {}", key);
            }
            println!("  instructions ({}):", instructions.len());
            for (i, ix) in instructions.iter().enumerate() {
                let program = account_keys
                    .get(ix.program_id_index as usize)
                    .map(|k| k.to_string())
                    .unwrap_or_else(|| "?".to_string());
                println!("    [{}] program={} data_len={}", i, program, ix.data.len());
            }
            if let Some(logs) = &transaction.transaction_status_meta.log_messages {
                println!("  logs ({}):", logs.len());
                for log in logs.iter().take(5) {
                    println!("    {}", log);
                }
            }
            println!();

            Ok(())
        }
        .boxed()
    }
}
