use std::sync::Arc;

use clickhouse::{Client, Row};
use dashmap::DashMap;
use futures_util::FutureExt;
use once_cell::sync::Lazy;
use serde::Serialize;
use solana_address::Address;
use solana_message::VersionedMessage;
use solana_transaction_status_client_types::TransactionTokenBalance;// etc.
use crate::{Plugin, PluginFuture};
use jetstreamer_firehose::firehose::{BlockData, TransactionData};

/// One row per (mint, slot, signature) — the fast lookup path for "all txs touching a mint".
#[derive(Row, Serialize, Clone, Debug)]
struct MintTransactionRow {
    mint: Address,
    slot: u64,
    block_time: u32,
    signature: String,
}

/// One row per (mint, account) pair ever observed together in a transaction that touched the
/// mint. Broad scope: every account key in the transaction's message (static + loaded), not
/// just token-balance owners, so admin-only ops (InitializeMint, SetAuthority, etc.) still
/// register the accounts involved.
#[derive(Row, Serialize, Clone, Debug)]
struct MintAccountRow {
    mint: Address,
    account: Address,
    slot: u64,
}

/// Full transaction detail, stored once per signature regardless of how many mints it touches.
/// mint_transactions/mint_accounts point back into this table by signature.
#[derive(Row, Serialize, Clone, Debug)]
struct TransactionFullRow {
    signature: String,
    slot: u64,
    block_time: u32,
    is_vote: bool,
    fee: u64,
    compute_units_consumed: u64,
    success: bool,
    account_keys: Vec<Address>,
    log_messages: String,
    pre_balances: String,
    post_balances: String,
    pre_token_balances: String,
    post_token_balances: String,
    inner_instructions: String,
}

struct PendingSlot {
    mint_txs: Vec<MintTransactionRow>,
    mint_accounts: Vec<MintAccountRow>,
    full_txs: Vec<TransactionFullRow>,
}

impl PendingSlot {
    fn new() -> Self {
        Self {
            mint_txs: Vec::new(),
            mint_accounts: Vec::new(),
            full_txs: Vec::new(),
        }
    }
}

static PENDING_BY_SLOT: Lazy<DashMap<u64, PendingSlot, ahash::RandomState>> =
    Lazy::new(|| DashMap::with_hasher(ahash::RandomState::new()));

#[derive(Debug, Clone)]
/// Indexes every transaction that touches any SPL mint (via pre/post token balances) into
/// ClickHouse, keyed for fast per-mint lookups.
///
/// Three tables are maintained:
/// - `mint_transactions`: (mint, slot, signature) — fast range scan per mint.
/// - `mint_accounts`: (mint, account, slot) — every account key seen in any tx touching the
///   mint (broad scope: includes admin-only ops with no balance change, not just transfers).
/// - `transactions_full`: (signature) — full transaction detail (accounts, logs, balances,
///   inner instructions), stored once per signature and joined to by the two tables above.
pub struct MintIndexPlugin;

impl MintIndexPlugin {
    /// Creates a new instance.
    pub const fn new() -> Self {
        Self
    }

    fn take_slot_events(slot: u64) -> Option<PendingSlot> {
        PENDING_BY_SLOT.remove(&slot).map(|(_, pending)| pending)
    }

    fn drain_all_pending() -> Vec<(u64, PendingSlot)> {
        let slots: Vec<u64> = PENDING_BY_SLOT.iter().map(|entry| *entry.key()).collect();
        slots
            .into_iter()
            .filter_map(|slot| PENDING_BY_SLOT.remove(&slot).map(|(_, p)| (slot, p)))
            .collect()
    }
}

impl Default for MintIndexPlugin {
    fn default() -> Self {
        Self::new()
    }
}

fn decode_mint(mint: &str) -> Option<Address> {
    let bytes = bs58::decode(mint).into_vec().ok()?;
    let arr: [u8; 32] = bytes.try_into().ok()?;
    Some(Address::from(arr))
}

impl Plugin for MintIndexPlugin {
    #[inline(always)]
    fn name(&self) -> &'static str {
        "Mint Index"
    }

    #[inline(always)]
    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        async move {
            let meta = &transaction.transaction_status_meta;

            // Collect distinct mints referenced by this tx's token balance deltas.
            let mut mints: Vec<String> = Vec::new();
            for tb in meta
                .pre_token_balances
                .iter()
                .flatten()
                .chain(meta.post_token_balances.iter().flatten())
            {
                if !mints.contains(&tb.mint) {
                    mints.push(tb.mint.clone());
                }
            }
            if mints.is_empty() {
                // Nothing token-related in this tx; not something a mint-scoped query cares about.
                return Ok(());
            }

            // Broad account set: static account keys + any loaded (v0 ALT) addresses.
            let mut account_keys: Vec<Address> = match &transaction.transaction.message {
                VersionedMessage::Legacy(msg) => msg.account_keys.clone(),
                VersionedMessage::V0(msg) => msg.account_keys.clone(),
            };
            account_keys.extend(meta.loaded_addresses.writable.iter().copied());
            account_keys.extend(meta.loaded_addresses.readonly.iter().copied());

            let slot = transaction.slot;
            let signature = transaction.signature.to_string();
            let block_time = 0u32; // filled in on_block via backfill, matches pubkey_stats pattern
            let success = meta.status.is_ok();

            let full_row = TransactionFullRow {
                signature: signature.clone(),
                slot,
                block_time,
                is_vote: transaction.is_vote,
                fee: meta.fee,
                compute_units_consumed: meta.compute_units_consumed.unwrap_or(0),
                success,
                account_keys: account_keys.clone(),
                log_messages: serde_json::to_string(&meta.log_messages).unwrap_or_default(),
                pre_balances: serde_json::to_string(&meta.pre_balances).unwrap_or_default(),
                post_balances: serde_json::to_string(&meta.post_balances).unwrap_or_default(),
                // Replace lines 174 and 176 with custom serialization
                pre_token_balances: serialize_token_balances(&meta.pre_token_balances),
                post_token_balances: serialize_token_balances(&meta.post_token_balances),
            
                //pre_token_balances: serde_json::to_string(&meta.pre_token_balances)
                  //  .unwrap_or_default(),
               // post_token_balances: serde_json::to_string(&meta.post_token_balances)
                 //   .unwrap_or_default(),
                inner_instructions: serde_json::to_string(&meta.inner_instructions)
                    .unwrap_or_default(),
            };

            let mut slot_entry = PENDING_BY_SLOT.entry(slot).or_insert_with(PendingSlot::new);
            slot_entry.full_txs.push(full_row);

            for mint_str in &mints {
                let Some(mint) = decode_mint(mint_str) else {
                    continue;
                };
                slot_entry.mint_txs.push(MintTransactionRow {
                    mint,
                    slot,
                    block_time,
                    signature: signature.clone(),
                });
                for account in &account_keys {
                    slot_entry.mint_accounts.push(MintAccountRow {
                        mint,
                        account: *account,
                        slot,
                    });
                }
            }

            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_block(
        &self,
        _thread_id: usize,
        db: Option<Arc<Client>>,
        block: &BlockData,
    ) -> PluginFuture<'_> {
        let slot = block.slot();
        let block_time = block.block_time();
        let was_skipped = block.was_skipped();
        async move {
            if was_skipped {
                return Ok(());
            }

            let Some(mut pending) = Self::take_slot_events(slot) else {
                return Ok(());
            };
            let ts = clamp_block_time(block_time);
            for row in &mut pending.mint_txs {
                row.block_time = ts;
            }
            for row in &mut pending.full_txs {
                row.block_time = ts;
            }

            if let Some(db_client) = db {
                tokio::spawn(async move {
                    if let Err(err) = write_pending(db_client, pending).await {
                        log::error!("failed to write mint index rows: {}", err);
                    }
                });
            }

            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_load(&self, db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move {
            log::info!("Mint Index Plugin loaded.");
            if let Some(db) = db {
                db.query(
                    r#"
                    CREATE TABLE IF NOT EXISTS mint_transactions (
                        mint         FixedString(32),
                        slot         UInt64,
                        block_time   DateTime('UTC'),
                        signature    String
                    )
                    ENGINE = ReplacingMergeTree(block_time)
                    ORDER BY (mint, slot, signature)
                    "#,
                )
                .execute()
                .await?;

                db.query(
                    r#"
                    CREATE TABLE IF NOT EXISTS mint_accounts (
                        mint     FixedString(32),
                        account  FixedString(32),
                        slot     UInt64
                    )
                    ENGINE = ReplacingMergeTree(slot)
                    ORDER BY (mint, account)
                    "#,
                )
                .execute()
                .await?;

                db.query(
                    r#"
                    CREATE TABLE IF NOT EXISTS transactions_full (
                        signature                String,
                        slot                      UInt64,
                        block_time                DateTime('UTC'),
                        is_vote                   Bool,
                        fee                       UInt64,
                        compute_units_consumed    UInt64,
                        success                   Bool,
                        account_keys              Array(FixedString(32)),
                        log_messages              String,
                        pre_balances              String,
                        post_balances             String,
                        pre_token_balances        String,
                        post_token_balances       String,
                        inner_instructions        String
                    )
                    ENGINE = ReplacingMergeTree(block_time)
                    ORDER BY signature
                    "#,
                )
                .execute()
                .await?;

                log::info!("Mint Index tables ready.");
            } else {
                log::warn!("Mint Index Plugin running without ClickHouse; data will not be persisted.");
            }
            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_exit(&self, db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move {
            if let Some(db_client) = db {
                for (_slot, pending) in Self::drain_all_pending() {
                    write_pending(Arc::clone(&db_client), pending)
                        .await
                        .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> {
                            Box::new(err)
                        })?;
                }
            }
            Ok(())
        }
        .boxed()
    }
}

async fn write_pending(db: Arc<Client>, pending: PendingSlot) -> Result<(), clickhouse::error::Error> {
    if !pending.mint_txs.is_empty() {
        let mut insert = db.insert::<MintTransactionRow>("mint_transactions").await?;
        for row in &pending.mint_txs {
            insert.write(row).await?;
        }
        insert.end().await?;
    }
    if !pending.mint_accounts.is_empty() {
        let mut insert = db.insert::<MintAccountRow>("mint_accounts").await?;
        for row in &pending.mint_accounts {
            insert.write(row).await?;
        }
        insert.end().await?;
    }
    if !pending.full_txs.is_empty() {
        let mut insert = db.insert::<TransactionFullRow>("transactions_full").await?;
        for row in &pending.full_txs {
            insert.write(row).await?;
        }
        insert.end().await?;
    }
    Ok(())
}

fn clamp_block_time(block_time: Option<i64>) -> u32 {
    match block_time {
        Some(ts) if ts > 0 && ts <= u32::MAX as i64 => ts as u32,
        Some(ts) if ts > u32::MAX as i64 => u32::MAX,
        _ => 0,
    }
}
fn serialize_token_balances(balances: &Option<Vec<TransactionTokenBalance>>) -> String {
    match balances {
        None => "[]".to_string(),
        Some(balances) => {
            let json_balances: Vec<serde_json::Value> = balances
                .iter()
                .map(|balance| {
                    serde_json::json!({
                        "account_index": balance.account_index,
                        "mint": balance.mint.to_string(),
                        "ui_token_amount": {
                            "ui_amount": balance.ui_token_amount.ui_amount,
                            "decimals": balance.ui_token_amount.decimals,
                            "amount": balance.ui_token_amount.amount,
                            "ui_amount_string": balance.ui_token_amount.ui_amount_string,
                        },
                        "owner": balance.owner.to_string(),
                        "program_id": balance.program_id.to_string(),
                    })
                })
                .collect();
            serde_json::to_string(&json_balances).unwrap_or_else(|_| "[]".to_string())
        }
    }
}