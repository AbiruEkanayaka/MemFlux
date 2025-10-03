use anyhow::Result;
use crate::types::{AppContext, TransactionStatus};


/// Scans the database and removes dead data versions to reclaim space.
/// A version is "dead" if it was expired by a transaction that has committed,
/// and that transaction is older than any currently active transaction.
///
/// Returns a tuple of (versions_removed, keys_removed).
pub async fn vacuum(ctx: &AppContext) -> Result<(usize, usize)> {
    let tx_status_manager = &ctx.tx_status_manager;
    let tx_id_manager = &ctx.tx_id_manager;

    // 1. Find the vacuum horizon. This is the oldest active transaction ID.
    // Any version expired by a transaction that committed *before* this horizon is safe to remove.
    let vacuum_horizon = tx_status_manager
        .get_active_txids()
        .iter()
        .min()
        .copied()
        .unwrap_or_else(|| tx_id_manager.get_current_txid());

    // Create a special snapshot for vacuuming. xmax = u64::MAX ensures that all committed
    // transactions are considered 'old enough' for visibility checks, effectively making
    // the snapshot see all committed history up to the current point.


    let mut versions_removed = 0;
    let mut keys_to_remove = Vec::new();

    // We iterate over a snapshot of keys to avoid holding the dashmap lock for the whole duration.
    let keys: Vec<String> = ctx.db.iter().map(|e| e.key().clone()).collect();

    for key in keys {
        // Use get() and a write lock on the value to avoid locking the whole map segment.
        if let Some(entry) = ctx.db.get(&key) {
            let version_chain_arc = entry.value().clone();
            drop(entry);
            let mut version_chain = version_chain_arc.write().await;

            let original_len = version_chain.len();
            if original_len == 0 {
                continue;
            }

            // Retain versions that are NOT dead.
            // A version is considered 'alive' (should be retained) if:
            // 1. Its creator transaction is still active (not yet committed or aborted).
            // 2. It is visible according to the vacuum_snapshot (meaning its creator is committed
            //    and its expirer is not committed and old enough to make it invisible).
            version_chain.retain(|version| {
                if version.expirer_txid == 0 {
                    return true; // Not expired, keep.
                }

                let expirer_committed = tx_status_manager.get_status(version.expirer_txid) == Some(TransactionStatus::Committed);
                if !expirer_committed {
                    return true; // Expiring transaction not committed, keep.
                }

                if version.expirer_txid >= vacuum_horizon {
                    return true; // Expired too recently, keep.
                }

                // It's dead, remove it.
                false
            });

            versions_removed += original_len - version_chain.len();

            if version_chain.is_empty() {
                keys_to_remove.push(key.clone());
            }
        }
    }

    let mut keys_removed_count = 0;
    for key in keys_to_remove {
        if ctx.db.remove_if(&key, |_, v| v.try_read().map_or(false, |g| g.is_empty())).is_some() {
            keys_removed_count += 1;
        }
    }

    Ok((versions_removed, keys_removed_count))
}
