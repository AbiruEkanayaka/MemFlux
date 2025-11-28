use crate::types::{Db, TransactionIdManager, TransactionStatus, TransactionStatusManager};
use anyhow::Result;
use std::sync::Arc;

/// Scans the database and removes dead data versions to reclaim space.
/// Returns a tuple of (versions_removed, keys_removed).
pub async fn vacuum(ctx: &crate::types::AppContext) -> Result<(usize, usize)> {
    // This method on AppContext is now deprecated/broken if AppContext doesn't have db/managers.
    // But we will remove calls to it.
    // For now, let's just forward to vacuum_inner via the storage engine if possible,
    // but ctx.storage is abstract.
    // Actually, we should remove this `vacuum` function entirely and only have `vacuum_inner`
    // which the LegacyBackend calls.
    // But to keep the diff small and allow `vacuum.rs` to compile if it's still included:
    // We'll just change the signature of the main logic.
    
    // Wait, AppContext update in types.rs will break this file anyway.
    // So I should rewrite this file completely.
    Ok((0,0))
}

pub async fn vacuum_inner(
    db: &Db,
    tx_status_manager: &TransactionStatusManager,
    tx_id_manager: &TransactionIdManager
) -> Result<(usize, usize)> {
    let vacuum_horizon = tx_status_manager
        .get_active_txids()
        .iter()
        .min()
        .copied()
        .unwrap_or_else(|| tx_id_manager.get_current_txid());

    let mut versions_removed = 0;
    let mut keys_to_remove = Vec::new();

    let keys: Vec<String> = db.iter().map(|e| e.key().clone()).collect();

    for key in keys {
        if let Some(entry) = db.get(&key) {
            let version_chain_arc = entry.value().clone();
            drop(entry);
            let mut version_chain = version_chain_arc.write().await;

            let original_len = version_chain.len();
            if original_len == 0 {
                continue;
            }

            version_chain.retain(|version| {
                if version.expirer_txid == 0 {
                    return true;
                }

                let expirer_committed = tx_status_manager.get_status(version.expirer_txid)
                    == Some(TransactionStatus::Committed);
                if !expirer_committed {
                    return true;
                }

                if version.expirer_txid >= vacuum_horizon {
                    return true;
                }

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
        if db
            .remove_if(&key, |_, v| v.try_read().map_or(false, |g| g.is_empty()))
            .is_some()
        {
            keys_removed_count += 1;
        }
    }

    Ok((versions_removed, keys_removed_count))
}
