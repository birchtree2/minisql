#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
  lsn_t checkpoint_lsn_{INVALID_LSN}; // The lsn of the last log record that has been persisted at this checkpoint.
  ATT active_txns_{}; // The active transactions at this checkpoint.
  KvDatabase persist_data_{}; // The data that has been persisted at this checkpoint.

  inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

  inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
 public:
  /**
   * Initialize the recovery manager with the last checkpoint.
   * @param last_checkpoint the last checkpoint
   */
  void Init(CheckPoint &last_checkpoint) {
    persist_lsn_ = last_checkpoint.checkpoint_lsn_;
    active_txns_ = last_checkpoint.active_txns_;
    data_ = last_checkpoint.persist_data_;
    return ;
  }

  /**
   * Rollback a single transaction.
   * @param txn_id the transaction to rollback.
   */
  void Rollback(txn_id_t txn_id) {
    lsn_t last_log_lsn = active_txns_[txn_id]; // Get the last log record of the active transaction.
    while (last_log_lsn != INVALID_LSN) {
      LogRecPtr log_rec = log_recs_[last_log_lsn];
      if (log_rec == nullptr) break;
      // Undo the log record.
      switch (log_rec->type_) {
        case LogRecType::kInsert: // Undo insert, erase the inserted key-value pair.
          data_.erase(log_rec->new_key_);
          break;
        case LogRecType::kDelete: // Undo delete, insert the deleted key-value pair.
          data_[log_rec->old_key_] = log_rec->old_val_;
          break;
        case LogRecType::kUpdate: // Undo update, insert the old key-value pair and erase the new key-value pair.
          data_.erase(log_rec->new_key_);
          data_[log_rec->old_key_] = log_rec->old_val_;
          break;
        default:
          break;
      }
      last_log_lsn = log_rec->prev_lsn_; // Undo the next.
    }
    return ;
  }

  /**
   * Redo a single log record depended on its type.
   * @param log_rec the log record to redo.
   */
  void Redo(LogRecPtr log_rec) {
    switch (log_rec->type_) {
      case LogRecType::kInsert: // Redo insert.
        data_[log_rec->new_key_] = log_rec->new_val_;
        break;
      case LogRecType::kDelete: // Redo delete.
        data_.erase(log_rec->old_key_);
        break;
      case LogRecType::kUpdate: // Redo update.
        data_.erase(log_rec->old_key_);
        data_[log_rec->new_key_] = log_rec->new_val_;
        break;
      case LogRecType::kAbort: // The transaction is aborted, need to rollback.
        Rollback(log_rec->txn_id_);
        [[fallthrough]];
      case LogRecType::kCommit: // Already commited log record, remove from active transactions.
        active_txns_.erase(log_rec->txn_id_);
        break;
      default: // Nothing to do with other log record types.
        break;
    }
    return ; 
  }

  /**
   * Redo all the log records that are after the last checkpoint.
   */
  void RedoPhase() {
    for (const auto& log: log_recs_) {
      if (log.first < persist_lsn_) continue; // Already persisted data, skip.
      // Find the 1st log record that is after persist_lsn_, need to redo.
      active_txns_[log.second->txn_id_] = log.second->lsn_; // Add active transaction.
      Redo(log.second);
    }
  }

  /**
   * Undo all the active transactions that are after the last checkpoint.
   */
  void UndoPhase() {
    for (const auto& atts : active_txns_) {
      Rollback(atts.first);
    }
    active_txns_.clear(); // Undo completed.
  }

  // used for test only
  void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

  // used for test only
  inline KvDatabase &GetDatabase() { return data_; }

 private:
  std::map<lsn_t, LogRecPtr> log_recs_{};
  lsn_t persist_lsn_{INVALID_LSN}; // The last lsn that has been persisted.
  ATT active_txns_{}; // Logs that is not persisted yet.
  KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
