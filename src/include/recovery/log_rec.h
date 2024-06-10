#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
  kInvalid,
  kInsert,
  kDelete,
  kUpdate,
  kBegin,
  kCommit,
  kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

struct LogRec {
  LogRec() = default;

  LogRec(LogRecType type, lsn_t lsn, txn_id_t txn_id, lsn_t prev_lsn)
      : type_(type), lsn_(lsn), txn_id_(txn_id), prev_lsn_(prev_lsn) {}

  LogRecType type_{LogRecType::kInvalid};
  lsn_t lsn_{INVALID_LSN};
  txn_id_t txn_id_{INVALID_TXN_ID};
  lsn_t prev_lsn_{INVALID_LSN};

  /**
   * The <key, value> pairs
   * For `insert`, the <new_key, new_val> pair is used.
   * For `delete`, the <old_key, old_val> pair is used.
   * For `update`, the <old_key, old_val> pair is used for the old record, and the <new_key, new_val> pair is used for the new record.
   */
  KeyType old_key_{};
  ValType old_val_{};
  KeyType new_key_{};
  ValType new_val_{};

  /* used for testing only */
  static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
  static lsn_t next_lsn_;

  /**
   * Get the previous lsn of the transaction from the map and update it with the current lsn. If not found, insert a new entry.
   * @param txn_id The transaction id.
   * @param cur_lsn The current lsn of the transaction.
   * @return The previous lsn of the transaction.
   * @note If the previous lsn is not found, it will be set to INVALID_LSN.
   */
  static lsn_t GetAndUpdatePrevLSN(txn_id_t txn_id, lsn_t cur_lsn) {
    lsn_t prev_lsn;
    prev_lsn = (prev_lsn_map_.count(txn_id)? prev_lsn_map_[txn_id] : INVALID_LSN);
    prev_lsn_map_[txn_id] = cur_lsn;
    return prev_lsn;
  }
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * Create a new insert log record.
 * @param txn_id The transaction id.
 * @param ins_key The key of the inserted record.
 * @param ins_val The value of the inserted record.
 * @return A shared pointer to the log record.
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
  lsn_t lsn = LogRec::next_lsn_++;
  LogRecPtr log = std::make_shared<LogRec>(LogRecType::kInsert, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
  log->new_key_ = std::move(ins_key);
  log->new_val_ = ins_val;
  return log;
}

/**
 * Create a new delete log record.
 * @param txn_id The transaction id.
 * @param del_key The key of the deleted record.
 * @param del_val The value of the deleted record.
 * @return A shared pointer to the log record.
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
  lsn_t lsn = LogRec::next_lsn_++;
  LogRecPtr log = std::make_shared<LogRec>(LogRecType::kDelete, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
  log->old_key_ = std::move(del_key);
  log->old_val_ = del_val;
  return log;
}

/**
 * Create a new update log record.
 * @param txn_id The transaction id.
 * @param old_key The key of the old record.
 * @param old_val The value of the old record.
 * @param new_key The key of the new record.
 * @param new_val The value of the new record.
 * @return A shared pointer to the log record.
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
  lsn_t lsn = LogRec::next_lsn_++;
  LogRecPtr log = std::make_shared<LogRec>(LogRecType::kUpdate, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
  log->old_key_ = std::move(old_key);
  log->old_val_ = old_val;
  log->new_key_ = std::move(new_key);
  log->new_val_ = new_val;
  return log;
}

/**
 * Create a new begin log record.
 * @param txn_id The transaction id.
 * @return A shared pointer to the log record.
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
  lsn_t lsn = LogRec::next_lsn_++;
  return std::make_shared<LogRec>(LogRecType::kBegin, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
}

/**
 * Create a new commit log record.
 * @param txn_id The transaction id.
 * @return A shared pointer to the log record.
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
  lsn_t lsn = LogRec::next_lsn_++;
  return std::make_shared<LogRec>(LogRecType::kCommit, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
}

/**
 * Create a new abort log record.
 * @param txn_id The transaction id.
 * @return A shared pointer to the log record.
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
  lsn_t lsn = LogRec::next_lsn_++;
  return std::make_shared<LogRec>(LogRecType::kAbort, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
}

#endif  // MINISQL_LOG_REC_H
