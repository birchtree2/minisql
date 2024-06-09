#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  // return 0;
  return (sizeof(uint32_t)*3 + table_meta_pages_.size()*(sizeof(table_id_t) + sizeof(page_id_t)) + index_meta_pages_.size()*(sizeof(index_id_t)+sizeof(page_id_t)));
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
      if(init){
        catalog_meta_ = CatalogMeta::NewInstance();
        buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
      }else{
        catalog_meta_ = CatalogMeta::DeserializeFrom(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());
        buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, false);
        for(auto it : catalog_meta_->table_meta_pages_){
          if(LoadTable(it.first, it.second) != DB_SUCCESS){
            throw std::runtime_error("Failed to load table");
          }
        }
        for(auto it : catalog_meta_->index_meta_pages_){
          if(LoadIndex(it.first, it.second) != DB_SUCCESS){
            throw std::runtime_error("Failed to load index");
          }
        }
        buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
        FlushCatalogMetaPage();
      }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  if (table_names_.find(table_name) != table_names_.end()) return DB_TABLE_ALREADY_EXIST;
  table_id_t table_id = catalog_meta_->GetNextTableId();
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr) return DB_FAILED;
  table_names_[table_name] = table_id;
  catalog_meta_->table_meta_pages_[table_id] = page_id;

  Schema *tmp_schema = Schema::DeepCopySchema(schema);
  auto table_meta = TableMetadata::Create(table_id, table_name, page_id, tmp_schema);
  table_meta->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(page_id, true);
  auto table_heap = TableHeap::Create(buffer_pool_manager_, tmp_schema, txn, log_manager_, lock_manager_);
  table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  tables_[table_id] = table_info;

  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
  // return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if (table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  table_id_t table_id = table_names_[table_name];
  table_info = tables_[table_id];
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for (auto it : tables_) {
    tables.push_back(it.second);
  }
  return DB_SUCCESS;
  // return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  if(table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;  
  if(index_names_.find(table_name) != index_names_.end() && index_names_[table_name].find(index_name) != index_names_[table_name].end()) return DB_INDEX_ALREADY_EXIST;
  
  table_id_t table_id = table_names_[table_name];
  auto table_info = tables_[table_id];
  auto table_schema = table_info->GetSchema();
  index_id_t index_id;
  std::vector<uint32_t> key_map;
  for(auto it : index_keys){
    if (table_schema->GetColumnIndex(it, index_id) == DB_COLUMN_NAME_NOT_EXIST) return DB_COLUMN_NAME_NOT_EXIST;
    else key_map.push_back(index_id);
  }

  index_id = catalog_meta_->GetNextIndexId();
  table_id = table_names_[table_name];
  page_id_t index_page_id;
  auto index_meta = IndexMetadata::Create(index_id, index_name, table_id, key_map);
  auto index_meta_page = buffer_pool_manager_->NewPage(index_page_id);
  index_meta->SerializeTo(index_meta_page->GetData());

  index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);

  (catalog_meta_->index_meta_pages_)[index_id] = index_page_id;
  indexes_[index_id] = index_info;
  index_names_[table_name][index_name] = index_id;

  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  return DB_SUCCESS;
  // return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if(table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  if(index_names_.at(table_name).find(index_name) == index_names_.at(table_name).end()) return DB_INDEX_NOT_FOUND;
  auto index_name_index = index_names_.find(table_name)->second.find(index_name);
  auto index_id = index_name_index->second;
  index_info = indexes_.find(index_id)->second;
  return DB_SUCCESS;
  // return DB_FAILED
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  if(index_names_.find(table_name) == index_names_.end()) return DB_TABLE_NOT_EXIST;
  for(auto it : index_names_.find(table_name)->second){
    indexes.push_back(indexes_.find(it.second)->second);
  }
  return DB_SUCCESS;
  // return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if(table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  
  auto table_id = table_names_[table_name];
  table_names_.erase(table_name);
  tables_.erase(table_id);
  // TableInfo *tmp=tables_[table_id];
  if(catalog_meta_->table_meta_pages_.find(table_id) != catalog_meta_->table_meta_pages_.end()){
    // buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_.at(table_id));
    catalog_meta_->table_meta_pages_.erase(table_id);
    FlushCatalogMetaPage();
  }
  // buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_.at(table_id));
  //???上面这里是什么意思
  return DB_SUCCESS;
  // return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if(index_names_.find(table_name) == index_names_.end()) return DB_TABLE_NOT_EXIST;
  if(index_names_.find(table_name)->second.find(index_name) == index_names_.find(table_name)->second.end()) return DB_INDEX_NOT_FOUND;
  index_id_t index_id = index_names_.find(table_name)->second.find(index_name)->second;
  index_names_.at(table_name).erase(index_name);
  indexes_.erase(index_id);
  if(catalog_meta_->index_meta_pages_.find(index_id) != catalog_meta_->index_meta_pages_.end()){
    // buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_.at(index_id));
    catalog_meta_->index_meta_pages_.erase(index_id);
    FlushCatalogMetaPage();
  }
  // buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_.at(index_id));
  return DB_SUCCESS;
  // return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  auto meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  if (!buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)) return DB_FAILED;
  return DB_SUCCESS;
  // return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  if(tables_.find(table_id) != tables_.end()) return DB_TABLE_ALREADY_EXIST;
  auto table_page = buffer_pool_manager_->FetchPage(page_id);
  if(table_page == nullptr) return DB_FAILED;

  TableMetadata *table_meta;
  TableMetadata::DeserializeFrom(table_page->GetData(), table_meta);
  table_names_[table_meta->GetTableName()] = table_id;

  auto table_info = TableInfo::Create();
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), table_meta->GetSchema(), log_manager_, lock_manager_);
  table_info->Init(table_meta, table_heap);
  tables_[table_id] = table_info;
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
  // return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  if(indexes_.find(index_id) != indexes_.end()) return DB_INDEX_ALREADY_EXIST;
  auto index_page = buffer_pool_manager_->FetchPage(page_id);
  if(index_page == nullptr) return DB_FAILED;

  IndexMetadata *index_meta;
  IndexMetadata::DeserializeFrom(index_page->GetData(), index_meta);
  table_id_t table_id = index_meta->GetTableId();
  string table_name = tables_[table_id]->GetTableName();
  string index_name = index_meta->GetIndexName();
  index_names_[table_name][index_name] = index_id;

  auto index_info = IndexInfo::Create();
  index_info->Init(index_meta, tables_[table_id], buffer_pool_manager_);
  indexes_[index_id] = index_info;
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
  // return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if(tables_.find(table_id) == tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info = tables_[table_id];
  return DB_SUCCESS;
}