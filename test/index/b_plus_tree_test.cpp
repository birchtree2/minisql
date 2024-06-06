#include "index/b_plus_tree.h"

#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  std::vector<Column *> columns = {
      new Column("int", TypeId::kTypeInt, 0, false, false),
  };
  Schema *table_schema = new Schema(columns);
  KeyManager KP(table_schema, 16);
  BPlusTree tree(0, engine.bpm_, KP);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 100000;
  // const int n=200;
  vector<GenericKey *> keys;
  vector<RowId> values;
  vector<GenericKey *> delete_seq;
  map<GenericKey *, RowId> kv_map;
  for (int i = 0; i < n; i++) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    keys.push_back(key);
    values.push_back(RowId(i));
    delete_seq.push_back(key);
  }
  vector<GenericKey *> keys_copy(keys);
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  for (int i = 0; i < n; i++) {
    tree.Insert(keys[i], values[i]);
    // if(i==168)  tree.PrintTree(mgr[0], table_schema);
    // if(i==169)  tree.PrintTree(mgr[1], table_schema);
    // LOG(INFO)<<i;
  }
  ASSERT_TRUE(tree.Check());
  // Print tree
  tree.PrintTree(mgr[1], table_schema);
  // Search keys
  vector<RowId> ans;
  for (int i = 0; i < n; i++) {
    // LOG(INFO)<<"expected"<<kv_map[keys_copy[i]].GetPageId()<<" "<<kv_map[keys_copy[i]].GetSlotNum();
    tree.GetValue(keys_copy[i], ans);
    
    ASSERT_EQ(kv_map[keys_copy[i]], ans[i]);
    // LOG(INFO)<<"ok "<<i;
  }
  ASSERT_TRUE(tree.Check());
  LOG(INFO)<<"search and delete ok";
  // Delete half keys
  auto printKey=[&](GenericKey *fkey)->void{//用来debug的函数
    Row ans;
    KP.DeserializeToKey(fkey, ans, table_schema);
    LOG(INFO) <<"remove: " <<ans.GetField(0)->toString();
  };
  for (int i = 0; i < n / 2; i++) {
    // printKey(delete_seq[i]);
    tree.Remove(delete_seq[i]);
  }
  LOG(INFO)<<"remove ok";
  tree.PrintTree(mgr[2], table_schema);

  // Check valid
  ans.clear();
  for (int i = 0; i < n / 2; i++) {
    // LOG(INFO)<<i;
    // printKey(delete_seq[i]);
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  for (int i = n / 2; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
}