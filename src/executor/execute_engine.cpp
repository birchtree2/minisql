#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(db_name.c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if(ast->child_==nullptr) return DB_FAILED;
  if(dbs_.find(current_db_)==dbs_.end()){
    cout<<"No database selected";
    return DB_NOT_EXIST;
  }
  std::string table_name=ast->child_->val_;//第一个儿子就是表名
  pSyntaxNode columns_node=ast->child_->next_->child_;
  //找到column definition 
  pSyntaxNode node=columns_node;
  vector<string>primary_keys,unique_keys;
  vector<Column*> columns;
  while(node!= nullptr){
    //向右走，找到primary key
    if(node->type_==kNodeColumnList&&string(node->val_)=="primary keys"){
      pSyntaxNode primary_node=node->child_;
      while(primary_node!= nullptr){
        primary_keys.push_back(string(primary_node->val_));
        primary_node=primary_node->next_;
      }
    }
    node=node->next_;
  }
  int col_id=0;
  node=columns_node;//记得回到起点！
  while(node!=nullptr){
    if(node->type_==kNodeColumnDefinition){
      pSyntaxNode identifier=node->child_;//kNodeIdentifier
      string name=identifier->val_;
      string type=identifier->next_->val_;
      Column *column;
      bool unique=false;
      if(node->val_!=nullptr&&string(node->val_)=="unique"){
        unique=true;
        unique_keys.push_back(name);
      }
      if(type=="int")column=new Column(name,kTypeInt,col_id,true,unique);
      else if(type=="float")column=new Column(name,kTypeFloat,col_id,true,unique);
      else if(type=="char"){
        pSyntaxNode lennode=identifier->next_->child_;//char(16) 后面的16
        if(lennode==nullptr) return DB_FAILED;
        string len=lennode->val_;
        for(char c:len){//检查长度字符串是否合法
          if(!isdigit(c))return DB_FAILED;
        }
        if(stoi(len)<0)return DB_FAILED;//char(-10)
        column=new Column(name,kTypeChar,stoi(len),col_id,true,unique);
      }
      col_id++;
      columns.push_back(column);
      
    }
    node=node->next_;
  }
  Schema* schema=new Schema(columns);//创建schema
  TableInfo* tableinfo;
  CatalogManager* catalog=context->GetCatalog();
  dberr_t err=catalog->CreateTable(table_name,schema,context->GetTransaction(),tableinfo);
  if(err!=DB_SUCCESS) return err;
  //创建index
  if(unique_keys.size()){
    for(auto s : unique_keys){
      IndexInfo *index_info;
      catalog->CreateIndex(table_name, "UNIQUE_"+s + "_"+"ON_" + table_name, 
        unique_keys, context->GetTransaction(), index_info, "btree");
    }
  }
  if(primary_keys.size()){
    IndexInfo *index_info;
    string s="";
    for(auto t : primary_keys) s+=t+"_";
    catalog->CreateIndex(table_name, "PK_"+s + "_"+"ON_" + table_name, 
        primary_keys, context->GetTransaction(), index_info, "btree");//用primary key建index
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
 if(current_db_.empty())return DB_FAILED;
  auto catalog=context->GetCatalog();
  string table_name(ast->child_->val_);
  dberr_t res=catalog->DropTable(table_name);//先drop table
  if(res!=DB_SUCCESS)return res;
  vector<IndexInfo*>indexes;
  catalog->GetTableIndexes(table_name,indexes);
  for(auto it:indexes){//再drop index
      // cout<<it->GetIndexName();
      catalog->DropIndex(table_name,it->GetIndexName());
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if(current_db_.empty())return DB_NOT_EXIST;
  CatalogManager* catalog=context->GetCatalog();
  vector<TableInfo*>tables;
  catalog->GetTables(tables);
  vector<IndexInfo*>indexes;
  for(auto it:tables){
    catalog->GetTableIndexes(it->GetTableName(),indexes);
  }
  std::stringstream ss;
  ResultWriter writer(ss);
  vector<int>width;
  width.push_back(10);
  for(auto it:indexes)
    width[0]=max(width[0],(int)it->GetIndexName().length());
  writer.Divider(width);
  writer.BeginRow();
  writer.WriteHeaderCell("Index",width[0]);
  writer.EndRow();
  writer.Divider(width);
  for(auto it:indexes){
    writer.BeginRow();
    writer.WriteCell(it->GetIndexName(),width[0]);
    writer.EndRow();
  }
  writer.Divider(width);
  std::cout<<writer.stream_.rdbuf();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if(current_db_.empty()) return DB_NOT_EXIST;//
  CatalogManager* catalog=context->GetCatalog();
  pSyntaxNode node=ast->child_;
  string index_name=node->val_;
  node=node->next_;
  string table_name=node->val_;
  node=node->next_->child_;//到columnlist
  vector<string>index_keys;
  while(node!= nullptr){
    index_keys.push_back(string(node->val_));
    node=node->next_;
    }
  IndexInfo* index_info;
  TableInfo* table_info;
  auto ret=catalog->CreateIndex(table_name,index_name,index_keys,context->GetTransaction(),index_info,"btree");
  if(ret!=DB_SUCCESS)return ret;
  //遍历堆表
  catalog->GetTable(table_name,table_info);//获取
  auto heap=table_info->GetTableHeap();
  Row row,key_row;
  TableIterator it=heap->Begin(context->GetTransaction());
  while(it!=heap->End()){
    row=*it;
    //提取出row里面作为key的部分
    row.GetKeyFromRow(table_info->GetSchema(),index_info->GetIndexKeySchema(),key_row);
    index_info->GetIndex()->InsertEntry(key_row,row.GetRowId(),context->GetTransaction());
    it++;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if(current_db_.empty()) return DB_NOT_EXIST;
  auto catalog=context->GetCatalog();
  string index_name=ast->child_->val_;
  vector<TableInfo*>tables;
  auto res=DB_INDEX_NOT_FOUND;
  catalog->GetTables(tables);
  for(auto it:tables)
    if(catalog->DropIndex(it->GetTableName(),index_name)==DB_SUCCESS)res=DB_SUCCESS;
  return res;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */

extern "C" {
int yyparse(void);//为了调用
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}


dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  auto start_time = std::chrono::system_clock::now();
  string filename=ast->child_->val_;
  ifstream fin;
  fin.open(filename);
  if(!fin.is_open()){
    printf("file %s not exist",filename.c_str());
    return DB_FAILED;
  }
  char cmd[1024];
  int total_inst=0,succ_inst=0;
  while(!fin.eof()){
    memset(cmd,0,sizeof(cmd));
    int ptr=0;
    char ch;
    while(!fin.eof()){
      ch=fin.get();
      if(ch!=EOF)cmd[ptr++]=ch;
      if(ch==';') break;
    }
    if(ch==EOF) break;
    total_inst++;
    // printf("%s\n",cmd);
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
      if (bp == nullptr) {
        LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
        exit(1);
      }
      yy_switch_to_buffer(bp);

      // init parser module
      MinisqlParserInit();

      // parse
      yyparse();

      // parse result handle
      if (MinisqlParserGetError()) {
        // error
        printf("%s\n", MinisqlParserGetErrorMessage());
      } else {
        // Comment them out if you don't need to debug the syntax tree
        // printf("[INFO] Sql syntax parse ok!\n");
        // SyntaxTreePrinter printer(MinisqlGetParserRootNode());
        // printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
      }

      auto result = Execute(MinisqlGetParserRootNode());

      // clean memory after parse
      MinisqlParserFinish();
      yy_delete_buffer(bp);
      yylex_destroy();

      // quit condition
      ExecuteInformation(result);
      if(result==DB_SUCCESS||result==DB_QUIT) succ_inst++;
      if(result==DB_QUIT) return DB_QUIT;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  printf("File \"%s\": Total %d insts, Succeeded %d insts (%.4f sec)\n",
  filename.c_str(),total_inst,succ_inst,duration_time/1000);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE _DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
 current_db_="";
 //sexit(0);
 return DB_QUIT;
}
