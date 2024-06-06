#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  memcpy(buf + offset, &SCHEMA_MAGIC_NUM, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  uint32_t column_count=GetColumnCount();
  //记录column数量，便于反序列化的时候读出vector中数据
  memcpy(buf + offset, &column_count, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  //对vector中每个column序列化
  for(int i=0;i<(int)column_count;i++){
    int d=columns_[i]->SerializeTo(buf+offset);
    offset+=d;
  }
  //TODO: is_manage变量?
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size=0;
  size+=2*sizeof(uint32_t);
  for(int i=0;i<(int)columns_.size();i++){
    size+=columns_[i]->GetSerializedSize();
  }
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t offset=0;
  uint32_t magic;
  memcpy(&magic,buf+offset,sizeof(magic));
  offset+=sizeof(magic);
  if (magic != SCHEMA_MAGIC_NUM) {
    LOG(ERROR) << "Wrong magic number." << std::endl;
  }
  uint32_t column_count;
  memcpy(&column_count,buf+offset,sizeof(column_count));
  offset+=sizeof(column_count);
  std::vector<Column*> tmp;
  tmp.reserve(column_count);//reduce allocation time
  for(int i=0;i<(int)column_count;i++){
    Column *p;
    offset+=Column::DeserializeFrom(buf+offset,p);
    tmp.push_back(p);
  }
  schema=new Schema(tmp,true);
  return offset;
}