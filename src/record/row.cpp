#include "record/row.h"

/**
 * TODO: Student Implement
 */

/**
 *  Row format:
 * -------------------------------------------
 * | Header | Field-1 | ... | Field-N |
 * -------------------------------------------
 *  Header format:
 * --------------------------------------------
 * | Field Nums | Null bitmap |
 * -------------------------------------------
 *
 *
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t offset = 0;
  // Field Nums
  uint32_t fieldNums = GetFieldCount();
  memcpy(buf, &fieldNums, sizeof(uint32_t));  //field Nums
  offset += sizeof(uint32_t);

  if(fieldNums == 0) return offset;

  // 用Null Bitmap标记
  int null_bitmap_size = (fieldNums + 7) / 8;
  char *null_bitmap = new char[null_bitmap_size];
  memset(null_bitmap, 0, null_bitmap_size);
  int index = 0;
  for(auto it = fields_.begin(); it != fields_.end(); it++,index++) {
    if(!((*it)->IsNull())) {
      null_bitmap[index / 8] |= 1 << (7 - (index % 8)); // 从char[0]开始，从高位至低位
    }
  }
  memcpy(buf + offset, null_bitmap, null_bitmap_size*sizeof(char)); // 序列化null bitmap
  offset += null_bitmap_size*sizeof(char);

  delete []null_bitmap;
  for(auto it = fields_.begin(); it != fields_.end(); it++) {
    if(!((*it)->IsNull())) {
      int ofs = (*it)->SerializeTo(buf + offset);
      offset += ofs;
    }
  }
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t offset = 0;
  TypeId type = TypeId::kTypeInvalid;
  
  uint32_t fieldNums = 0;
  memcpy(&fieldNums, buf, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  if(fieldNums == 0) return offset;

  uint32_t null_bitmap_size = (fieldNums + 7) / 8;
  char *null_bitmap = new char[null_bitmap_size];
  memcpy(null_bitmap, buf + offset, null_bitmap_size*sizeof(char));
  offset += null_bitmap_size*sizeof(char);
  for(int i = 0; i < fieldNums; i++){
    type = schema->GetColumn(i)->GetType();
    Field *f = nullptr;
    if(null_bitmap[i / 8] & (1 << (7 - (i % 8)))) {
      if(type == TypeId::kTypeInvalid){
        
      }else if(type == TypeId::kTypeInt){
        int32_t integer_ = 0;
        memcpy(&integer_, buf + offset, sizeof(int32_t));
        offset += sizeof(int32_t);
        f = new Field(type, integer_);
      }else if(type == TypeId::kTypeFloat){
        float float_ = 0;
        memcpy(&float_, buf + offset, sizeof(float));
        offset += sizeof(float);
        f = new Field(type, float_);
      }else{
        uint32_t len_ = 0;
        memcpy(&len_, buf + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        char *data_ = new char[len_];
        memcpy(data_, buf + offset, len_);
        offset += len_;
        f = new Field(type, data_, len_, true);
      }
    }
    fields_.push_back(f);
  }
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  if(schema->GetColumnCount() == 0) return 0;
  if(fields_.empty()) return 0;
  uint32_t fileNums = GetFieldCount();
  uint32_t null_bitmap_size = (fileNums + 7) / 8;
  uint32_t size = 0;
  for(auto it = fields_.begin(); it != fields_.end(); it++) {
    if(!((*it)->IsNull())) {
      size += (*it)->GetSerializedSize();
    }
  }
  return size + sizeof(uint32_t) + null_bitmap_size*sizeof(char);
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
