#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t offset = 0;
  memcpy(buf + offset, &COLUMN_MAGIC_NUM, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  uint32_t name_len = name_.size();
  memcpy(buf + offset, &name_len, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  const char *name_buf = name_.c_str();
  memcpy(buf + offset, name_buf, name_len);
  offset += name_len;

  memcpy(buf + offset, &type_, sizeof(TypeId));
  offset += sizeof(TypeId);

  // if (type_ == TypeId::kTypeChar) {
  //   memcpy(buf + offset, &len_, sizeof(uint32_t));
  //   offset += sizeof(uint32_t);
  // } else {
  //   memcpy(buf + offset, &len_, sizeof(int32_t));
  //   offset += sizeof(int32_t);
  // }
  memcpy(buf + offset, &len_, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  memcpy(buf + offset, &table_ind_, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  memcpy(buf + offset, &nullable_, sizeof(bool));
  offset += sizeof(bool);

  memcpy(buf + offset, &unique_, sizeof(bool));
  offset += sizeof(bool);

  return offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  uint32_t size = 0;
  size += 4*sizeof(uint32_t) + sizeof(TypeId) + 2*sizeof(bool) + name_.size();
  // replace with your code here
  // return 0;
  return size;//fix
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t magic_num;
  // std::string name;
  TypeId type;
  uint32_t len;
  uint32_t table_ind;
  bool nullable;
  bool unique;

  uint32_t offset = 0;

  memcpy(&magic_num, buf, sizeof(uint32_t));
  if (magic_num != COLUMN_MAGIC_NUM) {
    LOG(ERROR) << "Wrong magic number." << std::endl;
  }
  offset += sizeof(uint32_t);

  uint32_t nameSize = 0;
  memcpy(&nameSize, buf + offset, sizeof(uint32_t));
  char * name_buf = new char[nameSize + 1];
  offset += sizeof(uint32_t);
  memcpy(name_buf, buf + offset, nameSize);
  name_buf[nameSize] = '\0';
  // std::string name = name_buf(name_buf, nameSize);
  std::string name = std::string(name_buf);
  delete[] name_buf;
  offset += nameSize;

  memcpy(&type, buf + offset, sizeof(TypeId));
  offset += sizeof(TypeId);
  memcpy(&len, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  memcpy(&table_ind, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  memcpy(&nullable, buf + offset, sizeof(bool));
  offset += sizeof(bool);
  memcpy(&unique, buf + offset, sizeof(bool));
  offset += sizeof(bool);

  if(type == TypeId::kTypeChar) {
    column = new Column(name, type, len, table_ind, nullable, unique);
  } else {
    column = new Column(name, type, table_ind, nullable, unique);
  }
  // replace with your code here
  return offset;
}
