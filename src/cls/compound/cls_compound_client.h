#ifndef CEPH_CLS_COMPOUNT_CLIENT_H
#define CEPH_CLS_COMPOUNT_CLIENT_H

#include "include/rados/librados.hpp"
#include <string>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/locks.hpp>

namespace librados {
namespace cls_compound_client {


struct CompoundInfo {
public:
  std::string pool_name_;
  int compound_obj_num_; //2^N
  size_t stripe_size_;
  CompoundInfo(const std::string& pool_name, int compound_obj_num, size_t stripe_size) {
    pool_name_ = pool_name;
    compound_obj_num_ = compound_obj_num;
    stripe_size_ = stripe_size;
  }
  CompoundInfo() {}
};


/********************
name_len, uint, 4B
name, string, {name_len}B, with idx suffix

magic num, int, 4B (0x416e4574)
obj_total_len, uint64, 8B
stripe_num, int, 4B
stripe_idx, int, 4B
create_time, int, 4B
stripe_len, uint, 4B
stripe_flag, int, 4B
data_crc32, uint, 4B
stripe_data, string, {stripe_len}B
********************/

struct MetaInfo {
public:
  int magic_num_;
  uint64_t obj_total_len_;
  unsigned stripe_len_;
  int stripe_flag_;
  int stripe_num_;
  int stripe_idx_;
  int create_time_;
  unsigned data_crc32_;
  MetaInfo() {}
  MetaInfo(int magic_num, uint64_t obj_total_len, unsigned stripe_len, int stripe_flag, 
	int stripe_num, int stripe_idx, int create_time, unsigned data_crc32) {
    magic_num_ = magic_num;
    obj_total_len_ = obj_total_len;
    stripe_len_ = stripe_len;
    stripe_flag_ = stripe_flag;
    stripe_num_ = stripe_num;
    stripe_idx_ = stripe_idx;
    create_time_ = create_time;
    data_crc32_ = data_crc32;
  }
};


class CompoundIoCtx {
public:
  IoCtx io_ctx_;

  CompoundIoCtx(const CompoundInfo& info) {
    info_ = info;
  }
  void Close() {
    io_ctx_.close();
  }

  int WriteFullObj(const std::string& oid, bufferlist& bl, int create_time = GetCurrentTime());
  int Write(const std::string& oid, bufferlist& bl, uint64_t off, int create_time = GetCurrentTime());
  int WriteFinish(const std::string& oid, uint64_t total_size, int create_time = GetCurrentTime());

  int Read(const std::string& oid, bufferlist& bl, size_t len, uint64_t off);
  int ReadFullObj(const std::string& oid, bufferlist& bl, int* create_time = NULL);

  int Stat(const std::string& oid, uint64_t *psize, time_t *pmtime, MetaInfo* meta = NULL);

  int Remove(const std::string& oid);

  size_t GetPoolStripeSize();

  std::string GetFirstCompoundObjId(const std::string& oid);
  //only used when pool init
  int CreateAllCompObj();

  static int GetCurrentTime();

  ~CompoundIoCtx() {
    io_ctx_.close();
  }

private:
  CompoundInfo info_;

  void GetStripeName(const std::string& oid, int idx, std::string& stripe_name);
  std::string GetCompObjId(const std::string& oid);
  void PrependMeta(bufferlist& bl, const std::string& stripe_name, int idx, int num, uint64_t obj_total_len, int create_time);
  int ParseAndVerify(const std::string& stripe_name, bufferlist& bl, MetaInfo & meta, bufferlist* data_bl);
};

typedef boost::shared_ptr<CompoundIoCtx> CompoundIoCtxPtr;

class CompoundIoCtxFactory {
  public:
  CompoundIoCtxPtr GetCompoundIoCtx(const std::string& pool_name);
  static boost::shared_ptr<CompoundIoCtxFactory> GetInstance();

  private:
  std::map<std::string, CompoundIoCtxPtr> ioctx_map_;
  static boost::shared_ptr<CompoundIoCtxFactory> instance_;
  static boost::mutex instance_mutex_;
  static boost::mutex clients_mutex_;

  CompoundIoCtxFactory() {};
  CompoundIoCtxFactory(const CompoundIoCtxFactory&);
  CompoundIoCtxFactory& operator=(const CompoundIoCtxFactory&);
};

CompoundIoCtxPtr GetCompoundIoCtx(const std::string& pool_name);

}}

#endif

