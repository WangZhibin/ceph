#ifndef CEPH_CLS_DIR_COMPOUNT_CLIENT_H
#define CEPH_CLS_DIR_COMPOUNT_CLIENT_H

#include "include/rados/librados.hpp"
#include "cls_compound_common.h"
#include <string>
#include <vector>
#include <tr1/functional>
#include <tr1/unordered_map>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/locks.hpp>

namespace librados {
namespace cls_dir_compound_client {

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

typedef std::tr1::function<void (const std::string&, std::string&, std::string&)> HashCallback;
typedef std::tr1::unordered_map<std::string, bufferlist> String2BufferlistHMap;
typedef std::tr1::unordered_map<std::string, int> String2IntHMap;

class DirCompoundIoCtx {
public:
  IoCtx io_ctx_;

  DirCompoundIoCtx(const HashCallback* cb = NULL, size_t stripe_size = kDefaultStripeSize) {
    if (cb) {
      cb_ = *cb;
    } else {
      cb_ = DefaultHash; 
      
    }
    stripe_size_ = stripe_size;
  }
  void Close() {
    io_ctx_.close();
  }

  int WriteFullObj(const std::string& oid, bufferlist& bl, int create_time = GetCurrentTime());
  int BatchWriteFullObj(const String2BufferlistHMap& oid2data, int create_time = GetCurrentTime());
  int BatchWriteFullObj(const String2BufferlistHMap& oid2data, const String2IntHMap& oid2create_time);
  int Write(const std::string& oid, bufferlist& bl, uint64_t off, int create_time = GetCurrentTime());
  int WriteFinish(const std::string& oid, uint64_t total_size, int create_time = GetCurrentTime());

  int Read(const std::string& oid, bufferlist& bl, size_t len, uint64_t off);
  int ReadFullObj(const std::string& oid, bufferlist& bl, int* create_time = NULL);

  int Stat(const std::string& oid, uint64_t *psize, time_t *pmtime, MetaInfo* meta = NULL);

  int Remove(const std::string& oid);

  size_t GetPoolStripeSize();

  std::string GetFirstCompoundObjId(const std::string& oid);

  int GetStripeCompoundInfo(const std::string& oid, std::vector<std::string>& comp_oids, std::vector<uint64_t>& offset, std::vector<unsigned>& size);
  //only used when pool init
  int CreateAllCompObj();

  static int GetCurrentTime();

  ~DirCompoundIoCtx() {
    io_ctx_.close();
  }

private:
  HashCallback cb_;
  size_t stripe_size_;

  void GetStripeName(const std::string& oid, int idx, std::string& stripe_name);
  void GetCompObjId(const std::string& oid, int idx, std::string& comp_obj_id);
  static void DefaultHash(const std::string& oid, std::string& path, std::string& file);
  void PrependMeta(bufferlist& bl, const std::string& stripe_name, int idx, int num, uint64_t obj_total_len, int create_time);
  int ParseAndVerify(const std::string& stripe_name, bufferlist& bl, MetaInfo & meta, bufferlist* data_bl);
  int PrepareWriteData(const std::string& oid, const bufferlist& bl, uint64_t off, int create_time, std::list<std::pair<std::string, bufferlist> >& write_data);
};

typedef boost::shared_ptr<DirCompoundIoCtx> DirCompoundIoCtxPtr;

class DirCompoundIoCtxFactory {
  public:
  DirCompoundIoCtxPtr GetDirCompoundIoCtx(const std::string& pool_name);
  static boost::shared_ptr<DirCompoundIoCtxFactory> GetInstance();

  private:
  std::map<std::string, DirCompoundIoCtxPtr> ioctx_map_;
  static boost::shared_ptr<DirCompoundIoCtxFactory> instance_;
  static boost::mutex instance_mutex_;
  static boost::mutex clients_mutex_;

  DirCompoundIoCtxFactory() {};
  DirCompoundIoCtxFactory(const DirCompoundIoCtxFactory&);
  DirCompoundIoCtxFactory& operator=(const DirCompoundIoCtxFactory&);
};

DirCompoundIoCtxPtr GetDirCompoundIoCtx(const std::string& pool_name);

}}

#endif

