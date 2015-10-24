#include "include/ceph_hash.h"
#include "cls_compound_client.h"
#include "cls_compound_common.h"
#include "include/encoding.h"
#include <sstream>
#include <math.h>
#include <iostream>


using namespace std;
using namespace librados::cls_compound_client;
using namespace librados;

const std::map<std::string, const struct CompoundInfo>::value_type compound_pools_init[] = {
  std::map<std::string, const struct CompoundInfo>::value_type("VideoUploadTest", CompoundInfo("VideoUploadTest", (4096 * 256), (2 * 1024 * 1024) ) )
}; //never change these digital

const int comp_pool_num = sizeof(compound_pools_init) / sizeof(std::map<std::string, const struct CompoundInfo>::value_type);
const std::map<std::string, const struct CompoundInfo> compound_pools(compound_pools_init, compound_pools_init + comp_pool_num);

CompoundIoCtxPtr librados::cls_compound_client::GetCompoundIoCtx(const string& pool_name) {
  return CompoundIoCtxFactory::GetInstance()->GetCompoundIoCtx(pool_name);
}

/***********************CompoundIoCtxFactory*******************/
boost::shared_ptr<CompoundIoCtxFactory> CompoundIoCtxFactory::instance_;
boost::mutex CompoundIoCtxFactory::instance_mutex_;
boost::mutex CompoundIoCtxFactory::clients_mutex_;

boost::shared_ptr<CompoundIoCtxFactory> CompoundIoCtxFactory::GetInstance() {
  if (!instance_) {
    boost::unique_lock<boost::mutex> lock(instance_mutex_);
    if (!instance_) {
      instance_.reset(new CompoundIoCtxFactory());
    }
  }
  return instance_;
}

CompoundIoCtxPtr CompoundIoCtxFactory::GetCompoundIoCtx(const string& pool_name) {
  if (ioctx_map_.find(pool_name) == ioctx_map_.end()) {
    boost::unique_lock<boost::mutex> lock(clients_mutex_);
    if (ioctx_map_.find(pool_name) == ioctx_map_.end()) {
      std::map<std::string, const struct CompoundInfo>::const_iterator fit = compound_pools.find(pool_name);
      if ( fit != compound_pools.end()) {
        CompoundIoCtxPtr comp_ioctx(new CompoundIoCtx(fit->second));
        ioctx_map_.insert(make_pair(pool_name, comp_ioctx));
      } else {
        fprintf(stderr, "request pool NOT exist!\n");
        exit(-1);
      }
    }
  }
  return ioctx_map_[pool_name];
}
/***********************CompoundIoCtxFactory*******************/


/***********************CompoundIoCtx**************************/
int CompoundIoCtx::CreateAllCompObj() {
  bool exclusive = false;
  list<AioCompletion*> acps;
  for (int i = 0; i < info_.compound_obj_num_; ++i) {
    std::stringstream ss;
    ss << i;

    AioCompletion *acp = Rados::aio_create_completion();
    acps.push_back(acp);


    bufferlist in_bl;
    ::encode(exclusive, in_bl);

    bufferlist out_bl;
    int r = io_ctx_.aio_exec(ss.str(), acp, "compound", "create_comp_obj", in_bl, &out_bl);
    if (r != 0) {
      return r;
    }
  }

  int ret = 0;
  for (list<AioCompletion*>::iterator iter = acps.begin(); iter != acps.end(); ++iter) {
    AioCompletion* acp = *iter;
    acp->wait_for_complete();
    int r = acp->get_return_value();
    if (r < 0) {
      ret = r;
    }
    acp->release();
  }
  return ret;
}


void CompoundIoCtx::GetStripeName(const std::string& oid, int idx, std::string& stripe_name) {
  std::stringstream oss;
  oss << idx;
  stripe_name = oid + "_" + oss.str();
}

int CompoundIoCtx::GetCurrentTime() {
  return (int)time(NULL);
}

int CompoundIoCtx::Write(const std::string& oid, bufferlist& bl, uint64_t off, int create_time) {
  if (off % info_.stripe_size_ != 0) {
    return COMPOUND_WRITE_OFFSET_ERR;
  }

  size_t bl_len = bl.length();
  int idx = off / info_.stripe_size_;
  int num = idx + ceil(bl_len / (info_.stripe_size_ + 0.0));
  uint64_t wr_off = 0;
  while (wr_off < bl_len) {
    size_t stripe_len = ((wr_off + info_.stripe_size_ > bl_len) ? (bl_len - wr_off) : info_.stripe_size_);
    bufferlist stripe_bl;
    stripe_bl.substr_of(bl, wr_off, stripe_len);
    
    std::string stripe_name;
    GetStripeName(oid, idx, stripe_name);

    PrependMeta(stripe_bl, stripe_name, idx, num, bl_len + off, create_time);

    std::string comp_obj_id = GetCompObjId(stripe_name);
    
    bufferlist out_bl;
    int r = io_ctx_.exec(comp_obj_id, "compound", "append_set_xattr", stripe_bl, out_bl);
    if (r != 0) {
      return r;
    }

    wr_off += stripe_len;
    ++idx;
  }
  return 0;
}

int CompoundIoCtx::WriteFullObj(const std::string& oid, bufferlist& bl, int create_time) {
  return Write(oid, bl, 0, create_time);
}


int CompoundIoCtx::WriteFinish(const std::string& oid, uint64_t total_size, int create_time) {
  int num = ceil(total_size / (info_.stripe_size_ + 0.0));
  unsigned wr_meta_off = sizeof(int); //only skip magic_num field
  list<AioCompletion*> acps;

  for (int idx = 0; idx < num; ++idx) {

    std::string stripe_name;
    GetStripeName(oid, idx, stripe_name);

    std::string comp_obj_id = GetCompObjId(stripe_name);

    bufferlist part_meta_bl; //only encode obj_total_len, stripe_num, stripe_idx, create_time;
    ::encode(stripe_name, part_meta_bl);
    ::encode(wr_meta_off, part_meta_bl);
    ::encode(total_size, part_meta_bl);
    ::encode(num, part_meta_bl);
    ::encode(idx, part_meta_bl);
    ::encode(create_time, part_meta_bl);

    bufferlist out_bl;
    AioCompletion *acp = Rados::aio_create_completion();
    acps.push_back(acp);
    int r = io_ctx_.aio_exec(comp_obj_id, acp, "compound", "set_stripe_meta", part_meta_bl, NULL);
    if (r != 0) {
      return r;
    }
  }

  int ret = 0;
  for (list<AioCompletion*>::iterator iter = acps.begin(); iter != acps.end(); ++iter) {
    AioCompletion* acp = *iter;
    acp->wait_for_complete();
    int r = acp->get_return_value();
    if (r < 0) {
      ret = r;
    }
    acp->release();
  }
  return ret;
}

int CompoundIoCtx::Read(const std::string& oid, bufferlist& bl, size_t len, uint64_t off) {
  uint64_t rd_off = off;
  size_t rd_len = 0;
  while (rd_len < len) {
    int idx = floor(rd_off / info_.stripe_size_);
    std::string stripe_name;
    GetStripeName(oid, idx, stripe_name);

    uint64_t rd_stripe_off = rd_off % info_.stripe_size_;
    unsigned rd_stripe_len = info_.stripe_size_ - rd_stripe_off;
    if (rd_len + rd_stripe_len > len) {
      rd_stripe_len = len - rd_len;
    }
    if (rd_stripe_len == info_.stripe_size_) {
      rd_stripe_len = 0; //0 means all: stripe_meta, stripe_data(all)
    } 

    bufferlist info_bl;
    ::encode(stripe_name, info_bl); //string
    ::encode(rd_stripe_off, info_bl); //uint64 
    ::encode(rd_stripe_len, info_bl); //unsigned

    std::string comp_obj_id = GetCompObjId(stripe_name);
    bufferlist out_bl;
    int r = io_ctx_.exec(comp_obj_id, "compound", "read_with_xattr", info_bl, out_bl);
    if (r != 0) {
      return r;
    }

    if (rd_stripe_len == 0) {
      MetaInfo meta_info; 
      bufferlist data_bl;
      r = ParseAndVerify(stripe_name, out_bl, meta_info, &data_bl);
      if (r != 0) {
        return r;
      }
      bl.append(data_bl);
      rd_len += data_bl.length();
      rd_off += data_bl.length();
      if (data_bl.length() < info_.stripe_size_) {
        break;
      }
    } else {
      bl.append(out_bl);
      rd_len += out_bl.length();
      rd_off += out_bl.length();
      if (out_bl.length() < rd_stripe_len) {
        break;
      }
    }
  }
  return 0;
}

int CompoundIoCtx::ReadFullObj(const std::string& oid, bufferlist& bl, int* create_time) {
  uint64_t off = 0;
  size_t len = 0; //0 means all: stripe_meta, stripe_data(all)
  int idx = 0;
  int num = 1; //set 1 at first
 
  while(idx < num) {
    std::string stripe_name;
    GetStripeName(oid, idx, stripe_name);

    bufferlist info_bl;
    ::encode(stripe_name, info_bl); // string
    ::encode(off, info_bl); // uint64
    ::encode(len, info_bl); // unsigned
  
    std::string comp_obj_id = GetCompObjId(stripe_name);
    bufferlist out_bl;
    int r = io_ctx_.exec(comp_obj_id, "compound", "read_with_xattr", info_bl, out_bl);
    if (r != 0) {
      return r;
    }
    
    MetaInfo meta_info; 
    bufferlist data_bl;
    r = ParseAndVerify(stripe_name, out_bl, meta_info, &data_bl);
    if (r != 0) {
      return r;
    }

    bl.append(data_bl);
    if (idx == 0) { // first stripe
      num = meta_info.stripe_num_;
      if (create_time) {
        *create_time = meta_info.create_time_;
      }
    }

    ++idx;
  }
  return 0;
}

int CompoundIoCtx::Stat(const std::string& oid, uint64_t *psize, time_t *pmtime, MetaInfo* meta) {
  int idx = 0;
  std::string stripe_name;
  GetStripeName(oid, idx, stripe_name);

  bufferlist info_bl;
  ::encode(stripe_name, info_bl); // string

  std::string comp_obj_id = GetCompObjId(stripe_name);
  bufferlist out_bl;
  int r = io_ctx_.exec(comp_obj_id, "compound", "read_stripe_meta", info_bl, out_bl);
  if (r != 0) {
    return r;
  }

  MetaInfo meta_info; 
  r = ParseAndVerify(stripe_name, out_bl, meta_info, NULL);
  if (r != 0) {
    return r;
  }
  
  if (psize) {
    *psize = meta_info.obj_total_len_;
  }
  if (pmtime) {
    *pmtime = meta_info.create_time_;
  }
  if (meta) {
    *meta = meta_info;
  }
  return 0;
}

int CompoundIoCtx::Remove(const std::string& oid) {
  MetaInfo meta;
  int r = Stat(oid, NULL, NULL, &meta);
  if (r != 0) {
    return r;
  }
  for (int idx = 0; idx < meta.stripe_num_; ++idx) {
    std::string stripe_name;
    GetStripeName(oid, idx, stripe_name);

    bufferlist info_bl;
    ::encode(stripe_name, info_bl); // string

    std::string comp_obj_id = GetCompObjId(stripe_name);
    bufferlist out_bl;
    r = io_ctx_.exec(comp_obj_id, "compound", "remove_stripe", info_bl, out_bl);
    if (r != 0) {
      return r;
    }
  }
  return 0;
}

int CompoundIoCtx::ParseAndVerify(const std::string& stripe_name, bufferlist& bl, MetaInfo & meta, bufferlist* data_bl) {
  try {
    bufferlist::iterator it = bl.begin();
    ::decode(meta.magic_num_, it);
    if (meta.magic_num_ != kMagicNum) {
      return COMPOUND_MAGIC_NUM_ERR;
    }
   
    ::decode(meta.obj_total_len_, it);
    ::decode(meta.stripe_num_, it);
    ::decode(meta.stripe_idx_, it);
    ::decode(meta.create_time_, it);
    ::decode(meta.stripe_len_, it);
    ::decode(meta.stripe_flag_, it);
    ::decode(meta.data_crc32_, it);
    if (data_bl == NULL) { // NULL means bl only has meta
      return 0;
    }

    if (meta.stripe_len_ + kMetaInfoSize != bl.length()) {
      return COMPOUND_STRIPE_LEN_ERR;
    }
    data_bl->substr_of(bl, kMetaInfoSize, bl.length() - kMetaInfoSize);
    
    uint32_t crc = 0;
    bufferlist tmp_bl = *data_bl;
    tmp_bl.append(stripe_name);
    crc = tmp_bl.crc32c(crc);
    if (crc != meta.data_crc32_) {
      return COMPOUND_STRIPE_CRC_ERR;
    }
  } catch(std::exception & ex) {
    return COMPOUND_PARSE_META_ERR;
  }
  return 0;
}

std::string CompoundIoCtx::GetCompObjId(const std::string& oid) {
  int hash_val = ceph_str_hash_rjenkins(oid.data(), oid.size()) % (info_.compound_obj_num_);
  std::stringstream oss;
  oss << hash_val;
  return oss.str();
}

std::string CompoundIoCtx::GetFirstCompoundObjId(const std::string& oid) {
  int idx = 0;
  std::string stripe_name;
  GetStripeName(oid, idx, stripe_name);
  return GetCompObjId(stripe_name);
}

/********************
 * name_len, uint, 4B
 * name, string, {name_len}B, with idx suffix
 *
 * magic num, int, 4B (0x416e4574)
 * obj_total_len, uint64, 8B
 * stripe_num, int, 4B
 * stripe_idx, int, 4B
 * create_time, int, 4B
 * stripe_len, uint, 4B
 * stripe_flag, int, 4B
 * data_crc32, uint, 4B
 * stripe_data, string, {stripe_len}B
 * ********************/


void CompoundIoCtx::PrependMeta(bufferlist& bl, const std::string& stripe_name, int idx, int num, uint64_t obj_total_len, int create_time) {
  int flag = COMPOUND_STRIPE_FLAG_NORMAL;
  uint32_t crc = 0;
  bufferlist meta_bl;
  ::encode(stripe_name, meta_bl); //encode len auto
  ::encode(kMagicNum, meta_bl);
  ::encode(obj_total_len, meta_bl);
  ::encode(num, meta_bl);
  ::encode(idx, meta_bl);
  ::encode(create_time, meta_bl);
  ::encode(bl.length(), meta_bl);
  ::encode(flag, meta_bl);

  //// clac crc:  data + stripe_name
  bufferlist tmp_bl = bl;
  tmp_bl.append(stripe_name);
  crc = tmp_bl.crc32c(crc);

  ::encode(crc, meta_bl);
  bl.claim_prepend(meta_bl);
}


size_t CompoundIoCtx::GetPoolStripeSize() {
  return info_.stripe_size_;
}
/***********************CompoundIoCtx**************************/

