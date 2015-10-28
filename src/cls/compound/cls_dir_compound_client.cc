#include "include/ceph_hash.h"
#include "cls_dir_compound_client.h"
#include "include/encoding.h"
#include <sstream>
#include <math.h>
#include <iostream>


using namespace std;
using namespace librados::cls_dir_compound_client;
using namespace librados;

DirCompoundIoCtxPtr librados::cls_dir_compound_client::GetDirCompoundIoCtx(const string& pool_name) {
  return DirCompoundIoCtxFactory::GetInstance()->GetDirCompoundIoCtx(pool_name);
}

/***********************DirCompoundIoCtxFactory*******************/
boost::shared_ptr<DirCompoundIoCtxFactory> DirCompoundIoCtxFactory::instance_;
boost::mutex DirCompoundIoCtxFactory::instance_mutex_;
boost::mutex DirCompoundIoCtxFactory::clients_mutex_;

boost::shared_ptr<DirCompoundIoCtxFactory> DirCompoundIoCtxFactory::GetInstance() {
  if (!instance_) {
    boost::unique_lock<boost::mutex> lock(instance_mutex_);
    if (!instance_) {
      instance_.reset(new DirCompoundIoCtxFactory());
    }
  }
  return instance_;
}

DirCompoundIoCtxPtr DirCompoundIoCtxFactory::GetDirCompoundIoCtx(const string& pool_name) {
  if (ioctx_map_.find(pool_name) == ioctx_map_.end()) {
    boost::unique_lock<boost::mutex> lock(clients_mutex_);
    if (ioctx_map_.find(pool_name) == ioctx_map_.end()) {
        DirCompoundIoCtxPtr comp_ioctx(new DirCompoundIoCtx());
        ioctx_map_.insert(make_pair(pool_name, comp_ioctx));
    }
  }
  return ioctx_map_[pool_name];
}
/***********************DirCompoundIoCtxFactory*******************/


/***********************DirCompoundIoCtx**************************/
int DirCompoundIoCtx::CreateAllCompObj() {
  return 0;
}


void DirCompoundIoCtx::GetStripeName(const std::string& oid, int idx, std::string& stripe_name) {
  std::stringstream oss;
  oss << idx;
  stripe_name = oid + "_" + oss.str();
}

int DirCompoundIoCtx::GetCurrentTime() {
  return (int)time(NULL);
}

int DirCompoundIoCtx::PrepareWriteData(const std::string& oid, const bufferlist& bl, uint64_t off, int create_time, list<std::pair<std::string, bufferlist> >& write_data) {
  if (off % stripe_size_ != 0) {
    return COMPOUND_WRITE_OFFSET_ERR;
  }

  std::string path;
  std::string filename;
  cb_(oid, path, filename);
  if (filename.empty()) {
    return COMPOUND_DIR_FILE_NAME_ERR;
  }

  size_t bl_len = bl.length();
  int idx = off / stripe_size_;
  int num = idx + ceil(bl_len / (stripe_size_ + 0.0));
  uint64_t wr_off = 0;
  while (wr_off < bl_len) {
    size_t stripe_len = ((wr_off + stripe_size_ > bl_len) ? (bl_len - wr_off) : stripe_size_);
    bufferlist stripe_bl;
    stripe_bl.substr_of(bl, wr_off, stripe_len);
    
    std::string stripe_name;
    GetStripeName(filename, idx, stripe_name);

    PrependMeta(stripe_bl, stripe_name, idx, num, bl_len + off, create_time);

    std::string comp_obj_id;
    GetCompObjId(path, idx, comp_obj_id);
    
    write_data.push_back(std::make_pair<std::string, bufferlist>(comp_obj_id, stripe_bl));

    wr_off += stripe_len;
    ++idx;
  }
  return 0;
}

int DirCompoundIoCtx::Write(const std::string& oid, bufferlist& bl, uint64_t off, int create_time) {
  list<std::pair<std::string, bufferlist> > write_data;
  int r = PrepareWriteData(oid, bl, off, create_time, write_data);
  if (r == COMPOUND_DIR_FILE_NAME_ERR) {
    return io_ctx_.write(oid, bl, bl.length(), off); //use native method write
  }

  list<std::pair<std::string, bufferlist> >::iterator iter = write_data.begin();
  for (; iter != write_data.end(); ++iter) {
    bufferlist out_bl;
    r = io_ctx_.exec(iter->first, "compound", "append_set_xattr", iter->second, out_bl);
    if (r != 0) {
      return r;
    }
  }

  return 0;
}

int DirCompoundIoCtx::WriteFullObj(const std::string& oid, bufferlist& bl, int create_time) {
  return Write(oid, bl, 0, create_time);
}

int DirCompoundIoCtx::BatchWriteFullObj(const String2BufferlistHMap& oid2data, const String2IntHMap& oid2create_time) {
  String2BufferlistHMap comp_oid2data;// <comp_obj_id, connected_stripe_data>

  int r;
  list<std::pair<std::string, bufferlist> > write_data;// <comp_obj_id, single_stripe_data>
  for (String2BufferlistHMap::const_iterator iter = oid2data.begin(); iter != oid2data.end(); ++iter) {
    String2IntHMap::const_iterator ct_iter = oid2create_time.find(iter->first);
    int create_time = (ct_iter != oid2create_time.end()) ? ct_iter->second : GetCurrentTime(); 
    r = PrepareWriteData(iter->first, iter->second, 0, create_time, write_data);
    if (r == COMPOUND_DIR_FILE_NAME_ERR) {
      bufferlist tmp_bl(iter->second);
      r = io_ctx_.write_full(iter->first, tmp_bl); //use native method write
      if (r < 0) {
        return r;
      }
    }
  }
  
  list<std::pair<std::string, bufferlist> >::iterator l_iter = write_data.begin();
  for (; l_iter != write_data.end(); ++l_iter) {
    String2BufferlistHMap::iterator iter = comp_oid2data.find(l_iter->first);
    if ( iter != comp_oid2data.end() ) {
      (iter->second).append(l_iter->second);
    } else {
      comp_oid2data.insert( std::make_pair<std::string, bufferlist>(l_iter->first, l_iter->second) );
    }
  }
  
  for (String2BufferlistHMap::iterator iter = comp_oid2data.begin(); iter != comp_oid2data.end(); ++iter) {
    bufferlist out_bl;
    r = io_ctx_.exec(iter->first, "compound", "bth_apd_set_xatr", iter->second, out_bl);
    if (r != 0) {
      return r; //can't exactly know which failed
    }
  }
  return 0;
}

int DirCompoundIoCtx::BatchWriteFullObj(const String2BufferlistHMap& oid2data, int create_time) {
  String2IntHMap oid2create_time;
  for (String2BufferlistHMap::const_iterator iter = oid2data.begin(); iter != oid2data.end(); ++iter) {
    oid2create_time[iter->first] = create_time;
  }

  return BatchWriteFullObj(oid2data, oid2create_time);
}

int DirCompoundIoCtx::WriteFinish(const std::string& oid, uint64_t total_size, int create_time) {
  int num = ceil(total_size / (stripe_size_ + 0.0));
  unsigned wr_meta_off = sizeof(int); //only skip magic_num field
  list<AioCompletion*> acps;

  std::string path;
  std::string filename;
  cb_(oid, path, filename);
  if (filename.empty()) {
    return 0; //do nothing
  }

  for (int idx = 0; idx < num; ++idx) {

    std::string stripe_name;
    GetStripeName(filename, idx, stripe_name);

    std::string comp_obj_id;
    GetCompObjId(path, idx, comp_obj_id);

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

int DirCompoundIoCtx::Read(const std::string& oid, bufferlist& bl, size_t len, uint64_t off) {
  std::string path;
  std::string filename;
  cb_(oid, path, filename);
  if (filename.empty()) {
    int r = io_ctx_.read(oid, bl, len, off); //use native method write
    if (r < 0) {
      return r;
    }
    return 0;
  }

  uint64_t rd_off = off;
  size_t rd_len = 0;
  while (rd_len < len) {
    int idx = floor(rd_off / stripe_size_);
    std::string stripe_name;
    GetStripeName(filename, idx, stripe_name);

    uint64_t rd_stripe_off = rd_off % stripe_size_;
    unsigned rd_stripe_len = stripe_size_ - rd_stripe_off;
    if (rd_len + rd_stripe_len > len) {
      rd_stripe_len = len - rd_len;
    }
    if (rd_stripe_len == stripe_size_) {
      rd_stripe_len = 0; //0 means all: stripe_meta, stripe_data(all)
    } 

    bufferlist info_bl;
    ::encode(stripe_name, info_bl); //string
    ::encode(rd_stripe_off, info_bl); //uint64 
    ::encode(rd_stripe_len, info_bl); //unsigned

    std::string comp_obj_id;
    GetCompObjId(path, idx, comp_obj_id);
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
      if (data_bl.length() < stripe_size_) {
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

int DirCompoundIoCtx::ReadFullObj(const std::string& oid, bufferlist& bl, int* create_time) {
  uint64_t off = 0;
  size_t len = 0; //0 means all: stripe_meta, stripe_data(all)
  int idx = 0;
  int num = 1; //set 1 at first

  std::string path;
  std::string filename;
  cb_(oid, path, filename);
  if (filename.empty()) { //use native method write
    uint64_t obj_size;
    time_t c_time;
    int r = io_ctx_.stat(oid, &obj_size, &c_time);
    if (r < 0) {
      return r;
    }
    r = io_ctx_.read(oid, bl, obj_size, 0);
    if (r < 0) {
      return r;
    }
    return 0;
  }

  while(idx < num) {
    std::string stripe_name;
    GetStripeName(filename, idx, stripe_name);

    bufferlist info_bl;
    ::encode(stripe_name, info_bl); // string
    ::encode(off, info_bl); // uint64
    ::encode(len, info_bl); // unsigned
  
    std::string comp_obj_id;
    GetCompObjId(path, idx, comp_obj_id);
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

int DirCompoundIoCtx::Stat(const std::string& oid, uint64_t *psize, time_t *pmtime, MetaInfo* meta) {
  std::string path;
  std::string filename;
  cb_(oid, path, filename);
  if (filename.empty()) { //use native method write
    int r = io_ctx_.stat(oid, psize, pmtime);
    if (r < 0) {
      return r;
    }
    if (meta) {
      meta->stripe_num_ = 0;
    }
    return 0;
  }

  int idx = 0;
  std::string stripe_name;
  GetStripeName(filename, idx, stripe_name);

  bufferlist info_bl;
  ::encode(stripe_name, info_bl); // string

  std::string comp_obj_id;
  GetCompObjId(path, idx, comp_obj_id);
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

int DirCompoundIoCtx::Remove(const std::string& oid) {
  MetaInfo meta;
  int r = Stat(oid, NULL, NULL, &meta);
  if (r != 0) {
    return r;
  }

  std::string path;
  std::string filename;
  cb_(oid, path, filename);
  if (filename.empty()) { //use native method write
    return io_ctx_.remove(oid);
  }

  for (int idx = 0; idx < meta.stripe_num_; ++idx) {
    std::string stripe_name;
    GetStripeName(filename, idx, stripe_name);

    bufferlist info_bl;
    ::encode(stripe_name, info_bl); // string

    std::string comp_obj_id;
    GetCompObjId(path, idx, comp_obj_id);
    bufferlist out_bl;
    r = io_ctx_.exec(comp_obj_id, "compound", "remove_stripe", info_bl, out_bl);
    if (r != 0) {
      return r;
    }
  }
  return 0;
}

int DirCompoundIoCtx::ParseAndVerify(const std::string& stripe_name, bufferlist& bl, MetaInfo & meta, bufferlist* data_bl) {
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

void DirCompoundIoCtx::GetCompObjId(const std::string& oid, int idx, std::string& comp_obj_id) {
  return GetStripeName(oid, idx, comp_obj_id);
}

void DirCompoundIoCtx::DefaultHash(const std::string& oid, std::string& path, std::string& file) {
  size_t pos = oid.find_last_of('/');
  if (pos != string::npos) {
    path = oid.substr(0, pos);
    file = oid.substr(pos + 1);
  } else {
    path = oid;
    file.clear();
  }
}

std::string DirCompoundIoCtx::GetFirstCompoundObjId(const std::string& oid) {
  std::string path;
  std::string filename;
  cb_(oid, path, filename);
  if (filename.empty()) {
    return oid;
  }

  int idx = 0;
  std::string comp_obj_id;
  GetCompObjId(path, idx, comp_obj_id);
  return comp_obj_id;
}

int DirCompoundIoCtx::GetStripeCompoundInfo(const std::string& oid, std::vector<std::string>& comp_oids, std::vector<uint64_t>& offset_vec, std::vector<unsigned>& size_vec) {
  MetaInfo meta;
  int r = Stat(oid, NULL, NULL, &meta);
  if (r < 0) {
    return r;
  }

  size_t reserve_size = 2 * meta.stripe_num_;
  comp_oids.reserve(reserve_size);
  offset_vec.reserve(reserve_size);
  size_vec.reserve(reserve_size);
  
  std::string path;
  std::string filename;
  cb_(oid, path, filename);
  if (filename.empty()) {
    return COMPOUND_DIR_FILE_NAME_ERR;
  }
  
  for (int idx = 0; idx < meta.stripe_num_; ++idx) {
    std::string stripe_name;
    GetStripeName(filename, idx, stripe_name);

    std::string comp_obj_id;
    GetCompObjId(path, idx, comp_obj_id);

    std::map<std::string, bufferlist> out_map;
    std::set<std::string> keys;
    keys.insert(stripe_name);
    r = io_ctx_.omap_get_vals_by_keys(comp_obj_id, keys, &out_map);
    if (r < 0) {
      return r;
    }
   
    uint64_t offset;
    unsigned size;
    if (out_map.empty()) {
      comp_obj_id.clear();
      offset = 0;
      size = 0;
    } else {
      bufferlist::iterator iter = ( (out_map.begin())->second ).begin();
      ::decode(offset, iter);
      ::decode(size, iter);
    }
    
    comp_oids.push_back(comp_obj_id);
    offset_vec.push_back(offset);
    size_vec.push_back(size);
  }
  return 0;
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


void DirCompoundIoCtx::PrependMeta(bufferlist& bl, const std::string& stripe_name, int idx, int num, uint64_t obj_total_len, int create_time) {
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


size_t DirCompoundIoCtx::GetPoolStripeSize() {
  return stripe_size_;
}
/***********************DirCompoundIoCtx**************************/

