// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * This is a simple example RADOS class, designed to be usable as a
 * template from implementing new methods.
 *
 * Our goal here is to illustrate the interface between the OSD and
 * the class and demonstrate what kinds of things a class can do.
 *
 * Note that any *real* class will probably have a much more
 * sophisticated protocol dealing with the in and out data buffers.
 * For an example of the model that we've settled on for handling that
 * in a clean way, please refer to cls_lock or cls_version for
 * relatively simple examples of how the parameter encoding can be
 * encoded in a way that allows for forward and backward compatibility
 * between client vs class revisions.
 */

/*
 * A quick note about bufferlists:
 *
 * The bufferlist class allows memory buffers to be concatenated,
 * truncated, spliced, "copied," encoded/embedded, and decoded.  For
 * most operations no actual data is ever copied, making bufferlists
 * very convenient for efficiently passing data around.
 *
 * bufferlist is actually a typedef of buffer::list, and is defined in
 * include/buffer.h (and implemented in common/buffer.cc).
 */

#include <algorithm>
#include <string>
#include <sstream>
#include <errno.h>
#include <tr1/unordered_map>

#include "objclass/objclass.h"
#include "cls_compound_common.h"

CLS_VER(1,0)
CLS_NAME(compound)

cls_handle_t h_class;
cls_method_handle_t h_create_comp_obj;
cls_method_handle_t h_append_set_xattr;
cls_method_handle_t h_append_only;
cls_method_handle_t h_batch_append_set_xattr;
cls_method_handle_t h_read_with_xattr;
cls_method_handle_t h_set_stripe_meta;
cls_method_handle_t h_read_stripe_meta;
cls_method_handle_t h_remove_stripe;


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

static int parse_stripe_name(bufferlist::iterator& iter, string& name) {
  try {
    ::decode(name, iter);
  } catch (buffer::error& e) {
    return COMPOUND_PARSE_STRIPE_NAME_ERR; 
  }
  return 0;
}

static int parse_off_len(bufferlist::iterator& iter, uint64_t& off, unsigned& len) {
  try {
    ::decode(off, iter); //uint64
    ::decode(len, iter); //unsigned
  } catch (buffer::error& e) {
    return COMPOUND_PARSE_ERR; 
  }
  return 0;
}


static int create_comp_obj(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  bool exclusive = false;
  bufferlist::iterator iter = in->begin();
  try {
    ::decode(exclusive, iter);
  } catch(buffer::error& e) {
    return COMPOUND_PARSE_ERR;
  }

  return cls_cxx_create(hctx, exclusive);
}

static int append_only(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  uint64_t wr_offset = 0;
  int r = 0;
  int mod;

  if ( (r = cls_cxx_stat(hctx, &wr_offset, NULL)) == -ENOENT) {
    //obj not exist
    wr_offset = 0;
  } else if (r < 0) {
    return r;
  } else if ( (mod = (wr_offset % kDataBlockSize) ) != 0) {
    wr_offset = wr_offset + (kDataBlockSize - mod);
  }

  if ( (r = cls_cxx_write(hctx, wr_offset, in->length(), in)) < 0) {
    return r;  
  }

  return 0;
}

static int append_set_xattr(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  uint64_t wr_offset;
  int r = 0;
  int mod;

  if ( (r = cls_cxx_stat(hctx, &wr_offset, NULL)) == -ENOENT) {
    //obj not exist
    wr_offset = 0;
  } else if (r < 0) {
    return r;
  } else if ( (mod = (wr_offset % kDataBlockSize) ) != 0) {
    wr_offset = wr_offset + (kDataBlockSize - mod);
  }

  bufferlist::iterator iter = in->begin();
  string stripe_name;
  if ( (r = parse_stripe_name(iter, stripe_name)) < 0 ) {
    return r;
  }

  unsigned data_off = iter.get_off();
  bufferlist data_bl;
  data_bl.substr_of(*in, data_off, in->length() - data_off);
 
  if ( (r = cls_cxx_write(hctx, wr_offset, data_bl.length(), &data_bl)) < 0) {
    return r;  
  }

  bufferlist xattr_bl;
  ::encode(wr_offset, xattr_bl); //uint64
  ::encode(data_bl.length(), xattr_bl); //unsigned
  if ( (r = cls_cxx_map_set_val(hctx, stripe_name, &xattr_bl)) < 0 ) {
    return r;
  }
  
  return 0;
}

static int batch_append_set_xattr(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  uint64_t batch_wr_offset;
  int r = 0;
  int mod;

  if ( (r = cls_cxx_stat(hctx, &batch_wr_offset, NULL)) == -ENOENT) {
    //obj not exist
    batch_wr_offset = 0;
  } else if (r < 0) {
    return r;
  } else if ( (mod = (batch_wr_offset % kDataBlockSize) ) != 0) {
    batch_wr_offset = batch_wr_offset + (kDataBlockSize - mod);
  }

  uint64_t range_wr_offset = batch_wr_offset;
  std::map<std::string, bufferlist> toset_xattrs;
  bufferlist data_bl;

  bufferlist::iterator iter = in->begin();
  while(!iter.end()) {
    string stripe_name;
    if ( (r = parse_stripe_name(iter, stripe_name)) < 0 ) {
      return r;
    }
 
    bufferlist::iterator tmp_iter = iter;
    tmp_iter.advance(kStripeLenOffset);

    unsigned stripe_len; 
    try {
      ::decode(stripe_len, tmp_iter);
    } catch (std::exception &ex) {
      return COMPOUND_PARSE_ERR;
    }

    unsigned data_off = iter.get_off();
    unsigned data_len = stripe_len + kMetaInfoSize;
    bufferlist range_bl;
    range_bl.substr_of(*in, data_off, data_len);
    data_bl.append(range_bl);
    if ( (mod = (data_bl.length() % kDataBlockSize) ) != 0) {
      data_bl.append_zero(kDataBlockSize - mod);// padding for alignment
    }

    bufferlist xattr_bl;
    ::encode(range_wr_offset, xattr_bl); //uint64
    ::encode(data_len, xattr_bl); //unsigned,  data_len don't include padding len
    toset_xattrs.insert(std::make_pair<std::string, bufferlist>(stripe_name, xattr_bl));
    
    range_wr_offset = batch_wr_offset + data_bl.length();
    iter.advance(data_len);
  }

  if ( (r = cls_cxx_write(hctx, batch_wr_offset, data_bl.length(), &data_bl)) < 0) {
    return r;  
  }

  if ( (r = cls_cxx_map_set_vals(hctx, &toset_xattrs)) < 0 ) {
    return r;
  }
  
  return 0;
}

static int set_stripe_meta(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  int r;
  std::string stripe_name;
  bufferlist::iterator iter = in->begin();
  if ( (r = parse_stripe_name(iter, stripe_name)) < 0 ) {
    return r;
  }

  unsigned wr_meta_off; 
  try {
    ::decode(wr_meta_off, iter);
  } catch (std::exception &ex) {
    return COMPOUND_PARSE_ERR;
  }

  
  bufferlist xattr_bl;
  if ( (r = cls_cxx_map_get_val(hctx, stripe_name, &xattr_bl)) < 0 ) {
    return r;
  }
  uint64_t stripe_off;
  unsigned stripe_len;
  bufferlist::iterator x_iter = xattr_bl.begin();
  if ( (r = parse_off_len(x_iter, stripe_off, stripe_len)) < 0 ) {
    return r;
  }

  bufferlist tmp_out; 
  if ( (r = cls_cxx_read(hctx, stripe_off, sizeof(int), &tmp_out)) < 0 ) {
    return r;
  }
  int magic_num;
  bufferlist::iterator iter2 = tmp_out.begin();
  try {
    ::decode(magic_num, iter2);
  } catch (std::exception &ex) {
    return COMPOUND_PARSE_ERR;
  }
  if (magic_num != kMagicNum) {
    return COMPOUND_MAGIC_NUM_ERR;
  }

  bufferlist part_meta_bl;
  part_meta_bl.substr_of(*in, iter.get_off(), in->length() - iter.get_off());
  uint64_t wr_off = wr_meta_off + stripe_off;
  if ( (r = cls_cxx_write(hctx, wr_off, part_meta_bl.length(), &part_meta_bl)) < 0) {
    return r;
  }

  return 0;
}

static int remove_stripe(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  int r;
  std::string stripe_name;
  bufferlist::iterator iter = in->begin();
  if ( (r = parse_stripe_name(iter, stripe_name)) < 0 ) {
    return r;
  }

  bufferlist xattr_bl;
  if ( (r = cls_cxx_map_get_val(hctx, stripe_name, &xattr_bl)) < 0 ) {
    return r;
  }
  uint64_t stripe_off;
  unsigned stripe_len;
  iter = xattr_bl.begin();
  if ( (r = parse_off_len(iter, stripe_off, stripe_len)) < 0 ) {
    return r;
  }

  if ( (r = cls_cxx_read(hctx, stripe_off, kMetaInfoSize, out)) < 0 ) {
    return r;
  }

  iter = out->begin();
  try {
    int magic_num;
    ::decode(magic_num, iter);
    if (magic_num != kMagicNum) {
      return COMPOUND_MAGIC_NUM_ERR;
    }
  } catch(std::exception& ex) {
    return COMPOUND_PARSE_ERR;
  }
 
  if ( (r = cls_cxx_map_remove_key(hctx, stripe_name)) < 0 ) {
    return r;
  }

  bufferlist flag_bl;
  ::encode(COMPOUND_STRIPE_FLAG_REMOVED, flag_bl);
  uint64_t flag_off = stripe_off + kStripeFlagOffset;
  if ( (r = cls_cxx_write(hctx, flag_off, flag_bl.length(), &flag_bl)) < 0) {
    return r;
  }

  return 0;
}

static int read_stripe_meta(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  int r;
  std::string stripe_name;
  bufferlist::iterator iter = in->begin();
  if ( (r = parse_stripe_name(iter, stripe_name)) < 0 ) {
    return r;
  }

  bufferlist xattr_bl;
  if ( (r = cls_cxx_map_get_val(hctx, stripe_name, &xattr_bl)) < 0 ) {
    return r;
  }
  uint64_t stripe_off;
  unsigned stripe_len;
  iter = xattr_bl.begin();
  if ( (r = parse_off_len(iter, stripe_off, stripe_len)) < 0 ) {
    return r;
  }

  if ( (r = cls_cxx_read(hctx, stripe_off, kMetaInfoSize, out)) < 0 ) {
    return r;
  }
  return 0;
}

static int read_with_xattr(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  int r;
  std::string stripe_name;
  bufferlist::iterator iter = in->begin();
  if ( (r = parse_stripe_name(iter, stripe_name)) < 0 ) {
    return r;
  }

  uint64_t req_off;
  unsigned req_len; 
  if ( (r = parse_off_len(iter, req_off, req_len)) < 0 ) {
    return r;
  }
  

  bufferlist xattr_bl;
  if ( (r = cls_cxx_map_get_val(hctx, stripe_name, &xattr_bl)) < 0 ) {
    return r;
  }
  uint64_t stripe_off;
  unsigned stripe_len;
  iter = xattr_bl.begin();
  if ( (r = parse_off_len(iter, stripe_off, stripe_len)) < 0 ) {
    return r;
  }

  uint64_t rd_off;
  unsigned rd_len;
  if (req_len == 0) { //==0 means all: meta + data
    rd_off = stripe_off;
    rd_len = stripe_len;
  } else { //>0 means only data(req_off, req_len), no meta
    rd_off = stripe_off + kMetaInfoSize + req_off;
    rd_len = (stripe_len - rd_off < req_len ? stripe_len - rd_off : req_len);
  } 

  if ( (r = cls_cxx_read(hctx, rd_off, rd_len, out)) < 0 ) {
    return r;
  }
  return 0;
}

/**
 * initialize class
 *
 * We do two things here: we register the new class, and then register
 * all of the class's methods.
 */
void __cls_init() {
  // this log message, at level 0, will always appear in the ceph-osd
  // log file.
  CLS_LOG(0, "loading cls_compound");

  cls_register("compound", &h_class);

  // There are two flags we specify for methods:
  //
  //    RD : whether this method (may) read prior object state
  //    WR : whether this method (may) write or update the object
  //
  // A method can be RD, WR, neither, or both.  If a method does
  // neither, the data it returns to the caller is a function of the
  // request and not the object contents.
  cls_register_cxx_method(h_class, "create_comp_obj",
			  CLS_METHOD_WR,
			  create_comp_obj, &h_create_comp_obj);

  cls_register_cxx_method(h_class, "append_set_xattr",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  append_set_xattr, &h_append_set_xattr);

  cls_register_cxx_method(h_class, "append_only",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  append_only, &h_append_only);

  cls_register_cxx_method(h_class, "bth_apd_set_xatr",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  batch_append_set_xattr, &h_batch_append_set_xattr);

  cls_register_cxx_method(h_class, "read_with_xattr",
			  CLS_METHOD_RD,
			  read_with_xattr, &h_read_with_xattr);

  cls_register_cxx_method(h_class, "set_stripe_meta",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  set_stripe_meta, &h_set_stripe_meta);

  cls_register_cxx_method(h_class, "read_stripe_meta",
			  CLS_METHOD_RD,
			  read_stripe_meta, &h_read_stripe_meta);

  cls_register_cxx_method(h_class, "remove_stripe",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  remove_stripe, &h_remove_stripe);
}
