#ifndef CEPH_CLS_COMPOUNT_COMMON_H 
#define CEPH_CLS_COMPOUNT_COMMON_H
const int kMagicNum = 0x416e4574;
const unsigned kMetaInfoSize = 36; // 36B
const unsigned kStripeFlagOffset = 28; //28B
const unsigned kDataBlockSize = 4 * 1024;//4KiB
const unsigned kStripeLenOffset = 24;// magic num(int, 4B), obj_total_len(uint64, 8B), stripe_num(int, 4B), stripe_idx(int, 4B), create_time(int, 4B)
const unsigned kDefaultStripeSize = 2 * 1024 * 1024;//2MiB

enum {
  COMPOUND_STRIPE_FLAG_NORMAL = 0,
  COMPOUND_STRIPE_FLAG_REMOVED = 1,
};


const int COMPOUND_MAGIC_NUM_ERR = -2001;
const int COMPOUND_STRIPE_LEN_ERR = -2002;
const int COMPOUND_STRIPE_CRC_ERR = -2003;
const int COMPOUND_PARSE_META_ERR = -2004;
const int COMPOUND_PARSE_STRIPE_NAME_ERR = -2005;
const int COMPOUND_PARSE_REQ_META_ERR = -2006;
const int COMPOUND_WRITE_OFFSET_ERR = -2007;
const int COMPOUND_PARSE_ERR = -2008;


#endif
