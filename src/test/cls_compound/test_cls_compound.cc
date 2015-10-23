// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/encoding.h"
#include "include/types.h"
#include "include/rados/librados.h"
#include "cls/compound/cls_compound_client.h"

#include "gtest/gtest.h"
#include "test/librados/test.h"

#include <errno.h>
#include <string>
#include <vector>

using namespace std;
using namespace librados::cls_compound_client;
using namespace librados;

static char *random_buf(size_t len)
{
  char *b = new char[len + 1];
  for (size_t i = 0; i < len; i++) {
    b[i] = (rand() % (127 - 33)) + 33;
    if (i % 255 == 0) {
      b[i] = '\n';
    }
  }
  b[len] = 0;
  return b;
}

int test_case_key_num = 2;
std::string pool_name("VideoUploadTest");

std::string cluster_init(Rados & cluster) {
    char *id = getenv("CEPH_CLIENT_ID");
    if (id) {
        std::cerr << "Client id is: " << id << std::endl;
    }

    int ret;
    ret = cluster.init(id);
    if (ret) {
        std::ostringstream oss;
        oss << "cluster.init failed with error " << ret;
        return oss.str();
    }

    ret = cluster.conf_read_file(NULL);
    if (ret) {
        cluster.shutdown();
        std::ostringstream oss;
        oss << "cluster.conf_read_file failed with error " << ret;
        return oss.str();
    }

    cluster.conf_parse_env(NULL);
    ret = cluster.connect();
    if (ret) {
        cluster.shutdown();
        std::ostringstream oss;
        oss << "cluster.connect failed with error " << ret;
        return oss.str();
    }
    return "";  // means ok
}

class CompoundTest : public ::testing::Test {
//protected:
public:
  static void SetUpTestCase() {
    std::string err = cluster_init(cluster_);
    if (err.length() != 0) {
        std::cerr << "cluster init failed: " << err << std::endl;
    }

    pctx_ = GetCompoundIoCtx(pool_name);  ///stripe_size: 1024Byte
    int ret = cluster_.ioctx_create(pool_name.c_str(), pctx_->io_ctx_);
    if (ret < 0) {
        std::cerr << "could't set up ioctx err: " << ret << std::endl;
    }
    stripe_size_ = pctx_->GetPoolStripeSize();
    test_sizes.push_back(1);
    test_sizes.push_back(stripe_size_ - 1);
    test_sizes.push_back(stripe_size_);

    test_sizes.push_back(stripe_size_ + 1);
    test_sizes.push_back(2 * stripe_size_ - 1);
    test_sizes.push_back(2 * stripe_size_);

    test_sizes.push_back(2 * stripe_size_ + 1);
    test_sizes.push_back(3 * stripe_size_ - 1);
    test_sizes.push_back(3 * stripe_size_);

    test_sizes.push_back(3 * stripe_size_ + 1);
    test_sizes.push_back(4 * stripe_size_ - 1);
    test_sizes.push_back(4 * stripe_size_);

//    test_sizes.push_back(4 * stripe_size_ + 1);
//    test_sizes.push_back(5 * stripe_size_ - 1);
//    test_sizes.push_back(5 * stripe_size_);
    srand(time(NULL));
  }

  static void TearDownTestCase() {
    pctx_->Close();
  }

  static unsigned stripe_size_;
  static vector<unsigned> test_sizes;

  int TestWRFull(const string& obj_key, unsigned val_len);
  int TestWROff(const string& obj_key, unsigned val_len, int time);
  int TestWRCreatetime(const string& obj_key, unsigned val_len, int c_time);
  int TestStat(const string& obj_key, unsigned val_len, int time);
  static CompoundIoCtxPtr pctx_;
  static librados::Rados cluster_;

};

CompoundIoCtxPtr CompoundTest::pctx_;
librados::Rados CompoundTest::cluster_;
unsigned CompoundTest::stripe_size_ = 0;
vector<unsigned> CompoundTest::test_sizes;

int CompoundTest::TestWRFull(const string& obj_key, unsigned val_len) {
  string obj_val(random_buf(val_len), val_len);
  bufferlist in_bl;
  in_bl.append(obj_val);

  int r;
  r = pctx_->WriteFullObj(obj_key, in_bl);
  if (r) {
    cout << "WriteFull return " << r << endl;
    return r;
  }

  bufferlist out_bl;
  r = pctx_->ReadFullObj(obj_key, out_bl);
  if (r) {
    cout << "ReadFull return " << r << endl;
    return r;
  }

  std::string out_str(out_bl.c_str(), out_bl.length());
  r = obj_val.compare(out_str);
  if (r) {
    cout << obj_val << endl << out_str << endl;
    cout << "write and read value return " << r << endl;
    return r;
  }

  if (out_bl.length() != val_len) {
    cout << "buf_len " << out_bl.length() << " NOT equal " << val_len << endl;
    return -1;
  }

  return 0;
}

int CompoundTest::TestStat(const string& obj_key, unsigned val_len, int time) {
  string obj_val(random_buf(val_len), val_len);
  bufferlist in_bl;
  in_bl.append(obj_val);

  int r;
  r = pctx_->WriteFullObj(obj_key, in_bl, time);
  if (r) {
    cout << "WriteFull return " << r << endl;
    return r;
  }

  bufferlist out_bl;
  r = pctx_->ReadFullObj(obj_key, out_bl);
  if (r) {
    cout << "ReadFull return " << r << endl;
    return r;
  }

  std::string out_str(out_bl.c_str(), out_bl.length());
  r = obj_val.compare(out_str);
  if (r) {
    cout << obj_val << endl << out_str << endl;
    cout << "write and read value return " << r << endl;
    return r;
  }

  if (out_bl.length() != val_len) {
    cout << "buf_len " << out_bl.length() << " NOT equal " << val_len << endl;
    return -1;
  }


  uint64_t size;
  time_t c_time;
  MetaInfo meta;
  r = pctx_->Stat(obj_key, &size, &c_time, &meta);
  if (size != val_len) {
    cout << "WARN size NOT same: " << val_len << " " << size << endl;
    return -1;
  }

  if ((int)c_time != time) {
    cout << "WARN time NOT same: " << time << " " << (int)c_time << endl;
    return -1;
  }

  if (meta.stripe_num_ != ceil(val_len / (0.0 + stripe_size_)) ) {
    cout << "WARN stripe_num NOT same: " << ceil(val_len / (0.0 + stripe_size_)) << " " << meta.stripe_num_ << endl;
  }

  return 0;
}



int CompoundTest::TestWRCreatetime(const string& obj_key, unsigned val_len, int c_time) {
  string obj_val(random_buf(val_len), val_len);
  bufferlist in_bl;
  in_bl.append(obj_val);

  int r;
  r = pctx_->WriteFullObj(obj_key, in_bl, c_time);
  if (r) {
    cout << "WriteFull return " << r << endl;
    return r;
  }

  bufferlist out_bl;
  int out_c_time;
  r = pctx_->ReadFullObj(obj_key, out_bl, &out_c_time);
  if (r) {
    cout << "ReadFull return " << r << endl;
    return r;
  }

  std::string out_str(out_bl.c_str(), out_bl.length());
  r = obj_val.compare(out_str);
  if (r) {
    cout << obj_val << endl << out_str << endl;
    cout << "write and read value return " << r << endl;
    return r;
  }

  if (c_time != out_c_time) {
    cout << c_time << endl << out_c_time << endl;
    return -1;
  }
  return 0;
}

int CompoundTest::TestWROff(const string& obj_key, unsigned val_len, int time) {
  string obj_val(random_buf(val_len), val_len);
  bufferlist in_bl;
  in_bl.append(obj_val);

//////////write
  int r;
  unsigned off = 0;
  while (off < val_len) {
    int len = rand() % (val_len - off);
    if (len == 0) {
      len = stripe_size_;
    }
    len = ceil(len / (0.0 + stripe_size_)) * stripe_size_;
    if (off + len > val_len) {
      len = val_len - off;
    }
    
    bufferlist sub_bl;
    sub_bl.substr_of(in_bl, off, len);
    r = pctx_->Write(obj_key, sub_bl, off);
    if (r) {
      cout << "write with off return: " << r << endl;
      return r;
    }

    off += len;
  }
 
  r = pctx_->WriteFinish(obj_key, val_len, time);
  if (r) {
    cout << "WriteFinish return: " << r << endl;
    return r;
  }

//////////read
  off = 0;
  bufferlist out_bl;
  while (off < val_len) {
    bufferlist sub_bl;
    int len = rand() % (val_len - off);
    if (len == 0) {
      len = 1;
    }

    r = pctx_->Read(obj_key, sub_bl, len, off);
    if (r) {
      cout << "read with off return: " << r << endl;
      return r;
    }
    //cout << off << " " << len << " " << sub_bl.length() << endl;

    out_bl.append(sub_bl);
    off += sub_bl.length();
  } 
  //r = pctx_->ReadFullObj(obj_key, out_bl);

  if (out_bl.length() != in_bl.length()) {
    cout << "WRWithOff value-size NOT same, in: " << in_bl.length() << " out: " << out_bl.length() << endl;
    return -1;
  }

  std::string out_str(out_bl.c_str(), out_bl.length());
  r = obj_val.compare(out_str);
  if (r) {
    cout << obj_val << endl << out_str << endl;
    cout << "WRWithOff value NOT same, compare ret: " << r << " val_len: " << val_len << endl;
    return r;
  }

  uint64_t size;
  time_t c_time;
  MetaInfo meta;
  r = pctx_->Stat(obj_key, &size, &c_time, &meta);
  if (size != val_len) {
    cout << "WARN size NOT same: " << val_len << " " << size << endl;
    return -1;
  }

  if ((int)c_time != time) {
    cout << "WARN time NOT same: " << time << " " << (int)c_time << endl;
    return -1;
  }

  if (meta.stripe_num_ != ceil(val_len / (0.0 + stripe_size_)) ) {
    cout << "WARN stripe_num NOT same: " << ceil(val_len / (0.0 + stripe_size_)) << " " << meta.stripe_num_ << endl;
    return -1;
  }

  return 0;
}


TEST_F(CompoundTest, CreateAllCompObj)
{
  return;
  //int r = pctx_->CreateAllCompObj();
  //ASSERT_EQ(0, r);
}

TEST_F(CompoundTest, WriteReadFullObj)
{
//  return;
  string obj_key;
  int start_id = rand() % 200308413;
  for (int i = 0; i < test_case_key_num; i++) {
    std::stringstream oss;
    int id = start_id + i;
    oss << "test_objkey_wr_rd_full_" << id;
    obj_key = oss.str();
 
    for (int ii = 0; ii < CompoundTest::test_sizes.size(); ++ii) {
      ASSERT_EQ(0, TestWRFull(obj_key, CompoundTest::test_sizes[ii]));
    }
  }
}

TEST_F(CompoundTest, WriteReadCreatetime)
{
//  return;
  string obj_key;
  int start_id = rand() % 200308413;
  for (int i = 0; i < test_case_key_num; i++) {
    std::stringstream oss;
    int id = start_id + i;
    oss << "test_objkey_wr_rd_full_time_" << id;
    obj_key = oss.str();
 
    for (int ii = 0; ii < CompoundTest::test_sizes.size(); ++ii) {
      int c_time = pctx_->GetCurrentTime();
      ASSERT_EQ(0, TestWRCreatetime(obj_key, CompoundTest::test_sizes[ii], c_time));
    }
  }
}

TEST_F(CompoundTest, WriteReadWithOff)
{
  string obj_key;
  int start_id = rand() % 200308413;
  for (int i = 0; i < test_case_key_num; i++) {
    std::stringstream oss;
    int id = start_id + i;
    oss << "test_objkey_wr_off_" << id;
    obj_key = oss.str();
 
    int c_time = pctx_->GetCurrentTime();
    for (int ii = 0; ii < CompoundTest::test_sizes.size(); ++ii) {
      ASSERT_EQ(0, TestWROff(obj_key, CompoundTest::test_sizes[ii], c_time));
    }
  }
}

TEST_F(CompoundTest, Stat)
{
  string obj_key;
  int start_id = rand() % 200308413;
  for (int i = 0; i < test_case_key_num; i++) {
    std::stringstream oss;
    int id = start_id + i;
    oss << "test_objkey_stat_" << id;
    obj_key = oss.str();
 
    for (int ii = 0; ii < CompoundTest::test_sizes.size(); ++ii) {
      int c_time = pctx_->GetCurrentTime();
      ASSERT_EQ(0, TestStat(obj_key, CompoundTest::test_sizes[ii], c_time));
    }
  }
}


TEST_F(CompoundTest, RemoveStripe)
{
  string obj_key;
  MetaInfo meta_info;
  int start_id = rand() % 200308413;
  for (int ii = 0; ii < CompoundTest::test_sizes.size(); ++ii) {
    std::stringstream oss;
    int id = start_id + ii;
    oss << "test_objkey_remove_" << id;
    obj_key = oss.str();

    ASSERT_EQ(0, TestWRFull(obj_key, CompoundTest::test_sizes[ii]));
    ASSERT_EQ(0, pctx_->Stat(obj_key, NULL, NULL, &meta_info));
    ASSERT_EQ(0, pctx_->Remove(obj_key));
    ASSERT_EQ(-61, pctx_->Stat(obj_key, NULL, NULL, &meta_info));

  }
}


