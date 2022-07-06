/*
 *
 */
#ifndef H_TESTING
#define H_TESTING

#include <memory>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "util/base.hpp"
#include "util/types.hpp"
#include "io/directfile.hpp"
#include "io/linkpagedfile.hpp"
#include "io/fixedlendatapage.hpp"
#include "catalog/schema.hpp"
#include "io/filemanager.hpp"
#include "ds/isamtree.hpp"

#include <gsl/gsl_rng.h>
#include <functional>

using namespace std::placeholders;

namespace lsm { namespace testing {

std::unique_ptr<io::FileManager> g_fm;

std::unique_ptr<std::byte> test_page1()
{
    byte *test_page = (byte *) std::aligned_alloc(parm::SECTOR_SIZE, parm::PAGE_SIZE);
    int32_t cnt = (parm::PAGE_SIZE / sizeof(int32_t));
    for (int32_t i=0; i<cnt; i++) {
        ((int32_t *) test_page)[i] = i;
    }

    return std::unique_ptr<byte>(test_page);
}


std::unique_ptr<std::byte> test_page2()
{
    byte *test_page = (byte *) std::aligned_alloc(parm::SECTOR_SIZE, parm::PAGE_SIZE);
    size_t cnt = parm::PAGE_SIZE / sizeof(int16_t);
    for (int16_t i=cnt; i>= 0; i--) {
        ((int16_t *) test_page)[cnt - i] = i;
    }

    return std::unique_ptr<byte>(test_page);
}

std::string new_fname = "tests/data/new_file.dat";
std::string existing_fname1 = "tests/data/test_file1.dat";

void initialize_file1()
{
    auto data = testing::test_page1();
    auto fd = open(existing_fname1.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0640);
    pwrite(fd, data.get(), parm::PAGE_SIZE, 0);
    fsync(fd);
    close(fd);
}


void delete_file1()
{
    unlink(existing_fname1.c_str());
}


std::unique_ptr<io::DirectFile> create_dfile_empty()
{
    std::string fname = "tests/data/pagedfile_test_empty.dat";
    FileId flid = 123;

    auto dfile = io::DirectFile::create(fname);
    io::LinkPagedFile::initialize(dfile.get(), flid);

    return dfile;
}

std::string existing_fname2 = "tests/data/pagedfile_test_existing.dat";
void initialize_file2()
{
    auto dfile = io::DirectFile::create(existing_fname2);
    io::LinkPagedFile::initialize(dfile.get(), 123);
    auto pfile = new io::LinkPagedFile(std::move(dfile), false);

    auto data1 = testing::test_page1();
    auto data2 = testing::test_page2();
    
    pfile->allocate_page();
    pfile->allocate_page();

    pfile->write_page(1, data1.get());
    pfile->write_page(2, data2.get());

    delete pfile;
}

std::unique_ptr<byte> empty_aligned_buffer()
{
    byte *buf = (byte *) std::aligned_alloc(parm::SECTOR_SIZE, parm::PAGE_SIZE);
    return std::unique_ptr<byte>(buf);
}


io::FixedlenDataPage empty_test_page(byte *buffer, size_t keylen=8, size_t vallen=8, size_t userdata_len=0)
{
    auto schema = catalog::FixedKVSchema(keylen, vallen, io::RecordHeaderLength);
    io::FixedlenDataPage::initialize(buffer, schema.record_length(), userdata_len);

    return io::FixedlenDataPage(buffer);
}


io::FixedlenDataPage populated_test_page(byte *buffer)
{
    auto schema = catalog::FixedKVSchema(sizeof(int64_t), sizeof(int64_t), io::RecordHeaderLength);
    io::FixedlenDataPage::initialize(buffer, schema.record_length(), 256);
    auto page = io::FixedlenDataPage(buffer);

    for (size_t i=0; i<100; i++) {
        size_t val = i+3;
        auto recbuf = schema.create_record((byte *) &i, (byte *) &val);
        auto rec = io::Record(recbuf.get(), schema.record_length());
        page.insert_record(rec);
    }

    return page;
}

std::unique_ptr<catalog::FixedKVSchema> test_schema1(size_t val_len=sizeof(int64_t))
{
    return std::make_unique<catalog::FixedKVSchema>(sizeof(int64_t), val_len, io::RecordHeaderLength);
}


std::string fm_root_dir = "tests/data/filemanager/";
std::unique_ptr<io::FileManager> create_test_fm()
{
    return std::make_unique<io::FileManager>(fm_root_dir);
}


std::string generate_test_file1()
{
    auto fm = create_test_fm(); 
    auto test_file = fm->create_indexed_pfile();
    auto buf = empty_aligned_buffer();
    auto schema = test_schema1(sizeof(int64_t));

    int64_t key = -100;
    int64_t val = 8;
    PageOffset length = schema->record_length();
    for (size_t i=0; i<10; i++) {
        auto new_pid = test_file->allocate_page(); 
        io::FixedlenDataPage::initialize(buf.get(), length, 0);
        auto datapage = io::FixedlenDataPage(buf.get());
        for (size_t i=0; i<datapage.get_record_capacity(); i++) {
            auto recbuf = schema->create_record((byte *) &key, (byte *) &val);
            datapage.insert_record({recbuf.get(), length});
            key += 1;
            val += 1;
        }
        test_file->write_page(new_pid, buf.get());
    }

    return fm->get_name(test_file->get_flid());
}


void initialize_global_fm()
{
    g_fm = create_test_fm();
}


std::string generate_merge_test_file1(size_t page_cnt, size_t *reccnt)
{
    auto fm = g_fm.get();

    auto test_file = fm->create_indexed_pfile("merge_test_file1");
    auto buf = empty_aligned_buffer();
    auto schema = test_schema1(sizeof(int64_t));

    int64_t key = 0;
    int64_t val = 8;
    size_t cnt = 0;
    PageOffset length = schema->record_length();
    for (size_t i=0; i<page_cnt; i++) {
        auto new_pid = test_file->allocate_page(); 
        io::FixedlenDataPage::initialize(buf.get(), length, 0);
        auto datapage = io::FixedlenDataPage(buf.get());
        for (size_t i=0; i<datapage.get_record_capacity(); i++) {
            auto recbuf = schema->create_record((byte *) &key, (byte *) &val);
            datapage.insert_record({recbuf.get(), length});
            key += 3;
            val += 1;
            cnt++;
        }
        test_file->write_page(new_pid, buf.get());
    }

    if (reccnt) {
        *reccnt = cnt;
    }
    return fm->get_name(test_file->get_flid());
}


std::string generate_merge_test_file2(size_t page_cnt, size_t *reccnt)
{
    auto fm = g_fm.get();

    auto test_file = fm->create_indexed_pfile("merge_test_file2");
    auto buf = empty_aligned_buffer();
    auto schema = test_schema1(sizeof(int64_t));

    int64_t key = 0;
    int64_t val = 8;
    size_t cnt = 0;
    PageOffset length = schema->record_length();
    for (size_t i=0; i<page_cnt; i++) {
        auto new_pid = test_file->allocate_page(); 
        io::FixedlenDataPage::initialize(buf.get(), length, 0);
        auto datapage = io::FixedlenDataPage(buf.get());
        for (size_t i=0; i<datapage.get_record_capacity(); i++) {
            auto recbuf = schema->create_record((byte *) &key, (byte *) &val);
            datapage.insert_record({recbuf.get(), length});
            key += 2;
            val += 1;
            cnt++;
        }
        test_file->write_page(new_pid, buf.get());
    }

    if (reccnt) {
        *reccnt = cnt;
    }
    
    return fm->get_name(test_file->get_flid());
}


std::string generate_merge_test_file3(size_t page_cnt, size_t *reccnt)
{
    auto fm = g_fm.get();

    auto test_file = fm->create_indexed_pfile("merge_test_file3");
    auto buf = empty_aligned_buffer();
    auto schema = test_schema1(sizeof(int64_t));

    int64_t key = 0;
    int64_t val = 8;
    size_t cnt = 0;
    PageOffset length = schema->record_length();
    for (size_t i=0; i<page_cnt; i++) {
        auto new_pid = test_file->allocate_page(); 
        io::FixedlenDataPage::initialize(buf.get(), length, 0);
        auto datapage = io::FixedlenDataPage(buf.get());
        for (size_t i=0; i<datapage.get_record_capacity(); i++) {
            auto recbuf = schema->create_record((byte *) &key, (byte *) &val);
            datapage.insert_record({recbuf.get(), length});
            key += 1;
            val += 1;
            cnt++;
        }
        test_file->write_page(new_pid, buf.get());
    }

    if (reccnt) {
        *reccnt = cnt;
    }

    return fm->get_name(test_file->get_flid());
}

std::string generate_merge_test_file4(size_t page_cnt, size_t *reccnt)
{
    auto fm = g_fm.get();

    auto test_file = fm->create_indexed_pfile("merge_test_file4");
    auto buf = empty_aligned_buffer();
    auto schema = test_schema1(sizeof(int64_t));

    int64_t key = 10000;
    int64_t val = 8;
    size_t cnt = 0;
    PageOffset length = schema->record_length();
    for (size_t i=0; i<page_cnt; i++) {
        auto new_pid = test_file->allocate_page(); 
        io::FixedlenDataPage::initialize(buf.get(), length, 0);
        auto datapage = io::FixedlenDataPage(buf.get());
        for (size_t i=0; i<datapage.get_record_capacity(); i++) {
            auto recbuf = schema->create_record((byte *) &key, (byte *) &val);
            datapage.insert_record({recbuf.get(), length});
            key -= 1;
            val += 1;
            cnt++;
        }
        test_file->write_page(new_pid, buf.get());
    }

    if (reccnt) {
        *reccnt = cnt;
    }

    return fm->get_name(test_file->get_flid());
}

std::string generate_merge_test_file3a(size_t page_cnt, size_t *reccnt)
{
    auto fm = g_fm.get();

    auto test_file = fm->create_indexed_pfile("merge_test_file3a");
    auto buf = empty_aligned_buffer();
    auto schema = test_schema1(sizeof(int64_t));

    int64_t key = 100000;
    int64_t val = 8;
    size_t cnt = 0;
    PageOffset length = schema->record_length();
    for (size_t i=0; i<page_cnt; i++) {
        auto new_pid = test_file->allocate_page(); 
        io::FixedlenDataPage::initialize(buf.get(), length, 0);
        auto datapage = io::FixedlenDataPage(buf.get());
        for (size_t i=0; i<datapage.get_record_capacity(); i++) {
            auto recbuf = schema->create_record((byte *) &key, (byte *) &val);
            datapage.insert_record({recbuf.get(), length});
            key += 1;
            val += 1;
            cnt++;
        }
        test_file->write_page(new_pid, buf.get());
    }

    if (reccnt) {
        *reccnt = cnt;
    }

    return fm->get_name(test_file->get_flid());
}


std::string generate_isamtree_test_data1(size_t page_cnt, PageOffset val_len, size_t *reccnt)
{
    auto fm = g_fm.get();

    auto test_file = fm->create_indexed_pfile("isamtree_data_1");
    auto buf = empty_aligned_buffer();
    auto schema = test_schema1(val_len);

    int64_t key = -10;
    int64_t val = 8;
    size_t cnt = 0;
    PageOffset length = schema->record_length();
    for (size_t i=0; i<page_cnt; i++) {
        auto new_pid = test_file->allocate_page(); 
        io::FixedlenDataPage::initialize(buf.get(), length, 0);
        auto datapage = io::FixedlenDataPage(buf.get());
        for (size_t i=0; i<datapage.get_record_capacity(); i++) {
            auto recbuf = schema->create_record((byte *) &key, (byte *) &val);
            datapage.insert_record({recbuf.get(), length});
            key += 2;
            val += 1;
            cnt++;
        }
        test_file->write_page(new_pid, buf.get());
    }

    if (reccnt) {
        *reccnt = cnt;
    }

    return fm->get_name(test_file->get_flid());
}


std::string generate_isamtree_test_data2(size_t page_cnt, PageOffset val_len, size_t *reccnt)
{
    auto fm = g_fm.get();

    auto test_file = fm->create_indexed_pfile("isamtree_data_2");
    auto buf = empty_aligned_buffer();
    auto schema = test_schema1(val_len);

    int64_t key = -10;
    int64_t val = 8;
    size_t cnt = 0;
    PageOffset length = schema->record_length();
    for (size_t i=0; i<page_cnt; i++) {
        auto new_pid = test_file->allocate_page(); 
        io::FixedlenDataPage::initialize(buf.get(), length, 0);
        auto datapage = io::FixedlenDataPage(buf.get());
        for (size_t i=0; i<datapage.get_record_capacity(); i++) {
            auto recbuf = schema->create_record((byte *) &key, (byte *) &val);
            datapage.insert_record({recbuf.get(), length});
            key += 3;
            val += 1;
            cnt++;
        }
        test_file->write_page(new_pid, buf.get());
    }

    if (reccnt) {
        *reccnt = cnt;
    }

    return fm->get_name(test_file->get_flid());
}


std::string generate_isamtree_test_data3(size_t page_cnt, PageOffset val_len, size_t *reccnt)
{
    auto fm = g_fm.get();

    auto test_file = fm->create_indexed_pfile("isamtree_data_3");
    auto buf = empty_aligned_buffer();
    auto schema = test_schema1(val_len);

    int64_t key = 5;
    int64_t val = 8;
    size_t cnt = 0;
    PageOffset length = schema->record_length();
    for (size_t i=0; i<page_cnt; i++) {
        auto new_pid = test_file->allocate_page(); 
        io::FixedlenDataPage::initialize(buf.get(), length, 0);
        auto datapage = io::FixedlenDataPage(buf.get());
        for (size_t i=0; i<datapage.get_record_capacity(); i++) {
            auto recbuf = schema->create_record((byte *) &key, (byte *) &val);
            datapage.insert_record({recbuf.get(), length});
            key += 1;
            val += 1;
            cnt++;
        }
        test_file->write_page(new_pid, buf.get());
    }

    if (reccnt) {
        *reccnt = cnt;
    }

    return fm->get_name(test_file->get_flid());
}


std::string generate_isamtree_test_data_all_dupes(size_t page_cnt, PageOffset val_len, int64_t key, size_t *reccnt)
{
    auto fm = g_fm.get();

    auto test_file = fm->create_indexed_pfile();
    auto buf = empty_aligned_buffer();
    auto schema = test_schema1(val_len);

    int64_t val = 8;
    Timestamp time = 0;
    size_t cnt = 0;
    PageOffset length = schema->record_length();
    for (size_t i=0; i<page_cnt; i++) {
        auto new_pid = test_file->allocate_page(); 
        io::FixedlenDataPage::initialize(buf.get(), length, 0);
        auto datapage = io::FixedlenDataPage(buf.get());
        for (size_t i=0; i<datapage.get_record_capacity(); i++) {
            auto recbuf = schema->create_record((byte *) &key, (byte *) &val);
            auto rec = io::Record(recbuf.get(), length, time++, false);
            datapage.insert_record(rec);
            val += 1;
            cnt++;
        }
        test_file->write_page(new_pid, buf.get());
    }

    if (reccnt) {
        *reccnt = cnt;
    }

    return fm->get_name(test_file->get_flid());
}

std::string generate_isamtree_test_data_a(size_t page_cnt, global::g_state *state, size_t *reccnt)
{
    auto test_file = state->file_manager->create_indexed_pfile("isamtree_data_1");
    auto buf = empty_aligned_buffer();
    auto schema = state->record_schema.get();

    int64_t key = -10;
    int64_t val = 8;
    size_t cnt = 0;
    PageOffset length = schema->record_length();
    for (size_t i=0; i<page_cnt; i++) {
        auto new_pid = test_file->allocate_page(); 
        io::FixedlenDataPage::initialize(buf.get(), length, 0);
        auto datapage = io::FixedlenDataPage(buf.get());
        for (size_t i=0; i<datapage.get_record_capacity(); i++) {
            auto recbuf = schema->create_record((byte *) &key, (byte *) &val);
            datapage.insert_record({recbuf.get(), length});
            key += 2;
            val += 1;
            cnt++;
        }
        test_file->write_page(new_pid, buf.get());
    }

    if (reccnt) {
        *reccnt = cnt;
    }

    return state->file_manager->get_name(test_file->get_flid());
}


std::string generate_isamtree_test_data_b(size_t page_cnt, global::g_state *state, size_t *reccnt)
{
    auto test_file = state->file_manager->create_indexed_pfile("isamtree_data_2");
    auto buf = empty_aligned_buffer();
    auto schema = state->record_schema.get();

    int64_t key = -10;
    int64_t val = 8;
    size_t cnt = 0;
    PageOffset length = schema->record_length();
    for (size_t i=0; i<page_cnt; i++) {
        auto new_pid = test_file->allocate_page(); 
        io::FixedlenDataPage::initialize(buf.get(), length, 0);
        auto datapage = io::FixedlenDataPage(buf.get());
        for (size_t i=0; i<datapage.get_record_capacity(); i++) {
            auto recbuf = schema->create_record((byte *) &key, (byte *) &val);
            datapage.insert_record({recbuf.get(), length});
            key += 3;
            val += 2;
            cnt++;
        }
        test_file->write_page(new_pid, buf.get());
    }

    if (reccnt) {
        *reccnt = cnt;
    }

    return state->file_manager->get_name(test_file->get_flid());
}


std::string generate_contiguous_test_data(PageNum page_cnt, global::g_state *state, size_t *reccnt)
{
    auto test_file = state->file_manager->create_indexed_pfile();
    auto buf = empty_aligned_buffer();
    auto schema = state->record_schema.get();

    int64_t key = 0;
    int64_t val = 0;
    size_t cnt = 0;
    PageOffset length = schema->record_length();

    byte key_buf[schema->key_len()];
    byte val_buf[schema->val_len()];

    for (size_t i=0; i<page_cnt; i++) {
        auto new_pid = test_file->allocate_page(); 
        io::FixedlenDataPage::initialize(buf.get(), length, 0);
        auto datapage = io::FixedlenDataPage(buf.get());
        for (size_t i=0; i<datapage.get_record_capacity(); i++) {
            memcpy(key_buf, &key, sizeof(int64_t));
            memcpy(val_buf, &val, sizeof(int64_t));
            auto recbuf = schema->create_record(key_buf, val_buf);
            datapage.insert_record({recbuf.get(), length});
            key += 1;
            val += 1;
            cnt++;
        }
        test_file->write_page(new_pid, buf.get());
    }

    if (reccnt) {
        *reccnt = cnt;
    }

    return state->file_manager->get_name(test_file->get_flid());
}


std::unique_ptr<ds::ISAMTree> test_isamtree1(size_t page_cnt, global::g_state *state, size_t *cnt)
{
    std::vector<std::unique_ptr<iter::GenericIterator<io::Record>>> iter(1);
    auto fname1 = testing::generate_isamtree_test_data_a(page_cnt, state, cnt);
    auto *pfile1 = (lsm::io::IndexPagedFile *) state->file_manager->get_pfile(fname1);
    iter[0] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile1, state->cache.get());

    auto merge_iter = std::make_unique<iter::MergeIterator>(iter, state->record_schema->get_record_cmp());

    return ds::ISAMTree::create(std::move(merge_iter), page_cnt, false, state);
}


std::unique_ptr<ds::ISAMTree> test_isamtree2(size_t page_cnt, global::g_state *state, size_t *cnt)
{
    std::vector<std::unique_ptr<iter::GenericIterator<io::Record>>> iter(1);
    auto fname1 = testing::generate_isamtree_test_data_b(page_cnt, state, cnt);
    auto *pfile1 = (lsm::io::IndexPagedFile *) state->file_manager->get_pfile(fname1);
    iter[0] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile1, state->cache.get());

    auto merge_iter = std::make_unique<iter::MergeIterator>(iter, state->record_schema->get_record_cmp());

    return ds::ISAMTree::create(std::move(merge_iter), page_cnt, false, state);
}


std::unique_ptr<ds::ISAMTree> test_isamtree_cont(size_t page_cnt, global::g_state *state, size_t *cnt)
{
    std::vector<std::unique_ptr<iter::GenericIterator<io::Record>>> iter(1);
    auto fname1 = testing::generate_contiguous_test_data(page_cnt, state, cnt);
    auto *pfile1 = (lsm::io::IndexPagedFile *) state->file_manager->get_pfile(fname1);
    iter[0] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile1, state->cache.get());

    auto merge_iter = std::make_unique<iter::MergeIterator>(iter, state->record_schema->get_record_cmp());

    return ds::ISAMTree::create(std::move(merge_iter), page_cnt, false, state);
}


int keycmp1(const byte * a, const byte *b) {
    if (*((int64_t*) a) > *((int64_t*) b)) {
        return 1;
    } else if (*((int64_t*) a) < *((int64_t*) b)) {
        return -1;
    }

    return 0;
}


int reccmp1(const byte *a, const byte *b) {
    int64_t key1 = *((int64_t*) (a + io::RecordHeaderLength));
    int64_t key2 = *((int64_t*) (b + io::RecordHeaderLength));

    if (key1 < key2) {
        return -1;
    } else if (key1 > key2) {
        return 1;
    }

    Timestamp time1 = ((io::RecordHeader *) a)->time;
    Timestamp time2 = ((io::RecordHeader *) b)->time;

    if (time1 < time2) {
        return -1;
    } else if (time1 < time2) {
        return 1;
    }

    bool tombstone1 = ((io::RecordHeader *) a)->is_tombstone;
    bool tombstone2 = ((io::RecordHeader *) b)->is_tombstone;

    if (tombstone1 && !tombstone2) {
        return -1;
    } else if (tombstone2 && !tombstone1) {
        return 1;
    }

    return 0;
}

std::unique_ptr<global::g_state> make_state1()
{

    auto new_state = new global::g_state();

    const catalog::RecordCmpFunc reccmp = std::bind(&reccmp1, _1, _2);
    const catalog::KeyCmpFunc keycmp = std::bind(&keycmp1, _1, _2);

    gsl_rng_env_setup();

    new_state->cache = std::make_unique<io::ReadCache>(1024);
    new_state->file_manager = std::make_unique<io::FileManager>("tests/data/filemanager1/");
    new_state->record_schema = std::make_unique<catalog::FixedKVSchema>(sizeof(int64_t), sizeof(int64_t), io::RecordHeaderLength, keycmp, reccmp);
    new_state->rng = gsl_rng_alloc(gsl_rng_gfsr4);

    return std::unique_ptr<global::g_state>(new_state);
}


std::unique_ptr<global::g_state> make_state(size_t keylen, size_t vallen)
{
    auto new_state = new global::g_state();

    const catalog::RecordCmpFunc reccmp = std::bind(&reccmp1, _1, _2);
    const catalog::KeyCmpFunc keycmp = std::bind(&keycmp1, _1, _2);

    gsl_rng_env_setup();

    new_state->cache = std::make_unique<io::ReadCache>(1024);
    new_state->file_manager = std::make_unique<io::FileManager>("tests/data/filemanager1/");
    new_state->record_schema = std::make_unique<catalog::FixedKVSchema>(keylen, vallen, io::RecordHeaderLength, keycmp, reccmp);
    new_state->rng = gsl_rng_alloc(gsl_rng_gfsr4);

    return std::unique_ptr<global::g_state>(new_state);
}

}}

#endif
