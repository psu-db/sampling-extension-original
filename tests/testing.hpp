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

namespace lsm { namespace testing {

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
        auto recbuf = schema.create_record_unique((byte *) &i, (byte *) &val);
        auto rec = io::Record(recbuf.get(), schema.record_length());
        page.insert_record(rec);
    }

    return page;
}

std::unique_ptr<catalog::FixedKVSchema> test_schema1(size_t val_len=sizeof(int64_t))
{
    return std::make_unique<catalog::FixedKVSchema>(sizeof(int64_t), val_len, io::RecordHeaderLength);
}

}}

#endif
