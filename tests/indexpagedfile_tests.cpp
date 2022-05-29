/*
 *
 */

#include <check.h>
#include "io/indexpagedfile.hpp"
#include "testing.hpp"

using namespace lsm;
using std::byte;


START_TEST(t_initialize)
{
    FileId test_flid1 = 1;
    FileId test_flid2 = 11;

    std::string fname1 = "tests/data/pagedfile_test1.dat";
    std::string fname2 = "tests/data/pagedfile_test2.dat";

    auto dfile1 = io::DirectFile::create(fname1); 
    auto dfile2 = io::DirectFile::create(fname2); 
    dfile2->close_file();

    // initializing an open dfile should work
    ck_assert_int_eq(io::IndexPagedFile::initialize(dfile1.get(), test_flid1), 1);
    
    // initializing a closed dfile should fail
    ck_assert_int_eq(io::IndexPagedFile::initialize(dfile2.get(), test_flid2), 0);

    // the initialized file should have its first page allocated
    ck_assert_int_eq(dfile1->get_size(), parm::PAGE_SIZE);

    // validate that the header is correct
    byte *buf = (byte *) std::aligned_alloc(parm::SECTOR_SIZE, parm::PAGE_SIZE);
    pread(dfile1->get_fd(), buf, parm::PAGE_SIZE, 0);
    io::IndexPagedFileHeaderData *header = (io::IndexPagedFileHeaderData *) buf;

    ck_assert_int_eq(header->paged_header.page_count, 0);
    ck_assert_int_eq(header->paged_header.flid, test_flid1);
}
END_TEST


START_TEST(t_constructor_empty)
{
    auto dfile = testing::create_dfile_empty();
    auto pfile = io::IndexPagedFile(std::move(dfile), false);

    auto first_page = pfile.get_first_pid();
    auto last_page = pfile.get_last_pid();

    ck_assert_int_eq(first_page.page_number, INVALID_PNUM);
    ck_assert_int_eq(first_page.file_id, 123);

    ck_assert_int_eq(last_page.page_number, INVALID_PNUM);
    ck_assert_int_eq(last_page.file_id, 123);

    ck_assert_int_eq(pfile.get_page_count(), 0);
    ck_assert_int_eq(pfile.is_temporary(), false);
}
END_TEST


START_TEST(t_allocate_page)
{
    auto dfile = testing::create_dfile_empty();
    auto pfile = io::IndexPagedFile(std::move(dfile), false);

    ck_assert_int_eq(pfile.allocate_page().page_number, 1);
    ck_assert_int_eq(pfile.allocate_page().page_number, 2);

    ck_assert_int_eq(pfile.get_page_count(), 2);
    ck_assert_int_eq(pfile.get_first_pid().page_number, 1);
    ck_assert_int_eq(pfile.get_last_pid().page_number, 2);
}
END_TEST


START_TEST(t_allocate_bulk)
{
    auto dfile = testing::create_dfile_empty();
    auto pfile = io::IndexPagedFile(std::move(dfile), false);

    ck_assert_int_eq(pfile.allocate_page_bulk(10).page_number, 1);
    ck_assert_int_eq(pfile.get_page_count(), 10);
    ck_assert_int_eq(pfile.get_first_pid().page_number, 1);
    ck_assert_int_eq(pfile.get_last_pid().page_number, 10);

    ck_assert_int_eq(pfile.allocate_page_bulk(10).page_number, 11);
    ck_assert_int_eq(pfile.get_first_pid().page_number, 1);
    ck_assert_int_eq(pfile.get_last_pid().page_number, 20);
    ck_assert_int_eq(pfile.get_page_count(), 20);
}
END_TEST


START_TEST(t_free_page)
{
    auto dfile = testing::create_dfile_empty();
    auto pfile = io::IndexPagedFile(std::move(dfile), false);

    pfile.allocate_page();
    pfile.allocate_page();

    ck_assert_int_eq(pfile.supports_free(), 0);
    ck_assert_int_eq(pfile.free_page(1), 0);
    ck_assert_int_eq(pfile.free_page(2), 0);
}
END_TEST


START_TEST(t_write)
{
    auto dfile = testing::create_dfile_empty();
    std::string fname = dfile->get_fname();
    auto pfile = new io::IndexPagedFile(std::move(dfile), false);

    PageId pid = pfile->allocate_page();
    auto data1 = testing::test_page1();
    auto data2 = testing::test_page2();

    ck_assert_int_eq(pfile->write_page(pid, data1.get()), 1);
    ck_assert_int_eq(pfile->write_page(pid.page_number+1, data2.get()), 0);

    PageId pid2 = pfile->allocate_page();
    ck_assert_int_eq(pfile->write_page(pid2, data2.get()), 1);

    delete pfile;

    dfile = io::DirectFile::create(fname, false);
    int fd = dfile->get_fd();
    pfile = new io::IndexPagedFile(std::move(dfile), false);

    ck_assert_int_eq(pfile->get_page_count(), 2);
    ck_assert_int_eq(pfile->get_last_pid().page_number, 2);
    ck_assert_int_eq(pfile->get_first_pid().page_number, 1);

    byte *buf = (byte *) std::aligned_alloc(parm::SECTOR_SIZE, parm::PAGE_SIZE);
    pread(fd, buf, parm::PAGE_SIZE, parm::PAGE_SIZE);
    ck_assert_mem_eq(buf + io::PageHeaderSize, data1.get() + io::PageHeaderSize, parm::PAGE_SIZE - io::PageHeaderSize);

    pread(fd, buf, parm::PAGE_SIZE, 2*parm::PAGE_SIZE);
    ck_assert_mem_eq(buf + io::PageHeaderSize, data2.get() + io::PageHeaderSize, parm::PAGE_SIZE - io::PageHeaderSize);

    delete pfile;
    delete buf;
}
END_TEST


START_TEST(t_read)
{
    testing::initialize_file2();
    auto dfile = io::DirectFile::create(testing::existing_fname2, false);
    io::IndexPagedFile pfile = io::IndexPagedFile(std::move(dfile), false);

    auto data1 = testing::test_page1();
    auto data2 = testing::test_page2();

    auto buf = testing::empty_aligned_buffer();

    ck_assert_int_eq(pfile.read_page(1, buf.get()), 1);
    ck_assert_mem_eq(buf.get() + io::PageHeaderSize, data1.get() + io::PageHeaderSize, parm::PAGE_SIZE - io::PageHeaderSize);

    ck_assert_int_eq(pfile.read_page(2, buf.get()), 1);
    ck_assert_mem_eq(buf.get() + io::PageHeaderSize, data2.get() + io::PageHeaderSize, parm::PAGE_SIZE - io::PageHeaderSize);

    ck_assert_int_eq(pfile.read_page(10, buf.get()), 0);
}
END_TEST


START_TEST(t_iterator)
{
    auto fname = testing::generate_test_file1();
    auto pfile = io::IndexPagedFile::create(fname, false, 5);
    auto rcache = std::make_unique<io::ReadCache>();
    auto schema = testing::test_schema1(sizeof(int64_t));

    auto rec_itr = io::IndexPagedFileRecordIterator(pfile.get(), INVALID_PNUM, rcache.get());

    size_t rec_cnt = 0;
    int64_t key = -100;
    int64_t val = 8;

    while (rec_itr.next()) {
        auto rec = rec_itr.get_item();
        ck_assert_int_eq(schema->get_key(rec.get_data()).Int64(), key++);
        ck_assert_int_eq(schema->get_val(rec.get_data()).Int64(), val++);
        rec_cnt++;
    }

    rec_itr.end_scan();
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("IndexPagedFile Unit Testing");
    TCase *initialize = tcase_create("lsm::io::IndexPagedFile::initialize Testing");
    tcase_add_test(initialize, t_initialize);

    suite_add_tcase(unit, initialize);


    TCase *constructor = tcase_create("lsm::io::IndexPagedFile::IndexPagedFile() Testing");
    tcase_add_test(constructor, t_constructor_empty);

    suite_add_tcase(unit, constructor);


    TCase *allocate = tcase_create("lsm::io::IndexPagedFile::allocate Testing");
    tcase_add_test(allocate, t_allocate_page);
    tcase_add_test(allocate, t_allocate_bulk);

    suite_add_tcase(unit, allocate);


    TCase *free_page = tcase_create("lsm::io::IndexPagedFile::free_page Testing");
    tcase_add_test(free_page, t_free_page);

    suite_add_tcase(unit, free_page);


    TCase *write = tcase_create("lsm::io::IndexPagedFile::write Testing");
    tcase_add_test(write, t_write);

    suite_add_tcase(unit, write);


    TCase *read = tcase_create("lsm::io::IndexPagedFile::read Testing");
    tcase_add_test(read, t_read);

    suite_add_tcase(unit, read);



    TCase *iter = tcase_create("lsm::io::IndexPagedFile::IndexPagedFileRecordIterator Testing");
    tcase_add_test(iter, t_iterator);

    suite_add_tcase(unit, iter);

    return unit;
}


int run_unit_tests()
{
    int failed = 0;
    Suite *unit = unit_testing();
    SRunner *unit_runner = srunner_create(unit);

    srunner_run_all(unit_runner, CK_VERBOSE);
    failed = srunner_ntests_failed(unit_runner);
    srunner_free(unit_runner);

    return failed;
}


int main() 
{
    int unit_failed = run_unit_tests();

    return (unit_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
