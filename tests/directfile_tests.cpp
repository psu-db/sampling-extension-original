/*
 *
 */

#include <check.h>
#include "io/directfile.hpp"
#include "testing.hpp"

using namespace lsm;
using std::byte;

START_TEST(t_create_file)
{
    auto new_file = io::DirectFile::create(testing::new_fname);
    ck_assert_ptr_nonnull(new_file.get());

    int fd = new_file->get_fd();

    struct stat buf;
    fstat(fd, &buf);

    ck_assert_int_eq(buf.st_size, 0);

    // verify that the file is truncated properly on re-opening
    auto data = testing::test_page1();
    pwrite(fd, data.get(), parm::PAGE_SIZE, 0);
    fsync(fd);
    
    fstat(fd, &buf);
    ck_assert_int_eq(buf.st_size, parm::PAGE_SIZE);
    new_file.release();

    new_file = io::DirectFile::create(testing::new_fname);
    ck_assert_ptr_nonnull(new_file.get());

    fd = new_file->get_fd();
    fstat(fd, &buf);
    ck_assert_int_eq(buf.st_size, 0);
}
END_TEST


START_TEST(t_open_file)
{
    auto new_file = io::DirectFile::create(testing::existing_fname1, false);

    ck_assert_ptr_nonnull(new_file.get());
    struct stat sbuf;
    fstat(new_file->get_fd(), &sbuf);
    ck_assert_int_eq(sbuf.st_size, parm::PAGE_SIZE);

    byte *buf = (byte *) std::aligned_alloc(parm::SECTOR_SIZE, parm::PAGE_SIZE);
    pread(new_file->get_fd(), buf, parm::PAGE_SIZE, 0);
    auto ground_truth = testing::test_page1();

    ck_assert_mem_eq(buf, ground_truth.get(), parm::PAGE_SIZE);
    std::free(buf);
}
END_TEST


START_TEST(t_get_size_empty)
{
    auto new_file = io::DirectFile::create(testing::new_fname);
    auto test_size = new_file->get_size();

    ck_assert_int_eq(test_size, 0);
}
END_TEST


START_TEST(t_get_size_existing)
{
    auto new_file = io::DirectFile::create(testing::existing_fname1, false);
    auto test_size = new_file->get_size();

    ck_assert_int_eq(test_size, parm::PAGE_SIZE);
}
END_TEST


START_TEST(t_allocate_empty)
{
    auto new_file = io::DirectFile::create(testing::new_fname);
    ck_assert_int_eq(new_file->get_size(), 0);

    ck_assert_int_eq(new_file->allocate(parm::PAGE_SIZE), 1);
    ck_assert_int_eq(new_file->get_size(), parm::PAGE_SIZE);

    struct stat sbuf;
    fstat(new_file->get_fd(), &sbuf);
    ck_assert_int_eq(sbuf.st_size, parm::PAGE_SIZE);

    // allocation should fail if amount is not a multiple
    // of sector size, and file size should remain unchanged.
    ck_assert_int_eq(new_file->allocate(128), 0);
    ck_assert_int_eq(new_file->get_size(), parm::PAGE_SIZE);
    fstat(new_file->get_fd(), &sbuf);
    ck_assert_int_eq(sbuf.st_size, parm::PAGE_SIZE);

    // allocation should fail if the file is closed.
    ck_assert_int_eq(new_file->close_file(), 1);
    ck_assert_int_eq(new_file->allocate(parm::PAGE_SIZE), 0);

    // allocation should succeed once file is reopened
    ck_assert_int_eq(new_file->reopen(), 1);
    ck_assert_int_eq(new_file->allocate(parm::PAGE_SIZE), 1);
    ck_assert_int_eq(new_file->get_size(), 2*parm::PAGE_SIZE);
    fstat(new_file->get_fd(), &sbuf);
    ck_assert_int_eq(sbuf.st_size, 2*parm::PAGE_SIZE);

    // allocate a large amount of data
    ck_assert_int_eq(new_file->allocate(100 * parm::PAGE_SIZE), 1);
    ck_assert_int_eq(new_file->get_size(), 102*parm::PAGE_SIZE);
    fstat(new_file->get_fd(), &sbuf);
    ck_assert_int_eq(sbuf.st_size, 102*parm::PAGE_SIZE);

    // allocate a non-page-aligned amount of data
    ck_assert_int_eq(new_file->allocate(119 * parm::SECTOR_SIZE), 1);
    ck_assert_int_eq(new_file->get_size(), 102*parm::PAGE_SIZE + 119 * parm::SECTOR_SIZE);
    fstat(new_file->get_fd(), &sbuf);
    ck_assert_int_eq(sbuf.st_size, 102*parm::PAGE_SIZE + 119 * parm::SECTOR_SIZE);
}
END_TEST


START_TEST(t_allocate_existing)
{
    auto new_file = io::DirectFile::create(testing::existing_fname1, false);
    ck_assert_int_eq(new_file->get_size(), parm::PAGE_SIZE);

    ck_assert_int_eq(new_file->allocate(parm::PAGE_SIZE), 1);
    ck_assert_int_eq(new_file->get_size(), 2*parm::PAGE_SIZE);

    struct stat sbuf;
    fstat(new_file->get_fd(), &sbuf);
    ck_assert_int_eq(sbuf.st_size, 2*parm::PAGE_SIZE);

    // Validate that the correct data is still in the first page
    byte *buf = (byte *) std::aligned_alloc(parm::SECTOR_SIZE, parm::PAGE_SIZE);
    pread(new_file->get_fd(), buf, parm::PAGE_SIZE, 0);
    auto ground_truth = testing::test_page1();

    ck_assert_mem_eq(buf, ground_truth.get(), parm::PAGE_SIZE);
    std::free(buf);
}
END_TEST


START_TEST(t_open_close)
{
    auto new_file = io::DirectFile::create(testing::existing_fname1, false);

    // file should begin open
    ck_assert(new_file->is_open());

    // reopening an already open file should return 2
    ck_assert_int_eq(new_file->reopen(), 2);

    // closing an open file should succeed
    ck_assert_int_eq(new_file->close_file(), 1);

    // file should now be closed
    ck_assert_int_eq(new_file->is_open(), 0);

    // double close should return 2
    ck_assert_int_eq(new_file->close_file(), 2);

    // reopening a close file should work
    ck_assert_int_eq(new_file->reopen(), 1);

    // file should now be open
    ck_assert(new_file->is_open());
}
END_TEST


START_TEST(t_remove)
{
    auto new_file = io::DirectFile::create(testing::existing_fname1, false);

    ck_assert_int_eq(new_file->remove(), 1);

    // file should now be closed
    ck_assert_int_eq(new_file->is_open(), 0);

    // attempting to reopen should fail
    ck_assert_int_eq(new_file->reopen(), 0);

    // a double unlink should fail
    ck_assert_int_eq(new_file->remove(), 0);

    new_file.release();

    // should not be able to reopen the file
    new_file = io::DirectFile::create(testing::existing_fname1, false);
    ck_assert_ptr_null(new_file.get());
}


START_TEST(t_write_empty)
{
    auto new_file = io::DirectFile::create(testing::new_fname);

    auto buf = testing::test_page1();

    // writing to an empty file should fail.
    ck_assert_int_eq(new_file->write(buf.get(), parm::PAGE_SIZE, 0), 0);

    new_file->allocate(3*parm::PAGE_SIZE);

    // writing to an offset that isn't a multiple of the sector size should
    // fail
    ck_assert_int_eq(new_file->write(buf.get(), parm::PAGE_SIZE, 128), 0);

    // writing an amount that isn't a multiple of the sector size should
    // fail
    ck_assert_int_eq(new_file->write(buf.get(), 128, 0), 0);

    // writing to a page-aligned, allocated chunk of the file should work
    ck_assert_int_eq(new_file->write(buf.get(), parm::PAGE_SIZE, 0), 1);

    // you should also be able to write to chunks that aren't page-aligned, but
    // only sector aligned.
    ck_assert_int_eq(new_file->write(buf.get(), parm::SECTOR_SIZE, parm::PAGE_SIZE + parm::SECTOR_SIZE), 1);

    // validate that the data was written properly
    byte *rbuf = (byte *) std::aligned_alloc(parm::SECTOR_SIZE, parm::PAGE_SIZE);
    pread(new_file->get_fd(), rbuf, parm::PAGE_SIZE, 0);
    auto ground_truth = testing::test_page1();

    ck_assert_mem_eq(rbuf, ground_truth.get(), parm::PAGE_SIZE);
    std::free(rbuf);

    rbuf = (byte *) std::aligned_alloc(parm::SECTOR_SIZE, parm::SECTOR_SIZE);
    pread(new_file->get_fd(), rbuf, parm::SECTOR_SIZE, parm::PAGE_SIZE + parm::SECTOR_SIZE);
    ck_assert_mem_eq(rbuf, ground_truth.get(), parm::SECTOR_SIZE);
    std::free(rbuf);

    // writing from a buffer that isn't properly aligned should result in an error
    ck_assert_int_eq(new_file->write(buf.get() + 128, parm::PAGE_SIZE, 0), 0);

    // writing to a closed file should fail
    new_file->close_file();
    ck_assert_int_eq(new_file->write(buf.get(), parm::PAGE_SIZE, 0), 0);

    // after reopening, writing should work
    new_file->reopen();
    ck_assert_int_eq(new_file->write(buf.get(), parm::PAGE_SIZE, 0), 1);
}


START_TEST(t_read_write_existing)
{
    auto new_file = io::DirectFile::create(testing::existing_fname1, false);

    auto ground_truth1 = testing::test_page1();
    auto ground_truth2 = testing::test_page2();

    new_file->allocate(2*parm::PAGE_SIZE);
    ck_assert_int_eq(new_file->write(ground_truth2.get(), parm::PAGE_SIZE, parm::PAGE_SIZE), 1);
    ck_assert_int_eq(new_file->write(ground_truth1.get(), parm::PAGE_SIZE, 2*parm::PAGE_SIZE), 1);

    byte *buf = (byte *) aligned_alloc(parm::SECTOR_SIZE, parm::PAGE_SIZE + parm::SECTOR_SIZE);

    ck_assert_int_eq(new_file->read(buf, parm::PAGE_SIZE, 0), 1);
    ck_assert_mem_eq(buf, ground_truth1.get(), parm::PAGE_SIZE);

    ck_assert_int_eq(new_file->read(buf, parm::PAGE_SIZE, parm::PAGE_SIZE), 1);
    ck_assert_mem_eq(buf, ground_truth2.get(), parm::PAGE_SIZE);

    ck_assert_int_eq(new_file->read(buf, parm::PAGE_SIZE, 2*parm::PAGE_SIZE), 1);
    ck_assert_mem_eq(buf, ground_truth1.get(), parm::PAGE_SIZE);

    size_t old_sz = new_file->get_size();

    // delete and recreate object
    new_file.reset();
    new_file = io::DirectFile::create(testing::existing_fname1, false);
    ck_assert_int_eq(new_file->get_size(), old_sz);

    // validate data is still present
    ck_assert_int_eq(new_file->read(buf, parm::PAGE_SIZE, 0), 1);
    ck_assert_mem_eq(buf, ground_truth1.get(), parm::PAGE_SIZE);

    ck_assert_int_eq(new_file->read(buf, parm::PAGE_SIZE, parm::PAGE_SIZE), 1);
    ck_assert_mem_eq(buf, ground_truth2.get(), parm::PAGE_SIZE);

    ck_assert_int_eq(new_file->read(buf, parm::PAGE_SIZE, 2*parm::PAGE_SIZE), 1);
    ck_assert_mem_eq(buf, ground_truth1.get(), parm::PAGE_SIZE);

    std::free(buf);
}


START_TEST(t_read_empty) 
{
    auto new_file = io::DirectFile::create(testing::new_fname);
    byte *buf = (byte *) aligned_alloc(parm::SECTOR_SIZE, parm::PAGE_SIZE + parm::SECTOR_SIZE);

    // reading from an empty file w/o any allocated size should fail.
    ck_assert_int_eq(new_file->read(buf, parm::PAGE_SIZE, 0), 0);

    // after allocating, reading from the allocated region should work
    new_file->allocate(3*parm::PAGE_SIZE);
    ck_assert_int_eq(new_file->read(buf, parm::PAGE_SIZE, 0), 1);

    // reading into an unaligned buffer should fail
    ck_assert_int_eq(new_file->read(buf + 128, parm::PAGE_SIZE, 0), 0);

    // reading from an unaligned offset should fail
    ck_assert_int_eq(new_file->read(buf, parm::PAGE_SIZE, 128), 0);

    // reading an unaligned amount should fail.
    ck_assert_int_eq(new_file->read(buf, 128, 0), 0);

    // reading passed the end of the file should fail.
    ck_assert_int_eq(new_file->read(buf, parm::PAGE_SIZE, 2*parm::PAGE_SIZE + parm::SECTOR_SIZE), 0);

    // reading from a closed file should fail
    new_file->close_file();
    ck_assert_int_eq(new_file->read(buf, parm::PAGE_SIZE, 0), 0);

    // reading from a reopened file should work
    new_file->reopen();
    ck_assert_int_eq(new_file->read(buf, parm::PAGE_SIZE, 0), 1);

    std::free(buf);
}
END_TEST


START_TEST(t_read_existing)
{
    auto new_file = io::DirectFile::create(testing::existing_fname1, false);
    byte *buf = (byte *) aligned_alloc(parm::SECTOR_SIZE, parm::PAGE_SIZE + parm::SECTOR_SIZE);

    auto ground_truth = testing::test_page1();

    ck_assert_int_eq(new_file->read(buf, parm::PAGE_SIZE, 0), 1);
    ck_assert_mem_eq(buf, ground_truth.get(), parm::PAGE_SIZE);

    std::free(buf);
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("Direct File Unit Testing");
    TCase *create = tcase_create("lsm::io::DirectFile::create Testing");
    tcase_add_checked_fixture(create, testing::initialize_file1, testing::delete_file1);
    tcase_add_test(create, t_create_file);
    tcase_add_test(create, t_open_file);

    suite_add_tcase(unit, create);


    TCase *get_size = tcase_create("lsm::io::DirectFile::get_size Testing");
    tcase_add_checked_fixture(get_size, testing::initialize_file1, testing::delete_file1);
    tcase_add_test(get_size, t_get_size_empty);
    tcase_add_test(get_size, t_get_size_existing);

    suite_add_tcase(unit, get_size);


    TCase *allocate = tcase_create("lsm::io::DirectFile::allocate Testing");
    tcase_add_checked_fixture(allocate, testing::initialize_file1, testing::delete_file1);
    tcase_add_test(allocate, t_allocate_empty);
    tcase_add_test(allocate, t_allocate_existing);

    suite_add_tcase(unit, allocate);


    TCase *openclose = tcase_create("lsm::io::DirectFile::{open, close_file, is_open} Testing");
    tcase_add_checked_fixture(openclose, testing::initialize_file1, testing::delete_file1);
    tcase_add_test(openclose, t_open_close);

    suite_add_tcase(unit, openclose);


    TCase *remove = tcase_create("lsm::io::DirectFile::remove Testing");
    tcase_add_checked_fixture(remove, testing::initialize_file1, testing::delete_file1);
    tcase_add_test(remove, t_remove);

    suite_add_tcase(unit, remove);


    TCase *write = tcase_create("lsm::io::DirectFile::write Testing");
    tcase_add_checked_fixture(write, testing::initialize_file1, testing::delete_file1);
    tcase_add_test(write, t_write_empty);
    tcase_add_test(write, t_read_write_existing);

    suite_add_tcase(unit, write);


    TCase *read = tcase_create("lsm::io::DirectFile::read Testing");
    tcase_add_checked_fixture(read, testing::initialize_file1, testing::delete_file1);
    tcase_add_test(read, t_read_empty);
    tcase_add_test(read, t_read_existing);

    suite_add_tcase(unit, read);

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
