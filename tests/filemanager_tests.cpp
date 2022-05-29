/*
 *
 */

#include <check.h>
#include "io/filemanager.hpp"
#include "testing.hpp"

using namespace lsm;
using namespace lsm::io;
using std::byte;


START_TEST(t_create)
{
    std::string root_dir = "tests/data/filemanager/";
    auto test_fm = FileManager(root_dir);


    std::string metafile = "tests/data/fm_metadata";
    auto test_fm2 = FileManager(root_dir, metafile);
    auto test_metafile = test_fm2.get_metafile_name();
    ck_assert_str_eq(test_metafile.c_str(), metafile.c_str());
}
END_TEST


START_TEST(t_create_dfile)
{
    auto fm = testing::create_test_fm();

    // Creating files with auto-generated names
    FileId flid1;
    FileId flid2;
    auto file1 = fm->create_dfile("", &flid1);
    auto file2 = fm->create_dfile("", &flid2);

    ck_assert_ptr_nonnull(file1);
    ck_assert_ptr_nonnull(file2);
    ck_assert_int_eq(flid1, 1);
    ck_assert_int_eq(flid2, 2);

    ck_assert(file1->is_open());
    ck_assert(file2->is_open());

    ck_assert_int_eq(file1->get_size(), 0);
    ck_assert_int_eq(file2->get_size(), 0);
    
    ck_assert_int_eq(flid1, fm->get_flid(fm->get_name(flid1)));
    ck_assert_int_eq(flid2, fm->get_flid(fm->get_name(flid2)));

    // Test creating a file with a specified name
    std::string new_fname = "hello-there.txt";
    std::string new_pathname = testing::fm_root_dir + new_fname;
    FileId flid3;
    auto file3 = fm->create_dfile(new_fname, &flid3);
    ck_assert_ptr_nonnull(file3);
    ck_assert_int_eq(flid3, 3);

    ck_assert(file3->is_open());
    ck_assert_int_eq(file3->get_size(), 0);

    auto lookup_fname = fm->get_name(flid3);
    auto lookup_flid = fm->get_flid(new_pathname);

    ck_assert_str_eq(lookup_fname.c_str(), new_pathname.c_str());
    ck_assert_int_eq(flid3, lookup_flid);


    // Cannot create a file with a duplicate name
    auto file4 = fm->create_dfile(new_fname);
    ck_assert_ptr_null(file4);
} 
END_TEST


START_TEST(t_create_indexed_pfile)
{
    auto fm = testing::create_test_fm();

    // Creating files with auto-generated names
    auto file1 = fm->create_indexed_pfile("", false);
    auto file2 = fm->create_indexed_pfile("", true);
    FileId flid1 = file1->get_flid();
    FileId flid2 = file2->get_flid();

    ck_assert_ptr_nonnull(file1);
    ck_assert_ptr_nonnull(file2);
    ck_assert_int_eq(flid1, 1);
    ck_assert_int_eq(flid2, 2);

    ck_assert_int_eq(file1->get_page_count(), 0);
    ck_assert_int_eq(file2->get_page_count(), 1);  // feature not yet implemented
    ck_assert_int_eq(file1->virtual_header_initialized(), false);
    ck_assert_int_eq(file2->virtual_header_initialized(), true);
    
    ck_assert_int_eq(flid1, fm->get_flid(fm->get_name(flid1)));
    ck_assert_int_eq(flid2, fm->get_flid(fm->get_name(flid2)));

    // Test creating a file with a specified name
    std::string new_fname = "hello-there.txt";
    std::string new_pathname = testing::fm_root_dir + new_fname;
    auto file3 = fm->create_indexed_pfile(new_fname, false);
    FileId flid3 = file3->get_flid();
    ck_assert_ptr_nonnull(file3);
    ck_assert_int_eq(flid3, 3);

    auto lookup_fname = fm->get_name(flid3);
    auto lookup_flid = fm->get_flid(new_pathname);

    ck_assert_str_eq(lookup_fname.c_str(), new_pathname.c_str());
    ck_assert_int_eq(flid3, lookup_flid);

    // Cannot create a file with a duplicate name
    auto file4 = fm->create_indexed_pfile(new_fname);
    ck_assert_ptr_null(file4);
}
END_TEST


START_TEST(t_create_temp_file)
{
    auto fm = testing::create_test_fm();

    // Creating files with auto-generated names
    auto file1 = fm->create_temp_indexed_pfile();
    auto file2 = fm->create_temp_indexed_pfile();
    FileId flid1 = file1->get_flid();
    FileId flid2 = file2->get_flid();

    ck_assert_ptr_nonnull(file1);
    ck_assert_ptr_nonnull(file2);
    ck_assert_int_eq(flid1, 1);
    ck_assert_int_eq(flid2, 2);

    ck_assert_int_eq(file1->get_page_count(), 0);
    ck_assert_int_eq(file2->get_page_count(), 0);  // feature not yet implemented
    ck_assert_int_eq(file1->virtual_header_initialized(), false);
    ck_assert_int_eq(file2->virtual_header_initialized(), false);

    std::string name1 = fm->get_name(flid1);
    std::string name2 = fm->get_name(flid2);
    
    ck_assert_int_eq(flid1, fm->get_flid(fm->get_name(flid1)));
    ck_assert_int_eq(flid2, fm->get_flid(fm->get_name(flid2)));

    // Test creating a file with a specified name
    auto file3 = fm->create_temp_indexed_pfile();
    FileId flid3 = file3->get_flid();
    ck_assert_ptr_nonnull(file3);
    ck_assert_int_eq(flid3, 3);

    std::string name3 = fm->get_name(flid3);

    auto lookup_flid = fm->get_flid(name3);
    ck_assert_int_eq(flid3, lookup_flid);
    file3->make_permanent();

    fm.reset();

    // verify that they files have all been deleted
    ck_assert_int_ne(access(name1.c_str(), F_OK), 0);
    ck_assert_int_ne(access(name2.c_str(), F_OK), 0);
    ck_assert_int_eq(access(name3.c_str(), F_OK), 0);
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("FileManager Unit Testing");
    TCase *create = tcase_create("lsm::io::FileManager::constructor Testing");
    tcase_add_test(create, t_create);

    suite_add_tcase(unit, create);


    TCase *create_dfile = tcase_create("lsm::io::FileManager::create_dfile Testing");
    tcase_add_test(create_dfile, t_create_dfile);

    suite_add_tcase(unit, create_dfile);


    TCase *create_index_pfile = tcase_create("lsm::io::FileManager::create_index_pfile Testing");
    tcase_add_test(create_index_pfile, t_create_indexed_pfile);

    suite_add_tcase(unit, create_index_pfile);

    TCase *create_temp_file = tcase_create("lsm::io::FileManager::create_temp_index_pfile Testing");
    tcase_add_test(create_temp_file, t_create_temp_file);

    suite_add_tcase(unit, create_temp_file);

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
