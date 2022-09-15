#include <check.h>
#include <string>

#include "testing.h"
#include "ds/IsamTree.h"
#include "io/PagedFile.h"

using namespace lsm;

std::string g_fname1 = "tests/data/isamtree1.dat";
gsl_rng *g_rng = gsl_rng_alloc(gsl_rng_mt19937);

MemTable *create_test_memtable(size_t cnt);

ISAMTree *create_test_isam(size_t cnt, std::string fname, MemTable *mtbl) 
{
    auto mtable = create_test_memtable(cnt);
    auto pfile = PagedFile::create(fname);

    if (mtbl) {
        mtbl = mtable;
    } else {
        delete mtable;
    }

    return ISAMTree::create(pfile, mtable, g_rng);
}


MemTable *create_test_memtable(size_t cnt)
{
    auto mtable = new MemTable(cnt, true, 0, g_rng);

    for (size_t i=0; i<cnt; i++) {
        key_type key = rand();
        value_type val = rand();

        mtable->append((char*) &key, (char*) &val);
    }

    return mtable;
}


START_TEST(t_create_from_memtable)
{
    size_t cnt = 15000;
    size_t page_cnt = ((cnt*record_size) % PAGE_SIZE == 0) ? (cnt*record_size) / PAGE_SIZE : (cnt*record_size) / PAGE_SIZE + 1;
    auto mtable = create_test_memtable(cnt);
    auto pfile = PagedFile::create(g_fname1);
    auto isam = ISAMTree::create(pfile, mtable, g_rng);

    ck_assert_ptr_nonnull(isam);
    ck_assert_int_eq(isam->get_record_count(), mtable->get_record_count());
    ck_assert_int_eq(isam->get_leaf_page_count(), page_cnt);

    delete mtable;
    delete pfile;
    delete isam;
}
END_TEST


START_TEST(t_create_from_memtable_isam)
{
    size_t cnt_tbl = 10000;
    size_t cnt_isam = 15000;
    size_t cnt = cnt_tbl + cnt_isam;
    size_t page_cnt = ((cnt*record_size) % PAGE_SIZE == 0) ? (cnt*record_size) / PAGE_SIZE : (cnt*record_size) / PAGE_SIZE + 1;

    auto mtable = create_test_memtable(cnt_tbl);
    MemTable *mtable2;
    auto isam1 = create_test_isam(cnt_isam, "tests/data/testisam2.dat", mtable2);

    auto pfile = PagedFile::create("tests/data/testisam3.dat");
    auto isam = ISAMTree::create(pfile, mtable, isam1, g_rng);

    ck_assert_ptr_nonnull(isam);
    ck_assert_int_eq(isam->get_record_count(), cnt);
    ck_assert_int_eq(isam->get_leaf_page_count(), page_cnt);

    delete mtable;
    delete mtable2;
    delete pfile;
    delete isam;
    delete isam1;
}
END_TEST



Suite *unit_testing()
{
    Suite *unit = suite_create("IsamTree Unit Testing");
    TCase *create = tcase_create("lsm::ISAMTree::create Testing");
    tcase_add_test(create, t_create_from_memtable);
    tcase_add_test(create, t_create_from_memtable_isam);

    suite_add_tcase(unit, create);

    return unit;
}


int run_unit_tests()
{
    int failed = 0;
    Suite *unit = unit_testing();
    SRunner *unit_runner = srunner_create(unit);

    srunner_run_all(unit_runner, CK_NORMAL);
    failed = srunner_ntests_failed(unit_runner);
    srunner_free(unit_runner);

    return failed;
}


int main() 
{
    int unit_failed = run_unit_tests();

    return (unit_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}

