#include <check.h>
#include <string>

#include "testing.h"
#include "ds/IsamTree.h"
#include "io/PagedFile.h"

using namespace lsm;

std::string g_fname1 = "tests/data/isamtree1.dat";
gsl_rng *g_rng = gsl_rng_alloc(gsl_rng_mt19937);

static MemTable *create_test_memtable(size_t cnt);

static ISAMTree *create_test_isam(size_t cnt, std::string fname, MemTable **mtbl) 
{
    auto mtable = create_test_memtable(cnt);
    auto pfile = PagedFile::create(fname);

    if (mtbl) {
        *mtbl = mtable;
    } else {
        delete mtable;
    }

    return ISAMTree::create(pfile, mtable, g_rng);
}


static MemTable *create_test_memtable(size_t cnt)
{
    auto mtable = new MemTable(cnt, true, 0, g_rng);

    for (size_t i=0; i<cnt; i++) {
        key_type key = rand();
        value_type val = rand();

        mtable->append((char*) &key, (char*) &val);
    }

    return mtable;
}


static MemTable *create_sequential_memtable(size_t cnt) 
{
    auto mtable = new MemTable(cnt, true, 0, g_rng);

    for (size_t i=0; i<cnt; i++) {
        key_type key = i;
        value_type val = i;

        mtable->append((char*) &key, (char *) &val);
    }

    return mtable;
}


static size_t required_leaf_pages(size_t cnt)
{
    size_t records_per_page = PAGE_SIZE / record_size;
    size_t excess_records = cnt % records_per_page;

    return cnt / records_per_page + (excess_records != 0);
}

static size_t internal_record_size = key_size + MAXALIGN(sizeof(PageNum));
static size_t internal_records_per_page = (PAGE_SIZE - ISAMTreeInternalNodeHeaderSize) / internal_record_size;

static const char *get_internal_record(const char *internal_page_buffer, size_t idx) {
    return internal_page_buffer + ISAMTreeInternalNodeHeaderSize + internal_record_size * idx;
}

static const char *get_internal_key(const char *buffer) {
    return buffer;
}

static PageNum get_internal_value(const char *buffer) {
    return *((PageNum *) (buffer + key_size));
}


START_TEST(t_create_from_memtable)
{
    size_t cnt = 15000;
    size_t page_cnt = required_leaf_pages(cnt);
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
    size_t page_cnt = required_leaf_pages(cnt);

    auto mtable = create_test_memtable(cnt_tbl);
    MemTable *mtable2 = nullptr;
    auto isam1 = create_test_isam(cnt_isam, "tests/data/testisam2.dat", &mtable2);

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


START_TEST(t_verify_page_structure)
{
    size_t cnt = 1000000;
    size_t page_cnt = required_leaf_pages(cnt);
    size_t records_per_leaf = PAGE_SIZE / record_size;

    auto mtable = create_sequential_memtable(cnt);

    auto pfile = PagedFile::create("tests/data/sequential_isam.dat");
    auto isam = ISAMTree::create(pfile, mtable, g_rng);

    ck_assert_ptr_nonnull(isam);
    ck_assert_int_eq(isam->get_record_count(), cnt);
    ck_assert_int_eq(isam->get_leaf_page_count(), page_cnt);

    auto pg_iter = isam->start_scan();
    size_t total_records = 0;
    while (pg_iter->next()) {
        char *leaf_page = pg_iter->get_item();

            for (size_t i=0; i<records_per_leaf; i++) {
                if (total_records >= cnt) {
                    break;
                }

                key_type key = *(key_type*)get_key(leaf_page + i*record_size);

                ck_assert_int_eq(key, total_records);
                total_records++;
            }
        }

        ck_assert_int_eq(total_records, cnt);

        // check on the first internal level
        auto l1_iter = pfile->start_scan(isam->get_leaf_page_count() + 2);
        PageNum current_pnum = BTREE_FIRST_LEAF_PNUM;

        bool done = false;
        PageNum val;
        size_t rec_cnt = 0;
        while (l1_iter->next() && !done) {
            char *l1_page = l1_iter->get_item();
            rec_cnt += ((ISAMTreeInternalNodeHeader*) l1_page)->leaf_rec_cnt;
            for (size_t i=0; i<internal_records_per_page; i++) {
                if (current_pnum > 1 + isam->get_leaf_page_count()) {
                    done = true;
                    break;
                }

                auto rec = get_internal_record(l1_page, i);
                auto key = *(int64_t*) get_internal_key(rec);
                val = get_internal_value(rec);

                ck_assert_int_eq(current_pnum, val);
                current_pnum++;
            }
        }

        ck_assert_int_eq(val, isam->get_leaf_page_count() + 1);
        ck_assert_int_eq(rec_cnt, cnt);

        delete mtable;
        delete pfile;
        delete isam;
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("IsamTree Unit Testing");
    TCase *create = tcase_create("lsm::ISAMTree::create Testing");
    tcase_add_test(create, t_create_from_memtable);
    tcase_add_test(create, t_create_from_memtable_isam);
    tcase_add_test(create, t_verify_page_structure);

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

