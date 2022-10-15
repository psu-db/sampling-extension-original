#include <check.h>
#include <string>

#include "testing.h"
#include "lsm/IsamTree.h"
#include "lsm/InMemRun.h"
#include "io/PagedFile.h"
#include "ds/BloomFilter.h"

using namespace lsm;

std::string g_fname1 = "tests/data/isamtree1.dat";
gsl_rng *g_rng = gsl_rng_alloc(gsl_rng_mt19937);

static MemTable *create_test_memtable(size_t cnt);

static ISAMTree *create_test_isam(size_t cnt, std::string fname, MemTable **mtbl, BloomFilter **filter) 
{
    *filter = new BloomFilter(100, 9, g_rng);
    auto mtable = create_test_memtable(cnt);
    auto memrun = new InMemRun(mtable, *filter);
    auto pfile = PagedFile::create(fname);
    assert(pfile);

    if (mtbl) {
        *mtbl = mtable;
    } else {
        delete mtable;
    }

    (*filter)->clear();
    auto tree = new ISAMTree(pfile, g_rng, *filter, &memrun, 1, nullptr, 0);

    delete memrun;

    return tree;
}


static ISAMTree *create_isam_from_memtable(PagedFile *pfile, MemTable *mtable, BloomFilter **filter)
{
    *filter = new BloomFilter(100, 9, g_rng);
    auto memrun = new InMemRun(mtable, *filter);

    (*filter)->clear();
    auto tree = new ISAMTree(pfile, g_rng, *filter, &memrun, 1, nullptr, 0);
    delete memrun;

    return tree;
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

START_TEST(t_create_test_isam) 
{
    MemTable *tbl = nullptr;
    BloomFilter *filter = nullptr;
    char *buf = (char *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE);

    size_t n = 1000000;
    auto tree1 = create_test_isam(n, "tests/data/mrun_isam0.dat", &tbl, &filter);

    ck_assert_ptr_nonnull(tree1);
    ck_assert_ptr_nonnull(tbl);
    ck_assert_ptr_nonnull(filter);

    ck_assert_int_eq(tree1->get_record_count(), n);
    ck_assert_int_eq(tree1->get_leaf_page_count(), required_leaf_pages(n));
    ck_assert_int_eq(tree1->get_tombstone_count(), 0);

    auto tbl_data = tbl->sorted_output();
    auto iter = tree1->start_scan();
    ck_assert_ptr_nonnull(iter);

    size_t total_cnt = 0;
    while (iter->next()) {
        for (size_t i=0; i< PAGE_SIZE / record_size; i++) {
            if (++total_cnt > n) {
                break;
            }

            char *tbl_rec = tbl_data + ((total_cnt - 1) * record_size);
            char *tree_rec = iter->get_item() + (i * record_size);
            ck_assert(record_cmp(tree_rec, tbl_rec) == 0);
        }
    }

    ck_assert_int_eq(total_cnt -1 , n);
    delete iter;
    auto pf = tree1->get_pfile();
    delete tree1;
    delete pf;
    delete tbl;
    delete filter;
    free(buf);
}
END_TEST


START_TEST(t_get_lower_bound_index)
{
    MemTable *tbl = nullptr;
    BloomFilter *filter = nullptr;
    char *buf = (char *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE);

    size_t n = 1000000;
    auto tree1 = create_test_isam(n, "tests/data/mrun_isam0.dat", &tbl, &filter);

    ck_assert_ptr_nonnull(tree1);
    ck_assert_ptr_nonnull(tbl);
    ck_assert_ptr_nonnull(filter);

    ck_assert_int_eq(tree1->get_record_count(), n);
    ck_assert_int_eq(tree1->get_leaf_page_count(), required_leaf_pages(n));
    ck_assert_int_eq(tree1->get_tombstone_count(), 0);

    auto tbl_records = tbl->sorted_output();
    for (size_t i=0; i<n; i++) {
        char *tbl_rec = tbl_records + (i * record_size);
        auto tree_loc = tree1->get_lower_bound_index(get_key(tbl_rec), buf);
        ck_assert_int_ne(tree_loc.first, INVALID_PNUM);
        tree_loc.second;
    }


}
END_TEST

START_TEST(t_create_from_isams)
{
    MemTable *tbl1, *tbl2, *tbl3;
    BloomFilter *filter1, *filter2, *filter3;

    size_t n = 1000000;
    auto tree1 = create_test_isam(n, "tests/data/mrun_isam1.dat", &tbl1, &filter1);

    ck_assert_ptr_nonnull(tree1);
    ck_assert_ptr_nonnull(tbl1);
    ck_assert_ptr_nonnull(filter1);

    auto tree2 = create_test_isam(n, "tests/data/mrun_isam2.dat", &tbl2, &filter2);
    ck_assert_ptr_nonnull(tree2);
    ck_assert_ptr_nonnull(tbl2);
    ck_assert_ptr_nonnull(filter2);

    auto tree3 = create_test_isam(n, "tests/data/mrun_isam3.dat", &tbl3, &filter3);

    auto filter4 = new BloomFilter(100, 9, g_rng);
    auto pfile = PagedFile::create("tests/data/mrun_isam4.dat", true);

    ISAMTree *trees[3] = {tree1, tree2, tree3};

    auto tree4 = new ISAMTree(pfile, g_rng, filter4, nullptr, 0, trees, 3);

    ck_assert_int_eq(tree4->get_record_count(), 3*n);
    ck_assert_int_eq(tree4->get_leaf_page_count(), required_leaf_pages(3*n));
    ck_assert_int_eq(tree4->get_tombstone_count(), 0);

    auto tbl1_data = tbl1->sorted_output();
    auto tbl2_data = tbl2->sorted_output();
    auto tbl3_data = tbl3->sorted_output();

    auto iter = tree4->start_scan();
    ck_assert_ptr_nonnull(iter);

    size_t total_cnt = 0;
    size_t tbl1_idx = 0;
    size_t tbl2_idx = 0;
    size_t tbl3_idx = 0;

    while (iter->next()) {
        for (size_t i=0; i< PAGE_SIZE / record_size; i++) {
            if (++total_cnt > 3*n) {
                break;
            }

            char *tbl1_rec = tbl1_data + (tbl1_idx * record_size);
            char *tbl2_rec = tbl2_data + (tbl2_idx * record_size);
            char *tbl3_rec = tbl3_data + (tbl3_idx * record_size);

            char *tree_rec = iter->get_item() + (i * record_size);

            if (tbl1_idx < n && record_cmp(tree_rec, tbl1_rec) == 0) {
                tbl1_idx++;
            } else if (tbl2_idx < n && record_cmp(tree_rec, tbl2_rec) == 0) {
                tbl2_idx++;
            } else if (tbl3_idx < n && record_cmp(tree_rec, tbl3_rec) == 0) {
                tbl3_idx++;
            } else {
                assert(false);
            }
        }
    }
    ck_assert_int_eq(total_cnt -1 , 3*n);
    delete iter;

    // FIXME, need a better way to manage file eventually??
    auto file = tree1->get_pfile();
    delete tree1;
    delete file;
    delete tbl1;
    delete filter1; 

    file = tree2->get_pfile();
    delete tree2;
    delete file;
    delete tbl2;
    delete filter2;

    file = tree3->get_pfile();
    delete tree3;
    delete file;
    delete tbl3;
    delete filter3;

    file = tree4->get_pfile();
    delete tree4;
    delete file;
    delete filter4;
}
END_TEST


START_TEST(t_verify_page_structure)
{
    size_t cnt = 1000000;
    size_t page_cnt = required_leaf_pages(cnt);
    size_t records_per_leaf = PAGE_SIZE / record_size;

    auto mtable = create_sequential_memtable(cnt);
    BloomFilter *filter;

    auto pfile = PagedFile::create("tests/data/sequential_isam.dat");
    auto isam = create_isam_from_memtable(pfile, mtable, &filter);

    ck_assert_ptr_nonnull(isam);
    ck_assert_int_eq(isam->get_record_count(), cnt);
    ck_assert_int_eq(isam->get_leaf_page_count(), page_cnt);

    auto pg_iter = isam->start_scan();
    size_t total_records = 0;
    while (pg_iter->next()) {
        char *leaf_page = pg_iter->get_item();

        for (size_t i=0; i<records_per_leaf; i++) {
            if (total_records >= cnt) break;

            key_type key = *(key_type*)get_key(leaf_page + i*record_size);
            ck_assert_int_eq(key, total_records);
            total_records++;
        }
    }

    ck_assert_int_eq(total_records, cnt);

    delete pg_iter;

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

    // check the root page
    auto root_pg = l1_iter->get_item();
    current_pnum = isam->get_leaf_page_count() + 2;
    for (size_t i=0; i<16; i++) {
        auto rec = get_internal_record(root_pg, i);
        auto key = *(int64_t*) get_internal_key(rec);
        val = get_internal_value(rec);

        ck_assert_int_eq(current_pnum, val);
        current_pnum++;
    }

    ck_assert_int_eq(((ISAMTreeInternalNodeHeader *) root_pg)->leaf_rec_cnt, cnt);

    pfile->remove_file();

    delete l1_iter;
    delete mtable;
    delete pfile;
    delete isam;
    delete filter;
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("IsamTree Unit Testing");
    TCase *create = tcase_create("lsm::ISAMTree::create Testing");
    //tcase_add_test(create, t_create_from_memtable);
    //tcase_add_test(create, t_create_from_memtable_isam);
    tcase_add_test(create, t_create_test_isam);
    tcase_add_test(create, t_verify_page_structure);
    tcase_add_test(create, t_create_from_isams);

    tcase_set_timeout(create, 100);

    suite_add_tcase(unit, create);

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

