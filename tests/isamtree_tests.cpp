#include <check.h>
#include <string>

#include "testing.h"
#include "lsm/IsamTree.h"
#include "lsm/InMemRun.h"
#include "io/PagedFile.h"
#include "ds/BloomFilter.h"
#include "util/internal_record.h"

using namespace lsm;

std::string g_fname1 = "tests/data/isamtree1.dat";
gsl_rng *g_rng = gsl_rng_alloc(gsl_rng_mt19937);

static MemTable *create_test_memtable(size_t cnt);
static MemTable *create_test_memtable_dupes(size_t cnt);
static size_t required_leaf_pages(size_t);

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


static ISAMTree *create_test_isam_dupes(size_t cnt, std::string fname, MemTable **mtbl, BloomFilter **filter) 
{
    *filter = new BloomFilter(100, 9, g_rng);
    auto mtable = create_test_memtable_dupes(cnt);
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


static void check_test_isam(ISAMTree *tree, size_t record_cnt, size_t tombstone_cnt=0) 
{
    ck_assert_ptr_nonnull(tree);

    ck_assert_int_eq(tree->get_record_count(), record_cnt);
    ck_assert_int_eq(tree->get_leaf_page_count(), required_leaf_pages(record_cnt));
    ck_assert_int_eq(tree->get_tombstone_count(), tombstone_cnt);
}


static void free_isam(ISAMTree *tree, BloomFilter *filter, MemTable *tbl)
{
    if (tree) {
        auto file = tree->get_pfile();
        file->remove_file();
        delete file;
        delete tree;
    }

    if (filter) {
        delete filter;
    }

    if (tbl) {
        delete tbl;
    }
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


static MemTable *create_test_memtable_dupes(size_t cnt) 
{
    auto mtable = new MemTable(cnt, true, 0, g_rng);

    for (size_t i = 0; i < cnt / 2; i++) {
        key_type key = i;
        value_type val = i;

        mtable->append((char*) &key, (char *) &val);
    }

    for (size_t i = 0; i < cnt / 2; i++) {
        key_type key = i;
        value_type val = i + 1;

        mtable->append((char*) &key, (char *) &val);
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


START_TEST(t_create_test_isam) 
{
    MemTable *tbl = nullptr;
    BloomFilter *filter = nullptr;
    char *buf = (char *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE);

    size_t n = 1000000;
    auto tree = create_test_isam(n, "tests/data/mrun_isam0.dat", &tbl, &filter);
    check_test_isam(tree, n);

    tbl->sorted_output();
    auto iter = tree->start_scan();
    ck_assert_ptr_nonnull(iter);

    size_t total_cnt = 0;
    while (iter->next()) {
        for (size_t i=0; i< PAGE_SIZE / record_size; i++) {
            if (++total_cnt > n) {
                break;
            }

            const char *tbl_rec = tbl->get_record_at(total_cnt - 1);
            const char *tree_rec = iter->get_item() + (i * record_size);
            ck_assert(record_cmp(tree_rec, tbl_rec) == 0);
        }
    }

    ck_assert_int_eq(total_cnt - 1 , n);
    free_isam(tree, filter, tbl);
    delete iter;
    free(buf);
}
END_TEST


START_TEST(t_get_lower_bound_index)
{
    MemTable *tbl = nullptr;
    BloomFilter *filter = nullptr;
    char *buf = (char *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE);

    size_t n = 1000000;
    auto tree = create_test_isam(n, "tests/data/mrun_isam0.dat", &tbl, &filter);
    check_test_isam(tree, n);

    tbl->sorted_output();
    for (size_t i=0; i<n; i++) {
        const char *tbl_rec = tbl->get_record_at(i);
        auto tree_loc = tree->get_lower_bound_index(get_key(tbl_rec), buf);
        ck_assert_int_ne(tree_loc.first, INVALID_PNUM);
        size_t idx = tree_loc.second;

        const char *tree_key = get_key(buf + (idx * record_size));
        ck_assert_int_eq(*(key_type *) tree_key, *(key_type*) tbl_rec);
    }

    free_isam(tree, filter, tbl);
    free(buf);
}
END_TEST


START_TEST(t_get_upper_bound_index)
{
    MemTable *tbl = nullptr;
    BloomFilter *filter = nullptr;
    char *buf = (char *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE);

    size_t n = 1000000;
    auto tree = create_test_isam(n, "tests/data/mrun_isam0.dat", &tbl, &filter);
    check_test_isam(tree, n);

    tbl->sorted_output();
    for (size_t i=0; i<n; i++) {
        const char *tbl_rec = tbl->get_record_at(i);
        auto tree_loc = tree->get_upper_bound_index(get_key(tbl_rec), buf);
        ck_assert_int_ne(tree_loc.first, INVALID_PNUM);
        size_t idx = tree_loc.second;

        const char *tree_key = get_key(buf + (idx * record_size));
        ck_assert_int_eq(*(key_type *) tree_key, *(key_type*) tbl_rec);
    }

    free_isam(tree, filter, tbl);
    free(buf);
}
END_TEST


START_TEST(t_create_from_isams)
{
    MemTable *tbl1, *tbl2, *tbl3;
    BloomFilter *filter1, *filter2, *filter3;

    size_t n = 1000000;
    auto tree1 = create_test_isam(n, "tests/data/mrun_isam1.dat", &tbl1, &filter1);
    check_test_isam(tree1, n);

    auto tree2 = create_test_isam(n, "tests/data/mrun_isam2.dat", &tbl2, &filter2);
    check_test_isam(tree2, n);

    auto tree3 = create_test_isam(n, "tests/data/mrun_isam3.dat", &tbl3, &filter3);
    check_test_isam(tree3, n);

    ISAMTree *trees[3] = {tree1, tree2, tree3};
    auto filter4 = new BloomFilter(100, 9, g_rng);
    auto pfile = PagedFile::create("tests/data/mrun_isam4.dat", true);
    auto tree4 = new ISAMTree(pfile, g_rng, filter4, nullptr, 0, trees, 3);
    check_test_isam(tree4, sizeof(trees)/8*n);

    tbl1->sorted_output();
    tbl2->sorted_output();
    tbl3->sorted_output();

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

            const char *tbl1_rec = tbl1->get_record_at(tbl1_idx);
            const char *tbl2_rec = tbl2->get_record_at(tbl2_idx);
            const char *tbl3_rec = tbl3->get_record_at(tbl3_idx);

            const char *tree_rec = iter->get_item() + (i * record_size);

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

    free_isam(tree1, filter1, tbl1);
    free_isam(tree2, filter2, tbl2);
    free_isam(tree3, filter3, tbl3);
    free_isam(tree4, filter4, nullptr);
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
    check_test_isam(isam, cnt);

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

    delete l1_iter;
    free_isam(isam, filter, mtable);
}
END_TEST


START_TEST(t_get_lower_bound_index_dupes)
{
    MemTable *tbl = nullptr;
    BloomFilter *filter = nullptr;
    char *buf = (char *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    size_t n = 10000;

    auto tree = create_test_isam_dupes(n, "tests/data/mrun_isam0.dat", &tbl, &filter);
    check_test_isam(tree, n);

    auto tbl_records = tbl->sorted_output();
    for (size_t i=0; i<n; i++) {
        auto tbl_key_ptr = get_key(tbl->get_record_at(i));
        auto tbl_key = *(key_type *) tbl_key_ptr;

        auto pos = tree->get_lower_bound_index(tbl_key_ptr, buf);
        ck_assert_int_ne(pos.first, INVALID_PNUM);

        auto tree_key = *(key_type*) get_key(buf + (pos.second * record_size));
        auto tree_val = *(value_type*) get_val(buf + (pos.second * record_size));
        ck_assert_int_eq(tree_key, tbl_key);
        ck_assert(tbl_key == tree_val || tbl_key - 1 == tree_val);
        size_t overall_offset = (pos.first - BTREE_FIRST_LEAF_PNUM) * PAGE_SIZE / record_size + pos.second;
        ck_assert_int_le(overall_offset, i);
    }

    free_isam(tree, filter, tbl);
    free(buf);
}


START_TEST(t_get_upper_bound_index_dupes)
{
    MemTable *tbl = nullptr;
    BloomFilter *filter = nullptr;
    char *buf = (char *) aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    size_t n = 10000;

    auto tree = create_test_isam_dupes(n, "tests/data/mrun_isam0.dat", &tbl, &filter);
    check_test_isam(tree, n);

    auto tbl_records = tbl->sorted_output();
    for (size_t i=0; i<n; i++) {
        auto tbl_key_ptr = get_key(tbl->get_record_at(i));
        auto tbl_key = *(key_type *) tbl_key_ptr;

        auto pos = tree->get_upper_bound_index(tbl_key_ptr, buf);
        ck_assert_int_ne(pos.first, INVALID_PNUM);

        auto tree_key = *(key_type*) get_key(buf + (pos.second * record_size));
        auto tree_val = *(value_type*) get_val(buf + (pos.second * record_size));
        ck_assert_int_eq(tree_key, tbl_key);
        ck_assert(tbl_key == tree_val || tbl_key + 1 == tree_val);
        size_t overall_offset = (pos.first - BTREE_FIRST_LEAF_PNUM) * PAGE_SIZE / record_size + pos.second;
        ck_assert_int_ge(overall_offset, i);
    }

    free_isam(tree, filter, tbl);
    free(buf);
}


Suite *unit_testing()
{
    Suite *unit = suite_create("IsamTree Unit Testing");
    TCase *create = tcase_create("lsm::ISAMTree::create Testing");
    tcase_add_test(create, t_create_test_isam);
    tcase_add_test(create, t_verify_page_structure);
    tcase_add_test(create, t_create_from_isams);

    tcase_set_timeout(create, 100);
    suite_add_tcase(unit, create);

    TCase *bounds = tcase_create("lsm::ISAMTree::get_{lower,upper}_bound Testing");
    tcase_add_test(bounds, t_get_lower_bound_index);
    tcase_add_test(bounds, t_get_upper_bound_index);
    tcase_add_test(bounds, t_get_lower_bound_index_dupes);
    tcase_add_test(bounds, t_get_upper_bound_index_dupes);

    tcase_set_timeout(bounds, 1000);
    suite_add_tcase(unit, bounds);

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

