/*
 *
 */

#include <check.h>
#include "testing.hpp"

#include "ds/sortedrun.hpp"

using namespace lsm;
using std::byte;
using namespace std::placeholders;

auto g_schema = testing::test_schema1(sizeof(int64_t));
auto g_cache = new io::ReadCache(1024);

int compare_func(const byte *a, const byte * b) 
{
    auto key1 = g_schema->get_key(a).Int64();
    auto key2 = g_schema->get_key(b).Int64();

    if (key1 < key2) {
        return -1;
    } else if (key1 == key2) {
        return 0;
    }

    return 1;
}

int compare_func_key(const byte *a, const byte * b) 
{
    auto key1 = *((int64_t *) a);
    auto key2 = *((int64_t *) b);

    if (key1 < key2) {
        return -1;
    } else if (key1 == key2) {
        return 0;
    }

    return 1;
}

std::unique_ptr<iter::MergeIterator> test_merge_iterator(PageOffset value_size, size_t *rec_cnt=nullptr)
{
    auto schema = testing::test_schema1(value_size);

    std::vector<std::unique_ptr<iter::GenericIterator<Record>>> iters(3);
    size_t cnt1 = 0;
    auto fname1 = testing::generate_isamtree_test_data1(100, value_size, &cnt1);
    auto *pfile1 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname1);
    iters[0] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile1, g_cache);

    size_t cnt2 = 0;
    auto fname2 = testing::generate_isamtree_test_data2(100, value_size, &cnt2);
    auto pfile2 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname2);
    iters[1] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile2, g_cache);

    size_t cnt3 = 0;
    auto fname3 = testing::generate_isamtree_test_data3(200, value_size, &cnt3);
    auto pfile3 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname3);
    iters[2] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile3, g_cache);

    const catalog::RecordCmpFunc cmp = std::bind(&compare_func, _1, _2);

    if (rec_cnt) {
        *rec_cnt = cnt1 + cnt2 + cnt3;
    }

    return std::make_unique<iter::MergeIterator>(iters, cmp);
}


START_TEST(t_initialize)
{
    testing::initialize_global_fm();
    auto state = testing::make_state1();

    size_t rec_cnt;
    auto iterator = test_merge_iterator(state->record_schema->val_len(), &rec_cnt);
    g_schema = testing::test_schema1(state->record_schema->val_len());

    const catalog::KeyCmpFunc cmp = std::bind(&compare_func_key, _1, _2);

    auto buffer = mem::create_aligned_buffer(rec_cnt * g_schema->record_length());
    ds::SortedRun::initialize(buffer.get(), std::move(iterator), rec_cnt, state.get(),  false, 0);
    
    auto run = ds::SortedRun(std::move(buffer), rec_cnt, state.get(), 0);

    ck_assert_int_eq(run.record_count(), rec_cnt);
}
END_TEST


START_TEST(t_bounds_duplicates)
{
    testing::initialize_global_fm();
    auto state = testing::make_state1();

    PageNum pages_per_file = 1000;

    std::vector<std::unique_ptr<iter::GenericIterator<Record>>> iters(2);
    size_t cnt1 = 0;
    auto fname1 = testing::generate_isamtree_test_data_all_dupes(pages_per_file, state->record_schema->val_len(), 5, &cnt1);
    auto *pfile1 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname1);
    iters[0] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile1, g_cache);

    size_t cnt2 = 0;
    auto fname2 = testing::generate_isamtree_test_data_all_dupes(pages_per_file, state->record_schema->val_len(), 5, &cnt2);
    auto *pfile2 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname2);
    iters[1] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile2, g_cache);

    const catalog::RecordCmpFunc cmp = std::bind(&compare_func, _1, _2);
    auto iterator = std::make_unique<iter::MergeIterator>(iters, cmp);

    const catalog::RecordCmpFunc key_cmp = std::bind(&compare_func_key, _1, _2);

    auto buffer = mem::create_aligned_buffer(state->record_schema->record_length() * (cnt1 + cnt2));

    ds::SortedRun::initialize(buffer.get(), std::move(iterator), cnt1 + cnt2, state.get(), false, 0);
    auto run = ds::SortedRun(std::move(buffer), cnt1 + cnt2, state.get(), 0);

    int64_t key = 5;
    auto lb = run.get_lower_bound((byte *) &key);
    auto ub = run.get_upper_bound((byte *) &key);

    auto real_lb = 0;
    auto real_ub = cnt1 + cnt2 - 1; 

    ck_assert_int_eq(lb, real_lb);
    ck_assert_int_eq(ub, real_ub);
}
END_TEST


START_TEST(t_bounds_lower_out_of_range)
{
    testing::initialize_global_fm();
    auto state = testing::make_state1();

    PageNum pages_per_file = 1000;

    std::vector<std::unique_ptr<iter::GenericIterator<Record>>> iters(2);
    size_t cnt1 = 0;
    auto fname1 = testing::generate_isamtree_test_data_all_dupes(pages_per_file, state->record_schema->val_len(), 5, &cnt1);
    auto *pfile1 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname1);
    iters[0] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile1, g_cache);

    size_t cnt2 = 0;
    auto fname2 = testing::generate_isamtree_test_data_all_dupes(pages_per_file, state->record_schema->val_len(), 5, &cnt2);
    auto *pfile2 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname2);
    iters[1] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile2, g_cache);

    const catalog::RecordCmpFunc cmp = std::bind(&compare_func, _1, _2);
    auto iterator = std::make_unique<iter::MergeIterator>(iters, cmp);

    auto pfile = state->file_manager->create_indexed_pfile();
    const catalog::KeyCmpFunc key_cmp = std::bind(&compare_func_key, _1, _2);
    auto buffer = mem::create_aligned_buffer(state->record_schema->record_length() * (cnt1 + cnt2));

    ds::SortedRun::initialize(buffer.get(), std::move(iterator), cnt1 + cnt2, state.get(), false, 0);
    auto run = ds::SortedRun(std::move(buffer), cnt1 + cnt2, state.get(), 0);

    int64_t key = 7;
    auto lb = run.get_lower_bound((byte *) &key);
    auto ub = run.get_upper_bound((byte *) &key);

    auto real_lb = -1;
    auto real_ub = cnt1 + cnt2 - 1;

    ck_assert_int_eq(lb, real_lb);
    ck_assert_int_eq(ub, real_ub);

}
END_TEST


START_TEST(t_bounds_upper_out_of_range)
{
    testing::initialize_global_fm();
    auto state = testing::make_state1();

    PageNum pages_per_file = 1000;

    std::vector<std::unique_ptr<iter::GenericIterator<Record>>> iters(2);
    size_t cnt1 = 0;
    auto fname1 = testing::generate_isamtree_test_data_all_dupes(pages_per_file, state->record_schema->val_len(), 5, &cnt1);
    auto *pfile1 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname1);
    iters[0] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile1, g_cache);

    size_t cnt2 = 0;
    auto fname2 = testing::generate_isamtree_test_data_all_dupes(pages_per_file, state->record_schema->val_len(), 5, &cnt2);
    auto *pfile2 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname2);
    iters[1] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile2, g_cache);

    const catalog::RecordCmpFunc cmp = std::bind(&compare_func, _1, _2);
    auto iterator = std::make_unique<iter::MergeIterator>(iters, cmp);

    const catalog::KeyCmpFunc key_cmp = std::bind(&compare_func_key, _1, _2);
    auto buffer = mem::create_aligned_buffer(state->record_schema->record_length() * (cnt1 + cnt2));

    ds::SortedRun::initialize(buffer.get(), std::move(iterator), cnt1 + cnt2, state.get(), false, 0);
    auto run = ds::SortedRun(std::move(buffer), cnt1 + cnt2, state.get(), 0);

    int64_t key = 2;
    auto lb = run.get_lower_bound((byte *) &key);
    auto ub = run.get_upper_bound((byte *) &key);

    auto real_lb = 0;
    auto real_ub = -1;

    ck_assert_int_eq(lb, real_lb);
    ck_assert_int_eq(ub, real_ub);

}
END_TEST


START_TEST(t_bounds_general)
{
    testing::initialize_global_fm();
    auto state = testing::make_state1();

    PageNum pages_per_file = 1000;

    std::vector<std::unique_ptr<iter::GenericIterator<Record>>> iters(4);
    size_t cnt1 = 0;
    auto fname1 = testing::generate_isamtree_test_data_all_dupes(pages_per_file, state->record_schema->val_len(), 5, &cnt1);
    auto *pfile1 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname1);
    iters[0] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile1, g_cache);

    size_t cnt2 = 0;
    auto fname2 = testing::generate_isamtree_test_data_all_dupes(pages_per_file, state->record_schema->val_len(), 8, &cnt2);
    auto *pfile2 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname2);
    iters[1] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile2, g_cache);

    size_t cnt3 = 0;
    auto fname3 = testing::generate_isamtree_test_data_all_dupes(pages_per_file, state->record_schema->val_len(), 1, &cnt3);
    auto *pfile3 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname3);
    iters[2] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile3, g_cache);

    size_t cnt4 = 0;
    auto fname4 = testing::generate_isamtree_test_data_all_dupes(pages_per_file, state->record_schema->val_len(), 15, &cnt4);
    auto *pfile4 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname4);
    iters[3] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile4, g_cache);

    const catalog::RecordCmpFunc cmp = std::bind(&compare_func, _1, _2);
    auto iterator = std::make_unique<iter::MergeIterator>(iters, cmp);

    const catalog::KeyCmpFunc key_cmp = std::bind(&compare_func_key, _1, _2);
    auto buffer = mem::create_aligned_buffer(state->record_schema->record_length() * (cnt1 + cnt2 + cnt3 + cnt4));

    ds::SortedRun::initialize(buffer.get(), std::move(iterator), cnt1 + cnt2 + cnt3 + cnt4, state.get(), false, 0);
    auto run = ds::SortedRun(std::move(buffer), cnt1 + cnt2 + cnt3 + cnt4, state.get(), 0);

    int64_t l_key = 4;
    auto lb = run.get_lower_bound((byte *) &l_key);

    int64_t u_key = 9;
    auto ub = run.get_upper_bound((byte *) &u_key);

    auto real_lb = cnt3;
    auto real_ub = cnt1 + cnt2 + cnt3;

    ck_assert_int_eq(lb, real_lb);
    ck_assert_int_eq(ub, real_ub);
}
END_TEST


START_TEST(t_iterator)
{
    testing::initialize_global_fm();
    auto state = testing::make_state1();

    PageNum pages_per_file = 5;

    std::vector<std::unique_ptr<iter::GenericIterator<Record>>> iters(4);
    size_t cnt1 = 0;
    auto fname1 = testing::generate_isamtree_test_data_all_dupes(pages_per_file, state->record_schema->val_len(), 5, &cnt1);
    auto *pfile1 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname1);
    iters[0] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile1, g_cache);

    size_t cnt2 = 0;
    auto fname2 = testing::generate_isamtree_test_data_all_dupes(pages_per_file, state->record_schema->val_len(), 8, &cnt2);
    auto *pfile2 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname2);
    iters[1] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile2, g_cache);

    size_t cnt3 = 0;
    auto fname3 = testing::generate_isamtree_test_data1(pages_per_file, state->record_schema->val_len(), &cnt3);
    auto *pfile3 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname3);
    iters[2] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile3, g_cache);

    size_t cnt4 = 0;
    auto fname4 = testing::generate_isamtree_test_data2(pages_per_file, state->record_schema->val_len(), &cnt4);
    auto *pfile4 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname4);
    iters[3] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile4, g_cache);

    const catalog::RecordCmpFunc cmp = std::bind(&compare_func, _1, _2);
    auto iterator = std::make_unique<iter::MergeIterator>(iters, cmp);

    const catalog::KeyCmpFunc key_cmp = std::bind(&compare_func_key, _1, _2);

    auto buffer = mem::create_aligned_buffer(state->record_schema->record_length() * (cnt1 + cnt2 + cnt3 + cnt4));

    ds::SortedRun::initialize(buffer.get(), std::move(iterator), cnt1 + cnt2 + cnt3 + cnt4, state.get(), false, 0);
    auto run = ds::SortedRun(std::move(buffer), cnt1 + cnt2 + cnt3 + cnt4, state.get(), 0);

    auto tree_iterator = run.start_scan();

    int64_t prev_key = INT64_MIN;
    size_t reccnt = 0;
    while (tree_iterator->next()) {
        auto rec = tree_iterator->get_item();
        auto key_val = g_schema->get_key(rec.get_data()).Int64();

        ck_assert_int_ge(key_val, prev_key);
        prev_key = key_val;
        reccnt++;
    }

    ck_assert_int_eq(reccnt, cnt1 + cnt2 + cnt3 + cnt4);
    ck_assert_int_eq(reccnt, run.record_count());
}
END_TEST


START_TEST(t_get)
{
    size_t cnt = 0;
    auto state = testing::make_state1();
    auto isamtree1 = testing::test_isamtree1(100, state.get(), &cnt);
    auto tree_iterator1 = isamtree1->start_scan();

    while (tree_iterator1->next()) {
        auto rec = tree_iterator1->get_item();
        auto key_val = state->record_schema->get_key(rec.get_data()).Bytes();

        FrameId frid;
        auto res = isamtree1->get(key_val, &frid);
        ck_assert_int_eq(res.is_valid(), 1);
        ck_assert_int_eq(*(int64_t *) key_val, state->record_schema->get_key(res.get_data()).Int64());
    }
}
END_TEST


START_TEST(t_general_large_keys)
{
    PageNum page_count = 2000;
    size_t cnt = 0;

    auto state = testing::make_state(256, 512); 
    auto data_file = testing::generate_contiguous_test_data(page_count, state.get(), &cnt);
    std::vector<std::unique_ptr<iter::GenericIterator<Record>>> iters(1);
    auto pfile = (lsm::io::IndexPagedFile *) state->file_manager->get_pfile(data_file);
    iters[0] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile, state->cache.get());

    auto merged = std::make_unique<iter::MergeIterator>(iters, state->record_schema->get_record_cmp());
    auto tree_file = state->file_manager->create_indexed_pfile();
    ds::ISAMTree::initialize(tree_file, std::move(merged), page_count, state.get(), false, 0);
    auto isamtree = ds::ISAMTree(tree_file, state.get());

    int64_t key = 0;
    for (auto iter=isamtree.start_scan(); iter->next();) {
        auto rec = iter->get_item();
        auto rec_key_bytes = state->record_schema->get_key(rec.get_data()).Bytes();
        auto rec_key = state->record_schema->get_key(rec.get_data()).Int64();
        ck_assert_int_eq(key, rec_key);

        FrameId frid;
        auto get_rec = isamtree.get(rec_key_bytes, &frid);

        ck_assert_int_ne(frid, INVALID_FRID);
        auto get_rec_key = state->record_schema->get_key(get_rec.get_data()).Int64();
        ck_assert(get_rec.is_valid());
        ck_assert_int_eq(key, get_rec_key);

        state->cache->unpin(frid);
        key++;
    }
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("Sorted Run Unit Testing");

    TCase *init = tcase_create("lsm::ds::SortedRun::initialize");
    tcase_add_test(init, t_initialize);

    suite_add_tcase(unit, init);


    TCase *bounds = tcase_create("lsm::ds::ISAMTree::get_{upper,lower}_bound");
    tcase_add_test(bounds, t_bounds_duplicates);
    tcase_add_test(bounds, t_bounds_upper_out_of_range);
    tcase_add_test(bounds, t_bounds_lower_out_of_range);
    tcase_add_test(bounds, t_bounds_general);

    tcase_set_timeout(bounds, 10);

    suite_add_tcase(unit, bounds);


    TCase *iter = tcase_create("lsm::ds::SortedRun::start_scan");
    tcase_add_test(iter, t_iterator);

    suite_add_tcase(unit, iter);


    TCase *get = tcase_create("lsm::ds::SortedRun::get");
    tcase_add_test(get, t_get);

    suite_add_tcase(unit, get);

    /*
    TCase *misc = tcase_create("lsm::ds::SortedRun::miscellaneous");
    tcase_add_test(misc, t_general_large_keys);

    suite_add_tcase(unit, misc);
    */
    

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
