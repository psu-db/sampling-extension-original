/*
 *
 */

#include <check.h>
#include "testing.hpp"

#include "ds/staticbtree.hpp"

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

std::unique_ptr<iter::MergeIterator> test_merge_iterator(PageOffset value_size)
{
    auto schema = testing::test_schema1(value_size);

    std::vector<std::unique_ptr<iter::GenericIterator<Record>>> iters(3);
    size_t cnt1 = 0;
    auto fname1 = testing::generate_btree_test_data1(100, value_size, &cnt1);
    auto *pfile1 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname1);
    iters[0] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile1, INVALID_PNUM, g_cache);

    size_t cnt2 = 0;
    auto fname2 = testing::generate_btree_test_data2(100, value_size, &cnt2);
    auto pfile2 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname2);
    iters[1] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile2, INVALID_PNUM, g_cache);


    size_t cnt3 = 0;
    auto fname3 = testing::generate_btree_test_data3(200, value_size, &cnt3);
    auto pfile3 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname3);
    iters[2] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile3, INVALID_PNUM, g_cache);

    const iter::CompareFunc cmp = std::bind(&compare_func, _1, _2);

    return std::make_unique<iter::MergeIterator>(iters, cmp);
}


START_TEST(t_initialize)
{
    testing::initialize_global_fm();
    PageOffset value_size = 8;
    auto schema = testing::test_schema1(value_size);
    auto iterator = test_merge_iterator(value_size);
    g_schema = testing::test_schema1(value_size);

    auto pfile = testing::g_fm->create_indexed_pfile();
    const iter::CompareFunc cmp = std::bind(&compare_func_key, _1, _2);

    ds::StaticBTree::initialize(pfile, std::move(iterator), 400, schema.get(), cmp, nullptr);

    auto btree = ds::StaticBTree(pfile, schema.get(), cmp, g_cache);

    int64_t testkey = 40;

    btree.get_lower_bound((byte *) &testkey);
    btree.get_upper_bound((byte *) &testkey);

}
END_TEST


START_TEST(t_bounds_duplicates)
{
    testing::initialize_global_fm();
    auto my_cache = io::ReadCache(1024);
    PageOffset value_size = 8;
    g_schema = testing::test_schema1(value_size);

    std::vector<std::unique_ptr<iter::GenericIterator<Record>>> iters(2);
    size_t cnt1 = 0;
    auto fname1 = testing::generate_btree_test_data_all_dupes(100, value_size, &cnt1);
    auto *pfile1 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname1);
    iters[0] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile1, INVALID_PNUM, g_cache);

    size_t cnt2 = 0;
    auto fname2 = testing::generate_btree_test_data_all_dupes(100, value_size, &cnt2);
    auto *pfile2 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname2);
    iters[1] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile2, INVALID_PNUM, g_cache);

    const iter::CompareFunc cmp = std::bind(&compare_func, _1, _2);
    auto iterator = std::make_unique<iter::MergeIterator>(iters, cmp);

    auto pfile = testing::g_fm->create_indexed_pfile();
    const iter::CompareFunc key_cmp = std::bind(&compare_func_key, _1, _2);
    ds::StaticBTree::initialize(pfile, std::move(iterator), 200, g_schema.get(), key_cmp, nullptr);
    auto btree = ds::StaticBTree(pfile, g_schema.get(), key_cmp, &my_cache);

    auto buf = mem::page_alloc_unique();
    pfile->read_page(202, buf.get());

    int64_t key = 5;
    auto lb = btree.get_lower_bound((byte *) &key);
    auto up = btree.get_upper_bound((byte *) &key);

}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("Static BTree Unit Testing");

    TCase *iter = tcase_create("lsm::ds::StaticBTree::initialize");
    tcase_add_test(iter, t_initialize);

    suite_add_tcase(unit, iter);


    TCase *bounds = tcase_create("lsm::ds::StaticBTree::get_{upper,lower}_bound");
    tcase_add_test(bounds, t_bounds_duplicates);

    suite_add_tcase(unit, bounds);

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
