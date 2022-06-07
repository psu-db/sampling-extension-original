/*
 *
 */

#include <check.h>
#include "io/indexpagedfile.hpp"
#include "util/mergeiter.hpp"
#include "testing.hpp"

using namespace lsm;
using std::byte;
using namespace std::placeholders;

auto g_schema = testing::test_schema1(sizeof(int64_t));


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


START_TEST(t_iterator)
{
    testing::initialize_global_fm();

    auto rcache = std::make_unique<io::ReadCache>();
    auto schema = testing::test_schema1(sizeof(int64_t));

    std::vector<std::unique_ptr<iter::GenericIterator<Record>>> iters(3);
    size_t cnt1 = 0;
    auto fname1 = testing::generate_merge_test_file1(25, &cnt1);
    auto *pfile1 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname1);
    iters[0] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile1, rcache.get());

    size_t cnt2 = 0;
    auto fname2 = testing::generate_merge_test_file2(25, &cnt2);
    auto pfile2 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname2);
    iters[1] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile2, rcache.get());


    size_t cnt3 = 0;
    auto fname3 = testing::generate_merge_test_file3(50, &cnt3);
    auto pfile3 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname3);
    iters[2] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile3, rcache.get());


    const iter::CompareFunc cmp = std::bind(&compare_func, _1, _2);


    size_t rec_cnt = 0;
    auto merge_itr = iter::MergeIterator(iters, cmp);

    int64_t last_key = -1;
    while (merge_itr.next()) {
        auto rec = merge_itr.get_item();
        ck_assert_int_ge(schema->get_key(rec.get_data()).Int64(), last_key);
        last_key = schema->get_key(rec.get_data()).Int64();
        rec_cnt++;
    }

    ck_assert_int_eq(rec_cnt, cnt1 + cnt2 + cnt3);
    merge_itr.end_scan();
}
END_TEST


START_TEST(t_iterator_btree_data_512)
{
    testing::initialize_global_fm();

    PageOffset value_size = 512;

    auto rcache = std::make_unique<io::ReadCache>();
    auto schema = testing::test_schema1(512);

    std::vector<std::unique_ptr<iter::GenericIterator<Record>>> iters(3);
    size_t cnt1 = 0;
    auto fname1 = testing::generate_btree_test_data1(25, value_size, &cnt1);
    auto *pfile1 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname1);
    iters[0] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile1, rcache.get());

    size_t cnt2 = 0;
    auto fname2 = testing::generate_btree_test_data2(25, value_size, &cnt2);
    auto pfile2 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname2);
    iters[1] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile2, rcache.get());


    size_t cnt3 = 0;
    auto fname3 = testing::generate_btree_test_data3(50, value_size, &cnt3);
    auto pfile3 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname3);
    iters[2] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile3, rcache.get());

    const iter::CompareFunc cmp = std::bind(&compare_func, _1, _2);

    size_t rec_cnt = 0;
    auto merge_itr = iter::MergeIterator(iters, cmp);

    int64_t last_key = INT64_MIN;
    while (merge_itr.next()) {
        auto rec = merge_itr.get_item();
        ck_assert_int_ge(schema->get_key(rec.get_data()).Int64(), last_key);
        last_key = schema->get_key(rec.get_data()).Int64();
        rec_cnt++;
    }

    ck_assert_int_eq(rec_cnt, cnt1 + cnt2 + cnt3);
    merge_itr.end_scan();
}
END_TEST


START_TEST(t_iterator_btree_data_1024)
{
    testing::initialize_global_fm();

    PageOffset value_size = 1024;

    auto rcache = std::make_unique<io::ReadCache>();
    auto schema = testing::test_schema1(512);

    std::vector<std::unique_ptr<iter::GenericIterator<Record>>> iters(3);
    size_t cnt1 = 0;
    auto fname1 = testing::generate_btree_test_data1(25, value_size, &cnt1);
    auto *pfile1 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname1);
    iters[0] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile1, rcache.get());

    size_t cnt2 = 0;
    auto fname2 = testing::generate_btree_test_data2(25, value_size, &cnt2);
    auto pfile2 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname2);
    iters[1] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile2, rcache.get());


    size_t cnt3 = 0;
    auto fname3 = testing::generate_btree_test_data3(50, value_size, &cnt3);
    auto pfile3 = (lsm::io::IndexPagedFile *) testing::g_fm->get_pfile(fname3);
    iters[2] = std::make_unique<io::IndexPagedFileRecordIterator>(pfile3, rcache.get());

    const iter::CompareFunc cmp = std::bind(&compare_func, _1, _2);

    size_t rec_cnt = 0;
    auto merge_itr = iter::MergeIterator(iters, cmp);

    int64_t last_key = INT64_MIN;
    while (merge_itr.next()) {
        auto rec = merge_itr.get_item();
        ck_assert_int_ge(schema->get_key(rec.get_data()).Int64(), last_key);
        last_key = schema->get_key(rec.get_data()).Int64();
        rec_cnt++;
    }

    ck_assert_int_eq(rec_cnt, cnt1 + cnt2 + cnt3);
    merge_itr.end_scan();
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("Merge Iterator Unit Testing");

    TCase *iter = tcase_create("lsm::util::MergeIterator Testing");
    tcase_add_test(iter, t_iterator);
    tcase_add_test(iter, t_iterator_btree_data_512);
    tcase_add_test(iter, t_iterator_btree_data_1024);

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
