/*
 *
 */

#include <check.h>
#include "sampling/isamlevel.hpp"
#include "sampling/samplerange.hpp"
#include "testing.hpp"

using namespace lsm;
using std::byte;

START_TEST(t_create)
{
    auto state = testing::make_state1();

    size_t cnt = 0;
    auto isamtree1 = testing::test_isamtree1(100, state.get(), &cnt);

    auto filename = state->file_manager->get_name(isamtree1->get_pfile()->get_flid());

    isamtree1.reset();

    std::vector<io::IndexPagedFile *> files(1);
    files[0] = (io::IndexPagedFile *) state->file_manager->get_pfile(filename);
    auto test_level = sampling::ISAMTreeLevel(1, cnt, files, state.get(), 1.0, false);

    ck_assert_ptr_nonnull(test_level.get_run(0));
    ck_assert_int_eq(test_level.can_emplace_run(), 0);
    ck_assert_int_eq(test_level.can_merge_with(1), 0);

    ck_assert_int_eq(test_level.get_record_capacity(), cnt);
    ck_assert_int_eq(test_level.get_record_count(), cnt);
}
END_TEST


START_TEST(t_merge)
{
    auto state = testing::make_state1();

    size_t cnt1 = 0;
    auto isamtree1 = testing::test_isamtree1(100, state.get(), &cnt1);
    auto filename = state->file_manager->get_name(isamtree1->get_pfile()->get_flid());
    std::vector<io::IndexPagedFile *> files(1);
    files[0] = (io::IndexPagedFile *) state->file_manager->get_pfile(filename);
    auto test_level1 = sampling::ISAMTreeLevel(1, cnt1, files, state.get(), 1.0, false);

    size_t cnt2 = 0;
    auto isamtree2 = testing::test_isamtree2(100, state.get(), &cnt2);
    auto filename2 = state->file_manager->get_name(isamtree2->get_pfile()->get_flid());
    std::vector<io::IndexPagedFile *> files2(1);
    files2[0] = (io::IndexPagedFile *) state->file_manager->get_pfile(filename);
    auto test_level2 = sampling::ISAMTreeLevel(1, cnt2 + cnt1, files2, state.get(), 1.0, false);

    ck_assert_int_eq(test_level2.can_merge_with(&test_level1), 1);

    auto temp_run = test_level1.merge_runs();

    ck_assert_int_eq(test_level2.merge_with(std::move(temp_run)), 1);
    ck_assert_int_eq(test_level2.get_record_count(), cnt1 + cnt2);

    auto iter = test_level2.start_scan();

    int64_t prev_key = INT64_MIN;
    size_t reccnt = 0;
    while (iter->next()) {
        reccnt++;
        auto rec = iter->get_item();
        auto key_val = state->record_schema->get_key(rec.get_data()).Int64();

        ck_assert_int_ge(key_val, prev_key);
        prev_key = key_val;
    }

    ck_assert_int_eq(reccnt, cnt1 + cnt2);
}
END_TEST


START_TEST(t_sample_range)
{
    auto state = testing::make_state1();
    size_t cnt;
    auto isamtree = testing::test_isamtree_cont(1000, state.get(), &cnt);
    auto filename = state->file_manager->get_name(isamtree->get_pfile()->get_flid());

    isamtree.reset();

    std::vector<io::IndexPagedFile *> files(1);
    files[0] = (io::IndexPagedFile *) state->file_manager->get_pfile(filename);
    auto test_level = sampling::ISAMTreeLevel(1, cnt, files, state.get(), 1.0, false);

    int64_t key1 = 100;
    int64_t key2 = -7;
    int64_t key3 = cnt + 100;
    int64_t key4 = cnt / 2;
    int64_t key5 = -10;

    // lower range below upper one, shouldn't work
    auto range1 = test_level.get_sample_ranges((byte*) &key1, (byte*) &key2);
    ck_assert_int_eq(range1.size(), 0);

    // this one should work.
    auto range2 = test_level.get_sample_ranges((byte*) &key1, (byte*) &key3);
    ck_assert_int_eq(range2.size(), 1);
    ck_assert_ptr_nonnull(range2[0].get());
    ck_assert_int_gt(range2[0]->length(), 0);


    // should also work
    auto range3 = test_level.get_sample_ranges((byte*) &key1, (byte*) &key4);
    ck_assert_int_eq(range3.size(), 1);
    ck_assert_ptr_nonnull(range3[0].get());
    ck_assert_int_gt(range3[0]->length(), 0);

    // both lower and upper limit are out of range of test data
    auto range4 = test_level.get_sample_ranges((byte*) &key5, (byte*) &key2);
    ck_assert_int_eq(range4.size(), 0);

    fprintf(stderr, "[%ld, %ld]", key1, key4);
    size_t valid_recs;
    for (size_t i=0; i<10000; i++) {
        FrameId frid;
        auto rec = range3[0]->get(&frid);
        if (rec.is_valid()) {
            int64_t key = state->record_schema->get_key(rec.get_data()).Int64();
            ck_assert_int_le(key, key4);
            ck_assert_int_ge(key, key1);
            valid_recs++;
        }

        if (frid != INVALID_FRID) {
            state->cache->unpin(frid);
        }
    }

    ck_assert_int_gt(valid_recs, 0);
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("Level Unit Testing");
    TCase *create = tcase_create("lsm::sampling::Level Constructor Testing");
    tcase_add_test(create, t_create);

    suite_add_tcase(unit, create);


    TCase *merge = tcase_create("lsm::sampling::merge Testing");
    tcase_add_test(merge, t_merge);

    suite_add_tcase(unit, merge);


    TCase *range = tcase_create("lsm::sampling::get_sample_ranges Testing");
    tcase_add_test(range, t_sample_range);

    suite_add_tcase(unit, range);

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
