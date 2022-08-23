/*
 *
 */

#include <check.h>
#include <set>
#include "sampling/lsmtree.hpp"
#include "testing.hpp"

using namespace lsm;
using std::byte;

START_TEST(t_create)
{
    auto state = testing::make_state1();
    auto lsm = sampling::LSMTree::create(1000, 2, std::move(state));
}
END_TEST


START_TEST(t_insert)
{
    auto state = testing::make_state1();
    auto lsm = sampling::LSMTree::create(1000, 2, std::move(state));

    int64_t key = 150;
    int64_t value = 10;

    for (size_t i=0; i<1000; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    ck_assert_int_eq(lsm->depth(), 0);
    ck_assert_int_eq(lsm->record_count(), 1000);

    for (size_t i=0; i<1000; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
    }

    ck_assert_int_eq(lsm->depth(), 1);
    ck_assert_int_eq(lsm->record_count(), 2000);
}
END_TEST


START_TEST(t_insert_memlevels)
{
    auto state = testing::make_state1();

    auto lsm = sampling::LSMTree::create(1000, 2, std::move(state), F_NONE, lsm::sampling::LEVELING, 2);

    int64_t key = 150;
    int64_t value = 10;

    for (size_t i=0; i<1000; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    ck_assert_int_eq(lsm->depth(), 0);
    ck_assert_int_eq(lsm->record_count(), 1000);

    for (size_t i=0; i<10000; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
    }

    ck_assert_int_eq(lsm->record_count(), 11000);
    ck_assert_int_eq(lsm->depth(), 3);
}
END_TEST


START_TEST(t_range_sample)
{
    auto state = testing::make_state1();
    auto lsm = sampling::LSMTree::create(100, 2, std::move(state));

    int64_t key = 550;
    int64_t value = 10;

    for (size_t i=0; i < 10000; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    key = 250;
    for (size_t i=0; i < 300; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    int64_t start = 300;
    int64_t stop = 2000;

    size_t rejs = 0;
    size_t atmpts = 0;

    size_t sample_size = 1000;
    auto result = lsm->range_sample((byte *) &start, (byte *) &stop, sample_size, &rejs, &atmpts);

    ck_assert_ptr_nonnull(result.get());
    ck_assert_int_eq(result->sample_size(), sample_size);
    auto schema = lsm->schema();

    for (size_t i=0; i<result->sample_size(); i++) {
        auto rec = result->get(i);
        auto sample_key = schema->get_key(rec.get_data()).Int64();

        ck_assert_int_ge(sample_key, start);
        ck_assert_int_le(sample_key, stop);
    }
}
END_TEST


START_TEST(t_range_sample_memlevels)
{
    auto state = testing::make_state1();
    auto lsm = sampling::LSMTree::create(100, 2, std::move(state), F_NONE, lsm::sampling::LEVELING, 4);

    int64_t key = 550;
    int64_t value = 10;

    for (size_t i=0; i < 10000; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    key = 250;
    for (size_t i=0; i < 300; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    int64_t start = 300;
    int64_t stop = 2000;

    size_t rejs = 0;
    size_t atmpts = 0;

    size_t sample_size = 1000;
    auto result = lsm->range_sample((byte *) &start, (byte *) &stop, sample_size, &rejs, &atmpts);

    ck_assert_ptr_nonnull(result.get());
    ck_assert_int_eq(result->sample_size(), sample_size);
    auto schema = lsm->schema();

    for (size_t i=0; i<result->sample_size(); i++) {
        auto rec = result->get(i);
        auto sample_key = schema->get_key(rec.get_data()).Int64();

        ck_assert_int_ge(sample_key, start);
        ck_assert_int_le(sample_key, stop);
    }


    start = 550 + 8000;
    stop = 550 + 10000;

    rejs = 0;
    atmpts = 0;

    result = lsm->range_sample((byte *) &start, (byte *) &stop, sample_size, &rejs, &atmpts);

    ck_assert_ptr_nonnull(result.get());
    ck_assert_int_eq(result->sample_size(), sample_size);

    for (size_t i=0; i<result->sample_size(); i++) {
        auto rec = result->get(i);
        auto sample_key = schema->get_key(rec.get_data()).Int64();

        ck_assert_int_ge(sample_key, start);
        ck_assert_int_le(sample_key, stop);
    }
}
END_TEST


START_TEST(t_unsorted_sample_with_rejection)
{
    auto state = testing::make_state1();
    auto lsm = sampling::LSMTree::create(100, 2, std::move(state), lsm::sampling::F_LSM_REJSAMP);

    int64_t key = 550;
    int64_t value = 10;

    for (size_t i=0; i < 10000; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    key = 250;
    for (size_t i=0; i < 300; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    int64_t start = 300;
    int64_t stop = 2000;

    size_t rejs = 0;
    size_t atmpts = 0;

    size_t sample_size = 1000;
    auto result = lsm->range_sample((byte *) &start, (byte *) &stop, sample_size, &rejs, &atmpts);

    ck_assert_ptr_nonnull(result.get());
    ck_assert_int_eq(result->sample_size(), sample_size);
    auto schema = lsm->schema();

    for (size_t i=0; i<result->sample_size(); i++) {
        auto rec = result->get(i);
        auto sample_key = schema->get_key(rec.get_data()).Int64();

        ck_assert_int_ge(sample_key, start);
        ck_assert_int_le(sample_key, stop);
    }
}
END_TEST


START_TEST(t_erase)
{
    auto state = testing::make_state1();
    auto lsm = sampling::LSMTree::create(100, 2, std::move(state));

    int64_t key = 550;
    int64_t value = 10;
    Timestamp time = 0;

    for (size_t i=0; i < 10000; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value, time++), 1);
        key++;
        value++;
    }

    int64_t rm_key = 551;
    int64_t rm_val = 11;

    ck_assert_int_eq(lsm->remove((byte*) &rm_key, (byte*) &rm_val, time++), 1);

    FrameId frid;
    auto rec = lsm->get((byte*) &rm_key, &frid);
    ck_assert_int_eq(rec.is_valid(), 0);

    rm_key = 550 + 1200;
    rm_val = 10 + 1200;
    ck_assert_int_eq(lsm->remove((byte*) &rm_key, (byte*) &rm_val, time++), 1);

    for (size_t i=0; i < 105; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value, time++), 1);
        key++;
        value++;
    }
    ck_assert_int_eq(lsm->has_tombstone((byte*) &rm_key, (byte*) &rm_val, time), 1);

    rec = lsm->get((byte*) &rm_key, &frid);
    ck_assert_int_eq(rec.is_valid(), 0);

    ck_assert_int_eq(lsm->insert((byte*) &rm_key, (byte*) &rm_val, time++), 1);
    rec = lsm->get((byte*) &rm_key, &frid, time);
    ck_assert_int_eq(rec.is_valid(), 1);
    lsm->cache()->unpin(frid);
}
END_TEST


START_TEST(t_erase_memlevels)
{
    auto state = testing::make_state1();
    auto lsm = sampling::LSMTree::create(100, 2, std::move(state), F_NONE, lsm::sampling::LEVELING, 3);

    int64_t key = 550;
    int64_t value = 10;
    Timestamp time = 0;

    for (size_t i=0; i < 10000; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value, time++), 1);
        key++;
        value++;
    }

    int64_t rm_key = 551;
    int64_t rm_val = 11;

    ck_assert_int_eq(lsm->remove((byte*) &rm_key, (byte*) &rm_val, time++), 1);

    FrameId frid;
    auto rec = lsm->get((byte*) &rm_key, &frid);
    ck_assert_int_eq(rec.is_valid(), 0);

    rm_key = 550 + 10000 - 300;
    rm_val = 10 + 10000 - 300;
    ck_assert_int_eq(lsm->remove((byte*) &rm_key, (byte*) &rm_val, time++), 1);

    for (size_t i=0; i < 105; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value, time++), 1);
        key++;
        value++;
    }
    ck_assert_int_eq(lsm->has_tombstone((byte*) &rm_key, (byte*) &rm_val, time), 1);
    rec = lsm->get((byte*) &rm_key, &frid);
    ck_assert_int_eq(rec.is_valid(), 0);

    ck_assert_int_eq(lsm->insert((byte*) &rm_key, (byte*) &rm_val, time++), 1);
    rec = lsm->get((byte*) &rm_key, &frid, time);
    ck_assert_int_eq(rec.is_valid(), 1);
    lsm->cache()->unpin(frid);
}
END_TEST


START_TEST(t_bulk_erase)
{
    auto state = testing::make_state1();
    auto lsm = sampling::LSMTree::create(100, 2, std::move(state));

    int64_t key = 550;
    int64_t value = 10;

    for (size_t i=0; i < 10000; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }


    key = 550;
    value = 10;
    for (size_t i=0; i < 10000; i++) {
        ck_assert_int_eq(lsm->remove((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    key = 550;
    value = 10;
    for (size_t i=0; i < 10000; i++) {
        FrameId frid;
        auto rec = lsm->get((byte*) &key, &frid);
        ck_assert_int_eq(rec.is_valid(), 0);
        key++;
        value++;
        lsm->cache()->unpin(frid);
    }

    key = 550;
    value = 10;
    for (size_t i=0; i < 10000; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    key = 550;
    value = 10;
    for (size_t i=0; i < 10000; i++) {
        FrameId frid;
        auto rec = lsm->get((byte*) &key, &frid);
        ck_assert_int_eq(rec.is_valid(), 1);
        key++;
        value++;
        lsm->cache()->unpin(frid);
    }

}
END_TEST


START_TEST(t_range_sample_with_erase)
{
    auto state = testing::make_state1();
    auto lsm = sampling::LSMTree::create(100, 2, std::move(state));

    int64_t key = 550;
    int64_t value = 10;

    for (size_t i=0; i < 10000; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    key = 550 + 800;
    value = 10 + 800;
    for (size_t i=0; i < 500; i++) {
        ck_assert_int_eq(lsm->remove((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    key = 250;
    for (size_t i=0; i < 500; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    int64_t start = 300;
    int64_t stop = 2000;

    size_t rejs = 0;
    size_t atmpts = 0;

    size_t sample_size = 1000;
    auto result = lsm->range_sample((byte *) &start, (byte *) &stop, sample_size, &rejs, &atmpts);

    ck_assert_ptr_nonnull(result.get());
    ck_assert_int_eq(result->sample_size(), sample_size);
    auto schema = lsm->schema();

    for (size_t i=0; i<result->sample_size(); i++) {
        auto rec = result->get(i);
        auto sample_key = schema->get_key(rec.get_data()).Int64();

        ck_assert_int_ge(sample_key, start);
        ck_assert_int_le(sample_key, stop);
        ck_assert(sample_key < 550 + 800 || sample_key > 550 + 800 + 500);
    }
}
END_TEST


START_TEST(t_unsorted_sample_with_erase)
{
    auto state = testing::make_state1();
    auto lsm = sampling::LSMTree::create(100, 2, std::move(state));

    int64_t key = 550;
    int64_t value = 10;

    for (size_t i=0; i < 10000; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    key = 550 + 800;
    value = 10 + 800;
    for (size_t i=0; i < 500; i++) {
        ck_assert_int_eq(lsm->remove((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    key = 250;
    for (size_t i=0; i < 500; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    int64_t start = 300;
    int64_t stop = 2000;

    size_t rejs = 0;
    size_t atmpts = 0;

    size_t sample_size = 1000;
    auto result = lsm->range_sample((byte *) &start, (byte *) &stop, sample_size, &rejs, &atmpts);

    ck_assert_ptr_nonnull(result.get());
    ck_assert_int_eq(result->sample_size(), sample_size);
    auto schema = lsm->schema();

    for (size_t i=0; i<result->sample_size(); i++) {
        auto rec = result->get(i);
        auto sample_key = schema->get_key(rec.get_data()).Int64();

        ck_assert_int_ge(sample_key, start);
        ck_assert_int_le(sample_key, stop);
        ck_assert(sample_key < 550 + 800 || sample_key > 550 + 800 + 500);
    }
}
END_TEST


START_TEST(t_unsorted_sample_with_erase_and_bloom)
{
    auto state = testing::make_state1();
    auto lsm = sampling::LSMTree::create(100, 2, std::move(state), lsm::sampling::F_LSM_BLOOM);

    int64_t key = 550;
    int64_t value = 10;

    for (size_t i=0; i < 10000; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    key = 550 + 800;
    value = 10 + 800;
    for (size_t i=0; i < 500; i++) {
        ck_assert_int_eq(lsm->remove((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    key = 250;
    for (size_t i=0; i < 500; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value), 1);
        key++;
        value++;
    }

    int64_t start = 300;
    int64_t stop = 2000;

    size_t rejs = 0;
    size_t atmpts = 0;

    size_t sample_size = 1000;
    auto result = lsm->range_sample((byte *) &start, (byte *) &stop, sample_size, &rejs, &atmpts);

    ck_assert_ptr_nonnull(result.get());
    ck_assert_int_eq(result->sample_size(), sample_size);
    auto schema = lsm->schema();

    for (size_t i=0; i<result->sample_size(); i++) {
        auto rec = result->get(i);
        auto sample_key = schema->get_key(rec.get_data()).Int64();

        ck_assert_int_ge(sample_key, start);
        ck_assert_int_le(sample_key, stop);
        ck_assert(sample_key < 550 + 800 || sample_key > 550 + 800 + 500);
    }
}
END_TEST


START_TEST(t_erase_duplicate_keys)
{
    auto state = testing::make_state1();
    auto lsm = sampling::LSMTree::create(100, 2, std::move(state));

    int64_t key = 550;
    int64_t value = 10;
    Timestamp time = 0;

    for (size_t i=0; i < 10000; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value, time++), 1);
        key++;
        value++;
    }

    key = 550;
    value = 100;
    for (size_t i=0; i < 1000; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value, time++), 1);
        key++;
        value++;
    }

    key = 550 + 800;
    value = 10 + 800;
    for (size_t i=0; i < 500; i++) {
        ck_assert_int_eq(lsm->remove((byte*) &key, (byte*) &value, time++), 1);
        key++;
        value++;
    }

    key = 250;
    for (size_t i=0; i < 500; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value, time++), 1);
        key++;
        value++;
    }

    key = 550 + 800;
    value = 10 + 800;
    for (size_t i=0; i < 500; i++) {
        FrameId frid = INVALID_FRID;
        ck_assert(lsm->has_tombstone((byte*) &key, (byte*) &value, time));
        if (frid != INVALID_FRID) {
            lsm->cache()->unpin(frid);
        }
        key++;
        value++;
    }

    key = 550 + 800;
    value = 100 + 800;
    for (size_t i=0; i< 500; i++) {
        FrameId frid = INVALID_FRID;
        ck_assert(!lsm->has_tombstone((byte*) &key, (byte*) &value, time));
        if (frid != INVALID_FRID) {
            lsm->cache()->unpin(frid);
        }
        key++;
        value++;
    }

    key = 550 + 800;
    value = 10 + 800;
    for (size_t i=0; i < 500; i++) {
        ck_assert_int_eq(lsm->insert((byte*) &key, (byte*) &value, time++), 1);
        key++;
        value++;
    }

    key = 550 + 800;
    value = 10 + 800;
    for (size_t i=0; i < 500; i++) {
        FrameId frid = INVALID_FRID;
        ck_assert(lsm->get((byte*) &key, &frid, time).is_valid());
        if (frid != INVALID_FRID) {
            lsm->cache()->unpin(frid);
        }
        key++;
        value++;
    }
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("LSM Tree Unit Testing");

    TCase *create = tcase_create("lsm::sampling::LSMTree Constructor Testing");
    tcase_add_test(create, t_create);

    suite_add_tcase(unit, create);


    TCase *insert = tcase_create("lsm::sampling::LSMTree::insert Testing");
    tcase_add_test(insert, t_insert);
    tcase_add_test(insert, t_insert_memlevels);

    suite_add_tcase(unit, insert);


    TCase *sampling = tcase_create("lsm::sampling::LSMTree::range_sample Testing");
    tcase_add_test(sampling, t_range_sample);
    tcase_add_test(sampling, t_range_sample_memlevels);
    tcase_add_test(sampling, t_range_sample_with_erase);
    tcase_add_test(sampling, t_unsorted_sample_with_erase);
    tcase_add_test(sampling, t_unsorted_sample_with_erase_and_bloom);
    tcase_add_test(sampling, t_unsorted_sample_with_rejection);

    suite_add_tcase(unit, sampling);


    TCase *remove = tcase_create("lsm::sampling::LSMTree::remove Testing");
    tcase_add_test(remove, t_erase);
    tcase_add_test(remove, t_erase_memlevels);
    tcase_add_test(remove, t_bulk_erase);
    tcase_add_test(remove, t_erase_duplicate_keys);

    tcase_set_timeout(remove, 100);



    suite_add_tcase(unit, remove);

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
