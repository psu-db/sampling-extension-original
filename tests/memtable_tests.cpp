#include <check.h>
#include <string>
#include <thread>
#include <gsl/gsl_rng.h>
#include <vector>
#include <algorithm>

#include "testing.h"
#include "lsm/MemTable.h"

using namespace lsm;

START_TEST(t_create)
{
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto mtable = new MemTable(100, true, 50, rng);

    ck_assert_ptr_nonnull(mtable);
    ck_assert_int_eq(mtable->get_capacity(), 100);
    ck_assert_int_eq(mtable->get_record_count(), 0);
    ck_assert_int_eq(mtable->is_full(), false);
    ck_assert_ptr_nonnull(mtable->sorted_output());
    ck_assert_int_eq(mtable->get_tombstone_count(), 0);
    ck_assert_int_eq(mtable->get_tombstone_capacity(), 50);

    delete mtable;
    gsl_rng_free(rng);
}
END_TEST


START_TEST(t_insert)
{
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto mtable = new MemTable(100, true, 50, rng);

    lsm::key_t key = 0;
    lsm::value_t val = 5;

    for (size_t i=0; i<99; i++) {
        ck_assert_int_eq(mtable->append(key, val, false), 1);
        ck_assert_int_eq(mtable->check_tombstone(key, val), 0);

        key++;
        val++;

        ck_assert_int_eq(mtable->get_record_count(), i+1);
        ck_assert_int_eq(mtable->get_tombstone_count(), 0);
        ck_assert_int_eq(mtable->is_full(), 0);
    }

    ck_assert_int_eq(mtable->append(key, val, false), 1);

    key++;
    val++;

    ck_assert_int_eq(mtable->is_full(), 1);
    ck_assert_int_eq(mtable->append(key, val, false), 0);

    delete mtable;
    gsl_rng_free(rng);

}
END_TEST


START_TEST(t_insert_tombstones)
{
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto mtable = new MemTable(100, true, 50, rng);

    lsm::key_t key = 0;
    lsm::value_t val = 5;
    size_t ts_cnt = 0;

    for (size_t i=0; i<99; i++) {
        bool ts = false;
        if (i % 2 == 0) {
            ts_cnt++;
            ts=true;
        }

        ck_assert_int_eq(mtable->append(key, val, ts), 1);
        ck_assert_int_eq(mtable->check_tombstone(key, val), ts);

        key++;
        val++;

        ck_assert_int_eq(mtable->get_record_count(), i+1);
        ck_assert_int_eq(mtable->get_tombstone_count(), ts_cnt);
        ck_assert_int_eq(mtable->is_full(), 0);
    }

    // inserting one more tombstone should not be possible
    ck_assert_int_eq(mtable->append(key, val, true), 0);


    ck_assert_int_eq(mtable->append(key, val, false), 1);

    key++;
    val++;

    ck_assert_int_eq(mtable->is_full(), 1);
    ck_assert_int_eq(mtable->append(key, val, false), 0);

    delete mtable;
    gsl_rng_free(rng);
}
END_TEST


START_TEST(t_truncate)
{
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto mtable = new MemTable(100, true, 100, rng);

    lsm::key_t key = 0;
    lsm::value_t val = 5;
    size_t ts_cnt = 0;

    for (size_t i=0; i<100; i++) {
        bool ts = false;
        if (i % 2 == 0) {
            ts_cnt++;
            ts=true;
        }

        ck_assert_int_eq(mtable->append(key, val, ts), 1);
        ck_assert_int_eq(mtable->check_tombstone(key, val), ts);

        key++;
        val++;

        ck_assert_int_eq(mtable->get_record_count(), i+1);
        ck_assert_int_eq(mtable->get_tombstone_count(), ts_cnt);
    }

    ck_assert_int_eq(mtable->is_full(), 1);
    ck_assert_int_eq(mtable->append(key, val, false), 0);

    ck_assert_int_eq(mtable->truncate(), 1);

    ck_assert_int_eq(mtable->is_full(), 0);
    ck_assert_int_eq(mtable->get_record_count(), 0);
    ck_assert_int_eq(mtable->get_tombstone_count(), 0);
    ck_assert_int_eq(mtable->append(key, val, false), 1);

    delete mtable;
    gsl_rng_free(rng);

}
END_TEST


START_TEST(t_sorted_output)
{
    size_t cnt = 100;

    auto rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto mtable = new MemTable(cnt, true, cnt/2, rng);


    std::vector<lsm::key_t> keys(cnt);
    for (size_t i=0; i<cnt-2; i++) {
        keys[i] = rand();
    }

    // duplicate final two records for tombstone testing
    // purposes
    keys[cnt-2] =  keys[cnt-3];
    keys[cnt-1] =  keys[cnt-2];

    lsm::value_t val = 12345;
    for (size_t i=0; i<cnt-2; i++) {
        mtable->append(keys[i], val, false);
    }

    mtable->append(keys[cnt-2], val, true);
    mtable->append(keys[cnt-1], val, true);


    record_t *sorted_records = mtable->sorted_output();
    std::sort(keys.begin(), keys.end());

    for (size_t i=0; i<cnt; i++) {
        lsm::key_t table_key = sorted_records[i].key;
        ck_assert_int_eq(table_key, keys[i]);
    }

    delete mtable;
    gsl_rng_free(rng);
}
END_TEST


void insert_records(std::vector<std::pair<lsm::key_t, lsm::value_t>> *values, size_t start, size_t stop, MemTable *mtable)
{
    for (size_t i=start; i<stop; i++) {
        mtable->append((*values)[i].first, (*values)[i].second);
    }

}

START_TEST(t_multithreaded_insert)
{
    size_t cnt = 10000;
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto mtable = new MemTable(cnt, true, cnt/2, rng);

    std::vector<std::pair<lsm::key_t, lsm::value_t>> records(cnt);
    for (size_t i=0; i<cnt; i++) {
        records[i] = {rand(), rand()};
    }

    // perform a t_multithreaded insertion
    size_t thread_cnt = 8;
    size_t per_thread = cnt / thread_cnt;
    std::vector<std::thread> workers(thread_cnt);
    size_t start = 0;
    size_t stop = start + per_thread;
    for (size_t i=0; i<thread_cnt; i++) {
        workers[i] = std::thread(insert_records, &records, start, stop, mtable);
        start = stop;
        stop = std::min(start + per_thread, cnt);
    }

    for (size_t i=0; i<thread_cnt; i++) {
        if (workers[i].joinable()) {
            workers[i].join();
        }
    }

    ck_assert_int_eq(mtable->is_full(), 1);
    ck_assert_int_eq(mtable->get_record_count(), cnt);

    std::sort(records.begin(), records.end());
    record_t *sorted_records = mtable->sorted_output();
    for (size_t i=0; i<cnt; i++) {
        lsm::key_t table_key = sorted_records[i].key;
        ck_assert_int_eq(table_key, records[i].first);
    }

    delete mtable;
    gsl_rng_free(rng);
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("MemTable Unit Testing");
    TCase *initialize = tcase_create("lsm::MemTable Constructor Testing");
    tcase_add_test(initialize, t_create);

    suite_add_tcase(unit, initialize);


    TCase *append = tcase_create("lsm::MemTable::append Testing");
    tcase_add_test(append, t_insert);
    tcase_add_test(append, t_insert_tombstones);
    tcase_add_test(append, t_multithreaded_insert);

    suite_add_tcase(unit, append);


    TCase *truncate = tcase_create("lsm::MemTable::truncate Testing");
    tcase_add_test(truncate, t_truncate);

    suite_add_tcase(unit, truncate);


    TCase *sorted_out = tcase_create("lsm::MemTable::sorted_output");
    tcase_add_test(sorted_out, t_sorted_output);

    suite_add_tcase(unit, sorted_out);

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

