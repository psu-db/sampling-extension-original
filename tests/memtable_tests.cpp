#include <check.h>
#include <string>
#include <thread>
#include <gsl/gsl_rng.h>
#include <vector>
#include <algorithm>

#include "testing.h"
#include "lsm/MemTable.h"

using namespace lsm;

void fill_mtable(MemTable *mtable) {
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
}

MemTable *create_mtable(gsl_rng **rng) {
    *rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto mtable = new MemTable(100, true, 100, *rng);

    fill_mtable(mtable);

    return mtable;
}

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
    gsl_rng *rng;
    auto mtable = create_mtable(&rng);

    lsm::key_t key = 4;
    lsm::value_t val = 2;
    
    bool trunc_stat;
    // truncating without first initiating a merge should fail
    ck_assert_int_eq(mtable->truncate(&trunc_stat), 0);

    // After initiating a merge, truncation should work.
    ck_assert_ptr_nonnull(mtable->start_merge());
    ck_assert_int_eq(mtable->truncate(&trunc_stat), 1);

    ck_assert_int_eq(mtable->is_full(), 0);
    ck_assert_int_eq(mtable->get_record_count(), 0);
    ck_assert_int_eq(mtable->get_tombstone_count(), 0);
    ck_assert_int_eq(mtable->append(key, val, false), 1);

    // Should be unable to truncate without initialing another merge
    ck_assert_int_eq(mtable->truncate(&trunc_stat), 0);

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
        ck_assert_int_eq(sorted_records[i].key, keys[i]);
    }

    delete mtable;
    gsl_rng_free(rng);
}
END_TEST


void insert_records(std::vector<lsm::record_t> *values, size_t start, size_t stop, MemTable *mtable)
{
    for (size_t i=start; i<stop; i++) {
        mtable->append((*values)[i].key, (*values)[i].value);
    }

}

START_TEST(t_multithreaded_insert)
{
    size_t cnt = 10000;
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto mtable = new MemTable(cnt, true, cnt/2, rng);

    std::vector<lsm::record_t> records(cnt);
    for (size_t i=0; i<cnt; i++) {
        records[i] = {(lsm::key_t) rand(), (lsm::value_t) rand()};
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
        ck_assert_int_eq(sorted_records[i].key, records[i].key);
    }

    delete mtable;
    gsl_rng_free(rng);
}
END_TEST


START_TEST(t_defer_truncate)
{
    gsl_rng *rng;
    auto mtable = create_mtable(&rng);

    bool trunc_stat;
    // Truncate should return success when executed against a pinned
    // memtable
    ck_assert_int_eq(mtable->pin(), 1);
    ck_assert_ptr_nonnull(mtable->start_merge());
    ck_assert_int_eq(mtable->truncate(&trunc_stat), 1);
    ck_assert_int_eq(trunc_stat, false);

    lsm::key_t key = 5;
    lsm::value_t val = 2;
    // But the memtable should not actually be truncated yet
    ck_assert_int_eq(mtable->get_record_count(), 100);
    ck_assert_int_eq(mtable->append(key, val), 0);

    // after releasing the pin, the table should be truncated
    ck_assert_int_eq(mtable->unpin(), 1);
    ck_assert_int_eq(trunc_stat, true);
    ck_assert_int_eq(mtable->get_record_count(), 0);

    delete mtable;
    gsl_rng_free(rng);
}
END_TEST


START_TEST(t_pin) 
{
    gsl_rng *rng;
    auto mtable = create_mtable(&rng);

    ck_assert_int_eq(mtable->pin(), 1);
    
    ck_assert_ptr_nonnull(mtable->start_merge());

    bool trunc_stat = false;
    // Should be able to pin again following the end of a merge
    ck_assert_int_eq(mtable->unpin(), 1);
    ck_assert_int_eq(mtable->truncate(&trunc_stat), 1);
    ck_assert_int_eq(trunc_stat, true);
    ck_assert_int_eq(mtable->pin(), 1);

    ck_assert_int_eq(mtable->unpin(), 1);

    delete mtable;
    gsl_rng_free(rng);
}
END_TEST


START_TEST(t_start_merge) 
{
    gsl_rng *rng;
    auto mtable = create_mtable(&rng);

    // should be able to initiate a merge
    ck_assert_ptr_nonnull(mtable->start_merge());

    // however, cannot initiate another one while the
    // first is ongoing
    ck_assert_ptr_null(mtable->start_merge());

    bool trunc_stat = false;
    // Truncation allows a new merge to start, but merging
    // is blocked when the table is not full.
    ck_assert_int_eq(mtable->truncate(&trunc_stat), 1);
    ck_assert_int_eq(trunc_stat, true);
    ck_assert_ptr_null(mtable->start_merge());

    // refill the table, after this merging should be possible
    // again
    fill_mtable(mtable);

    ck_assert_ptr_nonnull(mtable->start_merge());
    ck_assert_int_eq(mtable->truncate(&trunc_stat), 1);
    ck_assert_int_eq(trunc_stat, true);

    // Merges are still blocked following a deferred
    // truncation
    mtable->pin();
    fill_mtable(mtable);
    ck_assert_ptr_nonnull(mtable->start_merge());
    ck_assert_int_eq(mtable->truncate(&trunc_stat), 1);
    ck_assert_int_eq(trunc_stat, false);
    ck_assert_ptr_null(mtable->start_merge());
    mtable->unpin();
    ck_assert_int_eq(trunc_stat, true);

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

    TCase *pin = tcase_create("lsm::MemTable::pin Testing");
    tcase_add_test(pin, t_pin);

    suite_add_tcase(unit, pin);

    TCase *truncate = tcase_create("lsm::MemTable::truncate Testing");
    tcase_add_test(truncate, t_truncate);
    tcase_add_test(truncate, t_defer_truncate);

    suite_add_tcase(unit, truncate);

    TCase *sorted_out = tcase_create("lsm::MemTable::sorted_output");
    tcase_add_test(sorted_out, t_sorted_output);

    suite_add_tcase(unit, sorted_out);


    TCase *merge = tcase_create("lsm::MemTable::start_merge");
    tcase_add_test(merge, t_start_merge);

    suite_add_tcase(unit, merge);

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

