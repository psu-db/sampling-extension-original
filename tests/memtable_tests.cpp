#include <check.h>
#include <string>
#include <thread>
#include <gsl/gsl_rng.h>
#include <vector>
#include <algorithm>

#include "testing.h"
#include "lsm/MemTable.h"

using namespace lsm;

MemTable *create_mtable(gsl_rng **rng) {
    *rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto mtable = new MemTable(100, true, 50, *rng);

    key_type key = 0;
    value_type val = 5;
    size_t ts_cnt = 0;

    for (size_t i=0; i<100; i++) {
        bool ts = false;
        if (i % 2 == 0) {
            ts_cnt++;
            ts=true;
        }

        ck_assert_int_eq(mtable->append((char*) &key, (char*) &val, ts), 1);
        ck_assert_int_eq(mtable->check_tombstone((char*) &key, (char*) &val), ts);

        key++;
        val++;

        ck_assert_int_eq(mtable->get_record_count(), i+1);
        ck_assert_int_eq(mtable->get_tombstone_count(), ts_cnt);
    }

    ck_assert_int_eq(mtable->is_full(), 1);
    ck_assert_int_eq(mtable->append((char*) &key, (char*) &val, false), 0);

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

    delete mtable;
    gsl_rng_free(rng);
}
END_TEST


START_TEST(t_insert)
{
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto mtable = new MemTable(100, true, 50, rng);

    key_type key = 0;
    value_type val = 5;

    for (size_t i=0; i<99; i++) {
        ck_assert_int_eq(mtable->append((char*) &key, (char*) &val, false), 1);
        ck_assert_int_eq(mtable->check_tombstone((char*) &key, (char*) &val), 0);

        key++;
        val++;

        ck_assert_int_eq(mtable->get_record_count(), i+1);
        ck_assert_int_eq(mtable->get_tombstone_count(), 0);
        ck_assert_int_eq(mtable->is_full(), 0);
    }

    ck_assert_int_eq(mtable->append((char*) &key, (char*) &val, false), 1);

    key++;
    val++;

    ck_assert_int_eq(mtable->is_full(), 1);
    ck_assert_int_eq(mtable->append((char*) &key, (char*) &val, false), 0);

    delete mtable;
    gsl_rng_free(rng);

}
END_TEST


START_TEST(t_insert_tombstones)
{
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto mtable = new MemTable(100, true, 50, rng);

    key_type key = 0;
    value_type val = 5;
    size_t ts_cnt = 0;

    for (size_t i=0; i<99; i++) {
        bool ts = false;
        if (i % 2 == 0) {
            ts_cnt++;
            ts=true;
        }

        ck_assert_int_eq(mtable->append((char*) &key, (char*) &val, ts), 1);
        ck_assert_int_eq(mtable->check_tombstone((char*) &key, (char*) &val), ts);

        key++;
        val++;

        ck_assert_int_eq(mtable->get_record_count(), i+1);
        ck_assert_int_eq(mtable->get_tombstone_count(), ts_cnt);
        ck_assert_int_eq(mtable->is_full(), 0);
    }

    ck_assert_int_eq(mtable->append((char*) &key, (char*) &val, false), 1);

    key++;
    val++;

    ck_assert_int_eq(mtable->is_full(), 1);
    ck_assert_int_eq(mtable->append((char*) &key, (char*) &val, false), 0);

    delete mtable;
    gsl_rng_free(rng);
}
END_TEST


START_TEST(t_truncate)
{
    gsl_rng *rng;
    auto mtable = create_mtable(&rng);

    key_type key = 4;
    value_type val = 2;
    
    bool trunc_stat;
    // truncating without first initiating a merge should fail
    ck_assert_int_eq(mtable->truncate(&trunc_stat), 0);

    // After initiating a merge, truncation should work.
    ck_assert_ptr_nonnull(mtable->start_merge());
    ck_assert_int_eq(mtable->truncate(&trunc_stat), 1);

    ck_assert_int_eq(mtable->is_full(), 0);
    ck_assert_int_eq(mtable->get_record_count(), 0);
    ck_assert_int_eq(mtable->get_tombstone_count(), 0);
    ck_assert_int_eq(mtable->append((char*) &key, (char*) &val, false), 1);

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


    std::vector<key_type> keys(cnt);
    for (size_t i=0; i<cnt-2; i++) {
        keys[i] = rand();
    }

    // duplicate final two records for tombstone testing
    // purposes
    keys[cnt-2] =  keys[cnt-3];
    keys[cnt-1] =  keys[cnt-2];

    value_type val = 12345;
    for (size_t i=0; i<cnt-2; i++) {
        mtable->append((char *) &keys[i], (char*) &val, false);
    }

    mtable->append((char *) &keys[cnt-2], (char*) &val, true);
    mtable->append((char *) &keys[cnt-1], (char*) &val, true);


    key_type ts_key = keys[cnt-1];
    char *sorted_records = mtable->sorted_output();
    std::sort(keys.begin(), keys.end());

    for (size_t i=0; i<cnt; i++) {
        key_type *table_key = (key_type *) get_key(sorted_records + i*record_size);
        ck_assert_int_eq(*table_key, keys[i]);
    }

    delete mtable;
    gsl_rng_free(rng);
}
END_TEST


void insert_records(std::vector<std::pair<key_type, value_type>> *values, size_t start, size_t stop, MemTable *mtable)
{
    for (size_t i=start; i<stop; i++) {
        mtable->append((char*) &((*values)[i].first), (char*) &((*values)[i].second));
    }

}

START_TEST(t_multithreaded_insert)
{
    size_t cnt = 10000;
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto mtable = new MemTable(cnt, true, cnt/2, rng);

    std::vector<std::pair<key_type, value_type>> records(cnt);
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
    char *sorted_records = mtable->sorted_output();
    for (size_t i=0; i<cnt; i++) {
        key_type *table_key = (key_type *) get_key(sorted_records + i*record_size);
        ck_assert_int_eq(*table_key, records[i].first);
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

    key_type key = 5;
    value_type val = 2;
    // But the memtable should not actually be truncated yet
    ck_assert_int_eq(mtable->get_record_count(), 100);
    ck_assert_int_eq(mtable->append((char*) &key, (char*) &val), 0);

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
    
    // Cannot add more pins once a merge starts (should use
    // other table, if possible).
    ck_assert_ptr_nonnull(mtable->start_merge());
    ck_assert_int_eq(mtable->pin(), 0);

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
    // Truncation allows a new merge to start
    ck_assert_int_eq(mtable->truncate(&trunc_stat), 1);
    ck_assert_int_eq(trunc_stat, true);

    ck_assert_ptr_nonnull(mtable->start_merge());
    ck_assert_int_eq(mtable->truncate(&trunc_stat), 1);
    ck_assert_int_eq(trunc_stat, true);

    // Merges are still blocked following a deferred
    // truncation
    mtable->pin();
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

