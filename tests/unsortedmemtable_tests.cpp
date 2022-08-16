/*
 *
 */

#include <check.h>
#include <set>
#include <thread>

#include "ds/unsorted_memtable.hpp"
#include "testing.hpp"

using namespace lsm;
using std::byte;

START_TEST(t_create)
{
    auto state = testing::make_state1();
    auto table = ds::UnsortedMemTable(500, state.get());

    ck_assert_int_eq(table.get_capacity(), 500);
    ck_assert_int_eq(table.get_record_count(), 0);
    ck_assert_int_eq(table.is_full(), 0);
}
END_TEST


START_TEST(t_insert)
{
    auto state = testing::make_state1();
    auto table = ds::UnsortedMemTable(500, state.get());

    int64_t key = 150;
    int64_t value = 10;

    ck_assert_int_eq(table.insert((byte*) &key, (byte*) &value), 1);
    ck_assert_int_eq(table.get_capacity(), 500);
    ck_assert_int_eq(table.get_record_count(), 1);
    ck_assert_int_eq(table.is_full(), 0);
    
    auto res = table.get((byte *) &key);
    ck_assert_int_eq(res.is_valid(), 1);

    auto r_key = state->record_schema->get_key(res.get_data()).Int64();
    auto r_val = state->record_schema->get_val(res.get_data()).Int64();

    ck_assert_int_eq(r_key, key);
    ck_assert_int_eq(r_val, value);
}
END_TEST


START_TEST(t_insert_to_full)
{
    size_t capacity = 1000;
    std::set<int64_t> keys;
    std::set<int64_t> vals;

    while (keys.size() < capacity) {
        int64_t key = rand();
        int64_t val = rand();

        if (keys.find(key) == keys.end()) {
            keys.insert(key);
            vals.insert(val);
        }
    }

    auto state = testing::make_state1();
    auto table = ds::UnsortedMemTable(capacity, state.get());

    std::vector<int64_t> keys_v;
    keys_v.insert(keys_v.begin(), keys.begin(), keys.end());

    std::vector<int64_t> vals_v;
    vals_v.insert(vals_v.begin(), vals.begin(), vals.end());
    

    for (size_t i=0; i < capacity; i++) {
        auto key_ptr = (byte *) &keys_v[i];
        auto val_ptr = (byte *) &vals_v[i];

        ck_assert_int_eq(table.insert(key_ptr, val_ptr), 1);
    }

    ck_assert_int_eq(table.is_full(), 1);
    ck_assert_int_eq(table.get_record_count(), capacity);

    int64_t new_key = -10;
    int64_t new_val = -11;

    ck_assert_int_eq(table.insert((byte*) &new_key, (byte *) &new_val), 0);

    for (size_t i=0; i<capacity; i++) {
        auto key_ptr = (byte *) &keys_v[i];
        auto val_ptr = (byte *) &vals_v[i];

        auto result = table.get(key_ptr);
        ck_assert(result.is_valid());

        auto table_key = state->record_schema->get_key(result.get_data()).Int64();
        auto table_val = state->record_schema->get_val(result.get_data()).Int64();

        ck_assert_int_eq(table_key, *(int64_t*) key_ptr);
        ck_assert_int_eq(table_val, *(int64_t*) val_ptr);
    }

    ck_assert_int_eq(table.get((byte*) &new_key).is_valid(), 0);
}
END_TEST


void thread_insert_records(ds::MemoryTable *table, std::vector<int64_t> *keys, std::vector<int64_t> *vals, size_t start_idx, size_t stop_idx)
{
    for (size_t i=start_idx; i<=stop_idx; i++) {
        if (i >= keys->size()) {
            break;
        }

        byte *key = (byte *) &(*keys)[i];
        byte *val = (byte *) &(*vals)[i];

        table->insert(key, val);
    }
}


void thread_get_records(ds::MemoryTable *table, std::vector<int64_t> *keys, std::vector<int64_t> *vals, size_t start_idx, size_t stop_idx, global::g_state *state)
{
    for (size_t i=start_idx; i<=stop_idx; i++) {
        auto key_ptr = (byte *) &(*keys)[i];
        auto val_ptr = (byte *) &(*vals)[i];

        auto result = table->get(key_ptr);
        ck_assert(result.is_valid());

        auto table_key = state->record_schema->get_key(result.get_data()).Int64();
        auto table_val = state->record_schema->get_val(result.get_data()).Int64();

        ck_assert_int_eq(table_key, *(int64_t*) key_ptr);
        ck_assert_int_eq(table_val, *(int64_t*) val_ptr);
    }
}


void threaded_insert(ds::MemoryTable *table, std::vector<int64_t> *keys_v, std::vector<int64_t> *vals_v)
{
    size_t count = keys_v->size();
    size_t threads = 4;
    size_t per_thread = count / threads;

    size_t start = 0;
    size_t stop = start + per_thread;
    std::thread first (thread_insert_records, table, keys_v, vals_v, start, stop);

    start = stop + 1;
    stop += per_thread;
    std::thread second (thread_insert_records, table, keys_v, vals_v, start, stop);

    start = stop + 1;
    stop += per_thread;
    std::thread third (thread_insert_records, table, keys_v, vals_v, start, stop);

    start = stop + 1;
    stop = count - 1;
    std::thread fourth (thread_insert_records, table, keys_v, vals_v, start, stop);

    first.join();
    second.join();
    third.join();
    fourth.join();
}


void threaded_check_records(ds::MemoryTable *table, std::vector<int64_t> *keys_v, std::vector<int64_t> *vals_v, global::g_state *state)
{
    size_t count = keys_v->size();
    size_t threads = 4;
    size_t per_thread = count / threads;

    size_t start = 0;
    size_t stop = start + per_thread;
    std::thread get_first (thread_get_records, table, keys_v, vals_v, start, stop, state);

    start = stop + 1;
    stop = start + per_thread;
    std::thread get_second (thread_get_records, table, keys_v, vals_v, start, stop, state);

    start = stop + 1;
    stop = start + per_thread;
    std::thread get_third (thread_get_records, table, keys_v, vals_v, start, stop, state);

    start = stop + 1;
    stop = count - 1;
    std::thread get_fourth (thread_get_records, table, keys_v, vals_v, start, stop, state);

    get_first.join();
    get_second.join();
    get_third.join();
    get_fourth.join();
}


void populate_test_vectors(size_t count, std::vector<int64_t> &keys_v, std::vector<int64_t> &vals_v)
{
    std::set<int64_t> keys;
    std::set<int64_t> vals;

    while (keys.size() < count) {
        int64_t key = rand();
        int64_t val = rand();

        if (keys.find(key) == keys.end()) {
            keys.insert(key);
            vals.insert(val);
        }
    }

    keys_v.insert(keys_v.begin(), keys.begin(), keys.end());
    vals_v.insert(vals_v.begin(), vals.begin(), vals.end());
}


START_TEST(t_insert_threaded)
{
    size_t capacity = 1000;
    size_t count = capacity * 0.9;

    auto state = testing::make_state1();
    auto table = ds::UnsortedMemTable(capacity, state.get());

    std::vector<int64_t> keys_v;
    std::vector<int64_t> vals_v;

    populate_test_vectors(count, keys_v, vals_v);

    threaded_insert(&table, &keys_v, &vals_v);

    ck_assert_int_eq(table.is_full(), 0);
    ck_assert_int_eq(table.get_record_count(), count);

    threaded_check_records(&table, &keys_v, &vals_v, state.get());
}
END_TEST


START_TEST(t_truncate_pinned)
{
    size_t capacity = 1000;
    size_t count = capacity;

    auto state = testing::make_state1();
    auto table = ds::UnsortedMemTable(capacity, state.get());

    std::vector<int64_t> keys_v;
    std::vector<int64_t> vals_v;

    populate_test_vectors(count, keys_v, vals_v);

    threaded_insert(&table, &keys_v, &vals_v);

    // should not be able to truncate while pinned
    table.thread_pin();
    ck_assert_int_eq(table.truncate(), 0);

    table.thread_unpin();
    ck_assert_int_eq(table.truncate(), 1);
}
END_TEST


START_TEST(t_truncate_sampling)
{
    size_t capacity = 1000;
    size_t count = capacity;

    auto state = testing::make_state1();
    auto table = ds::UnsortedMemTable(capacity, state.get());

    std::vector<int64_t> keys_v;
    std::vector<int64_t> vals_v;

    populate_test_vectors(count, keys_v, vals_v);

    threaded_insert(&table, &keys_v, &vals_v);

    {
        byte *lower = (byte *) &keys_v[0];
        byte *upper = (byte *) &keys_v[15];
        auto range = table.get_sample_range(lower, upper);
        ck_assert_int_eq(table.truncate(), 0);
    }

    ck_assert_int_eq(table.truncate(), 1);
}
END_TEST


void thread_iterate(ds::MemoryTable *table)
{
    auto iter = table->start_sorted_scan();
    sleep(3);

    size_t cnt = table->get_record_count();
    size_t i=0;
    while (iter->next()) {
        i++;
    }

    ck_assert_int_eq(cnt, i);
}


START_TEST(t_truncate_iterating)
{
    size_t capacity = 1000;
    size_t count = capacity;

    auto state = testing::make_state1();
    auto table = ds::UnsortedMemTable(capacity, state.get());

    std::vector<int64_t> keys_v;
    std::vector<int64_t> vals_v;

    populate_test_vectors(count, keys_v, vals_v);

    threaded_insert(&table, &keys_v, &vals_v);

    std::thread iter_thread (thread_iterate, &table);
    sleep(1);
    ck_assert_int_eq(table.truncate(), 0);
    iter_thread.join();

    ck_assert_int_eq(table.truncate(), 1);
}
END_TEST


START_TEST(t_iterator)
{
    size_t capacity = 1000;
    std::set<int64_t> keys;
    std::set<int64_t> vals;

    while (keys.size() < capacity) {
        int64_t key = rand();
        int64_t val = rand();

        if (keys.find(key) == keys.end()) {
            keys.insert(key);
            vals.insert(val);
        }
    }

    auto state = testing::make_state1();
    auto table = ds::UnsortedMemTable(capacity, state.get());

    std::vector<int64_t> keys_v;
    keys_v.insert(keys_v.begin(), keys.begin(), keys.end());

    std::vector<int64_t> vals_v;
    vals_v.insert(vals_v.begin(), vals.begin(), vals.end());
    
    for (size_t i=0; i < capacity; i++) {
        auto key_ptr = (byte *) &keys_v[i];
        auto val_ptr = (byte *) &vals_v[i];

        ck_assert_int_eq(table.insert(key_ptr, val_ptr), 1);
    }

    std::sort(keys_v.begin(), keys_v.end());

    auto iter = table.start_sorted_scan();
    size_t i=0;
    while (iter->next()) {
        auto rec = iter->get_item();

        auto rec_key = state->record_schema->get_key(rec.get_data()).Int64();
        ck_assert_int_eq(rec_key, keys_v[i]);

        i++;
    }

    ck_assert_int_eq(i, capacity);
}
END_TEST


START_TEST(t_sample_range)
{
    size_t capacity = 500;

    auto state = testing::make_state1();
    auto table = ds::UnsortedMemTable(capacity, state.get());

    int64_t test_val = 0;
    int64_t test_key = 0;
    for (size_t i=0; i<capacity; i++) {
        table.insert((byte*) &test_key, (byte *) &test_val);
        test_val++;
        test_key++;
    }

    int64_t start_key = 51;
    int64_t stop_key = 150;

    FrameId frid;

    auto range = table.get_sample_range((byte*) &start_key, (byte*) &stop_key);
    ck_assert_ptr_nonnull(range.get());
    ck_assert_int_eq(range->length(), 100);

    for (size_t i=0; i<1000; i++) {
        auto rec = range->get(&frid);
        auto range_key = state->record_schema->get_key(rec.get_data()).Int64();
        ck_assert_int_ge(range_key, start_key);
        ck_assert_int_le(range_key, stop_key);
        ck_assert_int_eq(frid, INVALID_FRID);
    }
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("UnsortedMemTable Unit Testing");
    TCase *create = tcase_create("lsm::sampling::UnsortedMemTable Constructor Testing");
    tcase_add_test(create, t_create);

    suite_add_tcase(unit, create);


    TCase *insert = tcase_create("lsm::sampling::UnsortedMemTable::insert Testing");
    tcase_add_test(insert, t_insert);
    tcase_add_test(insert, t_insert_to_full);
    tcase_add_test(insert, t_insert_threaded);

    tcase_set_timeout(insert, 100);

    suite_add_tcase(unit, insert);


    TCase *truncate = tcase_create("lsm::ds::UnsortedMemTable::truncate Testing");
    tcase_add_test(truncate, t_truncate_pinned);
    tcase_add_test(truncate, t_truncate_sampling);
    tcase_add_test(truncate, t_truncate_iterating);

    suite_add_tcase(unit, truncate);

    TCase *iter = tcase_create("lsm::sampling::UnsortedMemTable::start_sorted_scan Testing");
    tcase_add_test(iter, t_iterator);

    suite_add_tcase(unit, iter);


    TCase *sampling = tcase_create("lsm::sampling::UnsortedMemTable::create_sample_range Testing");
    tcase_add_test(sampling, t_sample_range);

    suite_add_tcase(unit, sampling);

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
