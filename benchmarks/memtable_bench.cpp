#include <thread>
#include <set>
#include <vector>
#include <cassert>
#include <chrono>

#include "ds/unsorted_memtable.hpp"
#include "ds/map_memtable.hpp"
#include "util/benchutil.hpp"

using std::byte;
using namespace lsm;

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
        assert(result.is_valid());

        auto table_key = state->record_schema->get_key(result.get_data()).Int64();
        auto table_val = state->record_schema->get_val(result.get_data()).Int64();

        assert(table_key == *(int64_t*) key_ptr);
        assert(table_val == *(int64_t*) val_ptr);
    }
}

void thread_delete_records(ds::MemoryTable *table, size_t delete_cnt, global::g_state *state)
{
    for (size_t i=0; i<delete_cnt; i++) {
        size_t max = table->get_record_count() - 1; 
        auto idx = gsl_rng_uniform_int(state->rng, max);
        auto record = table->get(idx);

        if (record.is_valid() && !record.is_tombstone()) {
            const byte *key = state->record_schema->get_key(record.get_data()).Bytes();
            const byte *val = state->record_schema->get_val(record.get_data()).Bytes();
            table->insert(const_cast<byte*>(key), const_cast<byte*>(val), 0, true);
        }
    }
}

void threaded_insert(ds::MemoryTable *table, std::vector<int64_t> *keys_v, std::vector<int64_t> *vals_v, size_t threads=4)
{
    size_t count = keys_v->size();
    size_t per_thread = count / threads;

    std::vector<std::thread> workers;
    size_t start = 0;
    size_t stop = start + per_thread;
    for (size_t i=0; i<threads; i++) {
        if (i == threads - 1) {
            stop = keys_v->size() - 1;
        }

        workers.push_back(std::thread(thread_insert_records, table, keys_v, vals_v, start, stop));
        start = stop + 1;
        stop = start + per_thread;
    }

    for (size_t i=0; i<threads; i++) {
        if (workers[i].joinable()) {
            workers[i].join();
        }
    }
}


void threaded_delete(ds::MemoryTable *table, size_t count, global::g_state *state, size_t threads=1)
{
    size_t per_thread = count / threads;

    std::vector<std::thread> workers;
    size_t start = 0;
    size_t stop = start + per_thread;
    for (size_t i=0; i<threads; i++) {
        if (i == threads - 1) {
            stop = count;
        }

        workers.push_back(std::thread(thread_delete_records, table, stop, state)); 
        start = stop + 1;
        stop = start + per_thread;
    }

    for (size_t i=0; i<threads; i++) {
        if (workers[i].joinable()) {
            workers[i].join();
        }
    }
}


void threaded_check_records(ds::MemoryTable *table, std::vector<int64_t> *keys_v, std::vector<int64_t> *vals_v, global::g_state *state, size_t threads=4)
{
    size_t count = keys_v->size();
    size_t per_thread = count / threads;

    std::vector<std::thread> workers;
    size_t start = 0;
    size_t stop = start + per_thread;
    for (size_t i=0; i<threads; i++) {
        if (i == threads - 1) {
            stop = keys_v->size() - 1;
        }

        workers.push_back(std::thread(thread_get_records, table, keys_v, vals_v, start, stop, state));
        start = stop + 1;
        stop = start + per_thread;
    }

    for (size_t i=0; i<threads; i++) {
        if (workers[i].joinable()) {
            workers[i].join();
        }
    }
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


int main(int argc, char **argv) 
{
    if (argc < 7) {
        fprintf(stderr, "bench <unsorted> <table_capacity> <record_count> <thread_count> <delete_cnt> <delete_threads>\n");
        exit(EXIT_FAILURE);
    }

    bool unsorted = atoi(argv[1]);
    size_t capacity = atol(argv[2]);
    size_t count = atol(argv[3]);
    size_t threads = atol(argv[4]);
    size_t deletes = atol(argv[5]);
    size_t d_threads = atol(argv[6]);

    std::vector<int64_t> key_v;
    std::vector<int64_t> val_v;

    populate_test_vectors(count, key_v, val_v);
    auto state = bench::bench_state();

    std::unique_ptr<ds::MemoryTable> table;

    size_t delete_cnt = std::min(capacity - count, deletes);

    if (unsorted) {
        table = std::make_unique<ds::UnsortedMemTable>(capacity, state.get());
    } else {
        table = std::make_unique<ds::MapMemTable>(capacity, state.get());
    }

    auto start_insert = std::chrono::high_resolution_clock::now();
    threaded_insert(table.get(), &key_v, &val_v, threads);
    if (deletes && d_threads) {
        threaded_delete(table.get(), delete_cnt, state.get(), d_threads);
    }
    auto stop_insert = std::chrono::high_resolution_clock::now();

    /*
    auto start_read = std::chrono::high_resolution_clock::now();
    threaded_check_records(table.get(), &key_v, &val_v, state.get(), threads);
    auto stop_read = std::chrono::high_resolution_clock::now();
    */

    size_t insert_time = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_insert - start_insert).count();
    //size_t read_time = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_read - start_read).count();

    double insert_tput = (double) count / (double) insert_time * 1e9;
    //double read_tput = (double) count / (double) read_time * 1e9;
    double read_tput = 0;


    fprintf(stderr, "Capacity, Count, Threads, Insert Tput (rec/s), Read Tput(rec/s)\n");
    fprintf(stdout, "%ld\t%ld\t%ld\t%.0lf\t%.0lf\n", capacity, count + delete_cnt, threads, insert_tput, read_tput);

    exit(EXIT_SUCCESS);
}

