#include <check.h>
#include <set>
#include <random>
#include <thread>

#include "lsm/LsmTree.h"

using namespace lsm;

gsl_rng *g_rng = gsl_rng_alloc(gsl_rng_mt19937);

std::string dir = "./tests/data/lsmtree";

START_TEST(t_create)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

    ck_assert_ptr_nonnull(lsm);
    ck_assert_int_eq(lsm->get_record_cnt(), 0);
    ck_assert_int_eq(lsm->get_height(), 0);

    lsm->await_merge_completion();
    delete lsm;
}
END_TEST


START_TEST(t_append)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

    key_type key = 0;
    value_type val = 0;
    for (size_t i=0; i<100; i++) {
        const char *key_ptr = (char *) &key;
        const char *val_ptr = (char *) &val;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 0, g_rng), 1);
        key++;
        val++;
    }

    ck_assert_int_eq(lsm->get_height(), 0);
    ck_assert_int_eq(lsm->get_record_cnt(), 100);

    lsm->await_merge_completion();
    delete lsm;
}
END_TEST


START_TEST(t_append_with_mem_merges)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

    key_type key = 0;
    value_type val = 0;
    for (size_t i=0; i<300; i++) {
        const char *key_ptr = (char *) &key;
        const char *val_ptr = (char *) &val;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 0, g_rng), 1);
        key++;
        val++;
    }

    ck_assert_int_eq(lsm->get_record_cnt(), 300);
    ck_assert_int_eq(lsm->get_height(), 1);

    lsm->await_merge_completion();
    delete lsm;
}
END_TEST


START_TEST(t_append_with_disk_merges)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

    key_type key = 0;
    value_type val = 0;
    for (size_t i=0; i<1000; i++) {
        const char *key_ptr = (char *) &key;
        const char *val_ptr = (char *) &val;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 0, g_rng), 1);
        key++;
        val++;
    }

    ck_assert_int_eq(lsm->get_record_cnt(), 1000);
    ck_assert_int_eq(lsm->get_height(), 3);

    lsm->await_merge_completion();
    delete lsm;
}
END_TEST


START_TEST(t_range_sample_memtable)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

    key_type key = 0;
    value_type val = 0;
    for (size_t i=0; i<100; i++) {
        const char *key_ptr = (char *) &key;
        const char *val_ptr = (char *) &val;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 0, g_rng), 1);
        key++;
        val++;
    }

    key_type lower_bound = 20;
    key_type upper_bound = 50;

    char *buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char *util_buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char sample_set[100*record_size];

    lsm->range_sample(sample_set, (char*) &lower_bound, (char*) &upper_bound, 100, buf, util_buf, g_rng);

    for(size_t i=0; i<100; i++) {
        auto rec = sample_set + (record_size * i);
        auto s_key = *(key_type*) get_key(rec);
        auto s_val = *(value_type*) get_val(rec);

        ck_assert_int_le(s_key, upper_bound);
        ck_assert_int_ge(s_key, lower_bound);
    }

    free(buf);
    free(util_buf);

    lsm->await_merge_completion();
    delete lsm;
}
END_TEST


START_TEST(t_range_sample_memlevels)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

    key_type key = 0;
    value_type val = 0;
    for (size_t i=0; i<300; i++) {
        const char *key_ptr = (char *) &key;
        const char *val_ptr = (char *) &val;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 0, g_rng), 1);
        key++;
        val++;
    }

    key_type lower_bound = 100;
    key_type upper_bound = 250;

    char *buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char *util_buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);

    char sample_set[100*record_size];
    lsm->range_sample(sample_set, (char*) &lower_bound, (char*) &upper_bound, 100, buf, util_buf, g_rng);

    for(size_t i=0; i<100; i++) {
        auto rec = sample_set + (record_size * i);
        auto s_key = *(key_type*) get_key(rec);
        auto s_val = *(value_type*) get_val(rec);

        ck_assert_int_le(s_key, upper_bound);
        ck_assert_int_ge(s_key, lower_bound);
    }

    free(buf);
    free(util_buf);

    lsm->await_merge_completion();
    delete lsm;
}
END_TEST


START_TEST(t_range_sample_disklevels)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

    key_type key = 0;
    value_type val = 0;
    for (size_t i=0; i<1000; i++) {
        const char *key_ptr = (char *) &key;
        const char *val_ptr = (char *) &val;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 0, g_rng), 1);
        key++;
        val++;
    }

    key_type lower_bound = 0;
    key_type upper_bound = 1000;

    char *buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char *util_buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);

    char sample_set[100*record_size];
    lsm->range_sample(sample_set, (char*) &lower_bound, (char*) &upper_bound, 100, buf, util_buf, g_rng);

    for(size_t i=0; i<100; i++) {
        auto rec = sample_set + (record_size * i);
        auto s_key = *(key_type*) get_key(rec);
        auto s_val = *(value_type*) get_val(rec);

        ck_assert_int_le(s_key, upper_bound);
        ck_assert_int_ge(s_key, lower_bound);
    }

    free(buf);
    free(util_buf);

    lsm->await_merge_completion();
    delete lsm;
}
END_TEST


START_TEST(t_sorted_array)
{
    size_t reccnt = 100000;
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

    std::set<std::pair<key_type, value_type>> records; 
    std::set<std::pair<key_type, value_type>> to_delete;
    std::set<std::pair<key_type, value_type>> deleted;

    while (records.size() < reccnt) {
        key_type key = rand();
        value_type val = rand();

        if (records.find({key, val}) != records.end()) continue;

        records.insert({key, val});
    }

    size_t deletes = 0;
    for (auto rec : records) {
        const char *key_ptr = (char *) &rec.first;
        const char *val_ptr = (char *) &rec.second;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 0, g_rng), 1);

         if (gsl_rng_uniform(g_rng) < 0.05 && !to_delete.empty()) {
            std::vector<std::pair<key_type, value_type>> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                const char *d_key_ptr = (char *) &del_vec[i].first;
                const char *d_val_ptr = (char *) &del_vec[i].second;
                lsm->append(d_key_ptr, d_val_ptr, true, g_rng);
                deletes++;
                to_delete.erase(del_vec[i]);
                deleted.insert(del_vec[i]);
            }
        }

        if (gsl_rng_uniform(g_rng) < 0.25 && deleted.find(rec) == deleted.end()) {
            to_delete.insert(rec);
        }
    }

    size_t len;
    auto flat = lsm->get_sorted_array(&len, g_rng);
    ck_assert_int_eq(len, reccnt - deletes);

    key_type prev_key = 0;
    for (size_t i=0; i<len; i++) {
        auto k = *(key_type*) get_key(&flat[i*record_size]);
        auto r = *(value_type*) get_val(&flat[i*record_size]);
        ck_assert_int_ge(k, prev_key);
        prev_key = k;
    }

    free(flat);

    lsm->await_merge_completion();
    delete lsm;
}
END_TEST


START_TEST(t_flat_isam)
{
    size_t reccnt = 100000;
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

    std::set<std::pair<key_type, value_type>> records; 
    std::set<std::pair<key_type, value_type>> to_delete;
    std::set<std::pair<key_type, value_type>> deleted;

    while (records.size() < reccnt) {
        key_type key = rand();
        value_type val = rand();

        if (records.find({key, val}) != records.end()) continue;

        records.insert({key, val});
    }

    size_t deletes = 0;
    for (auto rec : records) {
        const char *key_ptr = (char *) &rec.first;
        const char *val_ptr = (char *) &rec.second;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 0, g_rng), 1);

         if (gsl_rng_uniform(g_rng) < 0.05 && !to_delete.empty()) {
            std::vector<std::pair<key_type, value_type>> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                const char *d_key_ptr = (char *) &del_vec[i].first;
                const char *d_val_ptr = (char *) &del_vec[i].second;
                lsm->append(d_key_ptr, d_val_ptr, true, g_rng);
                deletes++;
                to_delete.erase(del_vec[i]);
                deleted.insert(del_vec[i]);
            }
        }

        if (gsl_rng_uniform(g_rng) < 0.25 && deleted.find(rec) == deleted.end()) {
            to_delete.insert(rec);
        }
    }

    auto flat = lsm->get_flat_isam_tree(g_rng);

    ck_assert_int_eq(flat->get_record_count(), reccnt - deletes);
    ck_assert_int_eq(flat->get_tombstone_count(), 0);

    auto iter = flat->start_scan();

    size_t recs = 0;
    key_type prev_key = 0;
    while (iter->next()) {
        auto pg = iter->get_item();
        for (size_t i=0; i<PAGE_SIZE/record_size; i++) {
            if (recs >= flat->get_record_count()) break;
            recs++;

            auto rec = pg + (i * record_size);
            auto k = *(key_type*) get_key(rec);
            auto r = *(value_type*) get_val(rec);
            ck_assert_int_ge(k, prev_key);
            prev_key = k;
        }
    }

    auto pfile = flat->get_pfile();


    delete iter;
    delete flat;

    lsm->await_merge_completion();
    delete lsm;

    delete pfile;
}
END_TEST


START_TEST(t_tombstone_merging_01)
{
    size_t reccnt = 100000;
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, .01, g_rng);

    std::set<std::pair<key_type, value_type>> records; 
    std::set<std::pair<key_type, value_type>> to_delete;
    std::set<std::pair<key_type, value_type>> deleted;

    while (records.size() < reccnt) {
        key_type key = rand();
        value_type val = rand();

        if (records.find({key, val}) != records.end()) continue;

        records.insert({key, val});
    }

    size_t deletes = 0;
    size_t cnt=0;
    for (auto rec : records) {
        const char *key_ptr = (char *) &rec.first;
        const char *val_ptr = (char *) &rec.second;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 0, g_rng), 1);

         if (gsl_rng_uniform(g_rng) < 0.05 && !to_delete.empty()) {
            std::vector<std::pair<key_type, value_type>> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                const char *d_key_ptr = (char *) &del_vec[i].first;
                const char *d_val_ptr = (char *) &del_vec[i].second;
                lsm->append(d_key_ptr, d_val_ptr, true, g_rng);
                deletes++;
                to_delete.erase(del_vec[i]);
                deleted.insert(del_vec[i]);
            }
        }

        if (gsl_rng_uniform(g_rng) < 0.25 && deleted.find(rec) == deleted.end()) {
            to_delete.insert(rec);
        }
    }

    lsm->await_merge_completion();
    ck_assert(lsm->validate_tombstone_proportion());

    delete lsm;
}
END_TEST

/*
 * functions for multithreaded testing
 */
void insert_records(std::vector<std::pair<key_type, value_type>> *records, size_t start, size_t stop, lsm::LSMTree *tree) 
{
    for (size_t i=start; i<stop; i++) {
        tree->append((char*) &((*records)[i].first), (char*) &((*records)[i].second), false, g_rng);
    }
}


void sample(LSMTree *lsm, size_t n) {
    auto buf1 = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    auto buf2 = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);

    size_t k = 1000;

    key_type lower_bound = 0;
    key_type upper_bound = gsl_rng_uniform_int(g_rng, 10000);

    const char *lower = (char *) &lower_bound;
    const char *upper = (char *) &upper_bound;

    for (size_t i=0; i<n; i++) {
        auto sample_set = new char[k * lsm::record_size]();
        lsm->range_sample(sample_set, lower, upper, k, buf1, buf2, g_rng);

        for (size_t j=0; i<k; i++) {
            auto rec = sample_set + i*record_size;
            auto s_key = *(key_type*) get_key(rec);
            auto s_val = *(value_type*) get_val(rec);

            ck_assert_int_le(s_key, upper_bound);
            ck_assert_int_ge(s_key, lower_bound);
        }

        delete[] sample_set;
    }

    free(buf1);
    free(buf2);
}

START_TEST(t_multithread_insert)
{
    size_t record_cnt = 10000;
    std::vector<std::pair<key_type, value_type>> records(record_cnt);
    for (size_t i=0; i<record_cnt; i++) {
        records[i] = {rand(), rand()};
    }

    auto lsm = new LSMTree(dir, 100, 1, 2, 1, .01, g_rng);
    size_t thread_cnt = 8;
    size_t per_thread = record_cnt / thread_cnt;

    std::vector<std::thread> workers(thread_cnt);
    size_t start = 0;
    size_t stop = start + per_thread;
    for (size_t i=0; i<thread_cnt; i++) {
        workers[i] = std::thread(insert_records, &records, start, stop, lsm);
        start = stop;
        stop = std::min(start + per_thread, record_cnt);
    }

    for (size_t i=0; i<thread_cnt; i++) {
        if (workers[i].joinable()) {
            workers[i].join();
        }
    }

    lsm->await_merge_completion();
    ck_assert_int_eq(lsm->get_record_cnt(), record_cnt);
    ck_assert_int_eq(lsm->get_tombstone_cnt(), 0);

    delete lsm;
}
END_TEST


START_TEST(t_multithread_sample)
{
    size_t record_cnt = 10000;
    auto lsm = new LSMTree(dir, 100, 1, 2, 1, .01, g_rng);

    for (size_t i=0; i<record_cnt; i++) {
        lsm->append((char *) &i, (char*) &i, false, g_rng);
    }

    size_t thread_cnt = 4;

    std::vector<std::thread> workers(thread_cnt);
    for (size_t i=0; i<thread_cnt; i++) {
        workers[i] = std::thread(sample, lsm, 100);
    }

    for (size_t i=0; i<thread_cnt; i++) {
        if (workers[i].joinable()) {
            workers[i].join();
        }
    }

    lsm->await_merge_completion();
    ck_assert_int_eq(lsm->get_record_cnt(), record_cnt);
    ck_assert_int_eq(lsm->get_tombstone_cnt(), 0);

    delete lsm;
}
END_TEST


START_TEST(t_multithread_mix)
{
    size_t record_cnt = 100000;
    auto lsm = new LSMTree(dir, 100, 50, 2, 1, .01, g_rng);

    std::vector<std::pair<key_type, value_type>> records(record_cnt);
    for (size_t i=0; i<record_cnt; i++) {
        records[i] = {rand(), rand()};
    }

    // load a few records to get started
    size_t warmup_record_cnt = record_cnt / 10;
    for (size_t i=0; i<warmup_record_cnt; i++) {
        lsm->append((char *) &i, (char*) &i, false, g_rng);
    }


    size_t insert_thread_cnt = 4;
    size_t sample_thread_cnt = 4;
    size_t per_thread = (record_cnt - warmup_record_cnt) / insert_thread_cnt;

    std::vector<std::thread> insert_workers(insert_thread_cnt);
    std::vector<std::thread> sample_workers(sample_thread_cnt);

    size_t start = warmup_record_cnt;
    size_t stop = start + per_thread;
    for (size_t i=0; i<insert_thread_cnt; i++) {
        insert_workers[i] = std::thread(insert_records, &records, start, stop, lsm);
        start = stop;
        stop = std::min(start + per_thread, record_cnt);
    }

    for (size_t i=0; i<sample_thread_cnt; i++) {
        sample_workers[i] = std::thread(sample, lsm, 100);
    }

    for (size_t i=0; i<sample_thread_cnt; i++) {
        if (sample_workers[i].joinable()) {
            sample_workers[i].join();
        }
    }

    for (size_t i=0; i<insert_thread_cnt; i++) {
        if (insert_workers[i].joinable()) {
            insert_workers[i].join();
        }
    }

    delete lsm;
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("lsm::LSMTree Unit Testing");

    TCase *create = tcase_create("lsm::LSMTree::constructor Testing");
    tcase_add_test(create, t_create);

    suite_add_tcase(unit, create);

    TCase *append = tcase_create("lsm::LSMTree::append Testing");
    tcase_add_test(append, t_append);
    tcase_add_test(append, t_append_with_mem_merges);
    tcase_add_test(append, t_append_with_disk_merges);

    tcase_set_timeout(append, 500);
    suite_add_tcase(unit, append);

    TCase *sampling = tcase_create("lsm::LSMTree::range_sample Testing");
    tcase_add_test(sampling, t_range_sample_memtable);
    tcase_add_test(sampling, t_range_sample_memlevels);
    tcase_add_test(sampling, t_range_sample_disklevels);

    tcase_set_timeout(sampling, 500);
    suite_add_tcase(unit, sampling);

    TCase *flat = tcase_create("lsm::LSMTree::get_flat_isam_tree Testing");
    tcase_add_test(flat, t_flat_isam);
    tcase_add_test(flat, t_sorted_array);

    suite_add_tcase(unit, flat);


    TCase *ts = tcase_create("lsm::LSMTree::tombstone_compaction Testing");
    tcase_add_test(ts, t_tombstone_merging_01);

    tcase_set_timeout(ts, 500);
    suite_add_tcase(unit, ts);


    TCase *mt = tcase_create("lsm::LSMTree Multithreaded Testing");
    tcase_add_test(mt, t_multithread_insert);
    tcase_add_test(mt, t_multithread_sample);
    tcase_add_test(mt, t_multithread_mix);
    
    tcase_set_timeout(mt, 500);
    suite_add_tcase(unit, mt);

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
