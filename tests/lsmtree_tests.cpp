#include <check.h>
#include <set>
#include <random>
#include <algorithm>

#include "lsm/LsmTree.h"

using namespace lsm;

gsl_rng *g_rng = gsl_rng_alloc(gsl_rng_mt19937);

std::string dir = "./tests/data/lsmtree";

bool roughly_equal(int n1, int n2, size_t mag, double epsilon) {
    return ((double) std::abs(n1 - n2) / (double) mag) < epsilon;
}

START_TEST(t_create)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 100, 1, g_rng);


    ck_assert_ptr_nonnull(lsm);
    ck_assert_int_eq(lsm->get_record_cnt(), 0);
    ck_assert_int_eq(lsm->get_height(), 0);

    delete lsm;
}
END_TEST


START_TEST(t_append)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 100, 1, g_rng);

    key_type key = 0;
    value_type val = 0;
    for (size_t i=0; i<100; i++) {
        const char *key_ptr = (char *) &key;
        const char *val_ptr = (char *) &val;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 1, false, g_rng), 1);
        key++;
        val++;
    }

    ck_assert_int_eq(lsm->get_height(), 0);
    ck_assert_int_eq(lsm->get_record_cnt(), 100);

    delete lsm;
}
END_TEST


START_TEST(t_append_with_mem_merges)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 100, 1, g_rng);

    key_type key = 0;
    value_type val = 0;
    for (size_t i=0; i<300; i++) {
        const char *key_ptr = (char *) &key;
        const char *val_ptr = (char *) &val;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 1, false, g_rng), 1);
        key++;
        val++;
    }

    ck_assert_int_eq(lsm->get_record_cnt(), 300);
    ck_assert_int_eq(lsm->get_height(), 1);

    delete lsm;
}
END_TEST


START_TEST(t_range_sample_memtable)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 100, 1, g_rng);

    key_type key = 0;
    value_type val = 0;
    for (size_t i=0; i<100; i++) {
        const char *key_ptr = (char *) &key;
        const char *val_ptr = (char *) &val;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 1, false, g_rng), 1);
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

    delete lsm;
}
END_TEST


START_TEST(t_range_sample_memlevels)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 100, 1, g_rng);

    key_type key = 0;
    value_type val = 0;
    for (size_t i=0; i<300; i++) {
        const char *key_ptr = (char *) &key;
        const char *val_ptr = (char *) &val;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 1, false, g_rng), 1);
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

    delete lsm;
}
END_TEST

START_TEST(t_range_sample_weighted)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 100, 1, g_rng);
    size_t n = 10000;

    std::vector<key_type> keys;

    key_type key = 1;
    for (size_t i=0; i< n / 2; i++) {
        keys.push_back(key);
    }

    // put in a quarter of the count with weight two.
    key = 2;
    for (size_t i=0; i< n / 4; i++) {
        keys.push_back(key);
    }

    // the remaining quarter with weight four.
    key = 3;
    for (size_t i=0; i< n / 4; i++) {
        keys.push_back(key);
    }

    std::random_device rd;
    std::mt19937 gen{rd()};
    std::shuffle(keys.begin(), keys.end(), gen);

    for (size_t i=0; i<keys.size(); i++) {
        double weight;
        if (keys[i] == 1)  {
            weight = 2.0;
        } else if (keys[i] == 2) {
            weight = 4.0;
        } else {
            weight = 8.0;
        }

        lsm->append((char*) &keys[i], (char*) &i, weight, false, g_rng);
    }
    size_t k = 1000;
    key_type lower_key = 0;
    key_type upper_key = 5;

    char *buffer = new char[k*lsm::record_size]();
    char *buffer1 = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char *buffer2 = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);

    size_t cnt[3] = {0};
    for (size_t i=0; i<1000; i++) {
        lsm->range_sample(buffer, (char*) &lower_key, (char*) &upper_key, k, buffer1, buffer2, g_rng);

        for (size_t j=0; j<k; j++) {
            cnt[(*(size_t *) get_key(buffer + j*lsm::record_size) ) - 1]++;
        }
    }

    ck_assert(roughly_equal(cnt[0] / 1000, (double) k/4.0, k, .05));
    ck_assert(roughly_equal(cnt[1] / 1000, (double) k/4.0, k, .05));
    ck_assert(roughly_equal(cnt[2] / 1000, (double) k/2.0, k, .05));

    delete lsm;
    delete[] buffer;
    free(buffer1);
    free(buffer2);
}
END_TEST


START_TEST(t_tombstone_merging_01)
{
    size_t reccnt = 100000;
    auto lsm = new LSMTree(dir, 100, 100, 2, 100, .01, g_rng);

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
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 1, false, g_rng), 1);

         if (gsl_rng_uniform(g_rng) < 0.05 && !to_delete.empty()) {
            std::vector<std::pair<key_type, value_type>> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                const char *d_key_ptr = (char *) &del_vec[i].first;
                const char *d_val_ptr = (char *) &del_vec[i].second;
                lsm->append(d_key_ptr, d_val_ptr, 1, true, g_rng);
                deletes++;
                to_delete.erase(del_vec[i]);
                deleted.insert(del_vec[i]);
            }
        }

        if (gsl_rng_uniform(g_rng) < 0.25 && deleted.find(rec) == deleted.end()) {
            to_delete.insert(rec);
        }

        ck_assert(lsm->validate_tombstone_proportion());
    }

    ck_assert(lsm->validate_tombstone_proportion());

    delete lsm;
}
END_TEST

lsm::LSMTree *create_test_tree(size_t reccnt, size_t memlevel_cnt) {
    auto lsm = new LSMTree(dir, 1000, 3000, 2, memlevel_cnt, 1, g_rng);

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
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 1, 0, g_rng), 1);

         if (gsl_rng_uniform(g_rng) < 0.05 && !to_delete.empty()) {
            std::vector<std::pair<key_type, value_type>> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                const char *d_key_ptr = (char *) &del_vec[i].first;
                const char *d_val_ptr = (char *) &del_vec[i].second;
                lsm->append(d_key_ptr, d_val_ptr, 1, true, g_rng);
                deletes++;
                to_delete.erase(del_vec[i]);
                deleted.insert(del_vec[i]);
            }
        }

        if (gsl_rng_uniform(g_rng) < 0.25 && deleted.find(rec) == deleted.end()) {
            to_delete.insert(rec);
        }
    }

    return lsm;
}

START_TEST(t_persist_mem) 
{
    size_t reccnt = 100000;
    auto lsm = create_test_tree(reccnt, 100);

    lsm->persist_tree(g_rng);

    std::string meta_fname = dir + "/meta/lsmtree.dat";
    auto lsm2 = new LSMTree(dir, 1000, 3000, 2, 100, 1, meta_fname, g_rng);

    ck_assert_int_eq(lsm->get_record_cnt(), lsm2->get_record_cnt());
    ck_assert_int_eq(lsm->get_tombstone_cnt(), lsm2->get_tombstone_cnt());
    //ck_assert_int_eq(lsm->get_aux_memory_utilization(), lsm2->get_aux_memory_utilization());
    ck_assert_int_eq(lsm->get_memory_utilization(), lsm2->get_memory_utilization());

    /*
    size_t len1;
    auto sorted1 = lsm->get_sorted_array(&len1, g_rng);

    size_t len2;
    auto sorted2 = lsm->get_sorted_array(&len2, g_rng);

    ck_assert_int_eq(len1, len2);

    for (size_t i=0; i<len1; i++) {
        char *rec1 = sorted1 + i * lsm::record_size;
        char *rec2 = sorted1 + i * lsm::record_size;

        ck_assert_mem_eq(rec1, rec2, lsm::record_size);
    }
    */

    delete lsm;
    delete lsm2;
    //free(sorted1);
    //free(sorted2);
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
    suite_add_tcase(unit, append);

    TCase *sampling = tcase_create("lsm::LSMTree::range_sample Testing");

    tcase_add_test(sampling, t_range_sample_memtable);
    tcase_add_test(sampling, t_range_sample_memlevels);
    tcase_add_test(sampling, t_range_sample_weighted);
    suite_add_tcase(unit, sampling);

    TCase *ts = tcase_create("lsm::LSMTree::tombstone_compaction Testing");
    tcase_add_test(ts, t_tombstone_merging_01);
    tcase_set_timeout(ts, 500);
    suite_add_tcase(unit, ts);

    TCase *persist = tcase_create("lsm::LSMTree::persistence Testing");
    tcase_add_test(persist, t_persist_mem);
    tcase_set_timeout(ts, 500);
    suite_add_tcase(unit, persist);

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
