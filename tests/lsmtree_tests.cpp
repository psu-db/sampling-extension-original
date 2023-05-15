#include <set>
#include <random>

#include "lsm/LsmTree.h"

#include <check.h>

using namespace lsm;

gsl_rng *g_rng = gsl_rng_alloc(gsl_rng_mt19937);

std::string dir = "./tests/data/lsmtree";

START_TEST(t_create)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

    ck_assert_ptr_nonnull(lsm);
    ck_assert_int_eq(lsm->get_record_cnt(), 0);
    ck_assert_int_eq(lsm->get_height(), 0);

    delete lsm;
}
END_TEST


START_TEST(t_append)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

    lsm::key_t key = 0;
    lsm::value_t val = 0;
    for (size_t i=0; i<100; i++) {
        ck_assert_int_eq(lsm->append(key, val, 0, g_rng), 1);
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
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

    lsm::key_t key = 0;
    lsm::value_t val = 0;
    for (size_t i=0; i<300; i++) {
        ck_assert_int_eq(lsm->append(key, val, 0, g_rng), 1);
        key++;
        val++;
    }

    ck_assert_int_eq(lsm->get_record_cnt(), 300);
    ck_assert_int_eq(lsm->get_height(), 1);

    delete lsm;
}
END_TEST


START_TEST(t_append_with_disk_merges)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

    lsm::key_t key = 0;
    lsm::value_t val = 0;
    for (size_t i=0; i<1000; i++) {
        ck_assert_int_eq(lsm->append(key, val, 0, g_rng), 1);
        key++;
        val++;
    }

    ck_assert_int_eq(lsm->get_record_cnt(), 1000);
    ck_assert_int_eq(lsm->get_height(), 3);

    delete lsm;
}
END_TEST


START_TEST(t_range_sample_memtable)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

    lsm::key_t key = 0;
    lsm::value_t val = 0;
    for (size_t i=0; i<100; i++) {
        ck_assert_int_eq(lsm->append(key, val, 0, g_rng), 1);
        key++;
        val++;
    }

    lsm::key_t lower_bound = 20;
    lsm::key_t upper_bound = 50;

    char *buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char *util_buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    record_t sample_set[100];

    lsm->range_sample(sample_set, lower_bound, upper_bound, 100, buf, util_buf, g_rng);

    for(size_t i=0; i<100; i++) {
        ck_assert_int_le(sample_set[i].key, upper_bound);
        ck_assert_int_ge(sample_set[i].key, lower_bound);
    }

    free(buf);
    free(util_buf);

    delete lsm;
}
END_TEST


START_TEST(t_range_sample_memlevels)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

    lsm::key_t key = 0;
    lsm::value_t val = 0;
    for (size_t i=0; i<300; i++) {
        ck_assert_int_eq(lsm->append(key, val, 0, g_rng), 1);
        key++;
        val++;
    }

    lsm::key_t lower_bound = 100;
    lsm::key_t upper_bound = 250;

    char *buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char *util_buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);

    record_t sample_set[100];
    lsm->range_sample(sample_set, lower_bound, upper_bound, 100, buf, util_buf, g_rng);

    for(size_t i=0; i<100; i++) {
        ck_assert_int_le(sample_set[i].key, upper_bound);
        ck_assert_int_ge(sample_set[i].key, lower_bound);
    }

    free(buf);
    free(util_buf);

    delete lsm;
}
END_TEST


START_TEST(t_range_sample_disklevels)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

    lsm::key_t key = 0;
    lsm::value_t val = 0;
    for (size_t i=0; i<1000; i++) {
        ck_assert_int_eq(lsm->append(key, val, 0, g_rng), 1);
        key++;
        val++;
    }

    lsm::key_t lower_bound = 0;
    lsm::key_t upper_bound = 1000;

    char *buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char *util_buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);

    record_t sample_set[100];
    lsm->range_sample(sample_set, lower_bound, upper_bound, 100, buf, util_buf, g_rng);

    for(size_t i=0; i<100; i++) {
        ck_assert_int_le(sample_set[i].key, upper_bound);
        ck_assert_int_ge(sample_set[i].key, lower_bound);
    }

    free(buf);
    free(util_buf);

    delete lsm;
}
END_TEST


START_TEST(t_tombstone_merging_01)
{
    size_t reccnt = 100000;
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, .01, g_rng);

    std::set<std::pair<lsm::key_t, lsm::value_t>> records; 
    std::set<std::pair<lsm::key_t, lsm::value_t>> to_delete;
    std::set<std::pair<lsm::key_t, lsm::value_t>> deleted;

    while (records.size() < reccnt) {
        lsm::key_t key = rand();
        lsm::value_t val = rand();

        if (records.find({key, val}) != records.end()) continue;

        records.insert({key, val});
    }

    size_t deletes = 0;
    size_t cnt=0;
    for (auto rec : records) {
        ck_assert_int_eq(lsm->append(rec.first, rec.second, 0, g_rng), 1);
        

        if (gsl_rng_uniform(g_rng) < 0.05 && !to_delete.empty()) {
           std::vector<std::pair<lsm::key_t, lsm::value_t>> del_vec;
           std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

           for (size_t i=0; i<del_vec.size(); i++) {
               ck_assert_int_eq(lsm->append(del_vec[i].first, del_vec[i].second, true, g_rng), 1);
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

    std::set<std::pair<lsm::key_t, lsm::value_t>> records; 
    std::set<std::pair<lsm::key_t, lsm::value_t>> to_delete;
    std::set<std::pair<lsm::key_t, lsm::value_t>> deleted;

    while (records.size() < reccnt) {
        lsm::key_t key = rand();
        lsm::value_t val = rand();

        if (records.find({key, val}) != records.end()) continue;

        records.insert({key, val});
    }

    size_t deletes = 0;
    for (auto rec : records) {
        ck_assert_int_eq(lsm->append(rec.first, rec.second, 0, g_rng), 1);

         if (gsl_rng_uniform(g_rng) < 0.05 && !to_delete.empty()) {
            std::vector<std::pair<lsm::key_t, lsm::value_t>> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                const char *d_key_ptr = (char *) &del_vec[i].first;
                const char *d_val_ptr = (char *) &del_vec[i].second;
                lsm->append(del_vec[i].first, del_vec[i].second, true, g_rng);
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
    suite_add_tcase(unit, append);

    TCase *sampling = tcase_create("lsm::LSMTree::range_sample Testing");

    tcase_add_test(sampling, t_range_sample_memtable);
    tcase_add_test(sampling, t_range_sample_memlevels);
    tcase_add_test(sampling, t_range_sample_disklevels);
    suite_add_tcase(unit, sampling);

    TCase *ts = tcase_create("lsm::LSMTree::tombstone_compaction Testing");
    tcase_add_test(ts, t_tombstone_merging_01);
    tcase_set_timeout(ts, 500);
    suite_add_tcase(unit, ts);

    // TCase *persist = tcase_create("lsm::LSMTree::persistence Testing");
    // tcase_add_test(persist, t_persist_mem);
    // tcase_add_test(persist, t_persist_disk);
    // tcase_set_timeout(ts, 500);
    // suite_add_tcase(unit, persist);

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
    srand(0);
    int unit_failed = run_unit_tests();

    return (unit_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
