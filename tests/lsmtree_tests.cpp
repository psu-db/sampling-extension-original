#include <check.h>
#include <set>
#include <random>

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


START_TEST(t_sorted_array)
{
    size_t reccnt = 100000;
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

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

    size_t len;
    record_t* flat = lsm->get_sorted_array(&len, g_rng);
    ck_assert_int_eq(len, reccnt - deletes);

    lsm::key_t prev_key = 0;
    for (size_t i=0; i<len; i++) {
        ck_assert_int_ge(flat[i].key, prev_key);
        prev_key = flat[i].key;
    }

    free(flat);
    delete lsm;
}
END_TEST


START_TEST(t_flat_isam)
{
    size_t reccnt = 100000;
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, 1, g_rng);

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
        ck_assert_int_eq(lsm->append(rec.first, rec.second, false, g_rng), 1);

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
    }

    auto flat = lsm->get_flat_isam_tree(g_rng);

    ck_assert_int_eq(flat->get_record_count(), reccnt - deletes);
    ck_assert_int_eq(flat->get_tombstone_count(), 0);

    auto iter = flat->start_scan();

    size_t recs = 0;
    lsm::key_t prev_key = 0;
    while (iter->next()) {
        auto pg = iter->get_item();
        for (size_t i=0; i<PAGE_SIZE/sizeof(record_t); i++) {
            if (recs >= flat->get_record_count()) break;
            recs++;

            auto rec = (record_t*)(pg + (i * sizeof(record_t)));
            //auto k = *(lsm::key_t*) get_key(rec);
            //auto r = *(lsm::value_t*) get_val(rec);
            ck_assert_int_ge(rec->key, prev_key);
            prev_key = rec->key;
        }
    }

    auto pfile = flat->get_pfile();

    delete iter;
    delete flat;
    delete lsm;
    delete pfile;
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

START_TEST(t_persist_mem) 
{
    size_t reccnt = 100000;
    auto lsm = create_test_tree(reccnt, 100);

    lsm->persist_tree(g_rng);

    std::string meta_fname = dir + "/meta/lsmtree.dat";
    auto lsm2 = new LSMTree(dir, 1000, 3000, 2, 100, 1, meta_fname, g_rng);

    ck_assert_int_eq(lsm->get_record_cnt(), lsm2->get_record_cnt());
    ck_assert_int_eq(lsm->get_tombstone_cnt(), lsm2->get_tombstone_cnt());

    // NOTE: The aux memory  usage is *not* the same between the two, because of tombstone
    // cancellation. The original tree uses more memory, as the bloom filters are allocated
    // based on the max number of tombstones possible on a level during a merge, before any
    // cancellations occur. The second tree is built using the *actual*, smaller, tombstone
    // number as it happens post merge. So this difference is not an error.
    //ck_assert_int_eq(lsm->get_aux_memory_utilization(), lsm2->get_aux_memory_utilization());
    
    ck_assert_int_eq(lsm->get_memory_utilization(), lsm2->get_memory_utilization());

    size_t len1;
    auto sorted1 = lsm->get_sorted_array(&len1, g_rng);

    size_t len2;
    auto sorted2 = lsm2->get_sorted_array(&len2, g_rng);

    ck_assert_int_eq(len1, len2);

    for (size_t i=0; i<len1; i++) {
        record_t *rec1 = sorted1 + i;
        record_t *rec2 = sorted2 + i;

        ck_assert_mem_eq((const char*)rec1, (const char*)rec2, sizeof(record_t));
    }

    delete lsm;
    delete lsm2;
    free(sorted1);
    free(sorted2);
}
END_TEST


START_TEST(t_persist_disk)
{
    size_t reccnt = 100000;
    auto lsm = create_test_tree(reccnt, 1);

    lsm->persist_tree(g_rng);

    std::string meta_fname = dir + "/meta/lsmtree.dat";
    auto lsm2 = new LSMTree(dir, 1000, 3000, 2, 1, 1, meta_fname, g_rng);

    ck_assert_int_eq(lsm->get_record_cnt(), lsm2->get_record_cnt());
    ck_assert_int_eq(lsm->get_tombstone_cnt(), lsm2->get_tombstone_cnt());

    // NOTE: The aux memory  usage is *not* the same between the two, because of tombstone
    // cancellation. The original tree uses more memory, as the bloom filters are allocated
    // based on the max number of tombstones possible on a level during a merge, before any
    // cancellations occur. The second tree is built using the *actual*, smaller, tombstone
    // number as it happens post merge. So this difference is not an error.
    //ck_assert_int_eq(lsm->get_aux_memory_utilization(), lsm2->get_aux_memory_utilization());
    
    ck_assert_int_eq(lsm->get_memory_utilization(), lsm2->get_memory_utilization());

    size_t len1;
    auto sorted1 = lsm->get_sorted_array(&len1, g_rng);

    size_t len2;
    auto sorted2 = lsm2->get_sorted_array(&len2, g_rng);

    ck_assert_int_eq(len1, len2);

    for (size_t i=0; i<len1; i++) {
        record_t *rec1 = sorted1 + i;
        record_t *rec2 = sorted2 + i;

        ck_assert_mem_eq((const char*)rec1, (const char*)rec2, sizeof(record_t));
    }

    delete lsm;
    delete lsm2;
    free(sorted1);
    free(sorted2);
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
    suite_add_tcase(unit, append);

    TCase *sampling = tcase_create("lsm::LSMTree::range_sample Testing");

    tcase_add_test(sampling, t_range_sample_memtable);
    tcase_add_test(sampling, t_range_sample_memlevels);
    tcase_add_test(sampling, t_range_sample_disklevels);
    suite_add_tcase(unit, sampling);

    TCase *flat = tcase_create("lsm::LSMTree::get_flat_isam_tree Testing");
    tcase_add_test(flat, t_flat_isam);
    tcase_add_test(flat, t_sorted_array);
    suite_add_tcase(unit, flat);

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
