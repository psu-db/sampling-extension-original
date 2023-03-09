
#include "lsm/WIRSRun.h"
#include "lsm/LsmTree.h"
#include "lsm/MemoryLevel.h"
#include "util/bf_config.h"

#include <check.h>
using namespace lsm;

bool roughly_equal(int n1, int n2, size_t mag, double epsilon) {
    return ((double) std::abs(n1 - n2) / (double) mag) < epsilon;
}


gsl_rng *g_rng = gsl_rng_alloc(gsl_rng_mt19937);

static MemTable *create_test_memtable(size_t cnt)
{
    auto mtable = new MemTable(cnt, cnt, g_rng);

    for (size_t i = 0; i < cnt; i++) {
        key_type key = rand();
        value_type val = rand();

        mtable->append((char*) &key, (char*) &val, 1);
    }

    return mtable;
}

static MemTable *create_weighted_memtable(size_t cnt)
{
    auto mtable = new MemTable(cnt, cnt, g_rng);
    
    // Put in half of the count with weight one.
    key_type key = 1;
    for (size_t i=0; i< cnt / 2; i++) {
        mtable->append((char *) &key, (char *) &i, 2);
    }

    // put in a quarter of the count with weight two.
    key = 2;
    for (size_t i=0; i< cnt / 4; i++) {
        mtable->append((char *) &key, (char *) &i, 4);
    }

    // the remaining quarter with weight four.
    key = 3;
    for (size_t i=0; i< cnt / 4; i++) {
        mtable->append((char *) &key, (char *) &i, 8);
    }

    return mtable;
}


static MemTable *create_double_seq_memtable(size_t cnt, bool ts=false) 
{
    auto mtable = new MemTable(cnt, cnt, g_rng);

    for (size_t i = 0; i < cnt / 2; i++) {
        key_type key = i;
        value_type val = i;

        mtable->append((char*) &key, (char *) &val, 1, ts);
    }

    for (size_t i = 0; i < cnt / 2; i++) {
        key_type key = i;
        value_type val = i + 1;

        mtable->append((char*) &key, (char *) &val, 1, ts);
    }

    return mtable;
}

START_TEST(t_memtable_init)
{
    auto mem_table = new MemTable(1024, 1024, g_rng);
    for (uint64_t i = 512; i > 0; i--) {
        uint32_t v = i;
        mem_table->append((const char*)&i, (const char*)&v, 1);
    }
    
    for (uint64_t i = 1; i <= 256; ++i) {
        uint32_t v = i;
        mem_table->append((const char*)&i, (const char*)&v, 1, true);
    }

    for (uint64_t i = 257; i <= 512; ++i) {
        uint32_t v = i + 1;
        mem_table->append((const char*)&i, (const char*)&v, 1);
    }

    BloomFilter* bf = new BloomFilter(BF_FPR, mem_table->get_tombstone_count(), BF_HASH_FUNCS, g_rng);
    WIRSRun* run = new WIRSRun(mem_table, bf, false);
    ck_assert_uint_eq(run->get_record_count(), 512);

    delete bf;
    delete mem_table;
    delete run;
}

START_TEST(t_inmemrun_init)
{
    size_t n = 512;
    auto memtable1 = create_test_memtable(n);
    auto memtable2 = create_test_memtable(n);
    auto memtable3 = create_test_memtable(n);

    BloomFilter* bf1 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    BloomFilter* bf2 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    BloomFilter* bf3 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    auto run1 = new WIRSRun(memtable1, bf1, false);
    auto run2 = new WIRSRun(memtable2, bf2, false);
    auto run3 = new WIRSRun(memtable3, bf3, false);

    BloomFilter* bf4 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    WIRSRun* runs[3] = {run1, run2, run3};
    auto run4 = new WIRSRun(runs, 3, bf4, false);

    ck_assert_int_eq(run4->get_record_count(), n * 3);
    ck_assert_int_eq(run4->get_tombstone_count(), 0);

    size_t total_cnt = 0;
    size_t run1_idx = 0;
    size_t run2_idx = 0;
    size_t run3_idx = 0;

    for (size_t i = 0; i < run4->get_record_count(); ++i) {
        auto rec1 = run1->get_record_at(run1_idx);
        auto rec2 = run2->get_record_at(run2_idx);
        auto rec3 = run3->get_record_at(run3_idx);

        auto cur_rec = run4->get_record_at(i);

        if (run1_idx < n && record_cmp(cur_rec, rec1) == 0) {
            ++run1_idx;
        } else if (run2_idx < n && record_cmp(cur_rec, rec2) == 0) {
            ++run2_idx;
        } else if (run3_idx < n && record_cmp(cur_rec, rec3) == 0) {
            ++run3_idx;
        } else {
           assert(false);
        }
    }

    delete memtable1;
    delete memtable2;
    delete memtable3;

    delete bf1;
    delete run1;
    delete bf2;
    delete run2;
    delete bf3;
    delete run3;
    delete bf4;
    delete run4;
}

START_TEST(t_full_cancelation)
{
    size_t n = 100;
    auto mtable = create_double_seq_memtable(n, false);
    auto mtable_ts = create_double_seq_memtable(n, true);
    BloomFilter* bf1 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    BloomFilter* bf2 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    BloomFilter* bf3 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);

    WIRSRun* run = new WIRSRun(mtable, bf1, false);
    WIRSRun* run_ts = new WIRSRun(mtable_ts, bf2, false);

    ck_assert_int_eq(run->get_record_count(), n);
    ck_assert_int_eq(run->get_tombstone_count(), 0);
    ck_assert_int_eq(run_ts->get_record_count(), n);
    ck_assert_int_eq(run_ts->get_tombstone_count(), n);

    WIRSRun* runs[] = {run, run_ts};

    WIRSRun* merged = new WIRSRun(runs, 2, bf3, false);

    ck_assert_int_eq(merged->get_tombstone_count(), 0);
    ck_assert_int_eq(merged->get_record_count(), 0);

    delete mtable;
    delete mtable_ts;
    delete bf1;
    delete bf2;
    delete bf3;
    delete run;
    delete run_ts;
    delete merged;
}
END_TEST


START_TEST(t_weighted_sampling)
{
    size_t n=1000;
    auto mtable = create_weighted_memtable(n);

    BloomFilter* bf = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    WIRSRun* run = new WIRSRun(mtable, bf, false);

    key_type lower_key = 0;
    key_type upper_key = 5;

    size_t k = 1000;

    char * buffer = new char[k*lsm::record_size]();
    size_t cnt[3] = {0};
    for (size_t i=0; i<1000; i++) {
        run->get_samples(buffer, k, nullptr, g_rng);

        for (size_t j=0; j<k; j++) {
            cnt[(*(size_t *) get_key(buffer + j*lsm::record_size) ) - 1]++;
        }
    }

    ck_assert(roughly_equal(cnt[0] / 1000, (double) k/4.0, k, .05));
    ck_assert(roughly_equal(cnt[1] / 1000, (double) k/4.0, k, .05));
    ck_assert(roughly_equal(cnt[2] / 1000, (double) k/2.0, k, .05));

    delete[] buffer;
    delete run;
    delete bf;
    delete mtable;
}
END_TEST


START_TEST(t_tombstone_check)
{
    size_t cnt = 1024;
    size_t ts_cnt = 256;
    auto mtable = new MemTable(cnt + ts_cnt, ts_cnt, g_rng);

    std::vector<std::pair<lsm::key_type, lsm::value_type>> tombstones;

    key_type key = 1000;
    value_type val = 101;
    for (size_t i = 0; i < cnt; i++) {
        mtable->append((char*) &key, (char*) &val, 1);
        key++;
        val++;
    }

    // ensure that the key range doesn't overlap, so nothing
    // gets cancelled.
    for (size_t i=0; i<ts_cnt; i++) {
        tombstones.push_back({i, i});
    }

    for (size_t i=0; i<ts_cnt; i++) {
        mtable->append((char*) &tombstones[i].first, (char*) &tombstones[i].second, 1.0, true);
    }

    BloomFilter* bf1 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    auto run = new WIRSRun(mtable, bf1, false);

    for (size_t i=0; i<tombstones.size(); i++) {
        ck_assert(run->check_tombstone((char*) &tombstones[i].first, (char*) &tombstones[i].second));
        ck_assert_int_eq(run->get_rejection_count(), i+1);
    }

    delete run;
    delete mtable;
    delete bf1;
}
END_TEST

Suite *unit_testing()
{
    Suite *unit = suite_create("WIRSRun Unit Testing");

    TCase *create = tcase_create("lsm::WIRSRun constructor Testing");
    tcase_add_test(create, t_memtable_init);
    tcase_add_test(create, t_inmemrun_init);
    tcase_set_timeout(create, 100);
    suite_add_tcase(unit, create);


    TCase *tombstone = tcase_create("lsm::WIRSRun::tombstone cancellation Testing");
    tcase_add_test(tombstone, t_full_cancelation);
    suite_add_tcase(unit, tombstone);


    TCase *sampling = tcase_create("lsm::WIRSRun::sampling Testing");
    tcase_add_test(sampling, t_weighted_sampling);

    suite_add_tcase(unit, sampling);

    TCase *check_ts = tcase_create("lsm::InMemRun::check_tombstone Testing");
    tcase_add_test(check_ts, t_tombstone_check);
    suite_add_tcase(unit, check_ts);

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
