#include <check.h>

#include "lsm/InMemRun.h"
#include "lsm/MemoryLevel.h"
#include "util/bf_config.h"

using namespace lsm;

gsl_rng *g_rng = gsl_rng_alloc(gsl_rng_mt19937);

static MemTable *create_test_memtable(size_t cnt)
{
    auto mtable = new MemTable(cnt, true, 0, g_rng);

    for (size_t i = 0; i < cnt; i++) {
        key_type key = rand();
        value_type val = rand();

        mtable->append((char*) &key, (char*) &val);
    }

    return mtable;
}


static MemTable *create_double_seq_memtable(size_t cnt) 
{
    auto mtable = new MemTable(cnt, true, 0, g_rng);

    for (size_t i = 0; i < cnt / 2; i++) {
        key_type key = i;
        value_type val = i;

        mtable->append((char*) &key, (char *) &val);
    }

    for (size_t i = 0; i < cnt / 2; i++) {
        key_type key = i;
        value_type val = i + 1;

        mtable->append((char*) &key, (char *) &val);
    }

    return mtable;
}

START_TEST(t_memtable_init)
{
    auto mem_table = new MemTable(1024, true, 50, g_rng);
    for (uint64_t i = 512; i > 0; i--) {
        uint32_t v = i;
        mem_table->append((const char*)&i, (const char*)&v);
    }
    
    for (uint64_t i = 1; i <= 256; ++i) {
        uint32_t v = i;
        mem_table->append((const char*)&i, (const char*)&v, true);
    }

    for (uint64_t i = 257; i <= 512; ++i) {
        uint32_t v = i + 1;
        mem_table->append((const char*)&i, (const char*)&v);
    }

    BloomFilter* bf = new BloomFilter(BF_FPR, mem_table->get_tombstone_count(), BF_HASH_FUNCS, g_rng);
    InMemRun* run = new InMemRun(mem_table, bf);
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
    auto run1 = new InMemRun(memtable1, bf1);
    auto run2 = new InMemRun(memtable2, bf2);
    auto run3 = new InMemRun(memtable3, bf3);

    BloomFilter* bf4 = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    InMemRun* runs[3] = {run1, run2, run3};
    auto run4 = new InMemRun(runs, 3, bf4);

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

START_TEST(t_get_lower_bound_index)
{
    size_t n = 10000;
    auto memtable = create_double_seq_memtable(n);

    ck_assert_ptr_nonnull(memtable);
    BloomFilter* bf = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    InMemRun* run = new InMemRun(memtable, bf);

    ck_assert_int_eq(run->get_record_count(), n);
    ck_assert_int_eq(run->get_tombstone_count(), 0);

    auto tbl_records = memtable->sorted_output();
    for (size_t i=0; i<n; i++) {
        const char *tbl_rec = memtable->get_record_at(i);
        tbl_records + (i * record_size);
        auto pos = run->get_lower_bound(get_key(tbl_rec));
        ck_assert_int_eq(*(key_type *) get_key(run->get_record_at(pos)), *(key_type*) get_key(tbl_rec));
        ck_assert_int_le(pos, i);
    }

    delete memtable;
    delete bf;
    delete run;
}

START_TEST(t_get_upper_bound_index)
{
    size_t n = 10000;
    auto memtable = create_double_seq_memtable(n);

    ck_assert_ptr_nonnull(memtable);
    BloomFilter* bf = new BloomFilter(100, BF_HASH_FUNCS, g_rng);
    InMemRun* run = new InMemRun(memtable, bf);

    ck_assert_int_eq(run->get_record_count(), n);
    ck_assert_int_eq(run->get_tombstone_count(), 0);

    auto tbl_records = memtable->sorted_output();
    for (size_t i=0; i<n; i++) {
        const char *tbl_rec = memtable->get_record_at(i);
        tbl_records + (i * record_size);
        auto pos = run->get_upper_bound(get_key(tbl_rec));
        ck_assert(pos == run->get_record_count() ||
                  *(key_type *) get_key(run->get_record_at(pos)) > *(key_type*) get_key(tbl_rec));
        ck_assert_int_ge(pos, i);
    }

    delete memtable;
    delete bf;
    delete run;
}

Suite *unit_testing()
{
    Suite *unit = suite_create("InMemRun Unit Testing");

    TCase *create = tcase_create("lsm::InMemRun constructor Testing");
    tcase_add_test(create, t_memtable_init);
    tcase_add_test(create, t_inmemrun_init);
    tcase_set_timeout(create, 100);
    suite_add_tcase(unit, create);

    TCase *bounds = tcase_create("lsm::InMemRun::get_{lower,upper}_bound Testing");
    tcase_add_test(bounds, t_get_lower_bound_index);
    tcase_add_test(bounds, t_get_upper_bound_index);
    tcase_set_timeout(bounds, 100);   
    suite_add_tcase(unit, bounds);

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
