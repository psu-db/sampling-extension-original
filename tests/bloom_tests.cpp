/*
 *
 */

#include <check.h>
#include "ds/persistent_bloom.hpp"
#include "testing.hpp"

using namespace lsm;
using std::byte;

START_TEST(t_create)
{
    auto state = testing::make_state1();
    auto pfile = state->file_manager->create_indexed_pfile();

    auto meta_pid = pfile->allocate_page();
    size_t size = 1000;

    auto bf = ds::PersistentBloomFilter::create(size, 8, 3, meta_pid, state.get());

    ck_assert_ptr_nonnull(bf.get());
    ck_assert_int_eq(pfile->get_page_count(), 2);

    int64_t val = 6;
    for (size_t i=0; i<100; i++) {
        byte *key_ptr = (byte *) &val;
        ck_assert_int_eq(bf->insert(key_ptr), 1);
        val += 2;
    }

    val = 6;
    for (size_t i=0; i<100; i++) {
        byte *key_ptr = (byte *) &val;
        ck_assert_int_eq(bf->lookup(key_ptr), 1);
        val += 2;
    }

    state->file_manager->close_file(pfile);
}
END_TEST


START_TEST(t_open)
{
    auto state = testing::make_state1();
    auto pfile = state->file_manager->create_indexed_pfile();

    auto meta_pid = pfile->allocate_page();
    size_t size = 100 * parm::PAGE_SIZE;

    auto bf = ds::PersistentBloomFilter::create(size, 8, 3, meta_pid, state.get());

    ck_assert_ptr_nonnull(bf.get());
    ck_assert_int_eq(pfile->get_page_count(), 102);

    int64_t val = 6;
    for (size_t i=0; i<100; i++) {
        byte *key_ptr = (byte *) &val;
        ck_assert_int_eq(bf->insert(key_ptr), 1);
        val += 2;
    }

    bf->flush();
    bf.reset();

    bf = ds::PersistentBloomFilter::open(meta_pid, state.get());
    ck_assert_ptr_nonnull(bf.get());

    val = 6;
    for (size_t i=0; i<100; i++) {
        byte *key_ptr = (byte *) &val;
        ck_assert_int_eq(bf->lookup(key_ptr), 1);
        val += 2;
    }

    state->file_manager->close_file(pfile);
}
END_TEST



Suite *unit_testing()
{
    Suite *unit = suite_create("PersistentBloomFilter Unit Testing");
    TCase *create = tcase_create("lsm::ds::PersistentBloomFilter::create Testing");
    tcase_add_test(create, t_create);

    suite_add_tcase(unit, create);

    TCase *open = tcase_create("lsm::ds::PersistentBloomFilter::open Testing");
    tcase_add_test(open, t_open);

    suite_add_tcase(unit, open);

    return unit;
}


int run_unit_tests()
{
    int failed = 0;
    Suite *unit = unit_testing();
    SRunner *unit_runner = srunner_create(unit);

    srunner_run_all(unit_runner, CK_VERBOSE);
    failed = srunner_ntests_failed(unit_runner);
    srunner_free(unit_runner);

    return failed;
}


int main() 
{
    int unit_failed = run_unit_tests();

    return (unit_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
