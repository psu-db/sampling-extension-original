/*
 *
 */

#include <check.h>
#include "ds/bitmap.hpp"
#include "testing.hpp"

using namespace lsm;
using std::byte;

START_TEST(t_create_small)
{
    auto state = testing::make_state1();
    auto pfile = state->file_manager->create_indexed_pfile();

    auto meta_pid = pfile->allocate_page();
    size_t size = 100;

    auto bm = ds::BitMap::create_persistent(size, meta_pid, state.get());

    ck_assert_ptr_nonnull(bm.get());
    ck_assert_int_eq(pfile->get_page_count(), 2);

    ck_assert_int_eq(bm->set(0), 1);
    ck_assert_int_eq(bm->set(size -1 ), 1);
    ck_assert_int_eq(bm->set(size), 0);

    state->file_manager->close_file(pfile);
}
END_TEST


START_TEST(t_create_large)
{
    auto state = testing::make_state1();
    auto pfile = state->file_manager->create_indexed_pfile();

    auto meta_pid = pfile->allocate_page();
    size_t size = 100 * parm::PAGE_SIZE;

    auto bm = ds::BitMap::create_persistent(size, meta_pid, state.get());

    ck_assert_ptr_nonnull(bm.get());
    ck_assert_int_eq(pfile->get_page_count(), 102);

    ck_assert_int_eq(bm->set(0), 1);
    ck_assert_int_eq(bm->set(size -1 ), 1);
    ck_assert_int_eq(bm->set(size), 0);

    state->file_manager->close_file(pfile);
}
END_TEST


START_TEST(t_create_volatile)
{
    size_t size = 100 * parm::PAGE_SIZE;
    auto bm = ds::BitMap::create_volatile(size);

    ck_assert_ptr_nonnull(bm.get());

    ck_assert_int_eq(bm->set(0), 1);
    ck_assert_int_eq(bm->set(size -1 ), 1);
    ck_assert_int_eq(bm->set(size), 0);
}
END_TEST


START_TEST(t_open)
{
    auto state = testing::make_state1();
    auto pfile = state->file_manager->create_indexed_pfile();

    auto meta_pid = pfile->allocate_page();
    size_t size = 100 * parm::PAGE_SIZE;

    auto bm = ds::BitMap::create_persistent(size, meta_pid, state.get());

    ck_assert_ptr_nonnull(bm.get());
    ck_assert_int_eq(pfile->get_page_count(), 102);

    for (size_t i=0; i<parm::PAGE_SIZE; i++) {
        ck_assert_int_eq(bm->set(i), 1);
    }

    ck_assert_int_eq(bm->set(size - 1), 1);
    ck_assert_int_eq(bm->set(size), 0);

    bm->flush();

    bm.reset();

    bm = ds::BitMap::open(meta_pid, state.get());

    ck_assert_ptr_nonnull(bm.get());

    for (size_t i=0; i<parm::PAGE_SIZE; i++) {
        ck_assert_int_eq(bm->is_set(i), 1);
    }

    for (size_t i=parm::PAGE_SIZE; i<size - 1; i++) {
        ck_assert_int_eq(bm->is_set(i), 0);
    }

    ck_assert_int_eq(bm->is_set(size - 1), 1);
}
END_TEST



Suite *unit_testing()
{
    Suite *unit = suite_create("BitMap Unit Testing");
    TCase *create = tcase_create("lsm::ds::BitMap::create_persistent Testing");
    tcase_add_test(create, t_create_small);
    tcase_add_test(create, t_create_large);

    suite_add_tcase(unit, create);

    TCase *volt = tcase_create("lsm::ds::BitMap::create_volatile Testing");
    tcase_add_test(volt, t_create_volatile);

    suite_add_tcase(unit, volt);

    TCase *open = tcase_create("lsm::ds::BitMap::open Testing");
    tcase_add_test(open, t_open);

    suite_add_tcase(unit, open);

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
