/*
 *
 */

#include <check.h>
#include <set>
#include "ds/map_memtable.hpp"
#include "testing.hpp"

using namespace lsm;
using std::byte;

START_TEST(t_create)
{
    auto state = testing::make_state1();
    auto table = ds::MapMemTable(500, state.get());

    ck_assert_int_eq(table.get_capacity(), 500);
    ck_assert_int_eq(table.get_record_count(), 0);
    ck_assert_int_eq(table.is_full(), 0);
}
END_TEST


START_TEST(t_insert)
{
    auto state = testing::make_state1();
    auto table = ds::MapMemTable(500, state.get());

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
    auto table = ds::MapMemTable(500, state.get());

    std::vector<int64_t> keys_v;
    keys_v.insert(keys_v.begin(), keys.begin(), keys.end());

    std::vector<int64_t> vals_v;
    vals_v.insert(vals_v.begin(), vals.begin(), vals.end());
    

    for (size_t i=0; i< capacity; i++) {
        auto key_ptr = (byte *) &keys_v[i];
        auto val_ptr = (byte *) &vals_v[i];

        ck_assert_int_eq(table.insert(key_ptr, val_ptr), 1);
    }

    ck_assert_int_eq(table.is_full(), 1);
    ck_assert_int_eq(table.get_record_count(), capacity);

    int64_t new_key = -10;
    int64_t new_val = -11;

    ck_assert_int_eq(table.insert())


}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("MapMemTable Unit Testing");
    TCase *create = tcase_create("lsm::sampling::MapMemTable Constructor Testing");
    tcase_add_test(create, t_create);

    suite_add_tcase(unit, create);

    TCase *insert = tcase_create("lsm::sampling::MapMemTable::insert Testing");
    tcase_add_test(insert, t_insert);

    suite_add_tcase(unit, insert);

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
