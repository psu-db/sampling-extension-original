/*
 *
 */

#include <check.h>
#include "io/record.hpp"
#include "io/fixedlendatapage.hpp"

#include "testing.hpp"

using namespace lsm;
using std::byte;


START_TEST(t_initialize)
{
    auto buf1 = testing::empty_aligned_buffer();
    auto schema = testing::test_schema1(sizeof(int64_t));

    int64_t key1 = 1234;
    int64_t val1 = 5678;
    auto test_rec = schema->create_record((byte *) &key1, (byte *) &val1);
    io::FixedlenDataPage::initialize(buf1.get(), schema->record_length(), 0);

    auto page1 = io::FixedlenDataPage(buf1.get());

    ck_assert_int_eq(page1.get_max_sid(), 0);
    ck_assert_int_eq(page1.get_min_sid(), 1);
    ck_assert_int_eq(page1.get_record_count(), 0);
}
END_TEST


START_TEST(t_insert)
{
    size_t field_len = sizeof(int64_t);
    auto buf = testing::empty_aligned_buffer();
    auto page = testing::empty_test_page(buf.get(), field_len, field_len);
    auto schema = testing::test_schema1(sizeof(int64_t));

    for (size_t i=0; i<100; i++) {
        size_t val = i+3;
        auto recbuf = schema->create_record_unique((byte*) &i, (byte*) &val);
        auto rec = io::Record(recbuf.get(), schema->record_length());
        ck_assert_int_eq(page.insert_record(rec), i+1);
        ck_assert_int_eq(page.get_min_sid(), 1);
        ck_assert_int_eq(page.get_max_sid(), i+1);
    }

    ck_assert_int_eq(page.get_record_count(), 100);

    for (size_t i=0; i<100; i++) {
        PageOffset record_offset = i*schema->record_length();
        auto rec = io::Record(page.get_page_data() + record_offset, schema->record_length());
        ck_assert_int_eq(schema->get_key(rec.get_data()).Int64(), i);
        ck_assert_int_eq(schema->get_val(rec.get_data()).Int64(), i + 3);
    }

}
END_TEST


START_TEST(t_get_record)
{
    auto buf = testing::empty_aligned_buffer();
    auto page = testing::populated_test_page(buf.get());

    auto schema = testing::test_schema1(sizeof(int16_t));

    for (size_t i=0; i<100; i++) {
        PageOffset record_offset = i*schema->record_length();
        auto rec = io::Record(page.get_page_data() + record_offset, schema->record_length());
        ck_assert_int_eq(schema->get_key(rec.get_data()).Int64(), i);
        ck_assert_int_eq(schema->get_val(rec.get_data()).Int16(), i + 3);
    }

    for (size_t i=1; i<=100; i++) {
        auto rec = page.get_record(i);
        ck_assert(rec.is_valid());
        ck_assert_int_eq(schema->get_key(rec.get_data()).Int64(), i - 1);
        ck_assert_int_eq(schema->get_val(rec.get_data()).Int16(), i + 2);
        ck_assert_int_eq(page.is_occupied(i), 1);
    }

    for (size_t i=101; i<= page.get_record_capacity(); i++) {
        ck_assert_int_eq(page.is_occupied(i), 0);
    }

    auto rec = page.get_record(200);
    ck_assert_int_eq(rec.is_valid(), 0);
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("FixedLenDataPage Unit Testing");
    TCase *initialize = tcase_create("lsm::io::FixedLenDataPage::initialize Testing");
    tcase_add_test(initialize, t_initialize);

    suite_add_tcase(unit, initialize);


    TCase *insert = tcase_create("lsm::io::FixedLenDataPage::insert_record Testing");
    tcase_add_test(insert, t_insert);

    suite_add_tcase(unit, insert);


    TCase *get = tcase_create("lsm::io::FixedLenDataPage::get_record Testing");
    tcase_add_test(get, t_get_record);

    suite_add_tcase(unit, get);

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
