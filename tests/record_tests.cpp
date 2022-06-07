/*
 *
 */

#include <check.h>
#include "io/record.hpp"
#include "testing.hpp"

using namespace lsm;
using std::byte;

START_TEST(t_create)
{
    int64_t key1 = 1234;
    int64_t key2 = 7890;
    int64_t key3 = 1236;

    double value1 = 3.14;
    int16_t value2 = 8;
    const char *value3 = "hello, there";

    auto schema1 = testing::test_schema1(sizeof(double));
    auto schema2 = testing::test_schema1(sizeof(int16_t));
    auto schema3 = testing::test_schema1(strlen(value3));

    byte *test = schema1->create_record_raw((byte *) &key1, (byte *) &value1); 
    byte *tombstone_test = schema2->create_record_raw((byte *) &key2, (byte *) &value2);
    byte *timestamp_test = schema3->create_record_raw((byte *) &key3, (byte *) value3);

    auto test_rec1 = io::Record(test, schema1->record_length());
    auto test_rec2 = io::Record(tombstone_test, schema2->record_length(), 0, true);
    auto test_rec3 = io::Record(timestamp_test, schema3->record_length(), 5, false);
    auto test_rec4 = io::Record();

    ck_assert(test_rec1.is_valid());
    ck_assert(test_rec2.is_valid());
    ck_assert(test_rec3.is_valid());
    ck_assert(!test_rec4.is_valid());

    ck_assert_int_eq(test_rec1.get_length(), schema1->record_length());
    ck_assert_int_eq(test_rec2.get_length(), schema2->record_length());
    ck_assert_int_eq(test_rec3.get_length(), schema3->record_length());

    ck_assert_int_eq(test_rec1.get_timestamp(), 0);
    ck_assert_int_eq(test_rec2.get_timestamp(), 0);
    ck_assert_int_eq(test_rec3.get_timestamp(), 5);

    ck_assert_int_eq(test_rec1.is_tombstone(), 0);
    ck_assert_int_eq(test_rec2.is_tombstone(), 1);
    ck_assert_int_eq(test_rec3.is_tombstone(), 0);

    ck_assert_int_eq(test_rec1.get_header()->is_tombstone, 0);
    ck_assert_int_eq(test_rec1.get_header()->time, 0);

    ck_assert_int_eq(test_rec2.get_header()->is_tombstone, 1);
    ck_assert_int_eq(test_rec2.get_header()->time, 0);

    ck_assert_int_eq(test_rec3.get_header()->is_tombstone, 0);
    ck_assert_int_eq(test_rec3.get_header()->time, 5);

    byte *data1 = test_rec1.get_data();
    byte *data2 = test_rec2.get_data();
    byte *data3 = test_rec3.get_data(); 
    byte *data4 = test_rec4.get_data();

    ck_assert_int_eq(schema1->get_key(data1).Int64(), key1);
    ck_assert_int_eq(schema2->get_key(data2).Int64(), key2);
    ck_assert_int_eq(schema3->get_key(data3).Int64(), key3);
    ck_assert_ptr_null(data4);

    ck_assert_double_eq(schema1->get_val(data1).Double(), value1);
    ck_assert_int_eq(schema2->get_val(data2).Int16(), value2);
    std::string data3_val = schema3->get_val(data3).Str();
    ck_assert_mem_eq(data3_val.c_str(), value3, strlen(value3));
}
END_TEST


START_TEST(t_deep_copy) 
{
    int64_t key = 1236;
    const char *value = "this is just a test of this.";

    auto schema1 = testing::test_schema1(strlen(value));
    byte *buf = schema1->create_record_raw((byte *) &key, (byte *) value);

    auto rec1 = io::Record(buf, schema1->record_length());
    auto copy = rec1.deep_copy();

    ck_assert_mem_eq(rec1.get_data(), copy.get_data(), rec1.get_length());
    ck_assert_int_eq(rec1.get_length(), copy.get_length());
    ck_assert_int_eq(rec1.get_timestamp(), copy.get_timestamp());

    int64_t new_key = 689;
    schema1->get_key(rec1.get_data()).SetInt64(new_key);

    ck_assert_mem_ne(rec1.get_data(), copy.get_data(), rec1.get_length());

    copy.free_data();
    ck_assert_int_eq(rec1.is_valid(), 1);
    ck_assert_int_eq(copy.is_valid(), 0);
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("Record Unit Testing");
    TCase *create = tcase_create("lsm::io::Record::create Testing");
    tcase_add_test(create, t_create);

    suite_add_tcase(unit, create);

    TCase *copy = tcase_create("lsm::io::Record::deep_copy Testing");
    tcase_add_test(copy, t_deep_copy);

    suite_add_tcase(unit, copy);

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
