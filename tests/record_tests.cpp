/*
 *
 */

#include <check.h>
#include "io/record.hpp"

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

    size_t len1, len2, len3;

    byte *test = io::Record::create((byte *) &key1, sizeof(int64_t), (byte *) &value1, sizeof(double), 0, false, &len1);
    byte *tombstone_test = io::Record::create((byte *) &key2, sizeof(int64_t), (byte *) &value2, sizeof(int16_t), 0, true, &len2);
    byte *timestamp_test = io::Record::create((byte *) &key3, sizeof(int64_t), (byte *) value3, strlen(value3), 5, false, &len3);

    auto test_rec1 = io::Record(test, len1, sizeof(int64_t));
    auto test_rec2 = io::Record(tombstone_test, len2, sizeof(int64_t));
    auto test_rec3 = io::Record(timestamp_test, len3, sizeof(int64_t));
    auto test_rec4 = io::Record();

    ck_assert(test_rec1.is_valid());
    ck_assert(test_rec2.is_valid());
    ck_assert(test_rec3.is_valid());
    ck_assert(!test_rec4.is_valid());

    ck_assert_int_eq(test_rec1.get_length(), len1);
    ck_assert_int_eq(test_rec2.get_length(), len2);
    ck_assert_int_eq(test_rec3.get_length(), len3);

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

    byte *data1 = test_rec1.get_data() + io::RecordHeaderLength;
    byte *data2 = test_rec2.get_data() + io::RecordHeaderLength;
    byte *data3 = test_rec3.get_data() + io::RecordHeaderLength;
    byte *data4 = test_rec4.get_data();

    ck_assert_int_eq(*((int64_t *) data1), key1);
    ck_assert_int_eq(*((int64_t *) data2), key2);
    ck_assert_int_eq(*((int64_t *) data3), key3);
    ck_assert_ptr_null(data4);

    ck_assert_int_eq(*((int64_t *) test_rec1.get_key()), key1);
    ck_assert_int_eq(*((int64_t *) test_rec2.get_key()), key2);
    ck_assert_int_eq(*((int64_t *) test_rec3.get_key()), key3);

    ck_assert_double_eq(*((double *) (data1 + sizeof(int64_t))), value1);
    ck_assert_int_eq(*((int16_t *) (data2 + sizeof(int64_t))), value2);
    ck_assert_mem_eq((data3 + sizeof(int64_t)), value3, strlen(value3));

    ck_assert_double_eq(*((double *) (test_rec1.get_value())), value1);
    ck_assert_int_eq(*((int16_t *) (test_rec2.get_value())), value2);
    ck_assert_mem_eq(test_rec3.get_value(), value3, strlen(value3));
}
END_TEST


START_TEST(t_deep_copy) 
{
    int64_t key = 1236;
    const char *value = "this is just a test of this.";

    size_t reclen;
    byte *buf = io::Record::create((byte *) &key, sizeof(int64_t), (byte*) value, strlen(value), 0, false, &reclen);

    auto rec1 = io::Record(buf, reclen, sizeof(int64_t));
    auto copy = rec1.deep_copy();

    ck_assert_mem_eq(rec1.get_data(), copy.get_data(), rec1.get_length());
    ck_assert_int_eq(rec1.get_length(), copy.get_length());
    ck_assert_int_eq(rec1.get_timestamp(), copy.get_timestamp());

    int64_t new_key = 689;
    memcpy(rec1.get_key(), &new_key, sizeof(int64_t));

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
