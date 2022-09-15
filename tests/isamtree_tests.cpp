#include <check.h>
#include <string>

#include "testing.h"
#include "ds/IsamTree.h"

using namespace lsm;

START_TEST(t_create)
{
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("IsamTree Unit Testing");
    TCase *initialize = tcase_create("lsm::ISAMTree::create Testing");
    tcase_add_test(initialize, t_create);

    suite_add_tcase(unit, initialize);

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

