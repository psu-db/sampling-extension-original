#!/bin/bash

echo "Running unit tests:"

rm -rf tests/data/filemanager
rm -rf tests/data/filemanager1

mkdir tests/data/filemanager
mkdir tests/data/filemanager1

for i in tests/*_tests
do
    $i 2>&1 | tee -a tests/tests.log
done

echo ""
