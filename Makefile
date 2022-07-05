CXX=g++-10
CXXFLAGS = -Wall -Wextra -Iinclude -faligned-new -std=c++17 $(OPTFLAGS) -DNO_BUFFER_MANAGER
LDLIBS = -lcheck -pthread -lrt -lsubunit -lm -lgsl -lgslcblas -lcds_d

SOURCES = $(wildcard src/**/*.cpp src/*.cpp)
OBJECTS = $(patsubst src/%.cpp,build/%.o,$(SOURCES))

TEST_SRC = $(wildcard tests/*_tests.cpp)
TESTS = $(patsubst %.cpp,%,$(TEST_SRC))

BENCH_SRC = $(wildcard benchmarks/*_bench.cpp)
BENCHMARKS = $(patsubst %.cpp,%,$(BENCH_SRC))

TARGET = lib/liblsm-sample.a

all: debug

release: CXXFLAGS += -O3
release: $(TARGET) benchmarks

bench: CXXFLAGS += -O3 -DDETAILED_BENCHMARKS
bench: $(TARGET) benchmarks

debug: CXXFLAGS += -O0 -ggdb -DUNIT_TESTING
debug: $(TARGET) tests benchmarks

.PHONY: build
build:
	mkdir -p build
	mkdir -p lib
	mkdir -p build/ds
	mkdir -p build/io
	mkdir -p build/util
	mkdir -p build/sampling
	mkdir -p tests/data/filemanager


build/%.o: src/%.cpp
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $(LDLIBS) -c $< -o $@


$(TARGET): build $(OBJECTS)
	ar rcs $@ $(OBJECTS)
	ranlib $@

.PHONY: tests
tests: LDLIBS := $(TARGET) $(LDLIBS)
tests: $(TESTS)
	sh ./tests/unit-tests.sh

.PHONY: benchmarks
benchmarks: LDLIBS := $(TARGET) $(LDLIBS)
benchmarks: $(BENCHMARKS)


clean:
	rm -rf $(TARGET)
	rm -rf lib bin build $(OBJECTS) $(TESTS) $(BENCHMARKS)
	rm -f tests/tests.log
