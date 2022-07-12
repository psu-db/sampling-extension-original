README
===========

Dependencies
-------------

- gsl
- check

You can install them on Ubuntu through:

```bash
  sudo apt install check libgsl-dev
```

Build
------------

At present, the makefile is not set up to automatically compile the
lockfree_skiplist library, so first compile that,

```
$ cd external/lockfree_skiplist
$ make
$ cd ../..
```

Then build the project. The default make target will build the project in
debug mode and automatically run all unit tests,
```
$ make
```

For benchmarking purposes, build the project in release mode
```
$ make release
```

If the lockfree skiplist library is not installed into the standard library
path, you must also add it to LD_LIBRARY_PATH to run the benchmarks/tests
```
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:external/lockfree_skiplist
```
