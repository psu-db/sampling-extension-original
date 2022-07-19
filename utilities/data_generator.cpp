/*
 *
 *
 */

#include <cstdio>
#include <cstdlib>

int main(int argc, char **argv) 
{
    if (argc < 2) {
        fprintf(stderr, "data_generator <count> [filename]");
        exit(EXIT_FAILURE);
    }

    FILE *output_file;
    if (argc >= 3) {
        auto fname = argv[2];
        output_file = fopen(fname, "w");
    } else {
        output_file = stdout;
    }

    size_t count = atol(argv[1]);

    for (size_t i=0; i<count; i++) {
        fprintf(output_file, "%ld\n", i);
    }

    exit(EXIT_SUCCESS);
}
