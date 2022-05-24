/*
* Walker's Alias Method structure
*/

#ifndef H_WALKER
#define H_WALKER

#include <vector>
#include <cstdlib>
#include <gsl/gsl_rng.h>

namespace walker {
    class AliasStructure {
    private:
        std::vector<size_t> alias_table;
        std::vector<double> probability_table;
        gsl_rng *rng;

    public:
        AliasStructure(std::vector<double> *weights, gsl_rng *rng);
        size_t get();
    };
}

#endif
