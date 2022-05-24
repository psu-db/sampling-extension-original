/*
* Walker alias structure implementation file
*/
#include "ds/walker.hpp"


walker::AliasStructure::AliasStructure(std::vector<double> *weights, gsl_rng *rng)
{
   size_t size = weights->size();

   this->alias_table = std::vector<size_t>(size);
   this->probability_table = std::vector<double>(size);
   this->rng = rng;

   auto overfull = std::vector<size_t>();
   auto underfull = std::vector<size_t>();

   // initialize the probability_table with n*p(i) as well as the overfull and
   // underfull lists.
   for (size_t i=0; i<size; i++) {
        this->probability_table[i] = (double) size * (*weights)[i];
        if (this->probability_table[i] > 1) {
            overfull.push_back(i);
        } else if (this->probability_table[i] < 1) {
            underfull.push_back(i);
        } else {
            alias_table[i] = i;
        }
    }

    while (overfull.size() > 0 && underfull.size() > 0) {
        auto i = overfull.back(); overfull.pop_back();
        auto j = underfull.back(); underfull.pop_back();

        alias_table[j] = i;
        probability_table[i] = probability_table[i] + probability_table[j] - 1;

        if (probability_table[i] > 1.0) {
            overfull.push_back(i);
        } else if (probability_table[i] < 1.0) {
            underfull.push_back(i);
        }
    }
}


size_t walker::AliasStructure::get()
{
    /*
    * On the topic of random number generation. The Wikipedia article lists an algorithm
    * that calculates y based on x--this couples the two numbers, but allows for only a
    * single call to the generator. I don't know to what degree that actually matters,
    * but in this version I'm erring on the side of correctness and generating both. This
    * does make this version a bit slower than the original, though.
    */
    double x = (double) this->alias_table.size() * gsl_rng_uniform(this->rng);
    double y = gsl_rng_uniform(this->rng);

    size_t i = x; // truncate x to an integer (always round down) to serve as an index

    return y < this->probability_table[i] ? i : this->alias_table[i];
}
