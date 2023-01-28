#pragma once

#include <gsl/gsl_rng.h>

#include <vector>

namespace lsm {

/*
 * XXX: This alias structure assumes input weights are already normalized!!
 */

class Alias {
public:
    Alias(const std::vector<double>& weights)
    : n(weights.size()) {
        m_alias = new size_t[n];
        m_cutoff = new double[n];
        auto overfull = std::vector<size_t>();
        auto underfull = std::vector<size_t>();

        // initialize the probability_table with n*p(i) as well as the overfull and
        // underfull lists.
        for (size_t i = 0; i < n; i++) {
            m_cutoff[i] = (double) n * weights[i];
            if (m_cutoff[i] > 1) {
                overfull.emplace_back(i);
            } else if (m_cutoff[i] < 1) {
                underfull.emplace_back(i);
            } else {
                m_alias[i] = i;
            }
        }

        while (overfull.size() > 0 && underfull.size() > 0) {
            auto i = overfull.back(); overfull.pop_back();
            auto j = underfull.back(); underfull.pop_back();

            m_alias[j] = i;
            m_cutoff[i] = m_cutoff[i] + m_cutoff[j] - 1.0;

            if (m_cutoff[i] > 1.0) {
                overfull.push_back(i);
            } else if (m_cutoff[i] < 1.0) {
                underfull.push_back(i);
            }
        }
    }

    ~Alias() {
        if (m_alias) delete[] m_alias;
        if (m_cutoff) delete[] m_cutoff;
    }

    size_t get(const gsl_rng* rng) {
        double coin1 = gsl_rng_uniform(rng);
        double coin2 = gsl_rng_uniform(rng);

        size_t k = ((double) n) * coin1;
        return coin2 < m_cutoff[k] ? k : m_alias[k];
    }

private:
    size_t n;
    size_t* m_alias;
    double* m_cutoff;
};

}