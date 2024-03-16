#ifndef YCSB_C_ZIPFIAN_GENERATOR_H_
#define YCSB_C_ZIPFIAN_GENERATOR_H_

#include "utils.h"
#include <cassert>
#include <cmath>
#include <cstdint>
#include <iostream>

namespace ycsbc {

class ZipfianGenerator : public Generator<uint64_t> {
public:
    constexpr static const double kZipfianConst = 0.9;
    static const uint64_t kMaxNumItems = (UINT64_MAX >> 24);

    ZipfianGenerator(uint64_t min, uint64_t max,
        double zipfian_const = kZipfianConst)
        : num_items_(max - min + 1)
        , base_(min)
        , theta_(zipfian_const)
        , zeta_n_(0)
        , n_for_zeta_(0)
    {
        std::cerr << "zipfian const: " << zipfian_const << std::endl;

        if (zipfian_const < 1.0) {
            assert(num_items_ >= 2 && num_items_ < kMaxNumItems);
            zeta_2_ = Zeta(2, theta_);
            alpha_ = 1.0 / (1.0 - theta_);
            RaiseZeta(num_items_);
            eta_ = Eta();

            Next();
        } else {
            std::cerr << "read from external file" << std::endl;
            fixed_values_ = new uint64_t[fv_i_thres_];
            FILE* fp = fopen("out.data", "r");
            uint64_t num;
            while (fscanf(fp, "%lu", &num) == 1) {
                if (fv_i_ >= fv_i_thres_) {
                    break;
                }
                fixed_values_[fv_i_++] = num;
            }
            fclose(fp);
        }
    }

    ZipfianGenerator(uint64_t num_items)
        : ZipfianGenerator(0, num_items - 1, kZipfianConst)
    {
    }

    uint64_t Next(uint64_t num_items);

    uint64_t Next() { return Next(num_items_); }

    uint64_t Last() { return last_value_; }

private:
    ///
    /// Compute the zeta constant needed for the distribution.
    /// Remember the number of items, so if it is changed, we can recompute zeta.
    ///
    void RaiseZeta(uint64_t num)
    {
        assert(num >= n_for_zeta_);
        zeta_n_ = Zeta(n_for_zeta_, num, theta_, zeta_n_);
        n_for_zeta_ = num;
    }

    double Eta()
    {
        return (1 - std::pow(2.0 / num_items_, 1 - theta_)) / (1 - zeta_2_ / zeta_n_);
    }

    ///
    /// Calculate the zeta constant needed for a distribution.
    /// Do this incrementally from the last_num of items to the cur_num.
    /// Use the zipfian constant as theta. Remember the new number of items
    /// so that, if it is changed, we can recompute zeta.
    ///
    static double Zeta(uint64_t last_num, uint64_t cur_num,
        double theta, double last_zeta)
    {
        double zeta = last_zeta;
        for (uint64_t i = last_num + 1; i <= cur_num; ++i) {
            zeta += 1 / std::pow(i, theta);
        }
        return zeta;
    }

    static double Zeta(uint64_t num, double theta)
    {
        return Zeta(0, num, theta, 0);
    }

    uint64_t num_items_;
    uint64_t base_; /// Min number of items to generate

    // Computed parameters for generating the distribution
    double theta_, zeta_n_, eta_, alpha_, zeta_2_;
    uint64_t n_for_zeta_; /// Number of items used to compute zeta_n
    uint64_t last_value_;

    const uint64_t fv_i_thres_ = 105 * 1000 * 1000;
    uint64_t* fixed_values_ = nullptr;
    uint64_t fv_i_ = 0;
    uint64_t fv_p_ = 0;
};

inline uint64_t ZipfianGenerator::Next(uint64_t num)
{
    assert(num >= 2 && num < kMaxNumItems);

    if (fixed_values_ != nullptr) {
        if (fv_p_ >= fv_i_) {
            fv_p_ = 0;
        }
        return last_value_ = fixed_values_[fv_p_++];
    }

    if (num > n_for_zeta_) { // Recompute zeta_n and eta
        RaiseZeta(num);
        eta_ = Eta();
    }

    double u = utils::RandomDouble();
    double uz = u * zeta_n_;

    if (uz < 1.0) {
        return last_value_ = 0;
    }

    if (uz < 1.0 + std::pow(0.5, theta_)) {
        return last_value_ = 1;
    }

    return last_value_ = base_ + num * std::pow(eta_ * u - eta_ + 1, alpha_);
}

}

#endif // YCSB_C_ZIPFIAN_GENERATOR_H_
