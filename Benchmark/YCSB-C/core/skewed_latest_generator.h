#ifndef YCSB_C_SKEWED_LATEST_GENERATOR_H_
#define YCSB_C_SKEWED_LATEST_GENERATOR_H_

#include <cstdint>

#include "counter_generator.h"
#include "generator.h"
#include "zipfian_generator.h"

namespace ycsbc {

class SkewedLatestGenerator : public Generator<uint64_t> {
   public:
    SkewedLatestGenerator(CounterGenerator& counter)
        : basis_(counter), zipfian_(basis_.Last()) {
        Next();
    }

    SkewedLatestGenerator(CounterGenerator& counter, 
            double zipfian_constant)
        : basis_(counter), 
        zipfian_(0, basis_.Last() - 1, zipfian_constant) {
        Next();
    }

    uint64_t Next();
    uint64_t Last() { return last_; }

   private:
    CounterGenerator& basis_;
    ZipfianGenerator zipfian_;
    uint64_t last_;
};

inline uint64_t SkewedLatestGenerator::Next() {
    uint64_t max = basis_.Last();
    return last_ = max - zipfian_.Next(max);
}

}  // namespace ycsbc

#endif  // YCSB_C_SKEWED_LATEST_GENERATOR_H_
