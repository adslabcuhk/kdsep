#pragma once

#include <random>
#include <cmath>

#include "generator.h"
#include "iostream"

namespace ycsbc {

//----- Defines -------------------------------------------------------------
#define PI         3.14159265   // The value of pi

class UP2XGenerator {
   public:
    UP2XGenerator() : 
//        mu_key_(10.45), sigma_key_(1.4), mu_value_(46.8), sigma_value_(11.8) 
        mu_key_(7.10), sigma_key_(1.4), mu_value_(47.3), sigma_value_(11.8) 
        // actual: 10.4676, 0.59507, 46.7672, 11.765 
    {
        rand_val(123456);
    }

    uint64_t KeyLength(const std::string& key) {
        int hash = 0;
        for (int i = ((key.size() > 8) ? key.size() - 8 : 0); 
                i < (int)key.size(); i++) {
	    hash = hash * 10 + (key[i] - '0');
	}

        int key_size = (int)norm_fixed(mu_key_, sigma_key_, hash); 
        if (key_size <= 4) {
            key_size = 4;
        }
        return (uint64_t)key_size;
    } 

    uint64_t ValueLength() {
        int value_size = (int)norm(mu_value_, sigma_value_);
        if (value_size <= 4) {
            value_size = 4;
        }
        return value_size;
    }

//    void SetFieldCount(uint64_t fc) { field_count_ = fc; }

   private:
//    uint64_t num_;
    double mu_key_;
    double sigma_key_;
    double mu_value_;
    double sigma_value_;
//    uint64_t max_value_{128 * 1024};
//    uint64_t field_count_;

    //==================================================== file = gennorm.c =====
    //=  Program to generate nomrally distributed random variables              =
    //===========================================================================
    //=  Notes: 1) Writes to a user specified output file                       =
    //=         2) Generates user specified number of samples                   =
    //=         3) Uses the Box-Muller method and only generates one of two     =
    //=            paired normal random variables.                              =
    //=-------------------------------------------------------------------------=
    //= Example user input:                                                     =
    //=                                                                         =
    //=   ---------------------------------------- gennorm.c -----              =
    //=   -  Program to generate normally distributed random     -              =
    //=   -  variables                                           -              =
    //=   --------------------------------------------------------              =
    //=   Output file name ===================================> output.dat      =
    //=   Random number seed =================================> 1               =
    //=   Mean ===============================================> 0               =
    //=   Standard deviation =================================> 1               =
    //=   Number of samples to generate ======================> 10              =
    //=   --------------------------------------------------------              =
    //=   -  Generating samples to file                          -              =
    //=   --------------------------------------------------------              =
    //=   --------------------------------------------------------              =
    //=   -  Done!                                                              =
    //=   --------------------------------------------------------              =
    //=-------------------------------------------------------------------------=
    //= Example output (from above user input):                                 =
    //=                                                                         =
    //=  3.015928                                                               =
    //=  1.446444                                                               =
    //=  0.294214                                                               =
    //=  0.372630                                                               =
    //=  0.802585                                                               =
    //=  -1.509856                                                              =
    //=  -0.672829                                                              =
    //=  1.033490                                                               =
    //=  0.759008                                                               =
    //=  0.078499                                                               =
    //=-------------------------------------------------------------------------=
    //=  Build: bcc32 gennorm.c                                                 =
    //=-------------------------------------------------------------------------=
    //=  Execute: gennorm                                                       =
    //=-------------------------------------------------------------------------=
    //=  Author: Kenneth J. Christensen                                         =
    //=          University of South Florida                                    =
    //=          WWW: http://www.csee.usf.edu/~christen                         =
    //=          Email: christen@csee.usf.edu                                   =
    //=-------------------------------------------------------------------------=
    //=  History: KJC (06/06/02) - Genesis                                      =
    //=           KJC (05/20/03) - Added Jain's RNG for finer granularity       =
    //===========================================================================

    //===========================================================================
    //=  Function to generate normally distributed random variable using the    =
    //=  Box-Muller method                                                      =
    //=    - Input: mean and standard deviation                                 =
    //=    - Output: Returns with normally distributed random variable          =
    //===========================================================================
    double norm(double mean, double std_dev)
    {
        double   u, r, theta;           // Variables for Box-Muller method
        double   x;                     // Normal(0, 1) rv
        double   norm_rv;               // The adjusted normal rv

        // Generate u
        u = 0.0;
        while (u == 0.0)
            u = rand_val(0);

        // Compute r
        r = sqrt(-2.0 * log(u));

        // Generate theta
        theta = 0.0;
        while (theta == 0.0)
            theta = 2.0 * PI * rand_val(0);

        // Generate x value
        x = r * cos(theta);

        // Adjust x value for specified mean and variance
        norm_rv = (x * std_dev) + mean;

        // Return the normally distributed RV value
        return(norm_rv);
    }

    double norm_fixed(double mean, double std_dev, int hash)
    {
        double   u, r, theta;           // Variables for Box-Muller method
        double   x;                     // Normal(0, 1) rv
        double   norm_rv;               // The adjusted normal rv

        // Generate u
        u = 0.0;
        while (u == 0.0)
            u = rand_val_fixed(hash);

        // Compute r
        r = sqrt(-2.0 * log(u));

        // Generate theta
        theta = 0.0;
        while (theta == 0.0)
            theta = 2.0 * PI * rand_val_fixed(hash);

        // Generate x value
        x = r * cos(theta);

        // Adjust x value for specified mean and variance
        norm_rv = (x * std_dev) + mean;

        // Return the normally distributed RV value
        return(norm_rv);
    }

    //=========================================================================
    //= Multiplicative LCG for generating uniform(0.0, 1.0) random numbers    =
    //=   - x_n = 7^5*x_(n-1)mod(2^31 - 1)                                    =
    //=   - With x seeded to 1 the 10000th x value should be 1043618065       =
    //=   - From R. Jain, "The Art of Computer Systems Performance Analysis," =
    //=     John Wiley & Sons, 1991. (Page 443, Figure 26.2)                  =
    //=========================================================================
    double rand_val(int seed)
    {
        const long  a =      16807;  // Multiplier
        const long  m = 2147483647;  // Modulus
        const long  q =     127773;  // m div a
        const long  r =       2836;  // m mod a
        static long x;               // Random int value
        long        x_div_q;         // x divided by q
        long        x_mod_q;         // x modulo q
        long        x_new;           // New x value

        // Set the seed if argument is non-zero and then return zero
        if (seed > 0)
        {
            x = seed;
            return(0.0);
        }

        // RNG using integer arithmetic
        x_div_q = x / q;
        x_mod_q = x % q;
        x_new = (a * x_mod_q) - (r * x_div_q);
        if (x_new > 0)
            x = x_new;
        else
            x = x_new + m;

        // Return a random value between 0.0 and 1.0
        return((double) x / m);
    }
    
    double rand_val_fixed(int seed) {
//        const long  a =      16807;  // Multiplier
        const long  m = 2147483647;  // Modulus
//        const long  q =     127773;  // m div a
//        const long  r =       2836;  // m mod a
        long x;               // Random int value
//        long        x_div_q;         // x divided by q
//        long        x_mod_q;         // x modulo q
//        long        x_new;           // New x value

        x = seed;
//        x_div_q = x / q;
//        x_mod_q = x % q;
//        x_new = (a * x_mod_q) - (r * x_div_q);
        return((double) x / m);
    }
};

}  // namespace ycsbc

