#pragma once

// #include "common/dataStructure.hpp"
#include <string>

enum class DebugOutPutLevel : int {
    NONE,
    ERROR,
    WARN,
    INFO,
    TRACE,
    ANY
};

class DebugManager {
public:
    static DebugManager& getInstance()
    {
        static DebugManager instance; // Guaranteed to be destroyed
        // Instantiated on first use
        return instance;
    }
    void setDebugLevel(int level) {
        debugLevel_ = (DebugOutPutLevel)level;
    }
    DebugOutPutLevel getDebugLevel() {
        return debugLevel_;
    }
private:
    DebugOutPutLevel debugLevel_ = DebugOutPutLevel::ERROR;
};

#define DEBUG_LEVEL DebugManager::getInstance().getDebugLevel() // DebugOutPutLevel::ERROR
// for color
#define RESET "\033[0m"
#define BLACK "\033[30m" /* Black */
#define RED "\033[31m" /* Red */
#define GREEN "\033[32m" /* Green */
#define YELLOW "\033[33m" /* Yellow */
#define BLUE "\033[34m" /* Blue */
#define MAGENTA "\033[35m" /* Magenta */
#define CYAN "\033[36m" /* Cyan */
#define WHITE "\033[37m" /* White */
#define BOLDBLACK "\033[1m\033[30m" /* Bold Black */
#define BOLDRED "\033[1m\033[31m" /* Bold Red */
#define BOLDGREEN "\033[1m\033[32m" /* Bold Green */
#define BOLDYELLOW "\033[1m\033[33m" /* Bold Yellow */
#define BOLDBLUE "\033[1m\033[34m" /* Bold Blue */
#define BOLDMAGENTA "\033[1m\033[35m" /* Bold Magenta */
#define BOLDCYAN "\033[1m\033[36m" /* Bold Cyan */
#define BOLDWHITE "\033[1m\033[37m" /* Bold White */

#define debug(fmt, ...)                                                           \
    do {                                                                          \
        if (DEBUG_LEVEL >= DebugOutPutLevel::ANY)                                 \
            fprintf(stderr, "[%s] %s:%d:%s(): " fmt, getTime().c_str(), __FILE__, \
                __LINE__, __func__, __VA_ARGS__);                                 \
    } while (0)

#define debug_warn(fmt, ...)                                                                   \
    do {                                                                                       \
        if (DEBUG_LEVEL >= DebugOutPutLevel::WARN)                                             \
            fprintf(stderr, YELLOW "[%s] %s:%d:%s(): " fmt RESET, getTime().c_str(), __FILE__, \
                __LINE__, __func__, __VA_ARGS__);                                              \
    } while (0)

#define debug_error(fmt, ...)                                                                   \
    do {                                                                                        \
        if (DEBUG_LEVEL >= DebugOutPutLevel::ERROR)                                             \
            fprintf(stderr, BOLDRED "[%s] %s:%d:%s(): " fmt RESET, getTime().c_str(), __FILE__, \
                __LINE__, __func__, __VA_ARGS__);                                               \
    } while (0)

#define debug_e(fmt)                                                                            \
    do {                                                                                        \
            fprintf(stderr, BOLDRED "[%s] %s:%d:%s(): " fmt "\n" RESET, getTime().c_str(), __FILE__, \
                __LINE__, __func__);                                               \
    } while (0)

#define debug_info(fmt, ...)                                                                 \
    do {                                                                                     \
        if (DEBUG_LEVEL >= DebugOutPutLevel::INFO)                                           \
            fprintf(stderr, CYAN "[%s] %s:%d:%s(): " fmt RESET, getTime().c_str(), __FILE__, \
                __LINE__, __func__, __VA_ARGS__);                                            \
    } while (0)

#define debug_trace(fmt, ...)                                                                   \
    do {                                                                                        \
        if (DEBUG_LEVEL >= DebugOutPutLevel::TRACE)                                             \
            fprintf(stderr, MAGENTA "[%s] %s:%d:%s(): " fmt RESET, getTime().c_str(), __FILE__, \
                __LINE__, __func__, __VA_ARGS__);                                               \
    } while (0)

#define print_green(fmt, ...)                      \
    do {                                           \
        printf(GREEN fmt RESET "\n", __VA_ARGS__); \
    } while (0)

#define print_blue(fmt, ...)                      \
    do {                                          \
        printf(BLUE fmt RESET "\n", __VA_ARGS__); \
    } while (0)

#define print_yellow(fmt, ...)                      \
    do {                                            \
        printf(YELLOW fmt RESET "\n", __VA_ARGS__); \
    } while (0)

#define print_bold_magenta(fmt, ...)                     \
    do {                                                 \
        printf(BOLDMAGENTA fmt RESET "\n", __VA_ARGS__); \
    } while (0)

namespace KDSEP_NAMESPACE {

void printhex(char* buf, int n);
std::string getTime();
std::string formatSize(uint64_t size);
void dumpBuf(const char* path, char* buf, int n);
void fillRandom(char* buf, int n);

}
