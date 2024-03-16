
#include "utils/timer.hpp"

namespace KDSEP_NAMESPACE {

Timer::Timer()
{
    _prev = 0;
}

Timer::~Timer()
{
}

void Timer::triggerTimer(bool start, const char* label)
{
    if (start) {
        _startTime = chrono::system_clock::now();
    } else {
        chrono::system_clock::time_point endTime = chrono::system_clock::now();
        cout << (label == 0 ? "NIL" : label) << ": " << (chrono::duration_cast<chrono::microseconds>(endTime - _startTime).count() + _prev) << " us" << endl;
    }
}

void Timer::startTimer()
{
    _prev = 0;
    triggerTimer(true);
}

void Timer::stopTimer(const char* label)
{
    triggerTimer(false, label);
}

void Timer::restartTimer()
{
    triggerTimer(true);
}

void Timer::pauseTimer()
{
    chrono::system_clock::time_point endTime = chrono::system_clock::now();
    _prev += chrono::duration_cast<chrono::microseconds>(endTime - _startTime).count();
}

} // namespace KDSEP_NAMESPACE