#pragma once

#include <bits/stdc++.h>
#include <boost/atomic.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
using namespace std;

namespace KDSEP_NAMESPACE {

template <typename T>
class messageQueue {
public:
    messageQueue();
    ~messageQueue() = default;
    boost::atomic<bool> done;
    bool tryPush(T& data);
    bool push(T& data);
    bool pop(T& data);
    bool isEmpty();

private:
    boost::lockfree::queue<T, boost::lockfree::capacity<5000>> lockFreeQueue_;
};

template <typename T>
messageQueue<T>::messageQueue()
{
    done = false;
}

template <typename T>
bool messageQueue<T>::push(T& data)
{
    while (!lockFreeQueue_.push(data))
        ;
    return true;
}

template <typename T>
bool messageQueue<T>::tryPush(T& data)
{
    return lockFreeQueue_.push(data);
}

template <typename T>
bool messageQueue<T>::pop(T& data)
{
    return lockFreeQueue_.pop(data);
}

template <typename T>
bool messageQueue<T>::isEmpty()
{
    return lockFreeQueue_.empty();
}
} // namespace KDSEP_NAMESPACE
