#pragma once

#include <bits/stdc++.h>
#include <boost/atomic.hpp>

using namespace std;

namespace KDSEP_NAMESPACE {

template <typename T>
class lockQueue {
public:
    lockQueue();
    ~lockQueue() = default;
    boost::atomic<bool> done;
    bool push(T& data);
    bool pop(T& data);
    bool isEmpty();

private:
    queue<T> q_;
    shared_mutex mtx_;
};

template <typename T>
lockQueue<T>::lockQueue()
{
    done = false;
}

template <typename T>
bool lockQueue<T>::push(T& data)
{
    scoped_lock<shared_mutex> lk(mtx_);
    q_.push(data);
    return true;
}

template <typename T>
bool lockQueue<T>::pop(T& data)
{
    scoped_lock<shared_mutex> lk(mtx_);
    if (q_.empty())
        return false;
    data = q_.front();
    q_.pop();
    return true;
}

template <typename T>
bool lockQueue<T>::isEmpty()
{
    shared_lock<shared_mutex> lk(mtx_);
    return q_.empty();
}

} // namespace KDSEP_NAMESPACE
