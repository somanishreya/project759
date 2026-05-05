/*
 * Copyright (c) 2020-2021 ARM Limited
 * All rights reserved.
 *
 * The license below extends only to copyright in the software and shall
 * not be construed as granting a license to any other intellectual
 * property including but not limited to intellectual property relating
 * to a hardware implementation of the functionality of the software
 * licensed hereunder.  You may use the software subject to the license
 * terms below provided that you ensure that this notice is replicated
 * unmodified and in its entirety in all distributions of the software,
 * modified or unmodified, in source code or in binary form.
 *
 * Copyright (c) 2012 Mark D. Hill and David A. Wood
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met: redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer;
 * redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution;
 * neither the name of the copyright holders nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "mem/ruby/common/Consumer.hh"

#include <algorithm>

namespace gem5
{

namespace ruby
{

std::atomic<bool> Consumer::s_parallel_active{false};

namespace {

// Insert `tick` into a sorted ascending vector if not already present.
// Returns true if an insertion happened.
inline bool
insertSorted(std::vector<Tick> &v, Tick tick)
{
    auto it = std::lower_bound(v.begin(), v.end(), tick);
    if (it != v.end() && *it == tick) {
        return false;
    }
    v.insert(it, tick);
    return true;
}

inline bool
containsSorted(const std::vector<Tick> &v, Tick tick)
{
    auto it = std::lower_bound(v.begin(), v.end(), tick);
    return it != v.end() && *it == tick;
}

inline void
eraseSorted(std::vector<Tick> &v, Tick tick)
{
    auto it = std::lower_bound(v.begin(), v.end(), tick);
    if (it != v.end() && *it == tick) {
        v.erase(it);
    }
}

} // namespace

namespace {

// RAII helper that locks `m` only when a Garnet parallel section is
// active. Outside parallel regions the simulator is single-threaded so
// the mutex is unnecessary; skipping it removes ~30-40 ns per Consumer
// call, which adds up to seconds across a 10M-cycle synthetic-traffic
// run (these methods fire on every flit/credit scheduling).
class MaybeLock
{
  public:
    explicit MaybeLock(std::recursive_mutex &m)
        : m_mutex(nullptr)
    {
        if (Consumer::s_parallel_active.load(std::memory_order_relaxed)) {
            m.lock();
            m_mutex = &m;
        }
    }

    ~MaybeLock()
    {
        if (m_mutex)
            m_mutex->unlock();
    }

    MaybeLock(const MaybeLock &) = delete;
    MaybeLock &operator=(const MaybeLock &) = delete;

  private:
    std::recursive_mutex *m_mutex;
};

} // namespace

Consumer::Consumer(ClockedObject *_em, Event::Priority ev_prio)
    : m_wakeup_event([this]{ processCurrentEvent(); },
                    "Consumer Event", false, ev_prio),
      em(_em)
{ }

bool
Consumer::alreadyScheduled(Tick time)
{
    MaybeLock lock(m_consumer_mutex);
    return containsSorted(m_wakeup_ticks, time);
}

void
Consumer::scheduleEvent(Cycles timeDelta)
{
    MaybeLock lock(m_consumer_mutex);
    insertSorted(m_wakeup_ticks, em->clockEdge(timeDelta));
    scheduleNextWakeup();
}

void
Consumer::scheduleEventAbsolute(Tick evt_time)
{
    MaybeLock lock(m_consumer_mutex);
    insertSorted(m_wakeup_ticks,
        divCeil(evt_time, em->clockPeriod()) * em->clockPeriod());
    scheduleNextWakeup();
}

void
Consumer::recordEvent(Cycles timeDelta)
{
    MaybeLock lock(m_consumer_mutex);
    insertSorted(m_wakeup_ticks, em->clockEdge(timeDelta));
}

void
Consumer::recordEventAbsolute(Tick evt_time)
{
    MaybeLock lock(m_consumer_mutex);
    insertSorted(m_wakeup_ticks,
        divCeil(evt_time, em->clockPeriod()) * em->clockPeriod());
}

void
Consumer::descheduleTick(Tick tick)
{
    MaybeLock lock(m_consumer_mutex);
    eraseSorted(m_wakeup_ticks, tick);
}

bool
Consumer::tryConsumeTick(Tick tick)
{
    MaybeLock lock(m_consumer_mutex);
    if (m_wakeup_ticks.empty()) {
        return false;
    }
    // Fast path: the smallest pending tick is the one we want.
    if (m_wakeup_ticks.front() == tick) {
        m_wakeup_ticks.erase(m_wakeup_ticks.begin());
        return true;
    }
    auto it = std::lower_bound(m_wakeup_ticks.begin(),
                               m_wakeup_ticks.end(), tick);
    if (it != m_wakeup_ticks.end() && *it == tick) {
        m_wakeup_ticks.erase(it);
        return true;
    }
    return false;
}

void
Consumer::scheduleNextWakeup()
{
    // Callers already hold the mutex (when needed) via MaybeLock.
    Tick now = em->clockEdge();
    auto it = std::lower_bound(m_wakeup_ticks.begin(),
                               m_wakeup_ticks.end(), now);
    if (it != m_wakeup_ticks.end()) {
        Tick when = *it;
        assert(when >= now);
        if (m_wakeup_event.scheduled() && (when < m_wakeup_event.when()))
            em->reschedule(m_wakeup_event, when, true);
        else if (!m_wakeup_event.scheduled())
            em->schedule(m_wakeup_event, when);
    }
}

void
Consumer::processCurrentEvent()
{
    // processCurrentEvent only ever fires from the main event queue,
    // which is paused while a parallel section is in flight, so locking
    // is never required here in practice.
    Tick current_tick = em->clockEdge();
    bool was_pending = false;
    if (!m_wakeup_ticks.empty() && m_wakeup_ticks.front() == current_tick) {
        // Common case: the smallest pending tick is the current one.
        m_wakeup_ticks.erase(m_wakeup_ticks.begin());
        was_pending = true;
    } else {
        auto it = std::lower_bound(m_wakeup_ticks.begin(),
                                   m_wakeup_ticks.end(), current_tick);
        if (it != m_wakeup_ticks.end() && *it == current_tick) {
            m_wakeup_ticks.erase(it);
            was_pending = true;
        }
    }

    // Garnet's globalWakeup() may have already serviced this consumer
    // for the current tick (and removed the entry via descheduleTick).
    // The event itself stays in the EventQueue, but we must not fire
    // wakeup() again - that would double-execute every Router on every
    // cycle that was driven by an external NetworkLink schedule.
    if (was_pending) {
        wakeup();
    }

    scheduleNextWakeup();
}

} // namespace ruby
} // namespace gem5
