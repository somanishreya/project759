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

namespace gem5
{

namespace ruby
{

std::atomic<bool> Consumer::s_parallel_active{false};

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
    return m_wakeup_ticks.find(time) != m_wakeup_ticks.end();
}

void
Consumer::scheduleEvent(Cycles timeDelta)
{
    MaybeLock lock(m_consumer_mutex);
    m_wakeup_ticks.insert(em->clockEdge(timeDelta));
    scheduleNextWakeup();
}

void
Consumer::scheduleEventAbsolute(Tick evt_time)
{
    MaybeLock lock(m_consumer_mutex);
    m_wakeup_ticks.insert(
        divCeil(evt_time, em->clockPeriod()) * em->clockPeriod());
    scheduleNextWakeup();
}

void
Consumer::recordEvent(Cycles timeDelta)
{
    MaybeLock lock(m_consumer_mutex);
    m_wakeup_ticks.insert(em->clockEdge(timeDelta));
}

void
Consumer::recordEventAbsolute(Tick evt_time)
{
    MaybeLock lock(m_consumer_mutex);
    m_wakeup_ticks.insert(
        divCeil(evt_time, em->clockPeriod()) * em->clockPeriod());
}

void
Consumer::descheduleTick(Tick tick)
{
    MaybeLock lock(m_consumer_mutex);
    m_wakeup_ticks.erase(tick);
}

void
Consumer::scheduleNextWakeup()
{
    // Callers already hold the mutex (when needed) via MaybeLock.
    auto it = m_wakeup_ticks.lower_bound(em->clockEdge());
    if (it != m_wakeup_ticks.end()) {
        Tick when = *it;
        assert(when >= em->clockEdge());
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
    auto it = m_wakeup_ticks.find(current_tick);
    bool was_pending = (it != m_wakeup_ticks.end());
    if (was_pending) {
        m_wakeup_ticks.erase(it);
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
