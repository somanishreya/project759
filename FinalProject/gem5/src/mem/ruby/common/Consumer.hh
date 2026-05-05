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
 * Copyright (c) 1999-2008 Mark D. Hill and David A. Wood
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

/*
 * This is the virtual base class of all classes that can be the
 * targets of wakeup events.  There is only two methods, wakeup() and
 * print() and no data members.
 */

#ifndef __MEM_RUBY_COMMON_CONSUMER_HH__
#define __MEM_RUBY_COMMON_CONSUMER_HH__

#include <atomic>
#include <iostream>
#include <mutex>
#include <vector>

#include "sim/clocked_object.hh"

namespace gem5
{

namespace ruby
{

class Consumer
{
  public:
    Consumer(ClockedObject *em,
             Event::Priority ev_prio = Event::Default_Pri);

    virtual
    ~Consumer()
    { }

    virtual void wakeup() = 0;
    virtual void print(std::ostream& out) const = 0;
    virtual void storeEventInfo(int info) {}

    bool alreadyScheduled(Tick time);

    ClockedObject *
    getObject()
    {
        return em;
    }

    void scheduleEventAbsolute(Tick timeAbs);
    void scheduleEvent(Cycles timeDelta);
    void recordEvent(Cycles timeDelta);
    void recordEventAbsolute(Tick timeAbs);
    void descheduleTick(Tick tick);

    // Atomic equivalent of `if (alreadyScheduled(t)) descheduleTick(t)`,
    // but with one binary-search instead of two. Returns whether `tick`
    // was present (i.e. caller should service this consumer for `tick`).
    // Used on the Garnet router-wakeup hot path.
    bool tryConsumeTick(Tick tick);

    // Set to true only while a Garnet parallel section is running. When
    // false, all Consumer mutex acquisitions are skipped (the simulator
    // is single-threaded outside the parallel region, so no locking is
    // needed). Marked relaxed-atomic because the value is published via
    // a CV signal/wait pair which already establishes happens-before.
    static std::atomic<bool> s_parallel_active;

  private:
    mutable std::recursive_mutex m_consumer_mutex;
    // Sorted ascending, no duplicates. Replaces std::set<Tick> because
    // virtually every Consumer in Garnet has 0-2 pending ticks at any
    // time, and a small sorted vector beats an RB-tree by ~5-10x on
    // both insert and lookup at this size while avoiding heap churn.
    std::vector<Tick> m_wakeup_ticks;
    EventFunctionWrapper m_wakeup_event;
    ClockedObject *em;

    void scheduleNextWakeup();
    void processCurrentEvent();
};


inline std::ostream&
operator<<(std::ostream& out, const Consumer& obj)
{
    obj.print(out);
    out << std::flush;
    return out;
}

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_COMMON_CONSUMER_HH__
