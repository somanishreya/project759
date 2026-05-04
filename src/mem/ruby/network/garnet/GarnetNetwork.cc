/*
 * Copyright (c) 2020 Advanced Micro Devices, Inc.
 * Copyright (c) 2008 Princeton University
 * Copyright (c) 2016 Georgia Institute of Technology
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


#include "mem/ruby/network/garnet/GarnetNetwork.hh"

#include <cassert>

#include "base/cast.hh"
#include "base/compiler.hh"
#include "debug/RubyNetwork.hh"
#include "mem/ruby/common/Consumer.hh"
#include "mem/ruby/common/NetDest.hh"
#include "mem/ruby/network/MessageBuffer.hh"
#include "mem/ruby/network/garnet/CommonTypes.hh"
#include "mem/ruby/network/garnet/CreditLink.hh"
#include "mem/ruby/network/garnet/GarnetLink.hh"
#include "mem/ruby/network/garnet/NetworkInterface.hh"
#include "mem/ruby/network/garnet/NetworkLink.hh"
#include "mem/ruby/network/garnet/Router.hh"
#include "mem/ruby/system/RubySystem.hh"

#include <omp.h>

#include <algorithm>
#include <iomanip>
#include <map>
#include <mutex>
#include <sstream>
#include <string>

#include "debug/FmtFlag.hh"
#include "debug/FmtTicksOff.hh"

namespace gem5
{

namespace ruby
{

namespace garnet
{

/*
 * GarnetNetwork sets up the routers and links and collects stats.
 * Default parameters (GarnetNetwork.py) can be overwritten from command line
 * (see configs/network/Network.py)
 */

std::mutex GarnetNetwork::g_scheduling_mutex;

thread_local GarnetNetwork::DeferredQueue *
    GarnetNetwork::t_deferred_queue = nullptr;

class BufferedLogger : public trace::Logger
{
  public:
    void logMessage(Tick when, const std::string &name,
                    const std::string &flag,
                    const std::string &message) override
    {
        std::stringstream ss;
        if (!debug::FmtTicksOff && (when != MaxTick))
            ss << std::setw(7) << when << ": ";
        if (debug::FmtFlag && !flag.empty())
            ss << flag << ": ";

        // Use thread-local context name if available
        std::string context_name = currentRouter ? currentRouter->name() : name;
        if (!context_name.empty())
            ss << context_name << ": ";

        ss << message;

        std::lock_guard<std::mutex> lock(mutex);
        buffer[context_name] += ss.str();
    }

    std::ostream &getOstream() override { return std::cerr; }

    static thread_local const Router* currentRouter;
    std::map<std::string, std::string> buffer;
    std::mutex mutex;
};

thread_local const Router* BufferedLogger::currentRouter = nullptr;

GarnetNetwork::GarnetNetwork(const Params &p)
    : Network(p),
      globalWakeupEvent([this]{ globalWakeup(); }, name(), false,
                        (Event::Priority) (Event::Default_Pri + 1)),
      m_next_packet_id(0),
      m_num_threads(0),
      // Empirically the parallel-region round-trip pays for itself at
      // ~16 routers when work is well-balanced across threads. Below
      // that the sequential branch wins.
      m_parallel_threshold(16)
{
    m_num_rows = p.num_rows;
    m_ni_flit_size = p.ni_flit_size;
    m_max_vcs_per_vnet = 0;
    m_buffers_per_data_vc = p.buffers_per_data_vc;
    m_buffers_per_ctrl_vc = p.buffers_per_ctrl_vc;
    m_routing_algorithm = p.routing_algorithm;

    m_enable_fault_model = p.enable_fault_model;
    if (m_enable_fault_model)
        fault_model = p.fault_model;

    m_vnet_type.resize(m_virtual_networks);

    for (int i = 0 ; i < m_virtual_networks ; i++) {
        if (m_vnet_type_names[i] == "response")
            m_vnet_type[i] = DATA_VNET_; // carries data (and ctrl) packets
        else
            m_vnet_type[i] = CTRL_VNET_; // carries only ctrl packets
    }

    // record the routers
    for (std::vector<BasicRouter*>::const_iterator i =  p.routers.begin();
         i != p.routers.end(); ++i) {
        Router* router = safe_cast<Router*>(*i);
        m_routers.push_back(router);

        // initialize the router's network pointers
        router->init_net_ptr(this);
    }

    // record the network interfaces
    for (std::vector<ClockedObject*>::const_iterator i = p.netifs.begin();
         i != p.netifs.end(); ++i) {
        NetworkInterface *ni = safe_cast<NetworkInterface *>(*i);
        m_nis.push_back(ni);
        ni->init_net_ptr(this);
    }

    // Print Garnet version
    inform("Garnet version %s\n", garnetVersion);

    fatal_if(m_routers.size() > 64,
        "GarnetNetwork parallel wakeup mask only supports up to 64 routers,"
        " got %d", (int)m_routers.size());

    // Choose the worker pool size. We deliberately cap by
    // num_routers/8: with 1-cycle work per router, anything larger
    // costs more in parallel-region overhead than it saves in
    // compute. Setting num_threads to 0 (the default) picks an
    // automatic value, 1 disables the pool entirely.
    int requested = p.num_threads;
    int hw = omp_get_num_procs();
    if (hw <= 0) hw = 1;
    int auto_threads = std::max(1, (int)m_routers.size() / 8);
    if (requested <= 0) {
        m_num_threads = std::min(hw, auto_threads);
    } else {
        m_num_threads = std::min(requested, hw);
    }

    // Build the static router-to-thread assignment as per-thread bit
    // masks so each worker can extract its routers with one AND.
    m_thread_router_mask.assign(m_num_threads, 0ULL);
    for (int id = 0; id < (int) m_routers.size(); id++) {
        m_thread_router_mask[id % m_num_threads] |= (1ULL << id);
    }

    // Per-worker deferred-schedule buffers.
    m_deferred_schedules.resize(m_num_threads);
    for (auto &q : m_deferred_schedules) {
        q.entries.reserve(64);
    }

    // Pre-warm the OpenMP thread pool. The runtime would otherwise
    // create threads lazily on the first parallel region, slowing
    // down the first cycle. Doing it once here also wires up each
    // worker's thread_local t_deferred_queue ahead of time so the
    // hot path on cycle 0 has nothing extra to do.
    if (m_num_threads > 1) {
        omp_set_num_threads(m_num_threads);
        #pragma omp parallel num_threads(m_num_threads)
        {
            int tid = omp_get_thread_num();
            t_deferred_queue = &m_deferred_schedules[tid];
        }
    }
}

void
GarnetNetwork::init()
{
    Network::init();

    for (int i=0; i < m_nodes; i++) {
        m_nis[i]->addNode(m_toNetQueues[i], m_fromNetQueues[i]);
    }

    // The topology pointer should have already been initialized in the
    // parent network constructor
    assert(m_topology_ptr != NULL);
    m_topology_ptr->createLinks(this);

    // Initialize topology specific parameters
    if (getNumRows() > 0) {
        // Only for Mesh topology
        // m_num_rows and m_num_cols are only used for
        // implementing XY or custom routing in RoutingUnit.cc
        m_num_rows = getNumRows();
        m_num_cols = m_routers.size() / m_num_rows;
        assert(m_num_rows * m_num_cols == m_routers.size());
    } else {
        m_num_rows = -1;
        m_num_cols = -1;
    }

    // FaultModel: declare each router to the fault model
    if (isFaultModelEnabled()) {
        for (std::vector<Router*>::const_iterator i= m_routers.begin();
             i != m_routers.end(); ++i) {
            Router* router = safe_cast<Router*>(*i);
            [[maybe_unused]] int router_id =
                fault_model->declare_router(router->get_num_inports(),
                                            router->get_num_outports(),
                                            router->get_vc_per_vnet(),
                                            getBuffersPerDataVC(),
                                            getBuffersPerCtrlVC());
            assert(router_id == router->get_id());
            router->printAggregateFaultProbability(std::cout);
            router->printFaultVector(std::cout);
        }
    }

    // Schedule first global wakeup
    schedule(globalWakeupEvent, clockEdge(Cycles(1)));
}

void
GarnetNetwork::registerWakeup(int router_id, Tick tick)
{
    // We only need a locked RMW when multiple workers might race on
    // the same mask slot. Outside the parallel section the simulator
    // is single-threaded, so a plain load/store avoids the LOCK
    // prefix on every router scheduling - this fires per
    // input/output cycle and adds up.
    int idx = (tick / clockPeriod()) % 128;
    auto &slot = m_wakeup_mask[idx].mask;
    const uint64_t bit = 1ULL << router_id;
    auto &active = (idx < 64) ? m_active_lo : m_active_hi;
    const uint64_t active_bit = 1ULL << (idx & 63);
    if (Consumer::s_parallel_active.load(std::memory_order_relaxed)) {
        slot.fetch_or(bit, std::memory_order_relaxed);
        // Idempotent: if the bit is already set, the OR is a no-op.
        active.fetch_or(active_bit, std::memory_order_relaxed);
    } else {
        slot.store(slot.load(std::memory_order_relaxed) | bit,
                   std::memory_order_relaxed);
        active.store(active.load(std::memory_order_relaxed) | active_bit,
                     std::memory_order_relaxed);
    }
}

void
GarnetNetwork::runRoutersFromMask(uint64_t mask, Tick current_tick)
{
    // tryConsumeTick() returns true iff this router still has the
    // current tick pending (i.e. has not yet been serviced for
    // current_tick by an earlier Default_Pri Consumer event), in which
    // case we run it here. Combining the check + erase avoids the
    // double binary-search over m_wakeup_ticks that we used to do.
    const bool log = GEM5_UNLIKELY(debug::RubyNetwork);
    while (mask) {
        int id = __builtin_ctzll(mask);
        Router *r = m_routers[id];
        if (r->tryConsumeTick(current_tick)) {
            if (log) BufferedLogger::currentRouter = r;
            r->wakeup();
            if (log) BufferedLogger::currentRouter = nullptr;
        }
        mask &= mask - 1; // clear lowest set bit
    }
}

Tick
GarnetNetwork::computeNextWakeupTick(int current_idx, Tick current_tick,
                                     Tick clock_period) const
{
    // O(1) lookup using the m_active_{lo,hi} companion bitmap. We mask
    // out bits at and below current_idx in the "current half", then
    // pick the lowest remaining bit; if none, wrap to the other half.
    // Falls back to one cycle if the bitmap is entirely empty (rare,
    // since registerWakeup keeps it in sync with m_wakeup_mask).
    uint64_t lo = m_active_lo.load(std::memory_order_relaxed);
    uint64_t hi = m_active_hi.load(std::memory_order_relaxed);

    auto first_bit_after = [&](int after_idx) -> int {
        // Returns the lowest set-bit position > after_idx in (lo, hi),
        // or -1 if there is none in [after_idx+1, 127].
        if (after_idx < 63) {
            // (lo >> n) << n masks off the low n bits without invoking
            // shift-by-64 UB.
            int sh = after_idx + 1;
            uint64_t lo_masked = (lo >> sh) << sh;
            if (lo_masked) return __builtin_ctzll(lo_masked);
            if (hi) return 64 + __builtin_ctzll(hi);
        } else if (after_idx == 63) {
            if (hi) return 64 + __builtin_ctzll(hi);
        } else if (after_idx < 127) {
            int sh = (after_idx - 64) + 1;
            uint64_t hi_masked = (hi >> sh) << sh;
            if (hi_masked) return 64 + __builtin_ctzll(hi_masked);
        }
        return -1;
    };

    int idx = first_bit_after(current_idx);
    int dist;
    if (idx >= 0) {
        dist = idx - current_idx;
    } else {
        // Wrap: pick the smallest set bit in the bitmap.
        if (lo) {
            idx = __builtin_ctzll(lo);
        } else if (hi) {
            idx = 64 + __builtin_ctzll(hi);
        } else {
            // Nothing scheduled; fall back to the next cycle so that
            // any registers we might miss don't strand the simulator.
            return current_tick + clock_period;
        }
        dist = 128 - (current_idx - idx);
    }
    return current_tick + dist * clock_period;
}

void
GarnetNetwork::drainDeferredSchedules()
{
    for (auto &q : m_deferred_schedules) {
        if (q.entries.empty()) continue;
        for (auto &e : q.entries) {
            e.consumer->scheduleEventAbsolute(e.tick);
        }
        q.entries.clear();
    }
}

void
GarnetNetwork::globalWakeup()
{
    gem5::EventQueue *eq = gem5::curEventQueue();
    BufferedLogger *buffered_logger = nullptr;
    trace::Logger *old_logger = nullptr;

    if (GEM5_UNLIKELY(debug::RubyNetwork)) {
        buffered_logger = new BufferedLogger();
        old_logger = trace::getDebugLogger();
        trace::setDebugLogger(buffered_logger);
    }

    Tick current_tick = curTick();
    Tick clock_period = clockPeriod();
    int current_idx = (current_tick / clock_period) % 128;
    uint64_t mask = m_wakeup_mask[current_idx].mask.exchange(0,
        std::memory_order_relaxed);

    if (mask != 0) {
        // Slot is now empty; clear the companion active bit. Workers
        // (if any) only register for FUTURE ticks, so they never touch
        // this slot until it wraps 128 cycles later, by which time we
        // will have re-set the bit via registerWakeup.
        if (current_idx < 64) {
            m_active_lo.fetch_and(~(1ULL << current_idx),
                                  std::memory_order_relaxed);
        } else {
            m_active_hi.fetch_and(~(1ULL << (current_idx - 64)),
                                  std::memory_order_relaxed);
        }

        int popcount = __builtin_popcountll(mask);

        // For tiny per-cycle work lists the parallel-region round
        // trip costs more than just running the routers inline.
        if (m_num_threads <= 1 || popcount < m_parallel_threshold) {
            runRoutersFromMask(mask, current_tick);
        } else {
            Consumer::s_parallel_active.store(true,
                std::memory_order_relaxed);

            // Dispatch via OpenMP. The implicit barrier at end-of-
            // region replaces the explicit done-CV wait used by the
            // previous std::thread implementation. libgomp keeps the
            // thread pool parked between regions, so this is a single
            // wake-all on entry and a barrier release on exit - the
            // same shape as the hand-rolled CV pair.
            #pragma omp parallel num_threads(m_num_threads)
            {
                int tid = omp_get_thread_num();
                // Wire the per-thread deferred-schedule buffer. After
                // the warm-up pass in the constructor this is a no-op
                // (libgomp reuses OS threads, so thread_local data
                // persists), but it costs one store and is robust if
                // the pool is ever recycled.
                t_deferred_queue = &m_deferred_schedules[tid];

                // Make schedule() etc. find the right event queue if
                // a Consumer call inside Router::wakeup needs it.
                gem5::curEventQueue(eq);

                uint64_t my_mask =
                    mask & m_thread_router_mask[tid];
                if (my_mask) {
                    runRoutersFromMask(my_mask, current_tick);
                }
            }
            // Implicit barrier above; all workers are now quiescent.

            Consumer::s_parallel_active.store(false,
                std::memory_order_relaxed);

            // s_parallel_active is now false, so it is safe to call
            // into the EventQueue and Consumer mutexes without locks.
            // Drain the per-worker deferred-schedule buffers serially
            // on the master.
            drainDeferredSchedules();
        }
    }

    if (buffered_logger) {
        trace::setDebugLogger(old_logger);

        // Print buffered messages in order of router name
        for (auto const& [name, message] : buffered_logger->buffer) {
            old_logger->getOstream() << message;
        }
        delete buffered_logger;
    }

    Tick next_tick = computeNextWakeupTick(current_idx, current_tick,
                                           clock_period);
    schedule(globalWakeupEvent, next_tick);
}

/*
 * This function creates a link from the Network Interface (NI)
 * into the Network.
 * It creates a Network Link from the NI to a Router and a Credit Link from
 * the Router to the NI
*/

void
GarnetNetwork::makeExtInLink(NodeID global_src, SwitchID dest, BasicLink* link,
                             std::vector<NetDest>& routing_table_entry)
{
    NodeID local_src = getLocalNodeID(global_src);
    assert(local_src < m_nodes);

    GarnetExtLink* garnet_link = safe_cast<GarnetExtLink*>(link);

    // GarnetExtLink is bi-directional
    NetworkLink* net_link = garnet_link->m_network_links[LinkDirection_In];
    net_link->setType(EXT_IN_);
    CreditLink* credit_link = garnet_link->m_credit_links[LinkDirection_In];

    m_networklinks.push_back(net_link);
    m_creditlinks.push_back(credit_link);

    PortDirection dst_inport_dirn = "Local";

    m_max_vcs_per_vnet = std::max(m_max_vcs_per_vnet,
                             m_routers[dest]->get_vc_per_vnet());

    /*
     * We check if a bridge was enabled at any end of the link.
     * The bridge is enabled if either of clock domain
     * crossing (CDC) or Serializer-Deserializer(SerDes) unit is
     * enabled for the link at each end. The bridge encapsulates
     * the functionality for both CDC and SerDes and is a Consumer
     * object similiar to a NetworkLink.
     *
     * If a bridge was enabled we connect the NI and Routers to
     * bridge before connecting the link. Example, if an external
     * bridge is enabled, we would connect:
     * NI--->NetworkBridge--->GarnetExtLink---->Router
     */
    if (garnet_link->extBridgeEn) {
        DPRINTF(RubyNetwork, "Enable external bridge for %s\n",
            garnet_link->name());
        NetworkBridge *n_bridge = garnet_link->extNetBridge[LinkDirection_In];
        m_nis[local_src]->
        addOutPort(n_bridge,
                   garnet_link->extCredBridge[LinkDirection_In],
                   dest, m_routers[dest],m_routers[dest]->get_vc_per_vnet());
        m_networkbridges.push_back(n_bridge);
    } else {
        m_nis[local_src]->addOutPort(net_link, credit_link, dest, m_routers[dest],
            m_routers[dest]->get_vc_per_vnet());
    }

    if (garnet_link->intBridgeEn) {
        DPRINTF(RubyNetwork, "Enable internal bridge for %s\n",
            garnet_link->name());
        NetworkBridge *n_bridge = garnet_link->intNetBridge[LinkDirection_In];
        m_routers[dest]->
            addInPort(dst_inport_dirn,
                      n_bridge,
                      garnet_link->intCredBridge[LinkDirection_In]);
        m_networkbridges.push_back(n_bridge);
    } else {
        m_routers[dest]->addInPort(dst_inport_dirn, net_link, credit_link);
    }

}

/*
 * This function creates a link from the Network to a NI.
 * It creates a Network Link from a Router to the NI and
 * a Credit Link from NI to the Router
*/

void
GarnetNetwork::makeExtOutLink(SwitchID src, NodeID global_dest,
                              BasicLink* link,
                              std::vector<NetDest>& routing_table_entry)
{
    NodeID local_dest = getLocalNodeID(global_dest);
    assert(local_dest < m_nodes);
    assert(src < m_routers.size());
    assert(m_routers[src] != NULL);

    GarnetExtLink* garnet_link = safe_cast<GarnetExtLink*>(link);

    // GarnetExtLink is bi-directional
    NetworkLink* net_link = garnet_link->m_network_links[LinkDirection_Out];
    net_link->setType(EXT_OUT_);
    CreditLink* credit_link = garnet_link->m_credit_links[LinkDirection_Out];

    m_networklinks.push_back(net_link);
    m_creditlinks.push_back(credit_link);

    PortDirection src_outport_dirn = "Local";

    m_max_vcs_per_vnet = std::max(m_max_vcs_per_vnet,
                             m_routers[src]->get_vc_per_vnet());

    /*
     * We check if a bridge was enabled at any end of the link.
     * The bridge is enabled if either of clock domain
     * crossing (CDC) or Serializer-Deserializer(SerDes) unit is
     * enabled for the link at each end. The bridge encapsulates
     * the functionality for both CDC and SerDes and is a Consumer
     * object similiar to a NetworkLink.
     *
     * If a bridge was enabled we connect the NI and Routers to
     * bridge before connecting the link. Example, if an external
     * bridge is enabled, we would connect:
     * NI<---NetworkBridge<---GarnetExtLink<----Router
     */
    if (garnet_link->extBridgeEn) {
        DPRINTF(RubyNetwork, "Enable external bridge for %s\n",
            garnet_link->name());
        NetworkBridge *n_bridge = garnet_link->extNetBridge[LinkDirection_Out];
        m_nis[local_dest]->
            addInPort(n_bridge, garnet_link->extCredBridge[LinkDirection_Out]);
        m_networkbridges.push_back(n_bridge);
    } else {
        m_nis[local_dest]->addInPort(net_link, credit_link);
    }

    if (garnet_link->intBridgeEn) {
        DPRINTF(RubyNetwork, "Enable internal bridge for %s\n",
            garnet_link->name());
        NetworkBridge *n_bridge = garnet_link->intNetBridge[LinkDirection_Out];
        m_routers[src]->
            addOutPort(src_outport_dirn,
                       n_bridge,
                       routing_table_entry, link->m_weight,
                       garnet_link->intCredBridge[LinkDirection_Out],
                       m_routers[src]->get_vc_per_vnet());
        m_networkbridges.push_back(n_bridge);
    } else {
        m_routers[src]->
            addOutPort(src_outport_dirn, net_link,
                       routing_table_entry,
                       link->m_weight, credit_link,
                       m_routers[src]->get_vc_per_vnet());
    }
}

/*
 * This function creates an internal network link between two routers.
 * It adds both the network link and an opposite credit link.
*/

void
GarnetNetwork::makeInternalLink(SwitchID src, SwitchID dest, BasicLink* link,
                                std::vector<NetDest>& routing_table_entry,
                                PortDirection src_outport_dirn,
                                PortDirection dst_inport_dirn)
{
    GarnetIntLink* garnet_link = safe_cast<GarnetIntLink*>(link);

    // GarnetIntLink is unidirectional
    NetworkLink* net_link = garnet_link->m_network_link;
    net_link->setType(INT_);
    CreditLink* credit_link = garnet_link->m_credit_link;

    m_networklinks.push_back(net_link);
    m_creditlinks.push_back(credit_link);

    m_max_vcs_per_vnet = std::max(m_max_vcs_per_vnet,
                             std::max(m_routers[dest]->get_vc_per_vnet(),
                             m_routers[src]->get_vc_per_vnet()));

    /*
     * We check if a bridge was enabled at any end of the link.
     * The bridge is enabled if either of clock domain
     * crossing (CDC) or Serializer-Deserializer(SerDes) unit is
     * enabled for the link at each end. The bridge encapsulates
     * the functionality for both CDC and SerDes and is a Consumer
     * object similiar to a NetworkLink.
     *
     * If a bridge was enabled we connect the NI and Routers to
     * bridge before connecting the link. Example, if a source
     * bridge is enabled, we would connect:
     * Router--->NetworkBridge--->GarnetIntLink---->Router
     */
    if (garnet_link->dstBridgeEn) {
        DPRINTF(RubyNetwork, "Enable destination bridge for %s\n",
            garnet_link->name());
        NetworkBridge *n_bridge = garnet_link->dstNetBridge;
        m_routers[dest]->addInPort(dst_inport_dirn, n_bridge,
                                   garnet_link->dstCredBridge);
        m_networkbridges.push_back(n_bridge);
    } else {
        m_routers[dest]->addInPort(dst_inport_dirn, net_link, credit_link);
    }

    if (garnet_link->srcBridgeEn) {
        DPRINTF(RubyNetwork, "Enable source bridge for %s\n",
            garnet_link->name());
        NetworkBridge *n_bridge = garnet_link->srcNetBridge;
        m_routers[src]->
            addOutPort(src_outport_dirn, n_bridge,
                       routing_table_entry,
                       link->m_weight, garnet_link->srcCredBridge,
                       m_routers[dest]->get_vc_per_vnet());
        m_networkbridges.push_back(n_bridge);
    } else {
        m_routers[src]->addOutPort(src_outport_dirn, net_link,
                        routing_table_entry,
                        link->m_weight, credit_link,
                        m_routers[dest]->get_vc_per_vnet());
    }
}

// Total routers in the network
int
GarnetNetwork::getNumRouters()
{
    return m_routers.size();
}

// Get ID of router connected to a NI.
int
GarnetNetwork::get_router_id(int global_ni, int vnet)
{
    NodeID local_ni = getLocalNodeID(global_ni);

    return m_nis[local_ni]->get_router_id(vnet);
}

void
GarnetNetwork::regStats()
{
    Network::regStats();

    // Packets
    m_packets_received
        .init(m_virtual_networks)
        .name(name() + ".packets_received")
        .flags(statistics::pdf | statistics::total | statistics::nozero |
            statistics::oneline)
        ;

    m_packets_injected
        .init(m_virtual_networks)
        .name(name() + ".packets_injected")
        .flags(statistics::pdf | statistics::total | statistics::nozero |
            statistics::oneline)
        ;

    m_packet_network_latency
        .init(m_virtual_networks)
        .name(name() + ".packet_network_latency")
        .flags(statistics::oneline)
        ;

    m_packet_queueing_latency
        .init(m_virtual_networks)
        .name(name() + ".packet_queueing_latency")
        .flags(statistics::oneline)
        ;

    for (int i = 0; i < m_virtual_networks; i++) {
        m_packets_received.subname(i, csprintf("vnet-%i", i));
        m_packets_injected.subname(i, csprintf("vnet-%i", i));
        m_packet_network_latency.subname(i, csprintf("vnet-%i", i));
        m_packet_queueing_latency.subname(i, csprintf("vnet-%i", i));
    }

    m_avg_packet_vnet_latency
        .name(name() + ".average_packet_vnet_latency")
        .flags(statistics::oneline);
    m_avg_packet_vnet_latency =
        m_packet_network_latency / m_packets_received;

    m_avg_packet_vqueue_latency
        .name(name() + ".average_packet_vqueue_latency")
        .flags(statistics::oneline);
    m_avg_packet_vqueue_latency =
        m_packet_queueing_latency / m_packets_received;

    m_avg_packet_network_latency
        .name(name() + ".average_packet_network_latency");
    m_avg_packet_network_latency =
        sum(m_packet_network_latency) / sum(m_packets_received);

    m_avg_packet_queueing_latency
        .name(name() + ".average_packet_queueing_latency");
    m_avg_packet_queueing_latency
        = sum(m_packet_queueing_latency) / sum(m_packets_received);

    m_avg_packet_latency
        .name(name() + ".average_packet_latency");
    m_avg_packet_latency
        = m_avg_packet_network_latency + m_avg_packet_queueing_latency;

    // Flits
    m_flits_received
        .init(m_virtual_networks)
        .name(name() + ".flits_received")
        .flags(statistics::pdf | statistics::total | statistics::nozero |
            statistics::oneline)
        ;

    m_flits_injected
        .init(m_virtual_networks)
        .name(name() + ".flits_injected")
        .flags(statistics::pdf | statistics::total | statistics::nozero |
            statistics::oneline)
        ;

    m_flit_network_latency
        .init(m_virtual_networks)
        .name(name() + ".flit_network_latency")
        .flags(statistics::oneline)
        ;

    m_flit_queueing_latency
        .init(m_virtual_networks)
        .name(name() + ".flit_queueing_latency")
        .flags(statistics::oneline)
        ;

    for (int i = 0; i < m_virtual_networks; i++) {
        m_flits_received.subname(i, csprintf("vnet-%i", i));
        m_flits_injected.subname(i, csprintf("vnet-%i", i));
        m_flit_network_latency.subname(i, csprintf("vnet-%i", i));
        m_flit_queueing_latency.subname(i, csprintf("vnet-%i", i));
    }

    m_avg_flit_vnet_latency
        .name(name() + ".average_flit_vnet_latency")
        .flags(statistics::oneline);
    m_avg_flit_vnet_latency = m_flit_network_latency / m_flits_received;

    m_avg_flit_vqueue_latency
        .name(name() + ".average_flit_vqueue_latency")
        .flags(statistics::oneline);
    m_avg_flit_vqueue_latency =
        m_flit_queueing_latency / m_flits_received;

    m_avg_flit_network_latency
        .name(name() + ".average_flit_network_latency");
    m_avg_flit_network_latency =
        sum(m_flit_network_latency) / sum(m_flits_received);

    m_avg_flit_queueing_latency
        .name(name() + ".average_flit_queueing_latency");
    m_avg_flit_queueing_latency =
        sum(m_flit_queueing_latency) / sum(m_flits_received);

    m_avg_flit_latency
        .name(name() + ".average_flit_latency");
    m_avg_flit_latency =
        m_avg_flit_network_latency + m_avg_flit_queueing_latency;


    // Hops
    m_avg_hops.name(name() + ".average_hops");
    m_avg_hops = m_total_hops / sum(m_flits_received);

    // Links
    m_total_ext_in_link_utilization
        .name(name() + ".ext_in_link_utilization");
    m_total_ext_out_link_utilization
        .name(name() + ".ext_out_link_utilization");
    m_total_int_link_utilization
        .name(name() + ".int_link_utilization");
    m_average_link_utilization
        .name(name() + ".avg_link_utilization");
    m_average_vc_load
        .init(m_virtual_networks * m_max_vcs_per_vnet)
        .name(name() + ".avg_vc_load")
        .flags(statistics::pdf | statistics::total | statistics::nozero |
            statistics::oneline)
        ;

    // Traffic distribution
    for (int source = 0; source < m_routers.size(); ++source) {
        m_data_traffic_distribution.push_back(
            std::vector<statistics::Scalar *>());
        m_ctrl_traffic_distribution.push_back(
            std::vector<statistics::Scalar *>());

        for (int dest = 0; dest < m_routers.size(); ++dest) {
            statistics::Scalar *data_packets = new statistics::Scalar();
            statistics::Scalar *ctrl_packets = new statistics::Scalar();

            data_packets->name(name() + ".data_traffic_distribution." + "n" +
                    std::to_string(source) + "." + "n" + std::to_string(dest));
            m_data_traffic_distribution[source].push_back(data_packets);

            ctrl_packets->name(name() + ".ctrl_traffic_distribution." + "n" +
                    std::to_string(source) + "." + "n" + std::to_string(dest));
            m_ctrl_traffic_distribution[source].push_back(ctrl_packets);
        }
    }
}

void
GarnetNetwork::collateStats()
{
    RubySystem *rs = params().ruby_system;
    double time_delta = double(curCycle() - rs->getStartCycle());

    for (int i = 0; i < m_networklinks.size(); i++) {
        link_type type = m_networklinks[i]->getType();
        int activity = m_networklinks[i]->getLinkUtilization();

        if (type == EXT_IN_)
            m_total_ext_in_link_utilization += activity;
        else if (type == EXT_OUT_)
            m_total_ext_out_link_utilization += activity;
        else if (type == INT_)
            m_total_int_link_utilization += activity;

        m_average_link_utilization +=
            (double(activity) / time_delta);

        std::vector<unsigned int> vc_load = m_networklinks[i]->getVcLoad();
        for (int j = 0; j < vc_load.size(); j++) {
            m_average_vc_load[j] += ((double)vc_load[j] / time_delta);
        }
    }

    // Ask the routers to collate their statistics
    for (int i = 0; i < m_routers.size(); i++) {
        m_routers[i]->collateStats();
    }

    //Collect injected and recieved flits from each network interface
    for (int ni_id = 0; ni_id < m_nodes; ++ni_id) {
        auto* ni = m_nis[ni_id];

        const auto& inj = ni->getLocalFlitsInjected();
        const auto& rec = ni->getLocalFlitsReceived();
        const auto& lat = ni->getLocalFlitNetworkLatency();
        const auto& qlat = ni->getLocalFlitQueueingLatency();
        const auto& pkts_inj = ni->getLocalPacketsInjected();
        const auto& pkts_recv = ni->getLocalPacketsReceived();
        const auto& pkt_lat = ni->getLocalPacketNetworkLatency();
        const auto& pkt_qlat = ni->getLocalPacketQueueingLatency();

        for (int v = 0; v < inj.size(); ++v) {
            m_flits_injected[v] += inj[v];
            m_flits_received[v] += rec[v];
            m_flit_network_latency[v] += lat[v];
            m_flit_queueing_latency[v]  += qlat[v];
            m_packets_injected[v] += pkts_inj[v];
            m_packets_received[v] += pkts_recv[v];
            m_packet_network_latency[v] += pkt_lat[v];
            m_packet_queueing_latency[v] += pkt_qlat[v];
        }
        m_total_hops += ni->getLocalTotalHops();
    }
}

void
GarnetNetwork::resetStats()
{
    for (int i = 0; i < m_routers.size(); i++) {
        m_routers[i]->resetStats();
    }
    for (int i = 0; i < m_networklinks.size(); i++) {
        m_networklinks[i]->resetStats();
    }
    for (int i = 0; i < m_creditlinks.size(); i++) {
        m_creditlinks[i]->resetStats();
    }
}

void
GarnetNetwork::print(std::ostream& out) const
{
    out << "[GarnetNetwork]";
}

void
GarnetNetwork::update_traffic_distribution(RouteInfo route)
{
    int src_node = route.src_router;
    int dest_node = route.dest_router;
    int vnet = route.vnet;

    // Called from NetworkInterface::flitisizeMessage which fires on the
    // main event queue thread only, so no locking is required.
    if (m_vnet_type[vnet] == DATA_VNET_)
        (*m_data_traffic_distribution[src_node][dest_node])++;
    else
        (*m_ctrl_traffic_distribution[src_node][dest_node])++;
}

bool
GarnetNetwork::functionalRead(Packet *pkt, WriteMask &mask)
{
    bool read = false;
    for (unsigned int i = 0; i < m_routers.size(); i++) {
        if (m_routers[i]->functionalRead(pkt, mask))
            read = true;
    }

    for (unsigned int i = 0; i < m_nis.size(); ++i) {
        if (m_nis[i]->functionalRead(pkt, mask))
            read = true;
    }

    for (unsigned int i = 0; i < m_networklinks.size(); ++i) {
        if (m_networklinks[i]->functionalRead(pkt, mask))
            read = true;
    }

    for (unsigned int i = 0; i < m_networkbridges.size(); ++i) {
        if (m_networkbridges[i]->functionalRead(pkt, mask))
            read = true;
    }

    return read;
}

uint32_t
GarnetNetwork::functionalWrite(Packet *pkt)
{
    uint32_t num_functional_writes = 0;

    for (unsigned int i = 0; i < m_routers.size(); i++) {
        num_functional_writes += m_routers[i]->functionalWrite(pkt);
    }

    for (unsigned int i = 0; i < m_nis.size(); ++i) {
        num_functional_writes += m_nis[i]->functionalWrite(pkt);
    }

    for (unsigned int i = 0; i < m_networklinks.size(); ++i) {
        num_functional_writes += m_networklinks[i]->functionalWrite(pkt);
    }

    return num_functional_writes;
}

} // namespace garnet
} // namespace ruby
} // namespace gem5
