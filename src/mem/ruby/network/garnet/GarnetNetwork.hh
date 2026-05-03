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


#ifndef __MEM_RUBY_NETWORK_GARNET_0_GARNETNETWORK_HH__
#define __MEM_RUBY_NETWORK_GARNET_0_GARNETNETWORK_HH__

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "mem/ruby/common/Consumer.hh"
#include "mem/ruby/network/Network.hh"
#include "mem/ruby/network/fault_model/FaultModel.hh"
#include "mem/ruby/network/garnet/CommonTypes.hh"
#include "params/GarnetNetwork.hh"

namespace gem5
{

namespace ruby
{

class FaultModel;
class NetDest;

namespace garnet
{

class NetworkInterface;
class Router;
class NetworkLink;
class NetworkBridge;
class CreditLink;

class GarnetNetwork : public Network
{
  public:
    typedef GarnetNetworkParams Params;
    GarnetNetwork(const Params &p);
    ~GarnetNetwork();

    void init();

    EventFunctionWrapper globalWakeupEvent;
    void globalWakeup();

    const char *garnetVersion = "3.0";

    // Configuration (set externally)

    // for 2D topology
    int getNumRows() const { return m_num_rows; }
    int getNumCols() { return m_num_cols; }

    // for network
    uint32_t getNiFlitSize() const { return m_ni_flit_size; }
    uint32_t getBuffersPerDataVC() { return m_buffers_per_data_vc; }
    uint32_t getBuffersPerCtrlVC() { return m_buffers_per_ctrl_vc; }
    int getRoutingAlgorithm() const { return m_routing_algorithm; }

    bool isFaultModelEnabled() const { return m_enable_fault_model; }
    FaultModel* fault_model;


    // Internal configuration
    bool isVNetOrdered(int vnet) const { return m_ordered[vnet]; }
    VNET_type
    get_vnet_type(int vnet)
    {
        return m_vnet_type[vnet];
    }
    int getNumRouters();
    static std::mutex g_scheduling_mutex;
    int get_router_id(int ni, int vnet);

    void registerWakeup(int router_id, Tick tick);

    // ---- Deferred link-schedule machinery (parallel mode) ------------
    //
    // Worker threads cannot safely call EventQueue::schedule(), which
    // is what NetworkLink/CreditLink::scheduleEventAbsolute() ends up
    // invoking. Instead, while running router wakeups in parallel we
    // record (consumer, tick) pairs into a per-worker thread-local
    // buffer; once all workers have finished the master drains the
    // buffers serially. This removes the formerly-global
    // g_scheduling_mutex from the hot path completely.
    struct DeferredEntry {
        Consumer *consumer;
        Tick tick;
    };
    struct alignas(64) DeferredQueue {
        std::vector<DeferredEntry> entries;
    };
    // Per-worker deferred buffers; main thread holds index 0 of the
    // pool implicitly (it never defers because it never schedules from
    // outside an EventQueue context).
    static thread_local DeferredQueue *t_deferred_queue;
    inline static void
    deferOrSchedule(Consumer *c, Tick tick)
    {
        if (t_deferred_queue) {
            t_deferred_queue->entries.push_back({c, tick});
        } else {
            c->scheduleEventAbsolute(tick);
        }
    }


    // Methods used by Topology to setup the network
    void makeExtOutLink(SwitchID src, NodeID dest, BasicLink* link,
                     std::vector<NetDest>& routing_table_entry);
    void makeExtInLink(NodeID src, SwitchID dest, BasicLink* link,
                    std::vector<NetDest>& routing_table_entry);
    void makeInternalLink(SwitchID src, SwitchID dest, BasicLink* link,
                          std::vector<NetDest>& routing_table_entry,
                          PortDirection src_outport_dirn,
                          PortDirection dest_inport_dirn);

    bool functionalRead(Packet *pkt, WriteMask &mask);
    //! Function for performing a functional write. The return value
    //! indicates the number of messages that were written.
    uint32_t functionalWrite(Packet *pkt);

    // Stats
    void collateStats();
    void regStats();
    void resetStats();
    void print(std::ostream& out) const;

    // increment counters
    void increment_injected_packets(int vnet) { 
        //std::lock_guard<std::mutex> lock(stats_mutex);
        //m_packets_injected[vnet]++; 
    }
    void increment_received_packets(int vnet) { 
        //std::lock_guard<std::mutex> lock(stats_mutex);
        //m_packets_received[vnet]++; 
    }

    void
    increment_packet_network_latency(Tick latency, int vnet)
    {
        //std::lock_guard<std::mutex> lock(stats_mutex);
        //m_packet_network_latency[vnet] += latency;
    }

    void
    increment_packet_queueing_latency(Tick latency, int vnet)
    {
        //std::lock_guard<std::mutex> lock(stats_mutex);
        //m_packet_queueing_latency[vnet] += latency;
    }

    void increment_injected_flits(int vnet) { 
        //std::lock_guard<std::mutex> lock(stats_mutex);
        //m_flits_injected[vnet]++; 
    }
    void increment_received_flits(int vnet) { 
        //std::lock_guard<std::mutex> lock(stats_mutex);
        //m_flits_received[vnet]++; 
    }

    void
    increment_flit_network_latency(Tick latency, int vnet)
    {
        //std::lock_guard<std::mutex> lock(stats_mutex);
        //m_flit_network_latency[vnet] += latency;
    }

    void
    increment_flit_queueing_latency(Tick latency, int vnet)
    {
        //std::lock_guard<std::mutex> lock(stats_mutex);
        //m_flit_queueing_latency[vnet] += latency;
    }

    void
    increment_total_hops(int hops)
    {
        //std::lock_guard<std::mutex> lock(stats_mutex);
        //m_total_hops += hops;
    }

    void update_traffic_distribution(RouteInfo route);
    // Packet IDs are only ever allocated from the main thread (the
    // network interfaces that call this fire on the main event queue,
    // not from worker threads), so a plain int suffices.
    int getNextPacketID() { return m_next_packet_id++; }

  protected:
    // Configuration
    int m_num_rows;
    int m_num_cols;
    uint32_t m_ni_flit_size;
    uint32_t m_max_vcs_per_vnet;
    uint32_t m_buffers_per_ctrl_vc;
    uint32_t m_buffers_per_data_vc;
    int m_routing_algorithm;
    bool m_enable_fault_model;

    // Statistical variables
    statistics::Vector m_packets_received;
    statistics::Vector m_packets_injected;
    statistics::Vector m_packet_network_latency;
    statistics::Vector m_packet_queueing_latency;

    statistics::Formula m_avg_packet_vnet_latency;
    statistics::Formula m_avg_packet_vqueue_latency;
    statistics::Formula m_avg_packet_network_latency;
    statistics::Formula m_avg_packet_queueing_latency;
    statistics::Formula m_avg_packet_latency;

    statistics::Vector m_flits_received;
    statistics::Vector m_flits_injected;
    statistics::Vector m_flit_network_latency;
    statistics::Vector m_flit_queueing_latency;

    statistics::Formula m_avg_flit_vnet_latency;
    statistics::Formula m_avg_flit_vqueue_latency;
    statistics::Formula m_avg_flit_network_latency;
    statistics::Formula m_avg_flit_queueing_latency;
    statistics::Formula m_avg_flit_latency;

    statistics::Scalar m_total_ext_in_link_utilization;
    statistics::Scalar m_total_ext_out_link_utilization;
    statistics::Scalar m_total_int_link_utilization;
    statistics::Scalar m_average_link_utilization;
    statistics::Vector m_average_vc_load;

    statistics::Scalar  m_total_hops;
    statistics::Formula m_avg_hops;

    std::vector<std::vector<statistics::Scalar *>> m_data_traffic_distribution;
    std::vector<std::vector<statistics::Scalar *>> m_ctrl_traffic_distribution;

  private:
    GarnetNetwork(const GarnetNetwork& obj);
    GarnetNetwork& operator=(const GarnetNetwork& obj);

    struct alignas(64) PaddedMask {
        std::atomic<uint64_t> mask;

        PaddedMask() : mask(0) {}
    };

    // Ring buffer of pending-router bitmasks indexed by (tick / period) %
    // 128. Each mask bit is router_id; up to 64 routers per network are
    // supported by this scheme.
    PaddedMask m_wakeup_mask[128];

    // Companion bitmap: bit i is 1 iff m_wakeup_mask[i].mask != 0.
    // Lets computeNextWakeupTick() find the next non-empty slot in O(1)
    // (ctz on a 128-bit value) instead of scanning all 128 entries.
    // Invariant: registerWakeup sets the bit when populating a slot;
    // globalWakeup clears it after the exchange empties the slot.
    alignas(64) std::atomic<uint64_t> m_active_lo{0};  // slots 0..63
    alignas(64) std::atomic<uint64_t> m_active_hi{0};  // slots 64..127

    std::vector<VNET_type > m_vnet_type;
    std::vector<Router *> m_routers;   // All Routers in Network
    std::vector<NetworkLink *> m_networklinks; // All flit links in the network
    std::vector<NetworkBridge *> m_networkbridges; // All network bridges
    std::vector<CreditLink *> m_creditlinks; // All credit links in the network
    std::vector<NetworkInterface *> m_nis;   // All NI's in Network
    int m_next_packet_id; // packet id allocator (main thread only)

    // ----- Parallel router-wakeup machinery ---------------------------
    //
    // Workers are persistent and statically own a subset of routers
    // (router_id % m_num_threads == thread_id). Each cycle the master
    // publishes (tick, mask, event_queue), wakes all workers via a
    // single notify_all, then waits on m_done_cv until every worker has
    // bumped m_workers_finished.
    int m_num_threads;
    int m_parallel_threshold;
    std::vector<std::thread> m_worker_threads;
    std::vector<uint64_t> m_thread_router_mask; // size = m_num_threads
    std::atomic<bool> m_terminate_workers;

    // Master -> workers
    std::mutex m_dispatch_mutex;
    std::condition_variable m_dispatch_cv;
    uint64_t m_dispatch_generation; // protected by m_dispatch_mutex

    // Workers -> master
    std::mutex m_done_mutex;
    std::condition_variable m_done_cv;
    std::atomic<int> m_workers_finished;

    // Per-cycle dispatch payload (written by master before notify_all,
    // read by workers after wait). No further sync needed because the
    // CV mutex provides happens-before.
    Tick m_current_tick;
    gem5::EventQueue *m_current_event_queue;
    uint64_t m_current_mask;

    void workerLoop(int thread_id);
    void runRoutersFromMask(uint64_t mask, Tick current_tick);
    Tick computeNextWakeupTick(int current_idx, Tick current_tick,
                               Tick clock_period) const;
    void drainDeferredSchedules();

    // Per-worker buffers (size m_num_threads). Each worker writes only
    // to its own index, the master drains all of them after the join.
    std::vector<DeferredQueue> m_deferred_schedules;
};

inline std::ostream&
operator<<(std::ostream& out, const GarnetNetwork& obj)
{
    obj.print(out);
    out << std::flush;
    return out;
}

} // namespace garnet
} // namespace ruby
} // namespace gem5

#endif //__MEM_RUBY_NETWORK_GARNET_0_GARNETNETWORK_HH__
