/*
 * Copyright (c) 2020 Inria
 * Copyright (c) 2016 Georgia Institute of Technology
 * Copyright (c) 2008 Princeton University
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


#include "mem/ruby/network/garnet/InputUnit.hh"

#include "debug/RubyNetwork.hh"
#include "mem/ruby/network/garnet/Credit.hh"
#include "mem/ruby/network/garnet/Router.hh"

namespace gem5
{

namespace ruby
{

namespace garnet
{

InputUnit::InputUnit(int id, PortDirection direction, Router *router)
  : Consumer(router), m_router(router), m_id(id), m_direction(direction),
    m_vc_per_vnet(m_router->get_vc_per_vnet())
{
    const int m_num_vcs = m_router->get_num_vcs();

    // Safety check for vnet division to avoid division by zero
    int num_vnets = (m_vc_per_vnet > 0) ? (m_num_vcs / m_vc_per_vnet) : 0;
    m_num_buffer_reads.resize(num_vnets, 0);
    m_num_buffer_writes.resize(num_vnets, 0);

    // Instantiating the virtual channels
    virtualChannels.reserve(m_num_vcs);
    for (int i=0; i < m_num_vcs; i++) {
        virtualChannels.emplace_back();
    }
}

void
InputUnit::wakeup()
{
    if (m_in_link->isReady(m_router->clockEdge())) {
        flit *t_flit = m_in_link->consumeLink();
        
        // Define VC first so it's available for the print and the credit
        int vc = t_flit->get_vc(); 
        t_flit->increment_hops();

        // --- THE TRACER PRINT (Now Safe) ---
        std::cout << "[TICK " << m_router->clockEdge() << "] ROUTER " << m_router->get_id() 
                  << " RECEIVED Flit ID: " << t_flit->get_id() 
                  << " on VC: " << vc << std::endl;

        if ((t_flit->get_type() == HEAD_) || (t_flit->get_type() == HEAD_TAIL_)) {
            set_vc_active(vc, m_router->clockEdge());
            int outport = m_router->route_compute(t_flit->get_route(), m_id, m_direction);
            grant_outport(vc, outport);
            
            std::cout << "  -> Flit " << t_flit->get_id() << " assigned Outport: " << outport << std::endl;
        }

        virtualChannels[vc].insertFlit(t_flit);
        
        // Send credit back to the previous router
        //increment_credit(vc, false, m_router->clockEdge());

        // Advance to SA_
        Cycles pipe_stages = m_router->get_pipe_stages();
        Tick ready_time = (pipe_stages == 1) ? m_router->clockEdge() : m_router->clockEdge(pipe_stages - Cycles(1));
        t_flit->advance_stage(SA_, ready_time);
        
        m_router->schedule_wakeup(Cycles(pipe_stages - Cycles(1)));
    }
}



void
InputUnit::increment_credit(int in_vc, bool free_signal, Tick curTime)
{
    Credit *t_credit = new Credit(in_vc, free_signal, curTime);
    
    // Use a simple lock to prevent corruption
    m_credit_lock.lock(); 
    m_staged_credits.push_back(t_credit);
    std::cout << "[STAGED] Router " << m_router->get_id() 
              << " | VC " << in_vc << " | Staged Count: " 
              << m_staged_credits.size() << std::endl;
    m_credit_lock.unlock();
}

void
InputUnit::updatePhase()
{
    if (!m_staged_credits.empty()) {
        for (Credit* c : m_staged_credits) {
            creditQueue.insert(c);
        }
        m_staged_credits.clear();
        m_staged_credit_wakeup = true;
    }
}




void
InputUnit::flushStagedEvents()
{
    std::lock_guard<std::mutex> lock(m_credit_lock); 
    if (!m_staged_credits.empty()) {
        for (auto& t_credit : m_staged_credits) {
            m_credit_link->getBuffer()->insert(static_cast<flit *>(t_credit));
        }
        m_staged_credits.clear();
        
        // FIX: Use alreadyScheduled with a specific tick. 
        // This is the standard way for a Ruby Consumer (like a Link).
        //Tick nextTick = m_router->clockEdge(Cycles(1));
        Tick nextTick = m_router->clockEdge() + m_router->cyclesToTicks(Cycles(1));
        if (!m_credit_link->alreadyScheduled(nextTick)) {
            m_credit_link->scheduleEventAbsolute(nextTick);
        }
    }
}

bool
InputUnit::functionalRead(Packet *pkt, WriteMask &mask)
{
    bool read = false;
    for (auto& virtual_channel : virtualChannels) {
        if (virtual_channel.functionalRead(pkt, mask))
            read = true;
    }
    return read;
}

uint32_t
InputUnit::functionalWrite(Packet *pkt)
{
    uint32_t num_functional_writes = 0;
    for (auto& virtual_channel : virtualChannels) {
        num_functional_writes += virtual_channel.functionalWrite(pkt);
    }
    return num_functional_writes;
}

void
InputUnit::resetStats()
{
    for (int j = 0; j < m_num_buffer_reads.size(); j++) {
        m_num_buffer_reads[j] = 0;
        m_num_buffer_writes[j] = 0;
    }
}

} // namespace garnet
} // namespace ruby
} // namespace gem5
