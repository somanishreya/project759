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


#include "mem/ruby/network/garnet/SwitchAllocator.hh"

#include "debug/RubyNetwork.hh"
#include "mem/ruby/network/garnet/GarnetNetwork.hh"
#include "mem/ruby/network/garnet/InputUnit.hh"
#include "mem/ruby/network/garnet/OutputUnit.hh"
#include "mem/ruby/network/garnet/Router.hh"

namespace gem5
{
namespace ruby
{
namespace garnet
{



SwitchAllocator::SwitchAllocator(Router *router)
    : Consumer(router)
{
    m_router = router;
    m_num_vcs = m_router->get_num_vcs();
    m_vc_per_vnet = m_router->get_vc_per_vnet();

    // Ensure these are initialized!
    m_num_inports = m_router->get_num_inports();
    m_num_outports = m_router->get_num_outports();

    m_input_arbiter_activity = 0;
    m_output_arbiter_activity = 0;
    m_last_cycle = Tick(-1);

    // Now reserve based on the initialized value
    m_staged_decisions.reserve(m_num_inports);
}

void
SwitchAllocator::init()
{
    m_num_inports = m_router->get_num_inports();
    m_num_outports = m_router->get_num_outports();

    // Use clear() and resize() to ensure no junk data
    m_round_robin_inport.assign(m_num_outports, 0);
    m_round_robin_invc.assign(m_num_inports, 0);
    m_port_requests.assign(m_num_inports, -1);
    m_vc_winners.assign(m_num_inports, -1);
}



void
SwitchAllocator::wakeup()
{
    // --- THE FIX: TICK GUARD ---
    // If we already did work this cycle, STOP.
    if (m_last_cycle == m_router->curCycle()) {
        return;
    }
    m_last_cycle = m_router->curCycle();
    // ---------------------------

    arbitrate_inports();
    arbitrate_outports();

    updatePhase(); 

    clear_request_vector();
    check_for_wakeup();
}

void
SwitchAllocator::arbitrate_inports()
{
    for (int inport = 0; inport < m_num_inports; inport++) {
        auto input_unit = m_router->getInputUnit(inport);
        if (!input_unit || input_unit->get_num_vcs() == 0) continue;

        int actual_vcs = input_unit->get_num_vcs();
        int invc = m_round_robin_invc[inport] % actual_vcs;

        for (int invc_iter = 0; invc_iter < actual_vcs; invc_iter++) {
            // DEBUG: See if there is a flit waiting for SA
            if (input_unit->need_stage(invc, SA_, m_router->clockEdge())) {
                int outport = input_unit->get_outport(invc);
                int outvc = input_unit->get_outvc(invc);

                if (send_allowed(inport, invc, outport, outvc)) {
                    // Success! This inport is making a request
                    m_port_requests[inport] = outport;
                    m_vc_winners[inport] = invc;
                    break;
                } else {
                    // DEBUG: Request exists but outport/outvc is blocked (no credits)
                    // std::cout << "Router " << m_router->get_id() << " Inport " << inport 
                    //           << " blocked by send_allowed for outport " << outport << std::endl;
                }
            }
            invc = (invc + 1) % actual_vcs;
        }
    }
}

void
SwitchAllocator::arbitrate_outports()
{
    Tick current_time = m_router->clockEdge();
    // This MUST stay outside the outport loop to work
    std::vector<bool> inport_busy(m_num_inports, false);

    for (int outport = 0; outport < m_num_outports; outport++) {
        int inport = m_round_robin_inport[outport];

        for (int inport_iter = 0; inport_iter < m_num_inports; inport_iter++) {
            
            // CRITICAL CHECK: Has another outport already taken this inport?
            if (!inport_busy[inport] && m_port_requests[inport] == outport) {
                
                auto input_unit = m_router->getInputUnit(inport);
                int invc = m_vc_winners[inport];

                // DOUBLE CHECK: Is the VC actually ready? 
                // If it was already staged by Port 2, it shouldn't be ready for Port 3.
                if (input_unit->get_vc_ptr(invc)->get_state() == ACTIVE_) {
                    
                    int outvc = input_unit->get_outvc(invc);
                    if (outvc == -1) outvc = vc_allocate(outport, inport, invc);

                    if (outvc != -1 && m_router->getOutputUnit(outport)->has_credit(outvc)) {
                        
                        // 1. Mark port as busy so no other outport can touch it
                        inport_busy[inport] = true;
                        
                        // 2. Mark the VC as "IDLE" or "WAITING" so the next loop 
                        // for a different outport doesn't see it as a candidate.
                        input_unit->get_vc_ptr(invc)->set_state(IDLE_, current_time);

                        m_router->getOutputUnit(outport)->decrement_credit(outvc);
                        m_staged_decisions.push_back({inport, invc, outport, outvc});
                        
                        break; // Move to the next outport
                    }
                }
            }
            inport = (inport + 1) % m_num_inports;
        }
    }
}





void
SwitchAllocator::updatePhase()
{
    if (m_staged_decisions.empty()) return;

    for (auto& decision : m_staged_decisions) {
        auto input_unit = m_router->getInputUnit(decision.inport);
        auto output_unit = m_router->getOutputUnit(decision.outport);

        // This call BOTH returns the pointer AND removes it from the buffer
        flit *t_flit = input_unit->get_vc_ptr(decision.invc)->getTopFlit();
        
        if (t_flit) {
            // Now we use the pointer we just grabbed
            bool is_tail = (t_flit->get_type() == TAIL_ || 
                            t_flit->get_type() == HEAD_TAIL_);
            
            // 1. Tell upstream a slot is free
            input_unit->increment_credit(decision.invc, is_tail, m_router->clockEdge());

            // 2. Execute crossing logic
            std::cout << "EXECUTE: Router " << m_router->get_id() 
                      << " | Flit " << t_flit->get_id() 
                      << " crossing to Port " << decision.outport << std::endl;
            
            m_router->grant_switch(decision.inport, t_flit); 

            t_flit->set_outport(decision.outport);
            t_flit->set_vc(decision.outvc);
            t_flit->advance_stage(ST_, m_router->clockEdge());

            // 3. Move flit to OutputUnit
            output_unit->insert_flit(t_flit);
            output_unit->wakeup(); 
            
        } else {
            // This should only happen if another thread or function 
            // emptied the buffer unexpectedly
            std::cerr << "ERROR: VC " << decision.invc << " was empty!" << std::endl;
        }
    }
    m_staged_decisions.clear();
    std::cout << "CLEARING" << std::endl;
}

bool
SwitchAllocator::send_allowed(int inport, int invc, int outport, int outvc)
{
    int vnet = get_vnet(invc);
    auto output_unit = m_router->getOutputUnit(outport);
    bool has_outvc = (outvc != -1);
    bool has_credit = false;

    if (!has_outvc) {
        if (output_unit->has_free_vc(vnet)) {
            has_outvc = true;
            has_credit = true;
        }
    } else {
        has_credit = output_unit->has_credit(outvc);
    }

    if (!has_outvc || !has_credit) return false;

    // Protocol ordering check
    if ((m_router->get_net_ptr())->isVNetOrdered(vnet)) {
        auto input_unit = m_router->getInputUnit(inport);
        Tick t_enqueue_time = input_unit->get_enqueue_time(invc);
        Tick current_time = m_router->clockEdge();

        int vc_base = vnet * m_vc_per_vnet;
        for (int vc_offset = 0; vc_offset < m_vc_per_vnet; vc_offset++) {
            int temp_vc = vc_base + vc_offset;
            if (input_unit->need_stage(temp_vc, SA_, current_time) &&
               (input_unit->get_outport(temp_vc) == outport) &&
               (input_unit->get_enqueue_time(temp_vc) < t_enqueue_time)) {
                return false;
            }
        }
    }
    return true;
}

int
SwitchAllocator::vc_allocate(int outport, int inport, int invc)
{
    int outvc = m_router->getOutputUnit(outport)->select_free_vc(get_vnet(invc));
    assert(outvc != -1);
    m_router->getInputUnit(inport)->grant_outvc(invc, outvc);
    return outvc;
}

void
SwitchAllocator::check_for_wakeup()
{
    Tick nextCycle = m_router->clockEdge(Cycles(1));

    if (m_router->alreadyScheduled(nextCycle)) return;

    for (int i = 0; i < m_num_inports; i++) {
        auto input_unit = m_router->getInputUnit(i);
        for (int j = 0; j < m_num_vcs; j++) {
            if (input_unit->need_stage(j, SA_, nextCycle)) {
                m_router->schedule_wakeup(Cycles(1));
                return;
            }
        }
    }
}

int SwitchAllocator::get_vnet(int invc) { return invc / m_vc_per_vnet; }

void
SwitchAllocator::clear_request_vector()
{
    std::fill(m_port_requests.begin(), m_port_requests.end(), -1);
}

void SwitchAllocator::resetStats() { m_input_arbiter_activity = 0; m_output_arbiter_activity = 0; }

} // namespace garnet
} // namespace ruby
} // namespace gem5
