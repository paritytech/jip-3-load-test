//! Plausible cost structures for pipeline events.
//!
//! Invariants that make the numbers look real (see the emission model §7):
//! `load_ns <= total.ns`, host-call breakdown sums to less than the total,
//! `gas` deterministic-looking and within protocol limits, `ns` tied to the
//! stage latency the simulation actually played out.

use jam_std_common::telemetry::{
    AccumulateCost, AccumulateHostCallCost, ExecCost, IsAuthorizedCost, RefineCost,
    RefineHostCallCost,
};

pub fn exec(gas: u64, ns: u64) -> ExecCost {
    ExecCost { gas, ns }
}

/// Authorization cost: small gas, sub-millisecond-to-millisecond runtime.
pub fn is_authorized_cost(gas: u64, ns: u64) -> IsAuthorizedCost {
    IsAuthorizedCost {
        total: exec(gas, ns),
        load_ns: ns * 28 / 100,
        host_call: exec(gas / 12, ns * 8 / 100),
    }
}

/// Refine cost for one work item; `ns` should reflect the simulated stage
/// duration so profiling panels line up with observable latencies.
pub fn refine_cost(gas: u64, ns: u64) -> RefineCost {
    let host_ns = ns * 70 / 100;
    let host_gas = gas * 80 / 100;
    RefineCost {
        total: exec(gas, ns),
        load_ns: ns * 18 / 100,
        host_call: RefineHostCallCost {
            lookup: exec(host_gas / 10, host_ns * 10 / 100),
            vm: exec(host_gas * 6 / 10, host_ns * 60 / 100),
            mem: exec(host_gas * 15 / 100, host_ns * 15 / 100),
            invoke: exec(host_gas / 10, host_ns * 10 / 100),
            other: exec(host_gas / 20, host_ns * 5 / 100),
        },
    }
}

/// Accumulate cost for one service in a block.
pub fn accumulate_cost(gas: u64, ns: u64, num_items: u32) -> AccumulateCost {
    let host_ns = ns * 60 / 100;
    let host_gas = gas * 70 / 100;
    AccumulateCost {
        num_calls: num_items,
        num_transfers: 0,
        num_items,
        total: exec(gas, ns),
        load_ns: ns * 15 / 100,
        host_call: AccumulateHostCallCost {
            state: exec(host_gas * 4 / 10, host_ns * 40 / 100),
            lookup: exec(host_gas * 2 / 10, host_ns * 20 / 100),
            preimage: exec(host_gas / 10, host_ns * 10 / 100),
            service: exec(host_gas / 10, host_ns * 10 / 100),
            transfer: exec(0, 0),
            transfer_dest_gas: 0,
            other: exec(host_gas / 5, host_ns * 20 / 100),
        },
    }
}
