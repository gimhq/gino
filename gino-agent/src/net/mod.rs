//! Guest network policy building blocks: rules, fake-address pool, DNS policy,
//! upstream connect/relay, limits, TUN and fd passing.
#![allow(dead_code, reason = "not wired into the runner yet")]

pub mod decision_log;
pub mod dns_policy;
pub mod fdpass;
pub mod limits;
pub mod pool;
pub mod rules;
pub mod tun;
pub mod upstream;
