//! Guest network policy building blocks: rules, DNS resolver with fake
//! addresses, upstream connect/relay, limits, TUN and fd passing.
#![allow(dead_code, reason = "not wired into the runner yet")]

pub mod decision_log;
pub mod dns;
pub mod fdpass;
pub mod limits;
pub mod rules;
pub mod tun;
pub mod upstream;
