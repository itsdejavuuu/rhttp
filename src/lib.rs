#![allow(clippy::needless_return)]
#[macro_use] extern crate gmod;

mod http;
mod worker;

use gmod::lua::State;

#[gmod13_open]
unsafe fn gmod13_open(lua: State) -> i32 {
    worker::init(lua);

    lua.push_function(http::request_lua);
    lua.set_global(lua_string!("rhttp"));

    0
}

#[gmod13_close]
unsafe fn gmod13_close(lua: State) -> i32 {
    worker::shutdown(lua);
    0
}
