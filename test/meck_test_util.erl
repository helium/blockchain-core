-module(meck_test_util).

-export([new/1, expect/1]).

new([Mod, Arg]) ->
    meck:new(Mod, [Arg]).

expect([Mod, Fun, ToExpect]) ->
    meck:expect(Mod, Fun, ToExpect).
