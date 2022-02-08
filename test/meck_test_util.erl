-module(meck_test_util).

-export([forward_submit_txn/1]).

forward_submit_txn(Pid) ->
    meck:expect(blockchain_txn_mgr, submit, fun(T, F) ->
        Pid ! {txn, T},
        F(ok),
        ok
    end),
    ok.
