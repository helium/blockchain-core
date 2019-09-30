-module(meck_test_util).

-export([forward_submit_txn/1]).

forward_submit_txn(Pid) ->
    meck:expect(blockchain_worker, submit_txn, fun(T) ->
        Pid ! {txn, T},
        ok
    end),
    ok.
