%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Behavior ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn).

-type hash() :: <<_:256>>. %% SHA256 digest

-callback hash(State::any()) -> hash().
