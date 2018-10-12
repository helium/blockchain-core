%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Gossip Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_gossip_handler).

-behavior(libp2p_group_gossip_handler).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([init_gossip_data/1, handle_gossip_data/2]).


init_gossip_data([]) -> ok.

handle_gossip_data(Data, []) ->
    case erlang:binary_to_term(Data) of
        {block, From, Block} ->
            blockchain_worker:add_block(Block, From);
        Other ->
            lager:notice("gossip handler got unknown data ~p", [Other])
    end,
    ok.
