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


init_gossip_data([Address]) ->
   lager:info("gossiping init"),
   case blockchain_worker:head_block() of
       undefined ->
           ok;
       Block ->
           lager:info("gossiping block to peers on init"),
           {send, term_to_binary({block, Address, Block})}
   end.

handle_gossip_data(Data, [_Address]) ->
    case erlang:binary_to_term(Data) of
        {block, From, Block} ->
            case blockchain_block:is_block(Block) of
                true ->
                    lager:info("Got block: ~p from: ~p", [Block, From]),
                    blockchain_worker:add_block(Block, From);
                _ ->
                    lager:notice("gossip_handler received invalid data")
            end;
        Other ->
            lager:notice("gossip handler got unknown data ~p", [Other])
    end,
    ok.
