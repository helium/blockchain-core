%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Gossip Stream Handler ==
%% NOTE: gossip protocol negotation is performed as part of group gossip
%%       as this is where stream dialing takes place
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_gossip_handler).

-behavior(libp2p_group_gossip_handler).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_gossip_handler_pb.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
         init_gossip_data/1,
         handle_gossip_data/5,
         gossip_data_v1/2,
         gossip_data_v2/3,
         regossip_block/2, regossip_block/4
        ]).

-ifdef(TEST).
-export([add_block/4]).
-endif.

init_gossip_data([SwarmTID, Blockchain]) ->
    lager:debug("gossiping init"),
    lager:debug("gossiping block to peers on init"),
    case application:get_env(blockchain, gossip_version, 1) of
        1 ->
            {ok, Block} = blockchain:head_block(Blockchain),
            {send, gossip_data_v1(SwarmTID, Block)};
        2 ->
            {ok, Height} = blockchain_ledger_v1:current_height(blockchain:ledger(Blockchain)),
            {ok, #block_info_v2{hash = Hash}} = blockchain:get_block_info(Height, Blockchain),
            lager:debug("gossiping block @ ht: ~p to peers on init", [Height]),
            {send, gossip_data_v2(SwarmTID, Hash, Height)}
    end;
init_gossip_data(WAT) ->
    lager:info("WAT ~p", [WAT]),
    {send, <<>>}.

handle_gossip_data(StreamPid, _Kind, _Peer, {_Path, Data}, Args) ->
    %% handle libp2p performance branch change in handle gossip data that includes the path
    handle_gossip_data(StreamPid, Data, Args).

handle_gossip_data(_StreamPid, Data, [SwarmTID, Blockchain]) ->
    try
        case blockchain_gossip_handler_pb:decode_msg(Data, blockchain_gossip_block_pb) of
            #blockchain_gossip_block_pb{from = From, hash = Hash, height = Height, block = <<>>} ->
                %% try to cheaply check if we have the block already
                case blockchain:get_block_hash(Height, Blockchain) of
                    {ok, Hash} -> ok;
                    {ok, OtherHash} when is_binary(OtherHash) ->
                        lager:warning("got non-matching hash ~p for height ~p from ~p",
                                      [Hash, Height, blockchain_utils:addr2name(From)]),
                        ok;
                    {error, not_found} ->
                        %% don't appear to have the block, do we have a plausible block?
                        case blockchain:have_plausible_block(Hash, Blockchain) of
                            true ->
                                case find_missing_blocks(Hash, Blockchain) of
                                    [] -> ok;
                                    Missing ->
                                        lager:info("requesting missing blocks ~p from ~p", [Missing, blockchain_utils:addr2name(From)]),
                                        blockchain_worker:target_sync(From, Missing, Hash)
                                end;
                            false ->
                                %% don't have it in plausible either, try to sync it from the sender.
                                blockchain_worker:target_sync(From, [], Hash)
                        end
                end;
            #blockchain_gossip_block_pb{from=From, block=BinBlock} ->
                SizeLimit = application:get_env(blockchain, gossip_block_limit_mb, 25) * 1024 * 1024,
                case byte_size(BinBlock) < SizeLimit of
                    true ->
                        Block = blockchain_block:deserialize(BinBlock),
                        case blockchain_block:type(Block) of
                            undefined ->
                                lager:notice("gossip_handler unknown block: ~p", [Block]);
                            _ ->
                                case blockchain:has_block(Block, Blockchain) of
                                    true ->
                                        %% already got this block, just return
                                        ok;
                                    _ ->
                                        case blockchain:is_block_plausible(Block, Blockchain) of
                                            true ->
                                                lager:debug("Got block: ~p from: ~p", [Block, From]),
                                                %% don't block the gossip server
                                                spawn(fun() -> add_block(Block, Blockchain, From, SwarmTID) end),
                                                ok;
                                            false ->
                                                blockchain_worker:maybe_sync(),
                                                ok
                                        end
                                end
                        end;
                    false ->
                        lager:warning("block was too big for gossip, skipping"),
                        ok
                end
        end
    catch
        _What:Why:Stack ->
            lager:notice("gossip handler got bad data: ~p", [Why]),
            lager:info("stack: ~p", [Stack])
    end,
    noreply.


-spec find_missing_blocks(Hash :: blockchain_block:hash(), Chain :: blockchain:blockchain()) -> [pos_integer()].
find_missing_blocks(Hash, Chain) ->
    {ok, Block} = blockchain:get_plausible_block(Hash, Chain),
    find_missing_blocks(blockchain_block:prev_hash(Block), blockchain_block:height(Block), Chain).


find_missing_blocks(Hash, LastHeight, Chain) ->
    case blockchain:get_plausible_block(Hash, Chain) of
        {ok, Block} ->
            BlockHeight = blockchain_block:height(Block),
            case BlockHeight == LastHeight - 1 of
                 true ->
                    find_missing_blocks(blockchain_block:prev_hash(Block), BlockHeight, Chain);
                false ->
                    %% some kind of wacky chain break, ignore the plausible block that does not fit
                    {ok, ChainHeight} = blockchain:height(Chain),
                    block_range(ChainHeight+1, LastHeight - 1)
            end;
        _ ->
            %% found a break in the plausible chain, so now we know we need any
            %% missing heights between this block and the chain's HEAD block
            {ok, ChainHeight} = blockchain:height(Chain),
            block_range(ChainHeight+1, LastHeight - 1)
    end.

block_range(A, B) when A >= B ->
    %% race condition, plausible block got absorbed
    [];
block_range(A, B) ->
    lists:seq(A, B).

add_block(Block, Chain, Sender, SwarmTID) ->
    lager:debug("Sender: ~p, MyAddress: ~p", [Sender, blockchain_swarm:pubkey_bin()]),
    case blockchain:has_block(Block, Chain) == false andalso blockchain:is_block_plausible(Block, Chain) of
        true ->
            %% eagerly re-gossip plausible blocks we don't have
            ok = regossip_block(Block, SwarmTID);
        false ->
            ok
    end,
    %% try to acquire the lock with a timeout
    case blockchain_lock:acquire(5000) of
        error ->
            %% fail quietly
            ok;
        ok ->
            case blockchain:add_block(Block, Chain) of
                ok ->
                    lager:info("got gossipped block ~p", [blockchain_block:height(Block)]),
                    %% pass it along
                    ok;
                plausible ->
                    lager:info("plausible gossipped block ~p doesn't fit with our chain, will start sync if not already active", [blockchain_block:height(Block)]),
                    blockchain_worker:maybe_sync(),
                    %% pass it along
                    ok;
                exists ->
                    ok;
                {error, disjoint_chain} ->
                    lager:warning("gossipped block ~p doesn't fit with our chain,"
                                " will start sync if not already active", [blockchain_block:height(Block)]),
                    blockchain_worker:maybe_sync(),
                    ok;
                {error, disjoint_assumed_valid_block} ->
                    %% harmless
                    ok;
                {error, block_higher_than_assumed_valid_height} ->
                    %% harmless
                    ok;
                {error, no_ledger} ->
                    %% just ignore this, we don't care right now
                    ok;
                Error ->
                    %% Uhm what is this?
                    lager:error("Something bad happened: ~p", [Error])
            end
    end.

-spec gossip_data_v1(libp2p_swarm:swarm(), blockchain_block:block()) -> binary().
gossip_data_v1(SwarmTID, Block) ->
    PubKeyBin = libp2p_swarm:pubkey_bin(SwarmTID),
    BinBlock = blockchain_block:serialize(Block),
    Msg = #blockchain_gossip_block_pb{from=PubKeyBin, block=BinBlock},
    blockchain_gossip_handler_pb:encode_msg(Msg).

-spec gossip_data_v2(libp2p_swarm:swarm(), binary(), pos_integer()) -> binary().
gossip_data_v2(SwarmTID, Hash, Height) ->
    PubKeyBin = libp2p_swarm:pubkey_bin(SwarmTID),
    Msg = #blockchain_gossip_block_pb{from=PubKeyBin, hash=Hash, height=Height},
    blockchain_gossip_handler_pb:encode_msg(Msg).

regossip_block(Block, SwarmTID) ->
        case application:get_env(blockchain, gossip_version, 1) of
            1 ->
                %% this is awful but safe
                regossip_block(Block, height, hash, SwarmTID);
            2 ->
                %% this is not super efficient but the cost should tail off over time.
                Height = blockchain_block:height(Block),
                Hash = blockchain_block:hash_block(Block),
                regossip_block(Block, Height, Hash, SwarmTID)
        end.

regossip_block(Block, Height, Hash, SwarmTID) ->
    Data =
        case application:get_env(blockchain, gossip_version, 1) of
            1 ->
                gossip_data_v1(SwarmTID, Block);
            2 ->
                gossip_data_v2(SwarmTID, Hash, Height)
        end,
    libp2p_group_gossip:send(
      SwarmTID,
      ?GOSSIP_PROTOCOL_V1,
      Data
     ),
    ok.
