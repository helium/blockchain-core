-module(blockchain_graph).

-behaviour(gen_server).

-include("blockchain_vars.hrl").

%% API
-export([
         start_link/0,
         target/3,
         build/5
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%% TODO: from blockchain_txn, should maybe? become a chain variable.
-define(BLOCK_DELAY, 50).

-record(state,
        {
         chain :: undefined | blockchain:blockchain(),
         height = 1 :: pos_integer(),
         map = #{} :: #{binary() => any()},
         old_maps = [] :: [{pos_integer(), #{}}]
        }).

-record(gw,
        {
         neighbors = [] :: [#gw{}],
         location :: undefined | integer()
        }).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

target(Hash, Ledger, Challenger) ->
    gen_server:call(?SERVER, {target, Hash, Ledger, Challenger}, infinity).

build(Hash, Target, Gateways, Height, Ledger) ->
    gen_server:call(?SERVER, {build, Hash, Target, Gateways, Height, Ledger}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    %% timeout into async setup
    ok = blockchain_event:add_handler(self()),
    {ok, #state{chain = blockchain_worker:blockchain()}, 0}.

%% handle_call({target, Hash, Ledger, Challenger}, _From,
%%             #state{map = Map} = State) ->
%%     {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
%%     ActiveGateways = active_gateways(Map, Height, Ledger, Challenger),
%%     ProbsAndGatewayAddrs = create_probs(ActiveGateways, Height, Ledger),
%%     %% subtly different than rand from hash :/
%%     Entropy = blockchain_poc_path:entropy(Hash),
%%     {RandVal, _} = rand:uniform_s(Entropy),
%%     Ret =
%%         case select_target(ProbsAndGatewayAddrs, RandVal) of
%%         {ok, Target} ->
%%             {Target, ActiveGateways};
%%         _ ->
%%             no_target
%%         end,

%%     {reply, Ret, State};
handle_call(_Request, _From, State) ->
    lager:warning("unexpected call ~p from ~p", [_Request, _From]),
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    lager:warning("unexpected cast ~p", [_Msg]),
    {noreply, State}.

handle_info({blockchain_event, {add_block, Hash, _Sync, Ledger}},
            #state{map = Map, old_maps = OldMaps, height = Height} = State)
  when State#state.chain /= undefined ->
    case blockchain:get_block(Hash, State#state.chain) of
        {ok, Block} ->
            BHeight = blockchain_block:height(Block),
            case BHeight of
                H when H > Height ->
                    %% if there is a vars txn that changed anything, recalc
                    case has_vars(Block) of
                        true ->
                            %% save off the old map
                            OldMaps1 = OldMaps ++ [{Height, Map}],
                            %% calculate the new map
                            Gateways = blockchain_ledger_v1:active_gateways(Ledger),
                            State1 = construct_map(Gateways, Ledger, State),
                            {noreply,
                             State1#state{old_maps = OldMaps1,
                                          height = BHeight}};
                        false ->
                            case has_gws(Block) of
                                {true, Gws} ->
                                    {noreply, insert_gws(Gws, Ledger,
                                                         State#state{height = BHeight})};
                                _ ->
                                    {noreply, State#state{height = BHeight}}
                            end
                    end;
                _ ->
                    {noreply, State}
            end;
        _Error ->
            lager:warning("didn't get gossiped block: ~p", [_Error]),
            {noreply, State}
    end;
handle_info({blockchain_event, {add_block, _Hash, _Sync, _Ledger}}, State) ->
    case State#state.chain of
        undefined ->
            Chain = blockchain_worker:blockchain(),
            {noreply, State#state{chain = Chain}, 0};
        _ ->
            {noreply, State}
    end;
handle_info(timeout, #state{chain = undefined} = State) ->
    %% we'll come back to this later once some blocks come in
    {noreply, State};
handle_info(timeout, #state{chain = Chain} = State) ->
    Ledger0 = blockchain:ledger(Chain),
    {ok, TopHeight} = blockchain_ledger_v1:current_height(Ledger0),
    %% grab the oldest available ledger on startup
    Ledger = blockchain_ledger_v1:mode(delayed, Ledger0),
    {ok, BottomHeight} = blockchain_ledger_v1:current_height(Ledger),
    Gateways = blockchain_ledger_v1:active_gateways(Ledger),
    State1= construct_map(Gateways, Ledger, State),
    %% get current height, replay the any potential txns
    State2 = lists:foldl(fun(Ht, S) ->
                                 {ok, B} = blockchain:get_block(Ht, Chain),
                                 case has_gws(B) of
                                     {true, Gws} ->
                                         %% TODO: extend ledger_at to
                                         %% not be insanely expensive
                                         %% to expand 1 block at a time?
                                         insert_gws(Gws, Ledger, S#state{height = Ht});
                                     false ->
                                         S
                                 end
                         end,
                         State1,
                         lists:seq(BottomHeight, TopHeight)),
    {noreply, State2};
handle_info(_Info, State) ->
    lager:warning("unexpected message ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

construct_map(Gateways, Ledger, State) ->
    InitMap =
        maps:fold(
          fun(Addr, Gw, Acc) ->
                  case blockchain_ledger_gateway_v1:location(Gw) of
                      undefined ->
                          Acc;
                      Loc ->
                              Acc#{Addr => #gw{location = Loc}}
                  end
          end,
          #{},
          Gateways),
    Gws = [{Addr, G#gw.location} || {Addr, G} <- maps:to_list(InitMap)],
    insert_gws(Gws, Ledger, State).

has_vars(Block) ->
    case lists:filter(fun(T) ->
                              blockchain_txn:type(T) == blockchain_txn_vars_v1
                      end, blockchain_block:transactions(Block)) of
        Txns when Txns /= [] ->
            Vars = lists:foldl(fun(T, Acc) ->
                                       M = blockchain_txn_vars_v1:decoded_vars(T),
                                       maps:merge(Acc, M)
                               end,
                               #{},
                               Txns),
            Names = maps:keys(Vars),
            GraphNames = [?h3_exclusion_ring_dist, ?h3_max_grid_distance, ?h3_neighbor_res],
            case GraphNames -- Names of
                L when length(L) < length(GraphNames) ->
                    true;
                _ ->
                    false
            end;
        _ ->
            false
    end.

has_gws(Block) ->
    case lists:filter(fun(T) ->
                              %% TODO: ideally move to versionless types?
                              blockchain_txn:type(T) == blockchain_txn_assert_location_v1
                      end, blockchain_block:transactions(Block)) of
        Txns when Txns /= [] ->
            {true,
             lists:map(fun(T) ->
                               {blockchain_txn_assert_location_v1:gateway(T),
                                blockchain_txn_assert_location_v1:location(T)}
                       end,
                       Txns)};
        _ ->
            false
    end.

insert_gws(Gws, Ledger, State) ->
    lists:foldl(fun(Gw, S) ->
                        insert_gw(Gw, Ledger, S)
                end,
                State,
                Gws).

insert_gw(Gw, Ledger, #state{map = Map,
                             height = Height,
                             old_maps = OldMaps} = S) ->
    Map1 = update_map(Gw, Map, Ledger),
    OldMaps1 = OldMaps ++ [{Height - 1, Map}],
    S#state{map = Map1,
            old_maps = OldMaps1}.


update_map({Addr, Location}, Map, Ledger) ->
    CleanMap =
        case Map of
            #{Addr := #gw{neighbors = Neighbors}} ->
                Map1 = del_neighbor(Addr, Neighbors, Map),
                maps:remove(Addr, Map1);
            _ ->
                Map
        end,
    add_neighbor(Addr, Location, CleanMap, Ledger).

del_neighbor(Addr, Neighbors, Map) ->
    lists:foldl(fun(N, M) ->
                        G = maps:get(N, M),
                        G1 = G#gw{neighbors = lists:delete(Addr, G#gw.neighbors)},
                        M#{N => G1}
                end,
                Map,
                Neighbors).

add_neighbor(Addr, Location, Map, Ledger) ->
    {ok, H3ExclusionRingDist} = blockchain:config(?h3_exclusion_ring_dist, Ledger),
    {ok, H3MaxGridDistance} = blockchain:config(?h3_max_grid_distance, Ledger),
    {ok, H3NeighborRes} = blockchain:config(?h3_neighbor_res, Ledger),
    ExclusionIndices = h3:k_ring(Location, H3ExclusionRingDist),
    ScaledLocation = h3:parent(Location, H3NeighborRes),
    maps:map(fun(_A, G) ->
                     NeighborLoc = G#gw.location,
                     ScaledNeighborLoc =
                         case h3:get_resolution(NeighborLoc) of
                             R when R > H3NeighborRes ->
                                 h3:parent(NeighborLoc, H3NeighborRes);
                             _ ->
                                 NeighborLoc
                         end,
                     case lists:member(ScaledNeighborLoc, ExclusionIndices) of
                         false ->
                             case (catch h3:grid_distance(ScaledLocation, ScaledNeighborLoc)) of
                                 {'EXIT', _} -> G;
                                 D when D > H3MaxGridDistance -> G;
                                 _ ->
                                     G#gw{neighbors = [Addr | G#gw.neighbors]}
                             end;
                         true -> G
                     end
             end,
             Map).



%% poc-path equivalent
%% effectively we're filtering the active gateways in a very
%% particular way
%% active_gateways(Map, Height, Ledger, Challenger) ->
%%     {ok, MinScore} = blockchain:config(?min_score, Ledger),
%%     Scores0 =
%%         [{A,
%%           begin
%%               {ok, LGw} = blockchain_ledger_v1:find_gateway_info(A, Ledger),
%%               {_A, _B, Score} = blockchain_ledger_gateway_v1:score(A, LGw, Height, Ledger),
%%               Score
%%           end}
%%          || A <- maps:keys(Map)],
%%     Scores = maps:from_list(Scores0),
%%     maps:fold(
%%       fun(A, _G, Acc) when A == Challenger ->
%%               Acc;
%%          (A, _G, Acc) ->
%%               #{A := Score} = Scores,
%%               case maps:is_key(A, Acc) orelse Score =< MinScore of
%%                   true ->
%%                       Acc;
%%                   _ ->
%%                       _Graph = build_graph([A], Map, Scores, #{}),
%%                       Acc
%%               end
%%       end,
%%       #{},
%%       %% we may need to do some work to get this into the right order
%%       %% for determinism reasons, or just use the real active gateways?
%%       Map).

%% build_graph([], _Map, _Scores, Acc) ->
%%     Acc;
%% build_graph([H|_T], Map, _Scores, Acc) ->
%%     #gw{neighbors = _N} = maps:get(H, Map),
%%     Acc.
