%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Utils ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_utils).

-export([
    shuffle_from_hash/2,
    shuffle/1,
    rand_from_hash/1, rand_state/1,
    normalize_float/1,
    challenge_interval/1,
    serialize_hash/1, deserialize_hash/1,
    hex_to_bin/1, bin_to_hex/1,
    pmap/2,
    addr2name/1,
    distance/2,
    score_gateways/1,
    free_space_path_loss/2,
    vars_binary_keys_to_atoms/1,
    icdf_select/2,
    find_txn/2,
    map_to_bitvector/1,
    bitvector_to_map/2,
    get_pubkeybin_sigfun/1,
    approx_blocks_in_week/1,
    vars_keys_to_list/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("blockchain_vars.hrl").

-define(FREQUENCY, 915).
-define(TRANSMIT_POWER, 28).
-define(MAX_ANTENNA_GAIN, 6).

-type zone_map() :: #{h3:index() => gateway_score_map()}.
-type gateway_score_map() :: #{libp2p_crypto:pubkey_bin() => {blockchain_ledger_gateway_v2:gateway(), float()}}.

-export_type([gateway_score_map/0, zone_map/0]).

%%--------------------------------------------------------------------
%% @doc Shuffle a list deterministically using a random binary as the seed.
%% @end
%%--------------------------------------------------------------------
-spec shuffle_from_hash(binary(), list()) -> list().
shuffle_from_hash(Hash, L) ->
    ?MODULE:rand_from_hash(Hash),
    [X ||{_, X} <- lists:sort([{rand:uniform(), E} || E <- L])].

%%--------------------------------------------------------------------
%% @doc Shuffle a list randomly.
%% @end
%%--------------------------------------------------------------------
shuffle(List) ->
    [X || {_,X} <- lists:sort([{rand:uniform(), N} || N <- List])].

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec rand_from_hash(binary()) -> any().
rand_from_hash(Hash) ->
    <<I1:86/integer, I2:85/integer, I3:85/integer, _/binary>> = Hash,
    rand:seed(exs1024, {I1, I2, I3}).

%%--------------------------------------------------------------------
%% @doc normalize a float by converting it to fixed point and back
%% using 16 bits of exponent precision. This should be well above
%% the floating point error threshold and doing this will prevent
%% errors from accumulating.
%% @end
%%--------------------------------------------------------------------
normalize_float(Float) ->
    round(Float * 65536) / 65536.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec challenge_interval(blockchain_ledger_v1:ledger()) -> non_neg_integer().
challenge_interval(Ledger) ->
    {ok, Interval} = blockchain:config(?poc_challenge_interval, Ledger),
    Interval.

-spec serialize_hash(binary()) -> string().
serialize_hash(Hash) ->
    libp2p_crypto:bin_to_b58(Hash).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec deserialize_hash(string()) -> binary().
deserialize_hash(String) ->
    libp2p_crypto:b58_to_bin(String).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec bin_to_hex(binary()) -> string().
bin_to_hex(Bin) ->
  lists:flatten([[io_lib:format("~2.16.0b",[X]) || <<X:8>> <= Bin ]]).

-spec hex_to_bin(binary()) -> binary().
hex_to_bin(Hex) ->
  << begin {ok, [V], []} = io_lib:fread("~16u", [X, Y]), <<V:8/integer-little>> end || <<X:8/integer, Y:8/integer>> <= Hex >>.

pmap(F, L) ->
    Width = application:get_env(blockchain, validation_width, 3),
    pmap(F, L, Width).

pmap(F, L, Width) ->
    Parent = self(),
    Len = length(L),
    Min = floor(Len/Width),
    Rem = Len rem Width,
    Lengths = lists:duplicate(Rem, Min+1)++ lists:duplicate(Width - Rem, Min),
    OL = partition_list(L, Lengths, []),
    St = lists:foldl(
           fun([], N) ->
                   N;
              (IL, N) ->
                   spawn_opt(
                     fun() ->
                             Parent ! {pmap, N, lists:map(F, IL)}
                     end, [{fullsweep_after, 0}]),
                   N+1
           end, 0, OL),
    L2 = [receive
              {pmap, N, R} -> {N,R}
          end || _ <- lists:seq(1, St)],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    lists:flatten(L3).

partition_list([], [], Acc) ->
    lists:reverse(Acc);
partition_list(L, [0 | T], Acc) ->
    partition_list(L, T, Acc);
partition_list(L, [H | T], Acc) ->
    {Take, Rest} = lists:split(H, L),
    partition_list(Rest, T, [Take | Acc]).

addr2name(Addr) ->
    B58Addr = libp2p_crypto:bin_to_b58(Addr),
    {ok, N} = erl_angry_purple_tiger:animal_name(B58Addr),
    N.

-spec rand_state(Hash :: binary()) -> rand:state().
rand_state(Hash) ->
    <<A:85/integer-unsigned-little, B:85/integer-unsigned-little,
      C:86/integer-unsigned-little, _/binary>> = crypto:hash(sha256, Hash),
    rand:seed_s(exs1024s, {A, B, C}).

distance(L1, L1) ->
    %% Same location, defaulting the distance to 1m
    0.001;
distance(L1, L2) ->
    %% distance in kms
    case vincenty:distance(h3:to_geo(L1), h3:to_geo(L2)) of
        {error, _} ->
            %% An off chance that the points are antipodal and
            %% vincenty_distance fails to converge. In this case
            %% we default to some max distance we consider good enough
            %% for witnessing
            1000;
        {ok, D} ->
            D - hex_adjustment(L1) - hex_adjustment(L2)
    end.

hex_adjustment(Loc) ->
    %% Distance from hex center to edge, sqrt(3)*edge_length/2.
    Res = h3:get_resolution(Loc),
    EdgeLength = h3:edge_length_kilometers(Res),
    EdgeLength * (round(math:sqrt(3) * math:pow(10, 3)) / math:pow(10, 3)) / 2.

-spec score_gateways(Ledger :: blockchain_ledger_v1:ledger()) -> gateway_score_map().
score_gateways(Ledger) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    case blockchain_ledger_v1:mode(Ledger) of
        delayed ->
            %% Use the cache in delayed ledger mode
            e2qc:cache(gw_cache, {Height},
                       fun() ->
                               score_tagged_gateways(Height, Ledger)
                       end);
        active ->
            %% recalculate in active ledger mode
            score_tagged_gateways(Height, Ledger)
    end.

-spec score_tagged_gateways(Height :: pos_integer(),
                            Ledger :: blockchain_ledger_v1:ledger()) -> gateway_score_map().
score_tagged_gateways(Height, Ledger) ->
    Gateways = blockchain_ledger_v1:active_gateways(Ledger),
    maps:map(fun(A, G) ->
                     {_, _, S} = blockchain_ledger_gateway_v2:score(A, G, Height, Ledger),
                     {G, S}
             end, Gateways).

-spec free_space_path_loss(h3:index(), h3:index()) -> float().
free_space_path_loss(Loc1, Loc2) ->
    Distance = blockchain_utils:distance(Loc1, Loc2),
    %% TODO support regional parameters for non-US based hotspots
    ?TRANSMIT_POWER - (32.44 + 20*math:log10(?FREQUENCY) + 20*math:log10(Distance) - ?MAX_ANTENNA_GAIN - ?MAX_ANTENNA_GAIN).

-spec vars_binary_keys_to_atoms(map()) -> map().
vars_binary_keys_to_atoms(Vars) ->
    %% This makes good men sad
    maps:fold(fun(K, V, Acc) -> maps:put(binary_to_atom(K, utf8), V, Acc)  end, #{}, Vars).

-spec get_pubkeybin_sigfun(pid()) -> {libp2p_crypto:pubkey_bin(), function()}.
get_pubkeybin_sigfun(Swarm) ->
    {ok, PubKey, SigFun, _} = libp2p_swarm:keys(Swarm),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    {PubKeyBin, SigFun}.

-spec icdf_select([{any(), float()}, ...], float()) -> {ok, any()} | {error, zero_weight}.
icdf_select(PopulationList, Rnd) ->
    Sum = lists:sum([Weight || {_Node, Weight} <- PopulationList]),
    icdf_select(PopulationList, normalize_float(Rnd * Sum), normalize_float(Rnd * Sum)).

-spec find_txn(Block :: blockchain_block:block(),
               PredFun :: fun()) -> [blockchain_txn:txn()].
find_txn(Block, PredFun) ->
    Txns = blockchain_block:transactions(Block),
    lists:filter(fun(T) -> PredFun(T) end, Txns).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
icdf_select([{_Node, 0.0}], _Rnd, _OrigRnd) ->
    {error, zero_weight};
icdf_select([{Node, _Weight}], _Rnd, _OrigRnd) ->
    {ok, Node};
icdf_select([{Node, Weight} | _], Rnd, _OrigRnd) when Rnd - Weight =< 0 ->
    {ok, Node};
icdf_select([{_Node, Weight} | Tail], Rnd, OrigRnd) ->
    icdf_select(Tail, normalize_float(Rnd - Weight), OrigRnd).



-spec map_to_bitvector(#{pos_integer() => boolean()}) -> binary().
map_to_bitvector(Map) ->
    Sz = maps:size(Map),
    Int = lists:foldl(
            fun({ID, true}, Acc) ->
                    Acc bor (1 bsl (ID - 1));
               (_, Acc) ->
                    Acc
            end,
            0,
            maps:to_list(Map)),
    BitSz = nearest_byte(Sz),
    <<Int:BitSz/little-unsigned-integer>>.

-spec bitvector_to_map(pos_integer(), binary()) -> #{pos_integer() => boolean()}.
bitvector_to_map(Count, Vector) ->
    Sz = 8 * size(Vector),
    <<Int:Sz/little-unsigned-integer>> = Vector,
    L = [begin
             B = case Int band (1 bsl (ID - 1)) of
                     0 ->
                         false;
                     _ ->
                         true
                 end,
             {ID, B}
         end
          || ID <- lists:seq(1, Count)],
    maps:from_list(L).

nearest_byte(X) ->
    (X div 8 + case X rem 8 of 0 -> 0; _ -> 1 end) * 8.

-spec approx_blocks_in_week(Ledger :: blockchain_ledger_v1:ledger()) -> pos_integer().
approx_blocks_in_week(Ledger) ->
    case blockchain:config(?block_time, Ledger) of
        {ok, BT} ->
            %% BT is in ms
            %% ms in a week = 7 * 24 * 60 * 60 * 1000
            trunc((7 * 24 * 60 * 60 * 1000) / BT);
        _ ->
            10000
    end.

-spec vars_keys_to_list( Base64String :: binary() ) -> [ binary() ].
%% @doc Price oracle public keys are encoded like this
%% <code>
%% <<KeyLen1/integer, Key1/binary, KeyLen2/integer, Key2/binary, ...>>
%% </code>
%% This function takes the encoded string and deserializes
%% it into a list of binary keys
%% @end
vars_keys_to_list(Data) when is_binary(Data) ->
    [ Key || << Len:8/unsigned-integer, Key:Len/binary >> <=Data ].

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

serialize_deserialize_test() ->
    Hash = <<"123abc">>,
    ?assertEqual(Hash, deserialize_hash(serialize_hash(Hash))).

pmap_test() ->
    Input = lists:seq(1, 21),
    {Pids, Results} = lists:unzip(pmap(fun(E) -> {self(), E} end, Input, 6)),
    Map = lists:foldl(fun(E, A) ->
                        maps:update_with(E, fun(X) -> X + 1 end, 1, A)
                end, #{}, Pids),
    ?assertEqual(6, maps:size(Map)),
    ?assertEqual([3, 3, 3, 4, 4, 4], lists:sort(maps:values(Map))),
    ?assertEqual(Input, Results).

get_pubkeybin_sigfun_test() ->
    BaseDir = test_utils:tmp_dir("get_pubkeybin_sigfun_test"),
    {ok, Swarm} = start_swarm(get_pubkeybin_sigfun_test, BaseDir),
    {ok, PubKey, PayerSigFun, _} = libp2p_swarm:keys(Swarm),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    ?assertEqual({PubKeyBin, PayerSigFun}, get_pubkeybin_sigfun(Swarm)),
    libp2p_swarm:stop(Swarm),
    ok.

start_swarm(Name, BaseDir) ->
    SwarmOpts = [
        {libp2p_nat, [{enabled, false}]},
        {base_dir, BaseDir}
    ],
    application:ensure_all_started(throttle),
    libp2p_swarm:start(Name, SwarmOpts).

bitvector_roundtrip_test() ->
    L1 = [begin B = case rand:uniform(2) of 1 -> true; _ -> false end, {N,B} end || N <- lists:seq(1, 16)],
    L2 = [begin B = case rand:uniform(2) of 1 -> true; _ -> false end, {N,B} end || N <- lists:seq(1, 19)],
    L3 = [begin B = case rand:uniform(2) of 1 -> true; _ -> false end, {N,B} end || N <- lists:seq(1, 64)],
    L4 = [begin B = case rand:uniform(2) of 1 -> true; _ -> false end, {N,B} end || N <- lists:seq(1, 122)],

    M1 = maps:from_list(L1),
    M2 = maps:from_list(L2),
    M3 = maps:from_list(L3),
    M4 = maps:from_list(L4),

    ?assertEqual(M1, bitvector_to_map(16, map_to_bitvector(M1))),
    ?assertEqual(M2, bitvector_to_map(19, map_to_bitvector(M2))),
    ?assertEqual(M3, bitvector_to_map(64, map_to_bitvector(M3))),
    ?assertEqual(M4, bitvector_to_map(122, map_to_bitvector(M4))),
    ok.

oracle_keys_test() ->
    #{ public := RawEccPK } = libp2p_crypto:generate_keys(ecc_compact),
    #{ public := RawEdPK } = libp2p_crypto:generate_keys(ed25519),
    EccPK = libp2p_crypto:pubkey_to_bin(RawEccPK),
    EdPK = libp2p_crypto:pubkey_to_bin(RawEdPK),
    TestStr = base64:encode(<< <<(byte_size(Key)):8/integer, Key/binary>> || Key <- [EccPK, EdPK] >>),
    Results = vars_keys_to_list(TestStr),
    ?assertEqual([EccPK, EdPK], Results),
    Results1 = [ libp2p_crypto:bin_to_pubkey(K) || K <- Results ],
    ?assertEqual([RawEccPK, RawEdPK], Results1).

-endif.
