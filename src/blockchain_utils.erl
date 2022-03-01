%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Utils ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_utils).

-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").

-export([
    shuffle_from_hash/2,
    shuffle/1,
    rand_from_hash/1, rand_state/1,
    normalize_float/1,
    challenge_interval/1,
    serialize_hash/1, deserialize_hash/1,
    hex_to_bin/1, bin_to_hex/1,
    poc_id/1,
    pfind/2, pfind/3,
    pmap/2,
    addr2name/1,
    distance/2,
    score_gateways/1,
    free_space_path_loss/2,
    free_space_path_loss/3,
    vars_binary_keys_to_atoms/1,
    icdf_select/2,
    find_txn/2,
    map_to_bitvector/1,
    bitvector_to_map/2,
    get_pubkeybin_sigfun/1,
    approx_blocks_in_week/1,
    keys_list_to_bin/1,
    bin_keys_to_list/1,
    prop_to_bin/1, prop_to_bin/2,
    bin_to_prop/1, bin_to_prop/2,
    calculate_dc_amount/2, calculate_dc_amount/3,
    do_calculate_dc_amount/2,
    deterministic_subset/3,
    fold_condition_checks/1,
    get_boolean_os_env_var/2,
    streaming_file_hash/1,
    streaming_transform_iolist/2,

    %% exports for simulations
    free_space_path_loss/4,
    free_space_path_loss/5,
    min_rcv_sig/1, min_rcv_sig/2,
    index_of/2,

    verify_multisig/3,
    count_votes/3,
    poc_per_hop_max_witnesses/1,

    get_vars/2, get_var/2,
    var_cache_stats/0,
    teardown_var_cache/0,
    init_var_cache/0

]).

-define(FREQUENCY, 915).
-define(TRANSMIT_POWER, 28).
-define(MAX_ANTENNA_GAIN, 6).
-define(POC_PER_HOP_MAX_WITNESSES, 5).
-define(BLOCK_READ_SIZE, 16*1024). % 16k

-type zone_map() :: #{h3:index() => gateway_score_map()}.
-type gateway_score_map() :: #{libp2p_crypto:pubkey_bin() => {blockchain_ledger_gateway_v2:gateway(), float()}}.

-export_type([gateway_score_map/0, zone_map/0]).

%%--------------------------------------------------------------------
%% @doc Calculate the amount of DC for the supplied payload
%% @end
%%--------------------------------------------------------------------

-spec calculate_dc_amount(Ledger :: blockchain_ledger_v1:ledger(),
                          PayloadSize :: non_neg_integer()) -> pos_integer() | {error, any()}.
calculate_dc_amount(Ledger, PayloadSize) ->
    case blockchain_ledger_v1:config(?dc_payload_size, Ledger) of
        {ok, DCPayloadSize} ->
            do_calculate_dc_amount(PayloadSize, DCPayloadSize);
        _ ->
            {error, dc_payload_size_not_set}
    end.

-spec calculate_dc_amount(Ledger :: blockchain_ledger_v1:ledger(),
                          PayloadSize :: non_neg_integer(),
                          DCPayloadSize :: pos_integer()) -> pos_integer().
calculate_dc_amount(_Ledger, PayloadSize, DCPayloadSize) ->
    do_calculate_dc_amount(PayloadSize, DCPayloadSize).

-spec do_calculate_dc_amount(PayloadSize :: non_neg_integer(), DCPayloadSize :: pos_integer()) -> pos_integer().
do_calculate_dc_amount(_PayloadSize, undefined) ->
    0;
do_calculate_dc_amount(PayloadSize, DCPayloadSize)->
    case PayloadSize =< DCPayloadSize of
        true ->
            1;
        false ->
            erlang:ceil(PayloadSize/DCPayloadSize)
    end.

%%--------------------------------------------------------------------
%% @doc Shuffle a list deterministically, given a binary seed.
%% @end
%%--------------------------------------------------------------------
-spec shuffle_from_hash(binary(), list()) -> list().
shuffle_from_hash(Hash, L) ->
    ?MODULE:rand_from_hash(Hash),
    shuffle(L).

%%--------------------------------------------------------------------
%% @doc Shuffle a list randomly.
%% @end
%%--------------------------------------------------------------------
-spec shuffle([A]) -> [A].
shuffle(Xs) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), X} || X <- Xs])].

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

-spec poc_id(libp2p_crypto:pubkey_bin()) -> binary().
poc_id(PubKeyBin) when is_binary(PubKeyBin) ->
    Hash = crypto:hash(sha256, PubKeyBin),
    ?BIN_TO_B64(Hash).


-spec pfind(F :: function(), list(list())) -> boolean() | {true, any()}.
pfind(F, ToDos) ->
    pfind(F, ToDos, infinity).

-spec pfind(F :: function(), list(list()), infinity | pos_integer()) -> boolean() | {true, any()}.
pfind(F, ToDos, Timeout) ->
    Opts = [
        {fullsweep_after, 0},
        {priority, high}
    ],
    Master = self(),
    Ref = erlang:make_ref(),
    erlang:spawn_opt(
        fun() ->
            Parent = self(),
            Workers = lists:foldl(
                fun(Args, Acc) ->
                    {Pid, _Ref} = 
                        erlang:spawn_opt(
                            fun() ->
                                Result = erlang:apply(F, Args),
                                Parent ! {Ref, Result}
                            end,
                            [monitor | Opts]
                        ),
                    [Pid | Acc]
                end,
                [],
                ToDos
            ),
            Results = pfind_rcv(Ref, false, erlang:length(ToDos)),
            [erlang:exit(Pid, done) || Pid <- Workers],
            Master ! {Ref, Results}
        end,
        Opts
    ),
    receive
        {Ref, Results} ->
            Results
    after Timeout ->
        false
    end.
 
pfind_rcv(_Ref, Result, 0) ->
    Result;
pfind_rcv(Ref, Result, Left) ->
    receive
        {'DOWN', _Ref, process, _Pid, normal} ->
            pfind_rcv(Ref, Result, Left);
        {'DOWN', _Ref, process, _Pid, _Info} ->
            pfind_rcv(Ref, Result, Left-1);
        {Ref, true} ->
            true;
        {Ref, {true, Data}} ->
            {true, Data};
        {Ref, _} ->
            pfind_rcv(Ref, Result, Left-1)
    end.

pmap(F, L) ->
    Width = validation_width(),
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

validation_width() ->
    case application:get_env(blockchain, validation_width, undefined) of
        undefined ->
            cpus();
        "" ->
            cpus();
        Str when is_list(Str) ->
            try
                list_to_integer(Str)
            catch _:_ ->
                    cpus()
            end;
        N when is_integer(N) ->
            N
    end.

cpus() ->
    Ct = erlang:system_info(schedulers_online),
    max(2, ceil(Ct/2) + 1).

addr2name(undefined) -> undefined;
addr2name(Addr) ->
    B58Addr = libp2p_crypto:bin_to_b58(Addr),
    {ok, N} = erl_angry_purple_tiger:animal_name(B58Addr),
    N.

-spec rand_state(Hash :: binary()) -> rand:state().
rand_state(Hash) ->
    <<A:85/integer-unsigned-little, B:85/integer-unsigned-little,
      C:86/integer-unsigned-little, _/binary>> = crypto:hash(sha256, Hash),
    rand:seed_s(exs1024s, {A, B, C}).

-spec distance(L1 :: h3:h3_index(), L2 :: h3:h3_index()) -> float().
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

-spec free_space_path_loss(Loc1 :: h3:index(),
                           Loc2 :: h3:index(),
                           Frequency :: float() | undefined) -> float().
free_space_path_loss(Loc1, Loc2, undefined) ->
    %% No frequency specified, defaulting to US915. Definitely incorrect.
    Distance = blockchain_utils:distance(Loc1, Loc2),
    10*math:log10(math:pow((4*math:pi()*(?FREQUENCY*?MHzToHzMultiplier)*(Distance*1000))/(299792458), 2));
free_space_path_loss(Loc1, Loc2, Frequency) ->
    Distance = blockchain_utils:distance(Loc1, Loc2),
    10*math:log10(math:pow((4*math:pi()*(Frequency*?MHzToHzMultiplier)*(Distance*1000))/(299792458), 2))-1.8-1.8.

free_space_path_loss(Loc1, Loc2, Gt, Gl) ->
    Distance = blockchain_utils:distance(Loc1, Loc2),
    %% TODO support regional parameters for non-US based hotspots
    %% TODO support variable Dt,Dr values for better FSPL values
    %% FSPL = 10log_10(Dt*Dr*((4*pi*f*d)/(c))^2)
    %%
    (10*math:log10(math:pow((4*math:pi()*(?FREQUENCY*?MHzToHzMultiplier)*(Distance*1000))/(299792458), 2)))-Gt-Gl.
free_space_path_loss(Loc1, Loc2, Frequency, Gt, Gl) ->
    Distance = blockchain_utils:distance(Loc1, Loc2),
    %% TODO support regional parameters for non-US based hotspots
    %% TODO support variable Dt,Dr values for better FSPL values
    %% FSPL = 10log_10(Dt*Dr*((4*pi*f*d)/(c))^2)
    %%
    (10*math:log10(math:pow((4*math:pi()*(Frequency*?MHzToHzMultiplier)*(Distance*1000))/(299792458), 2)))-Gt-Gl.

%% Subtract FSPL from our transmit power to get the expected minimum received signal.
-spec min_rcv_sig(float(), number()) -> float().
min_rcv_sig(Fspl, TxPower) ->
   TxPower - Fspl.
min_rcv_sig(Fspl) ->
   ?TRANSMIT_POWER - Fspl.

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
    Sum = lists:foldl(fun({_Node, Weight}, Acc) ->
                              Acc + Weight
                      end, 0, PopulationList),
    OrigRnd = normalize_float(Rnd * Sum),
    icdf_select_(PopulationList, OrigRnd).

-spec find_txn(Block :: blockchain_block:block(),
               PredFun :: fun()) -> [blockchain_txn:txn()].
find_txn(Block, PredFun) ->
    Txns = blockchain_block:transactions(Block),
    lists:filter(fun(T) -> PredFun(T) end, Txns).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
icdf_select_([{_Node, 0.0}], _Rnd) ->
    {error, zero_weight};
icdf_select_([{Node, _Weight}], _Rnd) ->
    {ok, Node};
icdf_select_([{Node, Weight} | _], Rnd) when Rnd - Weight =< 0 ->
    {ok, Node};
icdf_select_([{_Node, Weight} | Tail], Rnd) ->
    icdf_select_(Tail, normalize_float(Rnd - Weight)).



-spec map_to_bitvector(#{pos_integer() => boolean()}) -> binary().
map_to_bitvector(Map) ->
    %% grab the biggest index
    Sz = lists:max(maps:keys(Map)),
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


-spec bin_keys_to_list( Data :: binary() ) -> [ binary() ].
%% @doc Price oracle public keys and also staking keys are encoded like this
%% <code>
%% <<KeyLen1/integer, Key1/binary, KeyLen2/integer, Key2/binary, ...>>
%% </code>
%% This function takes the length tagged binary keys, removes the length tag
%% and returns a list of binary keys
%% @end
bin_keys_to_list(Data) when is_binary(Data) ->
    [ Key || << Len:8/unsigned-integer, Key:Len/binary >> <= Data ].

-spec keys_list_to_bin( [binary()] ) -> binary().
%% @doc Price oracle public keys and also staking keys are encoded like this
%% <code>
%% <<KeyLen1/integer, Key1/binary, KeyLen2/integer, Key2/binary, ...>>
%% </code>
%% This function takes the length tagged binary keys, removes the length tag
%% and returns a list of binary keys
%% @end
keys_list_to_bin(Keys) ->
    << <<(byte_size(Key)):8/integer, Key/binary>> || Key <- Keys >>.

-spec bin_to_prop( Data :: binary() ) -> [ {binary(), binary()} ].
%% @doc staking key mode mappings are encoded like this
%% <code>
%% <<KeyLen1/integer, Key1/binary, ValueLen1/integer, Value1/binary, KeyLen2/integer, Key2/binary, ValueLen2/integer, Value2/binary...>>
%% </code>
%% This function takes the length tagged binary keys & values, removes the length tag
%% and returns a binary keyed proplist
%% @end
bin_to_prop(Data) when is_binary(Data) ->
    bin_to_prop(Data, 8).

bin_to_prop(Data, Size) when is_binary(Data) ->
    [ {Key, Value} || << KeyLen:Size/unsigned-integer, Key:KeyLen/binary, ValueLen:Size/unsigned-integer, Value:ValueLen/binary >> <= Data ].

-spec prop_to_bin( [{binary(), binary()}] ) -> binary().
%% @doc staking key mode mappings are encoded like this
%% <code>
%% <<KeyLen1/integer, Key1/binary, ValueLen1/integer, Value1/binary, KeyLen2/integer, Key2/binary, ValueLen2/integer, Value2/binary...>>
%% </code>
%% This function takes a binary keyed proplist, tags the key and values with the length
%% and returns a list of binary keys
%% @end
prop_to_bin(Keys) ->
    prop_to_bin(Keys, 8).

prop_to_bin(Keys, Size) ->
    << <<(byte_size(Key)):Size/unsigned-integer, Key/binary, (byte_size(Value)):Size/unsigned-integer, Value/binary>> || {Key, Value} <- Keys >>.

%%--------------------------------------------------------------------
%% @doc deterministic random subset from a random seed
%% @end
%%--------------------------------------------------------------------
-spec deterministic_subset(pos_integer(), rand:state(), list()) -> {rand:seed(), list()}.
deterministic_subset(Limit, RandState, L) ->
    {RandState1, FullList} =
        lists:foldl(fun(Elt, {RS, Acc}) ->
                            {V, RS1} = rand:uniform_s(RS),
                            {RS1, [{V, Elt} | Acc]}
                    end,
                    {RandState, []},
                    L),
    TruncList0 = lists:sublist(lists:sort(FullList), Limit),
    {_, TruncList} = lists:unzip(TruncList0),
    {RandState1, TruncList}.

-spec index_of(any(), [any()]) -> pos_integer().
index_of(Item, List) -> index_of(Item, List, 1).

index_of(_, [], _)  -> not_found;
index_of(Item, [Item|_], Index) -> Index;
index_of(Item, [_|Tl], Index) -> index_of(Item, Tl, Index+1).

verify_multisig(Artifact, Sigs, Keys) ->
    %% using the number of keys is safe for the total because keys
    %% comes directly out of the ledger rather than from the submitter.
    Total = length(Keys),
    lager:debug("sigs ~p keys ~p", [Sigs, Keys]),
    Votes = count_votes(Artifact, Keys, Sigs),
    Majority = majority(Total),
    lager:info("votes ~p, majority: ~p", [Votes, Majority]),
    Votes >= Majority.

count_votes(Artifact, MultiKeys, Proofs) ->
    %% fold over the proofs as they're likely to be shorter than the list of keys
    {_UnusedKeys, Count} = lists:foldl(fun(Proof, {Keys, Count}) ->
                                               case find_key(Proof, Artifact, Keys) of
                                                   undefined ->
                                                       {Keys, Count};
                                                   GoodKey ->
                                                       %% remove a matched key from the list so it can't doublesign
                                                       %% and to reduce the search space, then increment the count
                                                       {Keys -- [GoodKey], Count + 1}
                                               end
                                       end, {MultiKeys, 0}, Proofs),
    Count.

find_key(_, _, []) ->
    undefined;
find_key(Proof, Artifact, [Key|Keys]) ->
    case libp2p_crypto:verify(Artifact, Proof,
                              libp2p_crypto:bin_to_pubkey(Key)) of
        true ->
            %% return early
            Key;
        false ->
            find_key(Proof, Artifact, Keys)
    end.

-spec poc_per_hop_max_witnesses(Ledger :: blockchain_ledger_v1:ledger()) -> pos_integer().
poc_per_hop_max_witnesses(Ledger) ->
    case blockchain:config(?poc_per_hop_max_witnesses, Ledger) of
        {ok, N} -> N;
        _ ->
            %% Defaulted to 5 to preserve backward compatability
            ?POC_PER_HOP_MAX_WITNESSES
    end.

%%--------------------------------------------------------------------
%% @doc Given a list of tuples of zero arity functions that return a
%% boolean and error tuples, evaluate each function. If a function
%% returns `false' then immediately return the associated error tuple.
%% Otherwise, if all conditions evaluate as `true', return `ok'.
%% @end
%%--------------------------------------------------------------------
-spec fold_condition_checks([{Condition :: fun(() -> boolean()),
                              Error :: {error, any()}}]) -> ok | {error, any()}.
fold_condition_checks(Conditions) ->
    do_condition_check(Conditions, undefined, true).

do_condition_check(_Conditions, PrevErr, false) -> PrevErr;
do_condition_check([], _PrevErr, true) -> ok;
do_condition_check([{Condition, Error}|Tail], _PrevErr, true) ->
    do_condition_check(Tail, Error, Condition()).

%% @doc If this os environment variable is set and is a "truthy" value,
%% return `true'; otherwise, `false'. Unset variables return the default value.
%%
%% False values are "0", "n", "no", "false". Values are converted to lowercase
%% before deciding if they are truthy or not ("FaLsE" is a false value.)
%%
%% True values are any value that isn't listed as explicitly false.
%% ("1", "tRuE", "yes" and "WOMBAT" are all true values.)
-spec get_boolean_os_env_var( VarName :: string(), Default :: boolean() ) -> boolean().
get_boolean_os_env_var(VarName, Default) ->
    case os:getenv(VarName) of
        Value when is_list(Value) -> is_truthy(string:lowercase(Value));
        _ -> Default
    end.

is_truthy("0") -> false;
is_truthy("false") -> false;
is_truthy("no") -> false;
is_truthy("n") -> false;
is_truthy(_) -> true.

-spec streaming_file_hash( File :: file:file_name() ) -> {ok, Hash :: binary()} | {error, Reason :: term()}.
streaming_file_hash(File) ->
    case file:open(File, [read, raw, binary]) of
        {ok, FD} ->
            RetVal = case do_hash(FD, crypto:hash_init(sha256)) of
                         {error, _E} = Err -> Err;
                         {ok, HashState} -> {ok, crypto:hash_final(HashState)}
                     end,
            file:close(FD),
            RetVal;
        {error, _E} = Err -> Err
    end.

do_hash(FD, HashState) ->
    case file:read(FD, ?BLOCK_READ_SIZE) of
        eof -> {ok, HashState};
        {ok, Data} -> do_hash(FD, crypto:hash_update(HashState, Data));
        {error, _E} = Err -> Err
    end.

streaming_transform_iolist(L, Fun) when is_list(L) ->
    do_transform_iolist(L, 1, length(L)+1, Fun).

do_transform_iolist(_L, Pos, End, Fun) when Pos >= End ->
    Fun(eof),
    ok;
do_transform_iolist(L, Pos, End, Fun) ->
    Fun(lists:sublist(L, Pos, 32000)),
    do_transform_iolist(L, Pos+32000, End, Fun).

majority(N) ->
    (N div 2) + 1.

-spec get_vars(VarList :: [atom()], Ledger :: blockchain_ledger_v1:ledger()) -> #{atom() => any()}.
get_vars(VarList, Ledger) ->
    {ok, VarsNonce} = blockchain_ledger_v1:vars_nonce(Ledger),
    IsAux = blockchain_ledger_v1:is_aux(Ledger),
    lists:foldl(
      fun(VarName, Acc) ->
              %% NOTE: This isn't ideal but in order for get_var/2 to
              %% correspond with blockchain:config/2, it returns {ok, ..} | {error, ..}
              %% So we just put undefined for any error lookups here.
              %% The callee must handle those situations.
              case get_var_(VarName, IsAux, VarsNonce, Ledger) of
                  {ok, VarValue} -> maps:put(VarName, VarValue, Acc);
                  _ -> maps:put(VarName, undefined, Acc)
              end
      end, #{}, VarList).

-spec get_var_(VarName :: atom(),
               IsAux :: boolean(),
               VarsNonce :: non_neg_integer(),
               Ledger :: blockchain_ledger_v1:ledger()) -> {ok, any()} | {error, any()}.
get_var_(VarName, HasAux, VarsNonce, Ledger) ->
    e2qc:cache(
        ?VAR_CACHE,
        {HasAux, VarsNonce, VarName},
        fun() ->
            get_var_(VarName, Ledger)
        end
    ).

-spec get_var(VarName :: atom(), Ledger :: blockchain_ledger_v1:ledger()) -> {ok, any()} | {error, any()}.
get_var(VarName, Ledger) ->
    get_var_(VarName, Ledger).

-spec get_var_(VarName :: atom(), Ledger :: blockchain_ledger_v1:ledger()) -> {ok, any()} | {error, any()}.
get_var_(VarName, Ledger) ->
    blockchain_ledger_v1:config(VarName, Ledger).

-spec var_cache_stats() -> list().
var_cache_stats() ->
    %%e2qc:stats(?VAR_CACHE).
    [].

-spec teardown_var_cache() -> ok.
teardown_var_cache() ->
    e2qc:teardown(?VAR_CACHE).

init_var_cache() ->
    %% TODO could pull cache settings from app env here
    e2qc:setup(?VAR_CACHE, []).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

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
    L01 = [begin B = case rand:uniform(3) of 1 -> true; 2 -> false; _ -> remove end, {N,B} end || N <- lists:seq(1, 16)],
    L02 = [begin B = case rand:uniform(3) of 1 -> true; 2 -> false; _ -> remove end, {N,B} end || N <- lists:seq(1, 19)],
    L03 = [begin B = case rand:uniform(3) of 1 -> true; 2 -> false; _ -> remove end, {N,B} end || N <- lists:seq(1, 64)],
    L04 = [begin B = case rand:uniform(3) of 1 -> true; 2 -> false; _ -> remove end, {N,B} end || N <- lists:seq(1, 122)],

    L1 = lists:filter(fun({_, remove}) -> false; (_) -> true end, L01),
    L2 = lists:filter(fun({_, remove}) -> false; (_) -> true end, L02),
    L3 = lists:filter(fun({_, remove}) -> false; (_) -> true end, L03),
    L4 = lists:filter(fun({_, remove}) -> false; (_) -> true end, L04),

    M1 = maps:from_list(L1),
    M2 = maps:from_list(L2),
    M3 = maps:from_list(L3),
    M4 = maps:from_list(L4),

    ?assert(compare(M1, bitvector_to_map(16, map_to_bitvector(M1)))),
    ?assert(compare(M2, bitvector_to_map(19, map_to_bitvector(M2)))),
    ?assert(compare(M3, bitvector_to_map(64, map_to_bitvector(M3)))),
    ?assert(compare(M4, bitvector_to_map(122, map_to_bitvector(M4)))),

    BV1 = map_to_bitvector(#{1 => true, 9 => true}),
    ?assertEqual(2, byte_size(BV1)),
    BV2 = map_to_bitvector(#{1 => true, 9 => true, 17 => true}),
    ?assertEqual(3, byte_size(BV2)),
    BV3 = map_to_bitvector(#{1 => true, 9 => true, 17 => true, 25 => true}),
    ?assertEqual(4, byte_size(BV3)),

    ok.

compare(M1, M2) ->
    maps:fold(
      fun(_, _, false) ->
              false;
         (K, V, true) ->
              case maps:find(K, M2) of
                  {ok, V} -> true;
                  _ -> false
              end
      end,
      true,
      M1).

oracle_keys_test() ->
    #{ public := RawEccPK } = libp2p_crypto:generate_keys(ecc_compact),
    #{ public := RawEdPK } = libp2p_crypto:generate_keys(ed25519),
    EccPK = libp2p_crypto:pubkey_to_bin(RawEccPK),
    EdPK = libp2p_crypto:pubkey_to_bin(RawEdPK),
    TestOracleKeys = keys_list_to_bin([EccPK, EdPK]),
    Results = bin_keys_to_list(TestOracleKeys),
    ?assertEqual([EccPK, EdPK], Results),
    Results1 = [ libp2p_crypto:bin_to_pubkey(K) || K <- Results ],
    ?assertEqual([RawEccPK, RawEdPK], Results1).

staking_keys_to_mode_mappings_test() ->
    #{ public := RawEccPK1 } = libp2p_crypto:generate_keys(ecc_compact),
    #{ public := RawEccPK2 } = libp2p_crypto:generate_keys(ecc_compact),
    #{ public := RawEdPK } = libp2p_crypto:generate_keys(ed25519),
    EccPK1 = libp2p_crypto:pubkey_to_bin(RawEccPK1),
    EccPK2 = libp2p_crypto:pubkey_to_bin(RawEccPK2),
    EdPK = libp2p_crypto:pubkey_to_bin(RawEdPK),
    BinMappings = prop_to_bin([{EccPK1, <<"dataonly">>}, {EccPK2, <<"light">>}, {EdPK, <<"full">>}]),
    Results = bin_to_prop(BinMappings),
    ?assertEqual([{EccPK1, <<"dataonly">>}, {EccPK2, <<"light">>}, {EdPK, <<"full">>}], Results),
    Results1 = [ libp2p_crypto:bin_to_pubkey(K) || {K, _V} <- Results ],
    ?assertEqual([RawEccPK1, RawEccPK2, RawEdPK], Results1).

calculate_dc_amount_test() ->{
    timeout,
    30,
    fun() ->
        BaseDir = test_utils:tmp_dir("calculate_dc_amount_test"),
        Ledger = blockchain_ledger_v1:new(BaseDir),

        meck:new(blockchain_ledger_v1, [passthrough]),
        meck:expect(blockchain_ledger_v1, config, fun(_, _) ->
            {ok, 24}
        end),

        ?assertEqual(1, calculate_dc_amount(Ledger, 1)),
        ?assertEqual(1, calculate_dc_amount(Ledger, 23)),
        ?assertEqual(1, calculate_dc_amount(Ledger, 24)),
        ?assertEqual(2, calculate_dc_amount(Ledger, 25)),
        ?assertEqual(2, calculate_dc_amount(Ledger, 47)),
        ?assertEqual(2, calculate_dc_amount(Ledger, 48)),
        ?assertEqual(3, calculate_dc_amount(Ledger, 49)),

        meck:unload(blockchain_ledger_v1),
        test_utils:cleanup_tmp_dir(BaseDir)
    end
    }.

count_votes_test() ->
    #{ public := PubKey1, secret := SecKey1} = libp2p_crypto:generate_keys(ecc_compact),
    #{ public := PubKey2, secret := SecKey2} = libp2p_crypto:generate_keys(ecc_compact),
    #{ public := PubKey3, secret := SecKey3} = libp2p_crypto:generate_keys(ecc_compact),
    #{ public := PubKey4, secret := SecKey4} = libp2p_crypto:generate_keys(ecc_compact),

    PKeys = [libp2p_crypto:pubkey_to_bin(PK) || PK <- [PubKey1, PubKey2, PubKey3, PubKey4]],

    Artifact = crypto:strong_rand_bytes(10),

    Sigs = [ (libp2p_crypto:mk_sig_fun(SK))(Artifact) || SK <- [SecKey1, SecKey2, SecKey3, SecKey4] ],

    %% check signatures cannot be double counted
    ?assertEqual(4, count_votes(Artifact, PKeys, Sigs)),
    ?assertEqual(4, count_votes(Artifact, PKeys, Sigs ++ [hd(Sigs)])),
    ?assertEqual(3, count_votes(Artifact, PKeys, tl(Sigs) ++ tl(Sigs))),

    %% check signatures from existing keys cannot be counted
    DupSig = (libp2p_crypto:mk_sig_fun(SecKey2))(Artifact),
    ?assertEqual(3, count_votes(Artifact, PKeys, tl(Sigs) ++ [DupSig])),

    %% check signatures from unknown keys do not count
    #{ public := PubKey5, secret := SecKey5} = libp2p_crypto:generate_keys(ecc_compact),
    ExtraSig = (libp2p_crypto:mk_sig_fun(SecKey5))(Artifact),
    ?assertEqual(4, count_votes(Artifact, PKeys, Sigs ++ [ExtraSig])),

    %% check adding the unknown key to the list does work
    ?assertEqual(5, count_votes(Artifact, PKeys ++ [libp2p_crypto:pubkey_to_bin(PubKey5)], Sigs ++ [ExtraSig])),
    ok.

fold_condition_checks_good_test() ->
    Conditions = [{fun() -> true end, {error, true_isnt_true}},
                  {fun() -> 100 > 10 end, {error, one_hundred_greater_than_10}},
                  {fun() -> <<"blort">> == <<"blort">> end, {error, blort_isnt_blort}}],
    ?assertEqual(ok, fold_condition_checks(Conditions)).

fold_condition_checks_bad_test() ->
    Bad = [{fun() -> true end, {error, true_isnt_true}},
           {fun() -> 10 > 100 end, {error, '10_not_greater_than_100'}},
           {fun() -> <<"blort">> == <<"blort">> end, {error, blort_isnt_blort}}],
    ?assertEqual({error, '10_not_greater_than_100'}, fold_condition_checks(Bad)).

pfind_test() ->
    erlang:trace(new_processes, true, [{tracer, self()}, procs]),

    Args = lists:seq(1, 6),
    F = fun(I) ->
        case I rem 2 == 0 of
            true ->
                case I == 2 of
                    true ->
                        {true, I};
                    false ->
                        timer:sleep(10),
                        {true, I}
                end;
            false ->
                false
        end
    end,
    %% In this case 2 should always win, 4 and 6 should take too long and be killed (aka done)
    %% The rest should just not match
    ?assertEqual({true, 2}, pfind(F, [[I] || I <- Args])),

    %% We should have N+1 pids spawned from self()
    %% where N = length(Args) and +1 = original process
    X = erlang:length(Args) + 1,
    Pids = spawned_rcv(self()),
    ?assertEqual(X, erlang:length(Pids)),

    %% 0 = normal (starting pid), 1 = normal, 2 normal (picked), 3 = normal, 4 = done, 5 = normal, 6 = done
    Exits = exit_rcv([]),
    ?assertEqual(X, erlang:length(Exits)),
    ?assertEqual(lists:sort(Pids), lists:sort([P || {P, _} <- Exits])),
    ?assertEqual(5, erlang:length([P || {P, Reason} <- Exits, Reason == normal])),
    ?assertEqual(2, erlang:length([P || {P, Reason} <- Exits, Reason == done])),

    erlang:trace(new_processes, false, [{tracer, self()}, procs]),
    ok.

spawned_rcv(SpawnedBy) ->
    receive
        {trace, Spawned, spawned, SpawnedBy, _} ->
            spawn_rcv(Spawned, [Spawned])
    after 10 ->
        erlang:throw(spawned_lvl_1)
    end.

spawn_rcv(SpawnedBy, Acc) ->
    receive
        {trace, Spawned, spawned, SpawnedBy, _} ->
            spawn_rcv(SpawnedBy, [Spawned|Acc])
    after 10 ->
        Acc
    end.

exit_rcv(Acc) ->
    receive
        {trace, Pid, exit, Reason} ->
            exit_rcv([{Pid, Reason}|Acc])
    after 10 ->
        Acc
    end.

-endif.
