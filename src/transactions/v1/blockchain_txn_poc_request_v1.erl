%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Create Proof of Coverage Request ==
%% Submitted by a gateway who wishes to initiate a PoC Challenge
%%%-------------------------------------------------------------------
-module(blockchain_txn_poc_request_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_caps.hrl").
-include("blockchain_json.hrl").

-include_lib("helium_proto/include/blockchain_txn_poc_request_v1_pb.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_utils.hrl").

-export([
    new/5,
    get_version/1,
    hash/1,
    challenger/1,
    secret_hash/1,
    onion_key_hash/1,
    block_hash/1,
    signature/1,
    version/1,
    fee/1,
    fee_payer/2,
    sign/2,
    is_valid/2,
    is_well_formed/1,
    is_prompt/2,
    absorb/2,
    print/1,
    json_type/0,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(T, #blockchain_txn_poc_request_v1_pb).

-type t() :: txn_poc_request().

-type txn_poc_request() :: ?T{}.

-export_type([t/0, txn_poc_request/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:pubkey_bin(), binary(), binary(), binary(),
          non_neg_integer()) -> txn_poc_request().
new(Challenger, SecretHash, OnionKeyHash, BlockHash, Version) ->
    #blockchain_txn_poc_request_v1_pb{
        challenger=Challenger,
        secret_hash=SecretHash,
        onion_key_hash=OnionKeyHash,
        block_hash=BlockHash,
        fee=0,
        signature = <<>>,
        version = Version
    }.

-spec get_version(blockchain:ledger()) -> integer().
get_version(Ledger) ->
    {ok, Mod} = blockchain:config(?predicate_callback_mod, Ledger),
    {ok, Fun} = blockchain:config(?predicate_callback_fun, Ledger),
    Mod:Fun().

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_poc_request()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_poc_request_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_poc_request_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec challenger(txn_poc_request()) -> libp2p_crypto:pubkey_bin().
challenger(Txn) ->
    Txn#blockchain_txn_poc_request_v1_pb.challenger.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec secret_hash(txn_poc_request()) -> blockchain_txn:hash().
secret_hash(Txn) ->
    Txn#blockchain_txn_poc_request_v1_pb.secret_hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec onion_key_hash(txn_poc_request()) -> binary().
onion_key_hash(Txn) ->
    Txn#blockchain_txn_poc_request_v1_pb.onion_key_hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec block_hash(txn_poc_request()) -> binary().
block_hash(Txn) ->
    Txn#blockchain_txn_poc_request_v1_pb.block_hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_poc_request()) -> 0.
fee(_Txn) ->
    0.

-spec fee_payer(txn_poc_request(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_poc_request()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_poc_request_v1_pb.signature.

-spec version(txn_poc_request()) -> non_neg_integer().
version(Txn) ->
    Txn#blockchain_txn_poc_request_v1_pb.version.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_poc_request(), libp2p_crypto:sig_fun()) -> txn_poc_request().
sign(Txn0, SigFun) ->
    Txn1 = Txn0#blockchain_txn_poc_request_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_poc_request_v1_pb:encode_msg(Txn1),
    Txn0#blockchain_txn_poc_request_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_poc_request(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Challenger = ?MODULE:challenger(Txn),
    ChallengerSignature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Challenger),
    BaseTxn = Txn#blockchain_txn_poc_request_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_poc_request_v1_pb:encode_msg(BaseTxn),

    case blockchain_txn:validate_fields([{{secret_hash, ?MODULE:secret_hash(Txn)}, {binary, 32}},
                                         {{onion_key_hash, ?MODULE:secret_hash(Txn)}, {binary, 32}},
                                         {{block_hash, ?MODULE:secret_hash(Txn)}, {binary, 32}}]) of
        ok ->
            case libp2p_crypto:verify(EncodedTxn, ChallengerSignature, PubKey) of
                false ->
                    {error, bad_signature};
                true ->
                    StartFind = maybe_start_duration(),
                    case blockchain_ledger_v1:find_gateway_info(Challenger, Ledger) of
                        {error, not_found} ->
                            {error, missing_gateway};
                        {error, _Reason}=Error ->
                            Error;
                        {ok, Info} ->
                            StartCap = maybe_log_duration(fetch_gw, StartFind),
                            %% check the gateway mode to determine if its allowed to issue POC requests
                            Mode = blockchain_ledger_gateway_v2:mode(Info),
                            case blockchain_ledger_gateway_v2:is_valid_capability(Mode, ?GW_CAPABILITY_POC_CHALLENGER, Ledger) of
                                false -> {error, {gateway_not_allowed, blockchain_ledger_gateway_v2:mode(Info)}};
                                true ->
                                    StartRest = maybe_log_duration(check_cap, StartCap),
                                    case blockchain_ledger_gateway_v2:location(Info) of
                                        undefined ->
                                            lager:info("no loc for challenger: ~p ~p", [Challenger, Info]),
                                            {error, no_gateway_location};
                                        _Location ->
                                            {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
                                            LastChallenge = blockchain_ledger_gateway_v2:last_poc_challenge(Info),
                                            PoCInterval = blockchain_utils:challenge_interval(Ledger),
                                            case LastChallenge == undefined orelse LastChallenge =< (Height+1  - PoCInterval) of
                                                false ->
                                                    {error, too_many_challenges};
                                                true ->
                                                    BlockHash = ?MODULE:block_hash(Txn),
                                                    case blockchain:get_block_height(BlockHash, Chain) of
                                                        {error, not_found} ->
                                                            {error, missing_challenge_block_hash};
                                                        {error, _}=Error ->
                                                            Error;
                                                        {ok, BlockHeight} ->
                                                            case (BlockHeight + PoCInterval) > (Height+1) of
                                                                false ->
                                                                    {error, replaying_request};
                                                                true ->
                                                                    Fee = ?MODULE:fee(Txn),
                                                                    Owner = blockchain_ledger_gateway_v2:owner_address(Info),
                                                                    R = blockchain_ledger_v1:check_dc_balance(Owner, Fee, Ledger),
                                                                    maybe_log_duration(rest, StartRest),
                                                                    R
                                                            end
                                                    end
                                            end
                                    end
                            end
                    end
            end;
        Error -> Error
    end.

-spec is_well_formed(t()) -> ok | {error, {contract_breach, any()}}.
is_well_formed(?T{}) ->
    ok.

-spec is_prompt(t(), blockchain:blockchain()) ->
    {ok, blockchain_txn:is_prompt()} | {error, any()}.
is_prompt(?T{}, _) ->
    {ok, yes}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_poc_request(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Challenger = ?MODULE:challenger(Txn),
    Version = version(Txn),
    SecretHash = ?MODULE:secret_hash(Txn),
    OnionKeyHash = ?MODULE:onion_key_hash(Txn),
    BlockHash = ?MODULE:block_hash(Txn),
    blockchain_ledger_v1:request_poc(OnionKeyHash, SecretHash, Challenger, BlockHash, Version, Ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_poc_request()) -> iodata().
print(undefined) -> <<"type=poc_request, undefined">>;
print(#blockchain_txn_poc_request_v1_pb{challenger=Challenger, secret_hash=SecretHash,
                                        onion_key_hash=OnionKeyHash, block_hash=BlockHash,
                                        fee=Fee, signature = Sig, version = Version }) ->
    %% XXX: Should we really print the secret hash in a log???
    io_lib:format("type=poc_request challenger=~p, secret_hash=~p, onion_key_hash=~p, block_hash=~p, fee=~p, signature=~p, version=~p",
                  [?TO_ANIMAL_NAME(Challenger), SecretHash, ?TO_B58(OnionKeyHash), BlockHash, Fee, Sig, Version]).

json_type() ->
    <<"poc_request_v1">>.

-spec to_json(txn_poc_request(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      challenger => ?BIN_TO_B58(challenger(Txn)),
      secret_hash => ?BIN_TO_B64(secret_hash(Txn)),
      onion_key_hash => ?BIN_TO_B64(onion_key_hash(Txn)),
      block_hash => ?BIN_TO_B64(block_hash(Txn)),
      version => version(Txn),
      fee => fee(Txn)
     }.

%% TODO: I'm not sure that this is actually faster than checking the time, but I suspect that it'll
%% be more lock-friendly?
maybe_start_duration() ->
    case application:get_env(blockchain, log_validation_times, false) of
        true ->
            erlang:monotonic_time(microsecond);
        _ -> 0
    end.

maybe_log_duration(Type, Start) ->
    case application:get_env(blockchain, log_validation_times, false) of
        true ->
            End = erlang:monotonic_time(microsecond),
            lager:info("~p took ~p ms", [Type, End - Start]),
            End;
        _ -> ok
    end.


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_poc_request_v1_pb{
        challenger= <<"gateway">>,
        secret_hash= <<"hash">>,
        onion_key_hash = <<"onion">>,
        block_hash = <<"block">>,
        fee=0,
        signature= <<>>,
        version = 1
    },
    ?assertEqual(Tx, new(<<"gateway">>, <<"hash">>, <<"onion">>, <<"block">>, 1)).

challenger_test() ->
    Tx = new(<<"gateway">>, <<"hash">>, <<"onion">>, <<"block">>, 1),
    ?assertEqual(<<"gateway">>, challenger(Tx)).

secret_hash_test() ->
    Tx = new(<<"gateway">>, <<"hash">>, <<"onion">>, <<"block">>, 1),
    ?assertEqual(<<"hash">>, secret_hash(Tx)).

onion_key_hash_test() ->
    Tx = new(<<"gateway">>, <<"hash">>, <<"onion">>, <<"block">>, 1),
    ?assertEqual(<<"onion">>, onion_key_hash(Tx)).

block_hash_test() ->
    Tx = new(<<"gateway">>, <<"hash">>, <<"onion">>, <<"block">>, 1),
    ?assertEqual(<<"block">>, block_hash(Tx)).

fee_test() ->
    Tx = new(<<"gateway">>, <<"hash">>, <<"onion">>, <<"block">>, 1),
    ?assertEqual(0, fee(Tx)).

signature_test() ->
    Tx = new(<<"gateway">>, <<"hash">>, <<"onion">>, <<"block">>, 1),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"gateway">>, <<"hash">>, <<"onion">>, <<"block">>, 1),
    ChallengerSigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, ChallengerSigFun),
    EncodedTx1 = blockchain_txn_poc_request_v1_pb:encode_msg(
        Tx1#blockchain_txn_poc_request_v1_pb{signature = <<>>}
    ),
    ?assert(libp2p_crypto:verify(EncodedTx1, signature(Tx1), PubKey)).


to_json_test() ->
    Tx = new(<<"gateway">>, <<"hash">>, <<"onion">>, <<"block">>, 1),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, challenger, secret_hash, onion_key_hash, block_hash, version, fee])).


-endif.
