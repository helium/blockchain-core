%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Create Proof of Coverage Request ==
%% Submitted by a gateway who wishes to initiate a PoC Challenge
%%%-------------------------------------------------------------------
-module(blockchain_txn_poc_request_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_poc_request_v1_pb.hrl").

-export([
    new/4,
    hash/1,
    challenger/1,
    secret_hash/1,
    onion_key_hash/1,
    block_hash/1,
    signature/1,
    fee/1,
    sign/2,
    is_valid/2,
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_poc_request() :: #blockchain_txn_poc_request_v1_pb{}.
-export_type([txn_poc_request/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:pubkey_bin(), binary(), binary(), binary()) -> txn_poc_request().
new(Challenger, SecretHash, OnionKeyHash, BlockHash) ->
    #blockchain_txn_poc_request_v1_pb{
        challenger=Challenger,
        secret_hash=SecretHash,
        onion_key_hash=OnionKeyHash,
        block_hash=BlockHash,
        fee=0,
        signature = <<>>
    }.

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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_poc_request()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_poc_request_v1_pb.signature.


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
-spec is_valid(txn_poc_request(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Challenger = ?MODULE:challenger(Txn),
    ChallengerSignature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Challenger),
    BaseTxn = Txn#blockchain_txn_poc_request_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_poc_request_v1_pb:encode_msg(BaseTxn),
    case libp2p_crypto:verify(EncodedTxn, ChallengerSignature, PubKey) of
        false ->
            {error, bad_signature};
        true ->
            case blockchain_ledger_v1:find_gateway_info(Challenger, Ledger) of
                {error, _Reason}=Error ->
                    Error;
                {ok, Info} ->
                    case blockchain_ledger_gateway_v1:location(Info) of
                        undefined ->
                            {error, no_gateway_location};
                        _Location ->
                            {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
                            LastChallenge = blockchain_ledger_gateway_v1:last_poc_challenge(Info),
                            PoCInterval = blockchain_poc:challenge_interval(Ledger),
                            case LastChallenge == undefined orelse LastChallenge =< (Height+1  - PoCInterval) of
                                false ->
                                    {error, too_many_challenges};
                                true ->
                                    BlockHash = ?MODULE:block_hash(Txn),
                                    case blockchain:get_block(BlockHash, Chain) of
                                        {error, _}=Error ->
                                            Error;
                                        {ok, Block1} ->
                                            case (blockchain_block:height(Block1) + PoCInterval) > (Height+1) of
                                                false ->
                                                    {error, replaying_request};
                                                true ->
                                                    Fee = ?MODULE:fee(Txn),
                                                    Owner = blockchain_ledger_gateway_v1:owner_address(Info),
                                                    blockchain_ledger_v1:check_balance(Owner, Fee, Ledger)
                                            end
                                    end
                            end
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_poc_request(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Challenger = ?MODULE:challenger(Txn),
    case blockchain_ledger_v1:find_gateway_info(Challenger, Ledger) of
        {ok, GwInfo} ->
            Fee = ?MODULE:fee(Txn),
            Owner = blockchain_ledger_gateway_v1:owner_address(GwInfo),
            case blockchain_ledger_v1:debit_fee(Owner, Fee, Ledger) of
                {error, _Reason}=Error ->
                    Error;
                ok ->
                    SecretHash = ?MODULE:secret_hash(Txn),
                    OnionKeyHash = ?MODULE:onion_key_hash(Txn),
                    blockchain_ledger_v1:request_poc(OnionKeyHash, SecretHash, Challenger, Ledger)
            end;
        {error, _Reason}=Error ->
            Error
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
        signature= <<>>
    },
    ?assertEqual(Tx, new(<<"gateway">>, <<"hash">>, <<"onion">>, <<"block">>)).

challenger_test() ->
    Tx = new(<<"gateway">>, <<"hash">>, <<"onion">>, <<"block">>),
    ?assertEqual(<<"gateway">>, challenger(Tx)).

secret_hash_test() ->
    Tx = new(<<"gateway">>, <<"hash">>, <<"onion">>, <<"block">>),
    ?assertEqual(<<"hash">>, secret_hash(Tx)).

onion_key_hash_test() ->
    Tx = new(<<"gateway">>, <<"hash">>, <<"onion">>, <<"block">>),
    ?assertEqual(<<"onion">>, onion_key_hash(Tx)).

block_hash_test() ->
    Tx = new(<<"gateway">>, <<"hash">>, <<"onion">>, <<"block">>),
    ?assertEqual(<<"block">>, block_hash(Tx)).

fee_test() ->
    Tx = new(<<"gateway">>, <<"hash">>, <<"onion">>, <<"block">>),
    ?assertEqual(0, fee(Tx)).

signature_test() ->
    Tx = new(<<"gateway">>, <<"hash">>, <<"onion">>, <<"block">>),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"gateway">>, <<"hash">>, <<"onion">>, <<"block">>),
    ChallengerSigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, ChallengerSigFun),
    EncodedTx1 = blockchain_txn_poc_request_v1_pb:encode_msg(
        Tx1#blockchain_txn_poc_request_v1_pb{signature = <<>>}
    ),
    ?assert(libp2p_crypto:verify(EncodedTx1, signature(Tx1), PubKey)).


-endif.
