%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Proof of Coverage Receipts ==
%%%-------------------------------------------------------------------
-module(blockchain_txn_poc_receipts_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_poc_receipts_v1_pb.hrl").

-export([
    new/5,
    hash/1,
    onion_key/1,
    challenger/1,
    secret/1,
    receipts/1,
    witnesses/1,
    fee/1,
    signature/1,
    sign/2,
    is_valid/3,
    absorb/3,
    create_secret_hash/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_poc_receipts() :: #blockchain_txn_poc_receipts_v1_pb{}.

-export_type([txn_poc_receipts/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:pubkey_bin(), blockchain_poc_receipt_v1:poc_receipts(),
          blockchain_poc_witness_v1:poc_witnesss(), libp2p_crypto:pubkey_bin(),
          binary()) -> txn_poc_receipts().
new(OnionKey, Receipts, Witnesses, Challenger, Secret) ->
    #blockchain_txn_poc_receipts_v1_pb{
        onion_key=OnionKey,
        receipts=Receipts,
        witnesses=Witnesses,
        challenger=Challenger,
        secret=Secret,
        fee=0,
        signature = <<>>
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_poc_receipts()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_poc_receipts_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_poc_receipts_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec onion_key(txn_poc_receipts()) -> binary().
onion_key(Txn) ->
    Txn#blockchain_txn_poc_receipts_v1_pb.onion_key.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec challenger(txn_poc_receipts()) -> libp2p_crypto:pubkey_bin().
challenger(Txn) ->
    Txn#blockchain_txn_poc_receipts_v1_pb.challenger.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec secret(txn_poc_receipts()) -> binary().
secret(Txn) ->
    Txn#blockchain_txn_poc_receipts_v1_pb.secret.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec receipts(txn_poc_receipts()) -> blockchain_poc_receipt_v1:poc_receipts().
receipts(Txn) ->
    Txn#blockchain_txn_poc_receipts_v1_pb.receipts.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec witnesses(txn_poc_receipts()) -> blockchain_poc_witness_v1:poc_witnesss().
witnesses(Txn) ->
    Txn#blockchain_txn_poc_receipts_v1_pb.witnesses.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_poc_receipts()) -> 0.
fee(_Txn) ->
    0.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_poc_receipts()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_poc_receipts_v1_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_poc_receipts(), libp2p_crypto:sig_fun()) -> txn_poc_receipts().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_poc_receipts_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_poc_receipts_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_poc_receipts_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_poc_receipts(),
               blockchain_block:block(),
               blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
is_valid(Txn, _Block, Ledger) ->
    Challenger0 = ?MODULE:challenger(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Challenger0),
    BaseTxn = Txn#blockchain_txn_poc_receipts_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_poc_receipts_v1_pb:encode_msg(BaseTxn),
    case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
        false ->
            {error, bad_signature};
        true ->
            case ?MODULE:receipts(Txn) =:= [] andalso ?MODULE:witnesses(Txn) =:= [] of
                true ->
                    {error, empty_receipts_witness};
                false ->
                    case blockchain_ledger_v1:find_poc(?MODULE:onion_key(Txn), Ledger) of
                        {error, not_found} ->
                            {error, poc_not_found};
                        {error, _}=Error ->
                            Error;
                        {ok, PoC} ->
                            Secret = ?MODULE:secret(Txn),
                            SecretHash = blockchain_ledger_poc_v1:secret_hash(PoC),
                            Challenger1 = blockchain_ledger_poc_v1:challenger(PoC),
                            case {crypto:hash(sha256, Secret) =:= SecretHash,
                                  Challenger0 =:= Challenger1} of
                                {true, true} ->
                                    ok;
                                    % TODO: Once chain rewing ready use blockchain:ledger_at(Height) to grap
                                    % ledger at time X. For now do nothing.
                                    % Use create_secret_hash to verify receipts

                                    % {Target, ActiveGateways} = blockchain_poc_path:target(Secret, Ledger),
                                    % lager:info("target: ~p", [Target]),
                                    % {ok, Path} = blockchain_poc_path:build(Target, ActiveGateways),
                                    % lager:info("path: ~p", [Path]),
                                    % ReceiptAddrs = lists:sort([blockchain_poc_receipt_v1:address(R) || R <- ?MODULE:receipts(Txn)]),
                                    % case ReceiptAddrs == lists:sort(Path) andalso
                                    %      lists:member(Target, ReceiptAddrs) of
                                    %     true ->
                                    %         lager:info("got receipts from all the members in the path!");
                                    %     false ->
                                    %         lager:warning("missing receipts!, reconstructedPath: ~p, receivedReceipts: ~p", [Path, ReceiptAddrs]),
                                    %         ok
                                    % end;
                                {false, _} ->
                                    lager:error("PoC receipt secret hash does not match: ~p =/= ~p", [crypto:hash(sha256, Secret), SecretHash]),
                                    {error, bad_secret_hash};
                                {_, false} ->
                                    lager:error("PoC receipt challenger does not match: ~p =/= ~p", [Challenger0, Challenger1]),
                                    {error, bad_challenger}
                            end
                    end
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_poc_receipts(),
             blockchain_block:block(),
             blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
absorb(Txn, _Block, Ledger) ->
    OnionKeyHash = ?MODULE:onion_key(Txn),
    % TODO: Update score here
    blockchain_ledger_v1:delete_poc(OnionKeyHash, Ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec create_secret_hash(binary(), non_neg_integer()) -> [binary()].
create_secret_hash(Secret, X) when X > 0 ->
    create_secret_hash(Secret, X, []).

-spec create_secret_hash(binary(), non_neg_integer(), [binary()]) -> [binary()].
create_secret_hash(_Secret, 0, Acc) ->
    Acc;
create_secret_hash(Secret, X, []) ->
    Bin = crypto:hash(sha256, Secret),
    <<Hash:4/binary, _/binary>> = Bin,
    create_secret_hash(Bin, X-1, [Hash]);
create_secret_hash(Secret, X, Acc) ->
    <<Hash:4/binary, _/binary>> = crypto:hash(sha256, Secret),
    create_secret_hash(Secret, X-1, [Hash|Acc]).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_poc_receipts_v1_pb{
        onion_key = <<"onion">>,
        challenger = <<"challenger">>,
        secret = <<"secret">>,
        receipts=[],
        witnesses=[],
        fee = 0,
        signature = <<>>
    },
    ?assertEqual(Tx, new(<<"onion">>, [], [], <<"challenger">>, <<"secret">>)).

onion_key_test() ->
    Tx = new(<<"onion">>, [], [], <<"challenger">>, <<"secret">>),
    ?assertEqual(<<"onion">>, onion_key(Tx)).

challenger_test() ->
    Tx = new(<<"onion">>, [], [], <<"challenger">>, <<"secret">>),
    ?assertEqual(<<"challenger">>, challenger(Tx)).

secret_test() ->
    Tx = new(<<"onion">>, [], [], <<"challenger">>, <<"secret">>),
    ?assertEqual(<<"secret">>, secret(Tx)).

receipts_test() ->
    Tx = new(<<"onion">>, [], [], <<"challenger">>, <<"secret">>),
    ?assertEqual([], receipts(Tx)).

witnesses_test() ->
    Tx = new(<<"onion">>, [], [], <<"challenger">>, <<"secret">>),
    ?assertEqual([], witnesses(Tx)).

fee_test() ->
    Tx = new(<<"onion">>, [], [], <<"challenger">>, <<"secret">>),
    ?assertEqual(0, fee(Tx)).

signature_test() ->
    Tx = new(<<"onion">>, [], [], <<"challenger">>, <<"secret">>),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Challenger = libp2p_crypto:pubkey_to_bin(PubKey),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx0 = new(<<"onion">>, [], [], Challenger, <<"secret">>),
    Tx1 = sign(Tx0, SigFun),
    Sig = signature(Tx1),
    EncodedTx1 = blockchain_txn_poc_receipts_v1_pb:encode_msg(Tx1#blockchain_txn_poc_receipts_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig, PubKey)).

create_secret_hash_test() ->
    Secret = crypto:strong_rand_bytes(8),
    Members = create_secret_hash(Secret, 10),
    ?assertEqual(10, erlang:length(Members)),

    Members2 = create_secret_hash(Secret, 10),
    ?assertEqual(10, erlang:length(Members2)),

    lists:foreach(
        fun(M) ->
            ?assert(lists:member(M, Members2))
        end,
        Members
    ),
    ok.

-endif.
