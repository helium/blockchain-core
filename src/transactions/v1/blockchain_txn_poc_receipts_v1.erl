%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Proof of Coverage Receipts ==
%%%-------------------------------------------------------------------
-module(blockchain_txn_poc_receipts_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_poc_receipts_v1_pb.hrl").

-export([
    new/3,
    receipts/1,
    signature/1,
    challenger/1,
    secret/1,
    sign/2,
    hash/1,
    is_valid/1,
    is/1,
    absorb/2,
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
-spec new(blockchain_poc_receipt_v1:poc_receipts(), libp2p_crypto:pubkey_bin(), binary()) -> txn_poc_receipts().
new(Receipts, Challenger, Secret) ->
    #blockchain_txn_poc_receipts_v1_pb{
        receipts=Receipts,
        challenger=Challenger,
        secret=Secret,
        signature = <<>>
    }.

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
-spec hash(txn_poc_receipts()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_poc_receipts_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_poc_receipts_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_poc_receipts()) -> boolean().
is_valid(Txn=#blockchain_txn_poc_receipts_v1_pb{challenger=Challenger, signature=Signature}) ->
    PubKey = libp2p_crypto:bin_to_pubkey(Challenger),
    BaseTxn = Txn#blockchain_txn_poc_receipts_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_poc_receipts_v1_pb:encode_msg(BaseTxn),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, blockchain_txn_poc_receipts_v1_pb).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_poc_receipts(), blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
absorb(Txn, Ledger) ->
    case ?MODULE:is_valid(Txn) of
        false ->
            {error, invalid_transaction};
        true ->
            case blockchain_ledger_v1:find_gateway_info(?MODULE:challenger(Txn), Ledger) of
                {ok, ChallengerInfo} ->
                    case blockchain_ledger_gateway_v1:last_poc_challenge(ChallengerInfo) of
                        undefined ->
                            lager:error("challenger: ~p, error: last_poc_challenge_undefined", [ChallengerInfo]),
                            {error, last_poc_challenge_undefined};
                        _Height ->
                            case blockchain_ledger_gateway_v1:last_poc_info(ChallengerInfo) of
                                undefined ->
                                    lager:error("challenger: ~p, error: last_poc_undefined_undefined", [ChallengerInfo]),
                                    {error, last_poc_info_undefined};
                                {Hash, _Onion} ->
                                    Secret = ?MODULE:secret(Txn),
                                    SecretHash = crypto:hash(sha256, Secret),
                                    case Hash =:= SecretHash of
                                        true ->
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
                                        false ->
                                            lager:error("POC receipt secret ~p does not match Challenger POC Request Hash: ~p", [SecretHash, Hash]),
                                            {error, incorrect_secret_hash}
                                    end
                            end
                    end;
                {error, _}=Error -> Error
            end
    end.

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
        receipts=[],
        challenger = <<"challenger">>,
        secret = <<"secret">>,
        signature = <<>>
    },
    ?assertEqual(Tx, new([], <<"challenger">>, <<"secret">>)).

receipts_test() ->
    Tx = new([], <<"challenger">>, <<"secret">>),
    ?assertEqual([], receipts(Tx)).

challenger_test() ->
    Tx = new([], <<"challenger">>, <<"secret">>),
    ?assertEqual(<<"challenger">>, challenger(Tx)).

secret_test() ->
    Tx = new([], <<"challenger">>, <<"secret">>),
    ?assertEqual(<<"secret">>, secret(Tx)).

signature_test() ->
    Tx = new([], <<"challenger">>, <<"secret">>),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Challenger = libp2p_crypto:pubkey_to_bin(PubKey),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx0 = new([], Challenger, <<"secret">>),
    Tx1 = sign(Tx0, SigFun),
    Sig = signature(Tx1),
    EncodedTx1 = blockchain_txn_poc_receipts_v1_pb:encode_msg(Tx1#blockchain_txn_poc_receipts_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig, PubKey)).

is_test() ->
    Tx = new([], <<"challenger">>, <<"secret">>),
    ?assert(is(Tx)).

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
