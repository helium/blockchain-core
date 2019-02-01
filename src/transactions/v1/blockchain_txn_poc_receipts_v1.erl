%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Proof of Coverage Receipts ==
%%%-------------------------------------------------------------------
-module(blockchain_txn_poc_receipts_v1).

-export([
    new/3,
    receipts/1,
    signature/1,
    challenger/1,
    secret/1,
    sign/2,
    is_valid/1,
    is/1,
    absorb/2,
    create_secret_hash/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(txn_poc_receipts_v1, {
    receipts :: blockchain_poc_receipt_v1:poc_receipts(),
    challenger :: libp2p_crypto:pubkey_bin(),
    secret :: binary(),
    signature :: binary()
}).

-type txn_poc_receipts() :: #txn_poc_receipts_v1{}.

-export_type([txn_poc_receipts/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(blockchain_poc_receipt_v1:poc_receipts(), libp2p_crypto:pubkey_bin(), binary()) -> txn_poc_receipts().
new(Receipts, Challenger, Secret) ->
    #txn_poc_receipts_v1{
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
    Txn#txn_poc_receipts_v1.receipts.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec challenger(txn_poc_receipts()) -> libp2p_crypto:pubkey_bin().
challenger(Txn) ->
    Txn#txn_poc_receipts_v1.challenger.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec secret(txn_poc_receipts()) -> binary().
secret(Txn) ->
    Txn#txn_poc_receipts_v1.secret.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_poc_receipts()) -> binary().
signature(Txn) ->
    Txn#txn_poc_receipts_v1.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_poc_receipts(), libp2p_crypto:sig_fun()) -> txn_poc_receipts().
sign(Txn, SigFun) ->
    BinTxn = erlang:term_to_binary(Txn#txn_poc_receipts_v1{signature = <<>>}),
    Txn#txn_poc_receipts_v1{signature=SigFun(BinTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_poc_receipts()) -> boolean().
is_valid(Txn=#txn_poc_receipts_v1{challenger=Challenger, signature=Signature}) ->
    PubKey = libp2p_crypto:bin_to_pubkey(Challenger),
    BinTxn = erlang:term_to_binary(Txn#txn_poc_receipts_v1{signature = <<>>}),
    libp2p_crypto:verify(BinTxn, Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, txn_poc_receipts_v1).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_poc_receipts(), blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
absorb(Txn, Ledger) ->
    case blockchain_txn_poc_receipts_v1:is_valid(Txn) of
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
    Tx = #txn_poc_receipts_v1{
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
    ?assert(libp2p_crypto:verify(erlang:term_to_binary(Tx1#txn_poc_receipts_v1{signature = <<>>}), Sig, PubKey)).

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
