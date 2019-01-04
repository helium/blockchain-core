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
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(txn_poc_receipts_v1, {
    receipts :: blockchain_poc_receipt_v1:poc_receipts(),
    challenger :: libp2p_crypto:address(),
    secret :: binary(),
    signature :: binary()
}).

-type txn_poc_receipts() :: #txn_poc_receipts_v1{}.

-export_type([txn_poc_receipts/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(blockchain_poc_receipt_v1:poc_receipts(), libp2p_crypto:address(), binary()) -> txn_poc_receipts().
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
-spec challenger(txn_poc_receipts()) -> libp2p_crypto:address().
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
    PubKey = libp2p_crypto:address_to_pubkey(Challenger),
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
            % TODO:
                % 1. Get POC request (check last few blocks)
                % 2. Validate poc request hash from secret
                % 3. Re-create entropy target and path from secret and block hash
            % TODO: Update score and last_poc_challenge
            case blockchain_ledger_v1:find_gateway_info(?MODULE:challenger(Txn), Ledger) of
                {ok, ChallengerInfo} ->
                    case blockchain_ledger_gateway_v1:last_poc_challenge(ChallengerInfo) of
                        undefined ->
                            lager:error("Challenger: ~p, Error: last_poc_challenge_undefined", [ChallengerInfo]),
                            _ = {error, last_poc_challenge_undefined};
                        Height ->
                            case blockchain_ledger_gateway_v1:last_poc_hash(ChallengerInfo) of
                                undefined ->
                                    lager:error("Challenger: ~p, Error: last_poc_undefined_undefined", [ChallengerInfo]),
                                    _ = {error, last_poc_hash_undefined};
                                Hash ->
                                    Secret = ?MODULE:secret(Txn),
                                    SecretHash = crypto:hash(sha256, Secret),
                                    case Hash == SecretHash of
                                        true ->
                                            lager:info("Txn: ~p", [Txn]),
                                            lager:info("Challenger: ~p, last_poc_challenge at height: ~p, last_poc_hash: ~p", [ChallengerInfo, Height, Hash]),
                                            lager:info("Hash: ~p, SecretHash: ~p", [Hash, crypto:hash(sha256, ?MODULE:secret(Txn))]),
                                            lager:info("POC Receipt secret matches Challenger POC Request Hash!"),
                                            <<_:8/binary, POCRequestHash/binary>> = Secret,
                                            lager:info("POCRequestHash: ~p", [POCRequestHash]),
                                            %% XXX: This needs to be fixed and we need to have ledger snapshots or
                                            %% rocksdb column families so we can build the path at the time the request
                                            %% was mined.
                                            {Target, ActiveGateways} = blockchain_poc_path:target(POCRequestHash, Ledger),
                                            lager:info("Target: ~p", [Target]),
                                            {ok, Path} = blockchain_poc_path:build(Target, ActiveGateways),
                                            lager:info("Path: ~p", [Path]),
                                            ReceiptAddrs = lists:sort([blockchain_poc_receipt_v1:address(R) || R <- ?MODULE:receipts(Txn)]),
                                            case ReceiptAddrs == lists:sort(Path) andalso
                                                 lists:member(Target, ReceiptAddrs) of
                                                true ->
                                                    lager:info("Got receipts from all the members in the path!");
                                                false ->
                                                    lager:error("Missing receipts!, ReconstructedPath: ~p, ReceivedReceipts: ~p", [Path, ReceiptAddrs]),
                                                    _ = {error, missing_receipts}
                                            end;
                                        false ->
                                            lager:error("POC Receipt secret ~p does not match Challenger POC Request Hash: ~p", [SecretHash, Hash]),
                                            _ = {error, incorrect_secret_hash}
                                    end
                            end
                    end;
                {error, _}=Error -> Error
            end,
            ok
    end.

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
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    Challenger = libp2p_crypto:pubkey_to_address(PubKey),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx0 = new([], Challenger, <<"secret">>),
    Tx1 = sign(Tx0, SigFun),
    Sig = signature(Tx1),
    ?assert(libp2p_crypto:verify(erlang:term_to_binary(Tx1#txn_poc_receipts_v1{signature = <<>>}), Sig, PubKey)).

is_test() ->
    Tx = new([], <<"challenger">>, <<"secret">>),
    ?assert(is(Tx)).

-endif.
