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
-spec absorb(txn_poc_receipts(), blockchain_ledger_v1:ledger()) -> {ok, blockchain_ledger_v1:ledger()}
                                                                | {error, any()}.
absorb(Txn, Ledger0) ->
    case blockchain_txn_poc_receipts_v1:is_valid(Txn) of
        false ->
            {error, invalid_transaction};
        true ->
            % TODO:
                % 1. Get POC request (check last few blocks)
                % 2. Validate poc request hash from secret
                % 3. Re-create entropy target and path from secret and block hash
            % TODO: Update score and last_poc_challenge
            {ok, Ledger0}
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
