%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Proof of Coverage Receipt ==
%%%-------------------------------------------------------------------
-module(blockchain_poc_receipt).

-export([
    new/3
    ,address/1
    ,timestamp/1
    ,hash/1
    ,signature/1
    ,sign/2
    ,is_valid/1
    ,encode/1
    ,decode/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(poc_receipt, {
    address :: libp2p_crypto:address()
    ,timestamp :: non_neg_integer()
    ,hash :: binary()
    ,signature :: binary()
}).

-type poc_receipt() :: #poc_receipt{}.
-type poc_receipts() :: [poc_receipt()].

-export_type([poc_receipt/0, poc_receipts/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:address(), non_neg_integer(), binary()) -> poc_receipt().
new(Address, Timestamp, Hash) ->
    #poc_receipt{
        address=Address
        ,timestamp=Timestamp
        ,hash=Hash
        ,signature = <<>>
    }.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec address(poc_receipt()) -> libp2p_crypto:address().
address(Receipt) ->
    Receipt#poc_receipt.address.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec timestamp(poc_receipt()) -> libp2p_crypto:address().
timestamp(Receipt) ->
    Receipt#poc_receipt.timestamp.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(poc_receipt()) -> binary().
hash(Receipt) ->
    Receipt#poc_receipt.hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(poc_receipt()) -> binary().
signature(Receipt) ->
    Receipt#poc_receipt.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(poc_receipt(), libp2p_crypto:sig_fun()) -> poc_receipt().
sign(Receipt, SigFun) ->
    BinReceipt = erlang:term_to_binary(Receipt#poc_receipt{signature = <<>>}),
    Receipt#poc_receipt{signature=SigFun(BinReceipt)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(poc_receipt()) -> boolean().
is_valid(Receipt=#poc_receipt{address=Address, signature=Signature}) ->
    PubKey = libp2p_crypto:address_to_pubkey(Address),
    BinReceipt = erlang:term_to_binary(Receipt#poc_receipt{signature = <<>>}),
    libp2p_crypto:verify(BinReceipt, Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec encode(poc_receipt()) -> binary().
encode(Receipt) ->
    erlang:term_to_binary(Receipt).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec decode(binary()) -> poc_receipt().
decode(Binary) ->
    erlang:binary_to_term(Binary).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Receipt = #poc_receipt{
        address= <<"address">>
        ,timestamp= 1
        ,hash= <<"hash">>
        ,signature = <<>>
    },
    ?assertEqual(Receipt, new(<<"address">>, 1, <<"hash">>)).

address_test() ->
    Receipt = new(<<"address">>, 1, <<"hash">>),
    ?assertEqual(<<"address">>, address(Receipt)).

timestamp_test() ->
    Receipt = new(<<"address">>, 1, <<"hash">>),
    ?assertEqual(1, timestamp(Receipt)).

hash_test() ->
    Receipt = new(<<"address">>, 1, <<"hash">>),
    ?assertEqual(<<"hash">>, hash(Receipt)).

signature_test() ->
    Receipt = new(<<"address">>, 1, <<"hash">>),
    ?assertEqual(<<>>, signature(Receipt)).

sign_test() ->
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    Address = libp2p_crypto:pubkey_to_address(PubKey),
    Receipt0 = new(Address, 1, <<"hash">>),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Receipt1 = sign(Receipt0, SigFun),
    Sig1 = signature(Receipt1),
    ?assert(libp2p_crypto:verify(erlang:term_to_binary(Receipt1#poc_receipt{signature = <<>>}), Sig1, PubKey)).

encode_decode_test() ->
    Receipt = new(<<"address">>, 1, <<"hash">>),
    ?assertEqual(Receipt, decode(encode(Receipt))).

-endif.
