%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Routing ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_routing_v1).

-behavior(blockchain_txn).

-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_routing_v1_pb.hrl").

-export([
    update_router_addresses/5,
    new_xor/5,
    update_xor/6,
    request_subnet/5,
    hash/1,
    oui/1,
    owner/1,
    action/1,
    fee/1,
    nonce/1,
    signature/1,
    sign/2,
    is_valid/2,
    absorb/2,
    print/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_routing() :: #blockchain_txn_routing_v1_pb{}.
-export_type([txn_routing/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec update_router_addresses(non_neg_integer(), libp2p_crypto:pubkey_bin(), [binary()], non_neg_integer(), non_neg_integer()) -> txn_routing().
update_router_addresses(OUI, Owner, Addresses, Fee, Nonce) ->
    #blockchain_txn_routing_v1_pb{
       oui=OUI,
       owner=Owner,
       update={update_routers, #update_routers_pb{router_addresses=Addresses}},
       fee=Fee,
       nonce=Nonce,
       signature= <<>>
      }.

-spec new_xor(non_neg_integer(), libp2p_crypto:pubkey_bin(), binary(), non_neg_integer(), non_neg_integer()) -> txn_routing().
new_xor(OUI, Owner, Xor, Fee, Nonce) ->
    #blockchain_txn_routing_v1_pb{
       oui=OUI,
       owner=Owner,
       update={new_xor, Xor},
       fee=Fee,
       nonce=Nonce,
       signature= <<>>
      }.

-spec update_xor(non_neg_integer(), libp2p_crypto:pubkey_bin(), non_neg_integer(), binary(), non_neg_integer(), non_neg_integer()) -> txn_routing().
update_xor(OUI, Owner, Index, Xor, Fee, Nonce) ->
    #blockchain_txn_routing_v1_pb{
       oui=OUI,
       owner=Owner,
       update={update_xor, #update_xor_pb{index=Index, filter=Xor}},
       fee=Fee,
       nonce=Nonce,
       signature= <<>>
      }.

-spec request_subnet(non_neg_integer(), libp2p_crypto:pubkey_bin(), binary(), non_neg_integer(), non_neg_integer()) -> txn_routing().
request_subnet(OUI, Owner, SubnetSize, Fee, Nonce) ->
    #blockchain_txn_routing_v1_pb{
       oui=OUI,
       owner=Owner,
       update={request_subnet, SubnetSize},
       fee=Fee,
       nonce=Nonce,
       signature= <<>>
      }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_routing()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_routing_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_routing_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec oui(txn_routing()) -> non_neg_integer().
oui(Txn) ->
    Txn#blockchain_txn_routing_v1_pb.oui.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner(txn_routing()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_routing_v1_pb.owner.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec action(txn_routing()) -> {update_routers, RouterAddresses::[binary()]} |
                               {new_xor, Filter::binary()} |
                               {update_xor, Index::non_neg_integer(), Filter::binary()} |
                               {request_subnet, SubnetSize::non_neg_integer()}.
action(Txn) ->
    case Txn#blockchain_txn_routing_v1_pb.update of
        {update_routers, #update_routers_pb{router_addresses=Addresses}} ->
            {update_routers, Addresses};
        {update_xor,  #update_xor_pb{index=Index, filter=Filter}} ->
            {update_xor, Index, Filter};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_routing()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_routing_v1_pb.fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(txn_routing()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_routing_v1_pb.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_routing()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_routing_v1_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_routing(), libp2p_crypto:sig_fun()) -> txn_routing().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_routing_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_routing_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_routing(),
               blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    OUI = ?MODULE:oui(Txn),
    case blockchain_ledger_v1:find_routing(OUI, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Routing} ->
            Owner = ?MODULE:owner(Txn),
            case Owner == blockchain_ledger_routing_v1:owner(Routing) of
                false ->
                    {error, bad_owner};
                true ->
                    Nonce = ?MODULE:nonce(Txn),
                    LedgerNonce = blockchain_ledger_routing_v1:nonce(Routing),
                    case Nonce == LedgerNonce + 1 of
                        false ->
                            {error, {bad_nonce, {routing, Nonce, LedgerNonce}}};
                        true ->
                            Signature = ?MODULE:signature(Txn),
                            PubKey = libp2p_crypto:bin_to_pubkey(Owner),
                            BaseTxn = Txn#blockchain_txn_routing_v1_pb{signature = <<>>},
                            EncodedTxn = blockchain_txn_routing_v1_pb:encode_msg(BaseTxn),
                            case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
                                false ->
                                    {error, bad_signature};
                                true ->
                                    case ?MODULE:action(Txn) of
                                        {update_routers, Addresses} ->
                                            case validate_addresses(Addresses) of
                                                false ->
                                                    {error, invalid_addresses};
                                                true ->
                                                    Fee = ?MODULE:fee(Txn),
                                                    Owner = ?MODULE:owner(Txn),
                                                    blockchain_ledger_v1:check_dc_balance(Owner, Fee, Ledger)
                                            end;
                                        {new_xor, Xor} when byte_size(Xor) > 1024*100 ->
                                            {error, filter_too_large};
                                        {new_xor, _Xor} ->
                                            case length(blockchain_ledger_routing_v1:filters(Routing)) < 5 of
                                                true ->
                                                    Fee = ?MODULE:fee(Txn),
                                                    Owner = ?MODULE:owner(Txn),
                                                    blockchain_ledger_v1:check_dc_balance(Owner, Fee, Ledger);
                                                false ->
                                                    {error, too_many_filters}
                                            end;
                                        {update_xor, Index, _Filter} when Index < 0 orelse Index >= 5 ->
                                            {error, invalid_xor_filter_index};
                                        {update_xor, _Index, Filter} when byte_size(Filter) > 1024*100 ->
                                            {error, filter_too_large};
                                        {update_xor, _Index, _Filter} ->
                                                    Fee = ?MODULE:fee(Txn),
                                                    Owner = ?MODULE:owner(Txn),
                                                    blockchain_ledger_v1:check_dc_balance(Owner, Fee, Ledger);
                                        {request_subnet, SubnetSize} when SubnetSize < 8 orelse SubnetSize > 65536 ->
                                            {error, invalid_subnet_size};
                                        {request_subnet, SubnetSize} ->
                                            %% subnet size should be between 8 and 65536 as a power of two
                                            Res = math:log2(SubnetSize),
                                            %% check there's no floating point components of the number
                                            %% Erlang will coerce between floats and ints when you use ==
                                            case trunc(Res) == Res of
                                                true ->
                                                    %% TODO check if we have space to allocate this
                                                    Fee = ?MODULE:fee(Txn),
                                                    Owner = ?MODULE:owner(Txn),
                                                    blockchain_ledger_v1:check_dc_balance(Owner, Fee, Ledger);
                                                false ->
                                                    {error, invalid_subnet_size}
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
-spec absorb(txn_routing(),
             blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Fee = ?MODULE:fee(Txn),
    Owner = ?MODULE:owner(Txn),
    case blockchain_ledger_v1:debit_fee(Owner, Fee, Ledger) of
        {error, _}=Error ->
            Error;
        ok ->
            OUI = ?MODULE:oui(Txn),
            Action = ?MODULE:action(Txn),
            Nonce = ?MODULE:nonce(Txn),
            blockchain_ledger_v1:update_routing(Owner, OUI, Action, Nonce, Ledger)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_routing()) -> iodata().
print(undefined) -> <<"type=routing undefined">>;
print(#blockchain_txn_routing_v1_pb{oui=OUI, owner=Owner,
                                    update=Update, fee=Fee,
                                    nonce=Nonce, signature=Sig}) ->
    io_lib:format("type=routing oui=~p owner=~p update=~p fee=~p nonce=~p signature=~p",
                  [OUI, ?TO_B58(Owner), Update, Fee, Nonce, Sig]).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec validate_addresses(string()) -> boolean().
validate_addresses([]) ->
    true;
validate_addresses(Addresses) ->
    case {erlang:length(Addresses), erlang:length(lists:usort(Addresses))} of
        {L, L} when L =< 3 ->
            ok == blockchain_txn:validate_fields([{{router_address, P}, {address, libp2p}} || P <- Addresses]);
        _ ->
            false
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-define(KEY1, <<0,105,110,41,229,175,44,3,221,73,181,25,27,184,120,84,
               138,51,136,194,72,161,94,225,240,73,70,45,135,23,41,96,78>>).
-define(KEY2, <<1,72,253,248,131,224,194,165,164,79,5,144,254,1,168,254,
                111,243,225,61,41,178,207,35,23,54,166,116,128,38,164,87,212>>).
-define(KEY3, <<1,124,37,189,223,186,125,185,240,228,150,61,9,164,28,75,
                44,232,76,6,121,96,24,24,249,85,177,48,246,236,14,49,80>>).
-define(KEY4, <<0,201,24,252,94,154,8,151,21,177,201,93,234,97,223,234,
                109,216,141,189,126,227,92,243,87,8,134,107,91,11,221,179,190>>).

new_test() ->
    Tx = #blockchain_txn_routing_v1_pb{
        oui= 0,
        owner= <<"owner">>,
        update = {update_routers, #update_routers_pb{router_addresses=[?KEY1]}},
        fee=1,
        nonce = 0,
        signature= <<>>
    },
    ?assertEqual(Tx, update_router_addresses(0, <<"owner">>, [?KEY1], 1, 0)).

oui_test() ->
    Tx = update_router_addresses(0, <<"owner">>, [?KEY1], 1, 0),
    ?assertEqual(0, oui(Tx)).

fee_test() ->
    Tx = update_router_addresses(0, <<"owner">>, [?KEY1], 1, 0),
    ?assertEqual(1, fee(Tx)).

owner_test() ->
    Tx = update_router_addresses(0, <<"owner">>, [?KEY1], 1, 0),
    ?assertEqual(<<"owner">>, owner(Tx)).

addresses_test() ->
    Tx = update_router_addresses(0, <<"owner">>, [?KEY1, ?KEY2], 1, 0),
    ?assertEqual({update_routers, [?KEY1, ?KEY2]}, action(Tx)).

nonce_test() ->
    Tx = update_router_addresses(0, <<"owner">>, [?KEY1], 1, 0),
    ?assertEqual(0, nonce(Tx)).

signature_test() ->
    Tx = update_router_addresses(0, <<"owner">>, [?KEY1], 1, 0),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = update_router_addresses(0, <<"owner">>, [?KEY1], 1, 0),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_routing_v1_pb:encode_msg(Tx1#blockchain_txn_routing_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

ecode_decode_test() ->
    Tx = update_router_addresses(0, <<"owner">>, [?KEY1, ?KEY2],  1, 0),
    ?assertEqual(Tx, blockchain_txn_routing_v1_pb:decode_msg(blockchain_txn_routing_v1_pb:encode_msg(Tx), blockchain_txn_routing_v1_pb)).

validate_addresses_test() ->
    ?assert(validate_addresses([])),
    ?assert(validate_addresses([?KEY1])),
    ?assertNot(validate_addresses([?KEY1, ?KEY1])),
    ?assert(validate_addresses([?KEY1, ?KEY2])),
    ?assert(validate_addresses([?KEY4, ?KEY3, ?KEY2])),
    ?assertNot(validate_addresses([?KEY4, ?KEY3, ?KEY2, ?KEY1])),
    ?assertNot(validate_addresses([<<"http://test.com">>])),
    ?assertNot(validate_addresses([?KEY1, <<"http://test.com">>])),
    ?assertNot(validate_addresses([?KEY1, ?KEY2, <<"http://test.com">>])),
    ok.

-endif.
