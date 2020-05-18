%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Routing ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_routing_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").

-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").
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
    print/1,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_routing() :: #blockchain_txn_routing_v1_pb{}.
-type action() :: {update_routers, RouterAddresses::[binary()]} |
                  {new_xor, Filter::binary()} |
                  {update_xor, Index::non_neg_integer(), Filter::binary()} |
                  {request_subnet, SubnetSize::non_neg_integer()}.

-export_type([txn_routing/0, action/0]).

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

-spec request_subnet(non_neg_integer(), libp2p_crypto:pubkey_bin(), pos_integer(), non_neg_integer(), non_neg_integer()) -> txn_routing().
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
-spec action(txn_routing()) -> action().
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

                            case blockchain:config(?max_xor_filter_size, Ledger) of
                                {ok, XORFilterSize} ->
                                    case blockchain:config(?max_xor_filter_num, Ledger) of
                                        {ok, XORFilterNum} ->
                                            case blockchain:config(?max_subnet_size, Ledger) of
                                                {ok, MaxSubnetSize} ->
                                                    case blockchain:config(?min_subnet_size, Ledger) of
                                                        {ok, MinSubnetSize} ->
                                                            case blockchain:config(?max_subnet_num, Ledger) of
                                                                {ok, MaxSubnetNum} ->
                                                                    case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
                                                                        false ->
                                                                            {error, bad_signature};
                                                                        true ->
                                                                            do_is_valid_checks(Txn, Ledger, Routing, XORFilterSize, XORFilterNum, MinSubnetSize, MaxSubnetSize, MaxSubnetNum)
                                                                    end;
                                                                _ ->
                                                                    {error, max_subnet_num_not_set}
                                                            end;
                                                        _ ->
                                                            {error, min_subnet_size_not_set}
                                                    end;
                                                _ ->
                                                    {error, max_subnet_size_not_set}
                                            end;
                                        _ ->
                                            {error, max_xor_filter_num_not_set}
                                    end;
                                _ ->
                                    {error, max_xor_filter_size_not_set}
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
    OUI = ?MODULE:oui(Txn),
    %% try to allocate a subnet before debiting fees
    Action = case ?MODULE:action(Txn) of
        {request_subnet, SubnetSize} ->
            {ok, Routing} = blockchain_ledger_v1:find_routing(OUI, Ledger),
            {ok, MaxSubnetNum} = blockchain:config(?max_subnet_num, Ledger),
            case subnets_left(Routing, MaxSubnetNum) of
                false ->
                    {error, max_subnets_reached};
                true ->
                    {ok, Subnet} = blockchain_ledger_v1:allocate_subnet(SubnetSize, Ledger),
                    {request_subnet, Subnet}
            end;
        {new_xor, _}=Action0 ->
            {ok, Routing} = blockchain_ledger_v1:find_routing(OUI, Ledger),
            {ok, XorFilterNum} = blockchain:config(?max_xor_filter_num, Ledger),
            case length(blockchain_ledger_routing_v1:filters(Routing)) < XorFilterNum of
                true ->
                    Action0;
                false ->
                    {error, max_filters_reached}
            end;
        Action0 ->
            Action0
    end,
    case Action of
        {error, _} = Error ->
            Error;
        _ ->
            case blockchain_ledger_v1:debit_fee(Owner, Fee, Ledger) of
                {error, _}=Error ->
                    Error;
                ok ->
                    OUI = ?MODULE:oui(Txn),
                    Nonce = ?MODULE:nonce(Txn),
                    blockchain_ledger_v1:update_routing(OUI, Action, Nonce, Ledger)
            end
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


-spec action_to_json(action(), blockchain_json:opts()) -> blockcahin_json:json_object().
action_to_json({update_routers, RouterAddresses}, _Opts) ->
    #{action => <<"update_routers">>,
      addresses =>[?BIN_TO_B58(A) || A <- RouterAddresses]};
action_to_json({new_xor, Filter}, _Opts) ->
    #{action => <<"new_xor">>,
      filter => ?BIN_TO_B64(Filter)};
action_to_json({update_xor, Index, Filter}, _Opts) ->
    #{action => <<"update_xor">>,
      index => Index,
      filter => ?BIN_TO_B64(Filter)};
action_to_json({request_subnet, SubnetSize}, _Opts) ->
    #{action => <<"request_subnet">>,
      subnet_size => SubnetSize}.

-spec to_json(txn_routing(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => <<"routing_v1">>,
      hash => ?BIN_TO_B64(hash(Txn)),
      oui => oui(Txn),
      owner => ?BIN_TO_B58(owner(Txn)),
      fee => fee(Txn),
      action => action_to_json(action(Txn), []),
      nonce => nonce(Txn)
     }.

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

-spec do_is_valid_checks(Txn :: txn_routing(),
                         Ledger :: blockchain_ledger_v1:ledger(),
                         Routing :: blockchain_ledger_routing_v1:routing(),
                         XORFilterSize :: pos_integer(),
                         XORFilterNum :: pos_integer(),
                         MinSubnetSize :: pos_integer(),
                         MaxSubnetSize :: pos_integer(),
                         MaxSubnetNum :: pos_integer()) -> ok | {error, any()}.
do_is_valid_checks(Txn, Ledger, Routing, XORFilterSize, XORFilterNum, MinSubnetSize, MaxSubnetSize, MaxSubnetNum) ->
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
        {new_xor, Xor} when byte_size(Xor) > XORFilterSize ->
            {error, filter_too_large};
        {new_xor, Xor} ->
            case length(blockchain_ledger_routing_v1:filters(Routing)) < XORFilterNum of
                true ->
                    %% the contain check does some structural checking of the filter
                    case catch xor16:contain({Xor, fun xxhash:hash64/1}, <<"anything">>) of
                        B when is_boolean(B) ->
                            Fee = ?MODULE:fee(Txn),
                            Owner = ?MODULE:owner(Txn),
                            blockchain_ledger_v1:check_dc_balance(Owner, Fee, Ledger);
                        _ ->
                            {error, invalid_filter}
                    end;
                false ->
                    {error, too_many_filters}
            end;
        {update_xor, Index, _Filter} when Index < 0 orelse Index >= XORFilterNum ->
            {error, invalid_xor_filter_index};
        {update_xor, _Index, Filter} when byte_size(Filter) > XORFilterSize ->
            {error, filter_too_large};
        {update_xor, Index, Filter} ->
            case Index < length(blockchain_ledger_routing_v1:filters(Routing)) of
                true ->
                    %% the contain check does some structural checking of the filter
                    case catch xor16:contain({Filter, fun xxhash:hash64/1}, <<"anything">>) of
                        B when is_boolean(B) ->
                            Fee = ?MODULE:fee(Txn),
                            Owner = ?MODULE:owner(Txn),
                            blockchain_ledger_v1:check_dc_balance(Owner, Fee, Ledger);
                        _ ->
                            {error, invalid_filter}
                    end;
                false ->
                    {error, invalid_filter_index}
            end;
        {request_subnet, SubnetSize} when SubnetSize < MinSubnetSize orelse SubnetSize > MaxSubnetSize ->
            {error, invalid_subnet_size};
        {request_subnet, SubnetSize} ->
            %% subnet size should be between 8 and 65536 as a power of two
            Res = math:log2(SubnetSize),
            %% check there's no floating point components of the number
            %% Erlang will coerce between floats and ints when you use ==
            case trunc(Res) == Res of
                true ->
                    case subnets_left(Routing, MaxSubnetNum) of
                        false ->
                            {error, max_subnets_reached};
                        true ->
                            case blockchain_ledger_v1:allocate_subnet(SubnetSize, Ledger) of
                                {ok, _} ->
                                    Fee = ?MODULE:fee(Txn),
                                    Owner = ?MODULE:owner(Txn),
                                    blockchain_ledger_v1:check_dc_balance(Owner, Fee, Ledger);
                                Error ->
                                    Error
                            end
                    end;
                false ->
                    {error, invalid_subnet_size}
            end
    end.

-spec subnets_left(Routing :: blockchain_ledger_routing_v1:routing(), MaxSubnetNum :: pos_integer()) -> boolean().
subnets_left(Routing, MaxSubnetNum) ->
    Subnets = length(blockchain_ledger_routing_v1:subnets(Routing)),
    Subnets < MaxSubnetNum.

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

to_json_test() ->
    Tx = update_router_addresses(0, <<"owner">>, [?KEY1, ?KEY2],  1, 0),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, oui, owner, fee, action, nonce])).

-endif.
