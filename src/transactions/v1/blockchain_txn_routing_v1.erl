%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Routing ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_routing_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_routing_v1_pb.hrl").

-export([
    update_router_addresses/4,
    new_xor/4,
    update_xor/5,
    request_subnet/4,
    hash/1,
    oui/1,
    owner/1,
    action/1,
    fee/1, fee/2,
    fee_payer/2,
    staking_fee/1, staking_fee/2,
    calculate_fee/2, calculate_fee/5, calculate_staking_fee/2, calculate_staking_fee/5,
    nonce/1,
    signature/1,
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

-define(T, #blockchain_txn_routing_v1_pb).

-type t() :: txn_routing().

-type txn_routing() :: ?T{}.

-type action() :: {update_routers, RouterAddresses::[binary()]} |
                  {new_xor, Filter::binary()} |
                  {update_xor, Index::non_neg_integer(), Filter::binary()} |
                  {request_subnet, SubnetSize::non_neg_integer()}.

-export_type([t/0, txn_routing/0, action/0]).

-spec update_router_addresses(non_neg_integer(), libp2p_crypto:pubkey_bin(), [binary()], non_neg_integer()) -> txn_routing().
update_router_addresses(OUI, Owner, Addresses, Nonce) ->
    #blockchain_txn_routing_v1_pb{
       oui=OUI,
       owner=Owner,
       update={update_routers, #update_routers_pb{router_addresses=Addresses}},
       fee=?LEGACY_TXN_FEE,
       staking_fee=0,
       nonce=Nonce,
       signature= <<>>
      }.

-spec new_xor(non_neg_integer(), libp2p_crypto:pubkey_bin(), binary(), non_neg_integer()) -> txn_routing().
new_xor(OUI, Owner, Xor, Nonce) ->
    #blockchain_txn_routing_v1_pb{
       oui=OUI,
       owner=Owner,
       update={new_xor, Xor},
       fee=?LEGACY_TXN_FEE,
       staking_fee=0,
       nonce=Nonce,
       signature= <<>>
      }.

-spec update_xor(non_neg_integer(), libp2p_crypto:pubkey_bin(), non_neg_integer(), binary(), non_neg_integer()) -> txn_routing().
update_xor(OUI, Owner, Index, Xor, Nonce) ->
    #blockchain_txn_routing_v1_pb{
       oui=OUI,
       owner=Owner,
       update={update_xor, #update_xor_pb{index=Index, filter=Xor}},
       fee=?LEGACY_TXN_FEE,
       staking_fee=0,
       nonce=Nonce,
       signature= <<>>
      }.

-spec request_subnet(non_neg_integer(), libp2p_crypto:pubkey_bin(), pos_integer(), non_neg_integer()) -> txn_routing().
request_subnet(OUI, Owner, SubnetSize, Nonce) ->
    #blockchain_txn_routing_v1_pb{
       oui=OUI,
       owner=Owner,
       update={request_subnet, SubnetSize},
       fee=?LEGACY_TXN_FEE,
       staking_fee=0,  %% routing did not implement staking fee until txn fees, so has to default to zero
       nonce=Nonce,
       signature= <<>>
      }.

-spec hash(txn_routing()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_routing_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_routing_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec oui(txn_routing()) -> non_neg_integer().
oui(Txn) ->
    Txn#blockchain_txn_routing_v1_pb.oui.

-spec owner(txn_routing()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_routing_v1_pb.owner.

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

-spec fee(txn_routing()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_routing_v1_pb.fee.

-spec fee(txn_routing(), non_neg_integer()) -> txn_routing().
fee(Txn, Fee) ->
    Txn#blockchain_txn_routing_v1_pb{fee=Fee}.

-spec fee_payer(txn_routing(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(Txn, _Ledger) ->
    owner(Txn).

-spec staking_fee(txn_routing()) -> non_neg_integer().
staking_fee(Txn) ->
    Txn#blockchain_txn_routing_v1_pb.staking_fee.

-spec staking_fee(txn_routing(), non_neg_integer()) -> txn_routing().
staking_fee(Txn, Fee) ->
    Txn#blockchain_txn_routing_v1_pb{staking_fee=Fee}.

-spec nonce(txn_routing()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_routing_v1_pb.nonce.

-spec signature(txn_routing()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_routing_v1_pb.signature.

-spec sign(txn_routing(), libp2p_crypto:sig_fun()) -> txn_routing().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_routing_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_routing_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% Calculate the txn fee
%% Returned value is txn_byte_size / 24
%% @end
%%--------------------------------------------------------------------
-spec calculate_fee(txn_routing(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(txn_routing(), blockchain_ledger_v1:ledger(), pos_integer(), pos_integer(), boolean()) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, _DCPayloadSize, _TxnFeeMultiplier, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, true) ->
    ?calculate_fee(Txn#blockchain_txn_routing_v1_pb{fee=0, staking_fee = 0, signature = <<0:512>>}, Ledger, DCPayloadSize, TxnFeeMultiplier).

%%--------------------------------------------------------------------
%% @doc
%% Calculate the staking fee using the price oracles
%% returns the fee in DC
%% @end
%%--------------------------------------------------------------------
-spec calculate_staking_fee(txn_routing(), blockchain:blockchain()) -> non_neg_integer().
calculate_staking_fee(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Fee = blockchain_ledger_v1:staking_fee_txn_oui_v1_per_address(Ledger),
    calculate_staking_fee(Txn, Ledger, Fee, [], blockchain_ledger_v1:txn_fees_active(Ledger)).

-spec calculate_staking_fee(txn_routing(), blockchain_ledger_v1:ledger(), non_neg_integer(), [{atom(), non_neg_integer()}], boolean()) -> non_neg_integer().
calculate_staking_fee(_Txn, _Ledger, _Fee, _ExtraData, false) ->
    0;
calculate_staking_fee(#blockchain_txn_routing_v1_pb{update = {request_subnet, SubnetSize}}=_Txn, _Ledger, Fee, _ExtraData, true) ->
    Fee * SubnetSize;
calculate_staking_fee(#blockchain_txn_routing_v1_pb{}, _Ledger, _Fee, _ExtraData, true) ->
    0.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_routing(),
               blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    OUI = ?MODULE:oui(Txn),
    case blockchain_ledger_v1:find_routing(OUI, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Routing} ->
            Owner = ?MODULE:owner(Txn),
            case validate_owner(Txn, Routing) of
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
                                                                            do_is_valid_checks(Txn, Ledger, Routing, XORFilterSize, XORFilterNum, MinSubnetSize, MaxSubnetSize, MaxSubnetNum, Chain)
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
-spec absorb(txn_routing(),
             blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    TxnFee = ?MODULE:fee(Txn),
    TxnHash = ?MODULE:hash(Txn),
    StakingFee = ?MODULE:staking_fee(Txn),
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
            AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
            case blockchain_ledger_v1:debit_fee(Owner, TxnFee + StakingFee, Ledger, AreFeesEnabled, TxnHash, Chain) of
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

json_type() ->
    <<"routing_v1">>.

-spec to_json(txn_routing(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
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

-spec validate_owner(Txn :: txn_routing(), Routing :: blockchain_ledger_routing_v1:routing()) -> boolean().
validate_owner(Txn, Routing) ->
    Owner = ?MODULE:owner(Txn),
    Addresses = blockchain_ledger_routing_v1:addresses(Routing),
    IsOwner = Owner == blockchain_ledger_routing_v1:owner(Routing),
    case ?MODULE:action(Txn) of
        {new_xor, _Xor} ->
            lists:member(Owner, Addresses) orelse IsOwner;
        {update_xor, _Index, _Filter} ->
            lists:member(Owner, Addresses) orelse IsOwner;
        _ ->
            IsOwner
    end.

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
                         MaxSubnetNum :: pos_integer(),
                         Chain :: blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
do_is_valid_checks(Txn, Ledger, Routing, XORFilterSize, XORFilterNum, MinSubnetSize, MaxSubnetSize, MaxSubnetNum, Chain) ->
    case ?MODULE:action(Txn) of
        {update_routers, Addresses} ->
            case validate_addresses(Addresses) of
                false ->
                    {error, invalid_addresses};
                true ->
                    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                    TxnFee = ?MODULE:fee(Txn),
                    ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
                    Owner = ?MODULE:owner(Txn),
                    case ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled of
                        false ->
                            {error, {wrong_txn_fee, {ExpectedTxnFee, TxnFee}}};
                        true ->
                            blockchain_ledger_v1:check_dc_or_hnt_balance(Owner, TxnFee, Ledger, AreFeesEnabled)
                    end
            end;
        {new_xor, Xor} when byte_size(Xor) > XORFilterSize ->
            {error, filter_too_large};
        {new_xor, Xor} ->
            case length(blockchain_ledger_routing_v1:filters(Routing)) < XORFilterNum of
                true ->
                    %% the contain check does some structural checking of the filter
                    case catch xor16:contain({Xor, fun xxhash:hash64/1}, <<"anything">>) of
                        B when is_boolean(B) ->
                            AreFeesEnabled =blockchain_ledger_v1:txn_fees_active(Ledger),
                            TxnFee = ?MODULE:fee(Txn),
                            ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
                            Owner = ?MODULE:owner(Txn),
                            case ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled of
                                false ->
                                    {error, {wrong_txn_fee, {ExpectedTxnFee, TxnFee}}};
                                true ->
                                    blockchain_ledger_v1:check_dc_or_hnt_balance(Owner, TxnFee, Ledger, AreFeesEnabled)
                            end;
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
                            AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                            TxnFee = ?MODULE:fee(Txn),
                            ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
                            Owner = ?MODULE:owner(Txn),
                            case ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled of
                                false ->
                                    {error, {wrong_txn_fee, {ExpectedTxnFee, TxnFee}}};
                                true ->
                                    blockchain_ledger_v1:check_dc_or_hnt_balance(Owner, TxnFee, Ledger, AreFeesEnabled)
                            end;
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
                                    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                                    StakingFee = ?MODULE:staking_fee(Txn),
                                    ExpectedStakingFee = ?MODULE:calculate_staking_fee(Txn, Chain),
                                    TxnFee = ?MODULE:fee(Txn),
                                    ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
                                    Owner = ?MODULE:owner(Txn),
                                    case {(ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled), ExpectedStakingFee == StakingFee} of
                                        {false,_} ->
                                            {error, {wrong_txn_fee, {ExpectedTxnFee, TxnFee}}};
                                        {_,false} ->
                                            {error, {wrong_staking_fee, {ExpectedStakingFee, StakingFee}}};
                                        {true, true} ->
                                            blockchain_ledger_v1:check_dc_or_hnt_balance(Owner, TxnFee + StakingFee, Ledger, AreFeesEnabled)
                                    end;
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
        fee=?LEGACY_TXN_FEE,
        staking_fee=0,
        nonce = 1,
        signature= <<>>
    },
    ?assertEqual(Tx, update_router_addresses(0, <<"owner">>, [?KEY1], 1)).

oui_test() ->
    Tx = update_router_addresses(0, <<"owner">>, [?KEY1], 1),
    ?assertEqual(0, oui(Tx)).

fee_test() ->
    Tx = update_router_addresses(0, <<"owner">>, [?KEY1], 1),
    ?assertEqual(?LEGACY_TXN_FEE, fee(Tx)).

staking_fee_test() ->
    Tx = update_router_addresses(0, <<"owner">>, [?KEY1], 1),
    ?assertEqual(0, staking_fee(Tx)).

owner_test() ->
    Tx = update_router_addresses(0, <<"owner">>, [?KEY1], 1),
    ?assertEqual(<<"owner">>, owner(Tx)).

addresses_test() ->
    Tx = update_router_addresses(0, <<"owner">>, [?KEY1, ?KEY2], 1),
    ?assertEqual({update_routers, [?KEY1, ?KEY2]}, action(Tx)).

nonce_test() ->
    Tx = update_router_addresses(0, <<"owner">>, [?KEY1], 1),
    ?assertEqual(1, nonce(Tx)).

signature_test() ->
    Tx = update_router_addresses(0, <<"owner">>, [?KEY1], 1),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = update_router_addresses(0, <<"owner">>, [?KEY1], 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_routing_v1_pb:encode_msg(Tx1#blockchain_txn_routing_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

ecode_decode_test() ->
    Tx = update_router_addresses(0, <<"owner">>, [?KEY1, ?KEY2],  1),
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
    Tx = update_router_addresses(0, <<"owner">>, [?KEY1, ?KEY2],  1),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, oui, owner, fee, action, nonce])).

-endif.
