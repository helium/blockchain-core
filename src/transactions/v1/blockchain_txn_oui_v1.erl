%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction OUI ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_oui_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_oui_v1_pb.hrl").

-export([
    new/5, new/6,
    hash/1,
    owner/1,
    addresses/1,
    filter/1,
    requested_subnet_size/1,
    payer/1,
    staking_fee/1, staking_fee/2,
    fee/1, fee/2,
    fee_payer/2,
    oui/1,
    owner_signature/1,
    payer_signature/1,
    sign/2,
    sign_payer/2,
    is_valid_owner/1,
    is_valid_payer/1,
    is_valid/2,
    is_well_formed/1,
    is_prompt/2,
    absorb/2,
    calculate_fee/2, calculate_fee/5, calculate_staking_fee/2, calculate_staking_fee/5,
    print/1,
    json_type/0,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(T, #blockchain_txn_oui_v1_pb).

-type t() :: txn_oui().

-type txn_oui() :: ?T{}.

-export_type([t/0, txn_oui/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(
        OUI :: pos_integer(),
        Owner :: libp2p_crypto:pubkey_bin(),
        Addresses :: [libp2p_crypto:pubkey_bin()],
        Filter :: binary() | undefined,
        RequestedSubnetSize :: pos_integer() | undefined) -> txn_oui().
new(OUI, Owner, Addresses, Filter, RequestedSubnetSize) ->
    #blockchain_txn_oui_v1_pb{
       oui = OUI,
       owner=Owner,
       addresses=Addresses,
       filter=Filter,
       requested_subnet_size=RequestedSubnetSize,
       payer= <<>>,
       staking_fee=?LEGACY_STAKING_FEE,
       fee=?LEGACY_TXN_FEE,
       owner_signature= <<>>,
       payer_signature= <<>>
    }.

-spec new(
        OUI :: pos_integer(),
        Owner :: libp2p_crypto:pubkey_bin(),
        Addresses :: [binary()],
        Filter :: binary() | undefined,
        RequestedSubnetSize :: pos_integer() | undefined,
        Payer :: libp2p_crypto:pubkey_bin()) -> txn_oui().
new(OUI, Owner, Addresses, Filter, RequestedSubnetSize, Payer) ->
    #blockchain_txn_oui_v1_pb{
       oui=OUI,
       owner=Owner,
       addresses=Addresses,
       filter=Filter,
       requested_subnet_size=RequestedSubnetSize,
       payer=Payer,
       staking_fee=?LEGACY_STAKING_FEE,
       fee=?LEGACY_TXN_FEE,
       owner_signature= <<>>,
       payer_signature= <<>>
    }.

-spec hash(txn_oui()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_oui_v1_pb{owner_signature = <<>>, payer_signature = <<>>},
    EncodedTxn = blockchain_txn_oui_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec owner(txn_oui()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.owner.

-spec addresses(txn_oui()) -> [libp2p_crypto:pubkey_bin()].
addresses(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.addresses.

-spec filter(txn_oui()) -> binary() | undefined.
filter(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.filter.

-spec requested_subnet_size(txn_oui()) -> pos_integer() | undefined.
requested_subnet_size(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.requested_subnet_size.

-spec payer(txn_oui()) -> libp2p_crypto:pubkey_bin() | <<>> | undefined.
payer(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.payer.

-spec staking_fee(txn_oui()) -> non_neg_integer().
staking_fee(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.staking_fee.

-spec staking_fee(txn_oui(), non_neg_integer()) -> txn_oui().
staking_fee(Txn, Fee) ->
    Txn#blockchain_txn_oui_v1_pb{staking_fee=Fee}.

-spec fee(txn_oui()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.fee.

-spec fee(txn_oui(), non_neg_integer()) -> txn_oui().
fee(Txn, Fee) ->
    Txn#blockchain_txn_oui_v1_pb{fee=Fee}.

-spec fee_payer(txn_oui(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(Txn, _Ledger) ->
    payer(Txn).

-spec oui(txn_oui()) -> pos_integer().
oui(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.oui.

-spec owner_signature(txn_oui()) -> binary().
owner_signature(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.owner_signature.

-spec payer_signature(txn_oui()) -> binary().
payer_signature(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.payer_signature.

-spec sign(txn_oui(), libp2p_crypto:sig_fun()) -> txn_oui().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_oui_v1_pb{owner_signature= <<>>, payer_signature= <<>>},
    EncodedTxn = blockchain_txn_oui_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_oui_v1_pb{owner_signature=SigFun(EncodedTxn)}.

-spec sign_payer(txn_oui(), libp2p_crypto:sig_fun()) -> txn_oui().
sign_payer(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_oui_v1_pb{owner_signature= <<>>, payer_signature= <<>>},
    EncodedTxn = blockchain_txn_oui_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_oui_v1_pb{payer_signature=SigFun(EncodedTxn)}.

-spec is_valid_owner(txn_oui()) -> boolean().
is_valid_owner(#blockchain_txn_oui_v1_pb{owner=PubKeyBin,
                                         owner_signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_oui_v1_pb{owner_signature= <<>>, payer_signature= <<>>},
    EncodedTxn = blockchain_txn_oui_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

-spec is_valid_payer(txn_oui()) -> boolean().
is_valid_payer(#blockchain_txn_oui_v1_pb{payer=undefined}) ->
    %% no payer
    true;
is_valid_payer(#blockchain_txn_oui_v1_pb{payer= <<>>, payer_signature= <<>>}) ->
    %% empty payer, empty payer_signature
    true;
is_valid_payer(#blockchain_txn_oui_v1_pb{payer=PubKeyBin,
                                         payer_signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_oui_v1_pb{owner_signature= <<>>, payer_signature= <<>>},
    EncodedTxn = blockchain_txn_oui_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

-spec is_valid(txn_oui(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    case {?MODULE:is_valid_owner(Txn),
          ?MODULE:is_valid_payer(Txn)} of
        {false, _} ->
            {error, bad_owner_signature};
        {_, false} ->
            {error, bad_payer_signature};
        {true, true} ->
            do_oui_validation_checks(Txn, Chain)
    end.

-spec is_well_formed(t()) -> ok | {error, {contract_breach, any()}}.
is_well_formed(?T{}) ->
    ok.

-spec is_prompt(t(), blockchain:blockchain()) ->
    {ok, blockchain_txn:is_prompt()} | {error, any()}.
is_prompt(?T{}, _) ->
    {ok, yes}.

-spec absorb(txn_oui(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    TxnFee = ?MODULE:fee(Txn),
    TxnHash = ?MODULE:hash(Txn),
    StakingFee = ?MODULE:staking_fee(Txn),
    Owner = ?MODULE:owner(Txn),
    Payer = ?MODULE:payer(Txn),
    OUI = ?MODULE:oui(Txn),
    ActualPayer = case Payer == undefined orelse Payer == <<>> of
        true -> Owner;
        false -> Payer
    end,
    SubnetSize = ?MODULE:requested_subnet_size(Txn),
    case blockchain_ledger_v1:allocate_subnet(SubnetSize, Ledger) of
        {ok, Subnet} ->
            case validate_oui(OUI, Ledger) of
                {false, LedgerOUI} ->
                    {error, {invalid_oui, {OUI, LedgerOUI}}};
                true ->
                    case blockchain_ledger_v1:debit_fee(ActualPayer, TxnFee + StakingFee, Ledger, AreFeesEnabled, TxnHash, Chain) of
                        {error, _}=Error ->
                            Error;
                        ok ->
                            Addresses = ?MODULE:addresses(Txn),
                            Filter = ?MODULE:filter(Txn),
                            blockchain_ledger_v1:add_oui(Owner, Addresses, Filter, Subnet, Ledger)
                    end
            end;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Calculate the txn fee
%% Returned value is txn_byte_size / 24
%% @end
%%--------------------------------------------------------------------
-spec calculate_fee(txn_oui(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(txn_oui(), blockchain_ledger_v1:ledger(), pos_integer(), pos_integer(), boolean()) -> non_neg_integer().

calculate_fee(_Txn, _Ledger, _DCPayloadSize, _TxnFeeMultiplier, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, true) ->
    case Txn#blockchain_txn_oui_v1_pb.payer of
        Payer when Payer == undefined; Payer == <<>> ->
            %% no payer signature if there's no payer
            ?calculate_fee(Txn#blockchain_txn_oui_v1_pb{fee=0, staking_fee = 0,
                                                        owner_signature = <<0:512>>,
                                                        payer_signature = <<>>}, Ledger, DCPayloadSize, TxnFeeMultiplier);
        _ ->
            ?calculate_fee(Txn#blockchain_txn_oui_v1_pb{fee=0, staking_fee = 0,
                                                        owner_signature = <<0:512>>,
                                                        payer_signature = <<0:512>>}, Ledger, DCPayloadSize, TxnFeeMultiplier)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Calculate the staking fee using the price oracles
%% returns the fee in DC
%% @end
%%--------------------------------------------------------------------
-spec calculate_staking_fee(txn_oui(), blockchain:blockchain()) -> non_neg_integer().
calculate_staking_fee(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    OUIStakingFee = blockchain_ledger_v1:staking_fee_txn_oui_v1(Ledger),
    OUIPerAddress = blockchain_ledger_v1:staking_fee_txn_oui_v1_per_address(Ledger),
    ExtraData = [{per_address, OUIPerAddress}],
    calculate_staking_fee(Txn, Ledger, OUIStakingFee, ExtraData, blockchain_ledger_v1:txn_fees_active(Ledger)).

-spec calculate_staking_fee(txn_oui(), blockchain_ledger_v1:ledger(), non_neg_integer(), [{atom(), non_neg_integer()}], boolean()) -> non_neg_integer().
calculate_staking_fee(_Txn, _Ledger, _Fee, _ExtraData, false) ->
    ?LEGACY_STAKING_FEE;
calculate_staking_fee(Txn, _Ledger, Fee, ExtraData, true) ->
    OIUPerAddress = proplists:get_value(per_address, ExtraData),
    TxnPriceUSD = Fee + (requested_subnet_size(Txn) * OIUPerAddress),
    trunc(TxnPriceUSD).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_oui()) -> iodata().
print(undefined) -> <<"type=oui, undefined">>;
print(#blockchain_txn_oui_v1_pb{owner=Owner, addresses=Addresses,
                                payer=Payer, staking_fee=StakingFee,
                                fee=Fee}) ->
    io_lib:format("type=oui, owner=~p, addresses=~p, payer=~p, staking_fee=~p, fee=~p",
                  [?TO_B58(Owner), [?TO_B58(A) || A <- Addresses], ?TO_B58(Payer), StakingFee, Fee]).

json_type() ->
    <<"oui_v1">>.

-spec to_json(txn_oui(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      owner => ?BIN_TO_B58(owner(Txn)),
      addresses => [?BIN_TO_B58(Addr) || Addr <- addresses(Txn)],
      payer => ?BIN_TO_B58(payer(Txn)),
      staking_fee => staking_fee(Txn),
      fee => fee(Txn),
      filter => ?MAYBE_B64(filter(Txn)),
      requested_subnet_size => requested_subnet_size(Txn),
      oui => oui(Txn)
     }.


%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec validate_addresses([binary()]) -> boolean().
validate_addresses([]) ->
    true;
validate_addresses(Addresses) ->
    case {erlang:length(Addresses), erlang:length(lists:usort(Addresses))} of
        {L, L} when L =< 3 ->
            ok == blockchain_txn:validate_fields([{{router_address, P}, {address, libp2p}} || P <- Addresses]);
        _ ->
            false
    end.

validate_subnet_size(Size) when Size < 8; Size > 65536 ->
    false;
validate_subnet_size(Size) ->
    %% subnet size should be between 8 and 65536 as a power of two
    Res = math:log2(Size),
    %% check there's no floating point components of the number
    %% Erlang will coerce between floats and ints when you use ==
    trunc(Res) == Res.

validate_filter(Filter) ->
    %% the contain check does some structural checking of the filter
    case catch xor16:contain({Filter, fun xxhash:hash64/1}, <<"anything">>) of
        B when is_boolean(B) -> true;
        _ -> false
    end.

-spec validate_oui(OUI :: pos_integer(), Ledger :: blockchain_ledger_v1:ledger()) -> true | {false, non_neg_integer()}.
validate_oui(OUI, Ledger) ->
    case blockchain_ledger_v1:get_oui_counter(blockchain_ledger_v1:remove_context(Ledger)) of
        {error, _} ->
            %% no existing oui on chain
            true;
        {ok, 0} ->
            true;
        {ok, 1} ->
            true;
        {ok, OUI} ->
            true;
        {ok, OtherOUI} ->
            {false, OtherOUI}
    end.

-spec do_oui_validation_checks(txn_oui(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
%% TODO - get rid of this nested bunch of cases
do_oui_validation_checks(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Owner = ?MODULE:owner(Txn),
    OUI = ?MODULE:oui(Txn),
    Addresses = ?MODULE:addresses(Txn),
    case validate_oui(OUI, Ledger) of
        {false, LedgerOUI} ->
            {error, {invalid_oui, {OUI, LedgerOUI}}};
        true ->
            case validate_addresses(Addresses) of
                false ->
                    {error, invalid_addresses};
                true ->
                    case validate_subnet_size(?MODULE:requested_subnet_size(Txn)) of
                        false ->
                            {error, invalid_subnet_size};
                        true ->
                            case validate_filter(?MODULE:filter(Txn)) of
                                false ->
                                    {error, invalid_filter};
                                true ->
                                    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                                    StakingFee = ?MODULE:staking_fee(Txn),
                                    ExpectedStakingFee = ?MODULE:calculate_staking_fee(Txn, Chain),
                                    TxnFee = ?MODULE:fee(Txn),
                                    ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
                                    case {(ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled), ExpectedStakingFee == StakingFee} of
                                        {false,_} ->
                                            {error, {wrong_txn_fee, {ExpectedTxnFee, TxnFee}}};
                                        {_,false} ->
                                            {error, {wrong_staking_fee, {ExpectedStakingFee, StakingFee}}};
                                        {true, true} ->
                                            Owner = ?MODULE:owner(Txn),
                                            Payer = ?MODULE:payer(Txn),
                                            ActualPayer = case Payer == undefined orelse Payer == <<>> of
                                                              true -> Owner;
                                                              false -> Payer
                                                          end,
                                            blockchain_ledger_v1:check_dc_or_hnt_balance(ActualPayer, TxnFee + StakingFee, Ledger, AreFeesEnabled)
                                    end
                            end
                    end
            end
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

missing_payer_signature_new() ->
    #{public := PubKey, secret := _PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    #blockchain_txn_oui_v1_pb{
       oui = 1,
       owner= <<"owner">>,
       addresses = [?KEY1],
       payer= libp2p_crypto:pubkey_to_bin(PubKey),
       payer_signature= <<>>,
       staking_fee=?LEGACY_STAKING_FEE,
       fee=?LEGACY_TXN_FEE,
       owner_signature= <<>>
      }.

new_test() ->
    Tx = #blockchain_txn_oui_v1_pb{
        oui = 1,
        owner= <<"owner">>,
        addresses = [?KEY1],
        payer = <<>>,
        staking_fee=?LEGACY_STAKING_FEE,
        fee=?LEGACY_TXN_FEE,
        owner_signature= <<>>,
        payer_signature = <<>>
    },
    ?assertEqual(Tx, new(1, <<"owner">>, [?KEY1], <<>>, 0)).

owner_test() ->
    Tx = new(1, <<"owner">>, [?KEY1], undefined, undefined),
    ?assertEqual(<<"owner">>, owner(Tx)).

addresses_test() ->
    Tx = new(1, <<"owner">>, [?KEY1], undefined, undefined),
    ?assertEqual([?KEY1], addresses(Tx)).

staking_fee_test() ->
    Tx = new(1, <<"owner">>, [?KEY1], undefined, undefined),
    ?assertEqual(?LEGACY_STAKING_FEE, staking_fee(Tx)).

fee_test() ->
    Tx = new(1, <<"owner">>, [?KEY1], undefined, undefined),
    ?assertEqual(?LEGACY_TXN_FEE, fee(Tx)).

oui_test() ->
    Tx = new(1, <<"owner">>, [?KEY1], undefined, undefined),
    ?assertEqual(1, oui(Tx)).

payer_test() ->
    Tx = new(1, <<"owner">>, [?KEY1], undefined, undefined, <<"payer">>),
    ?assertEqual(<<"payer">>, payer(Tx)).

owner_signature_test() ->
    Tx = new(1, <<"owner">>, [?KEY1], undefined, undefined),
    ?assertEqual(<<>>, owner_signature(Tx)).

payer_signature_test() ->
    Tx = new(1, <<"owner">>, [?KEY1], undefined, undefined),
    ?assertEqual(<<>>, payer_signature(Tx)).

missing_payer_signature_test() ->
    Tx = missing_payer_signature_new(),
    ?assertNot(is_valid_payer(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(1, <<"owner">>, [?KEY1], undefined, undefined),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = owner_signature(Tx1),
    EncodedTx1 = blockchain_txn_oui_v1_pb:encode_msg(Tx1#blockchain_txn_oui_v1_pb{owner_signature = <<>>, payer_signature= <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

sign_payer_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(1, <<"owner">>, [?KEY1], undefined, undefined, <<"payer">>),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_payer(Tx0, SigFun),
    Sig1 = payer_signature(Tx1),
    EncodedTx1 = blockchain_txn_oui_v1_pb:encode_msg(Tx1#blockchain_txn_oui_v1_pb{owner_signature = <<>>, payer_signature= <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

validate_addresses_test() ->
    ?assert(validate_addresses([])),
    ?assert(validate_addresses([?KEY1])),
    ?assertNot(validate_addresses([?KEY1, ?KEY1])),
    ?assert(validate_addresses([?KEY1, ?KEY2])),
    ?assert(validate_addresses([?KEY1, ?KEY2, ?KEY3])),
    ?assertNot(validate_addresses([?KEY1, ?KEY2, ?KEY3, ?KEY4])),
    ?assertNot(validate_addresses([<<"http://test.com">>])),
    ?assertNot(validate_addresses([?KEY1, <<"http://test.com">>])),
    ?assertNot(validate_addresses([?KEY1, ?KEY1, <<"http://test.com">>])),
    ok.

to_json_test() ->
    Tx = new(1, <<"owner">>, [?KEY1], undefined, undefined, <<"payer">>),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, owner, addresses, payer, staking_fee, fee, filter, requested_subnet_size, oui])).


-endif.
