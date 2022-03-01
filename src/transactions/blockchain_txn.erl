%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Behavior ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn).

%% The union type of all transactions is defined in
%% blockchain_txn.proto. The txn() type below should reflec that
%% union.
-include_lib("helium_proto/include/blockchain_txn_pb.hrl").

-type hash() :: <<_:256>>. %% SHA256 digest
-type txn() :: blockchain_txn_add_gateway_v1:txn_add_gateway()
             | blockchain_txn_assert_location_v1:txn_assert_location()
             | blockchain_txn_coinbase_v1:txn_coinbase()
             | blockchain_txn_security_coinbase_v1:txn_security_coinbase()
             | blockchain_txn_consensus_group_v1:txn_consensus_group()
             | blockchain_txn_consensus_group_failure_v1:txn_consensus_group()
             | blockchain_txn_gen_gateway_v1:txn_genesis_gateway()
             | blockchain_txn_payment_v1:txn_payment()
             | blockchain_txn_security_exchange_v1:txn_security_exchange()
             | blockchain_txn_oui_v1:txn_oui()
             | blockchain_txn_routing_v1:txn_routing()
             | blockchain_txn_create_htlc_v1:txn_create_htlc()
             | blockchain_txn_redeem_htlc_v1:txn_redeem_htlc()
             | blockchain_txn_poc_request_v1:txn_poc_request()
             | blockchain_txn_poc_receipts_v1:txn_poc_receipts()
             | blockchain_txn_vars_v1:txn_vars()
             | blockchain_txn_rewards_v1:txn_rewards()
             | blockchain_txn_token_burn_v1:txn_token_burn()
             | blockchain_txn_dc_coinbase_v1:txn_dc_coinbase()
             | blockchain_txn_token_burn_exchange_rate_v1:txn_token_burn_exchange_rate()
             | blockchain_txn_bundle_v1:txn_bundle()
             | blockchain_txn_payment_v2:txn_payment_v2()
             | blockchain_txn_state_channel_open_v1:txn_state_channel_open()
             | blockchain_txn_update_gateway_oui_v1:txn_update_gateway_oui()
             | blockchain_txn_price_oracle_v1:txn_price_oracle()
             | blockchain_txn_gen_price_oracle_v1:txn_genesis_price_oracle()
             | blockchain_txn_transfer_hotspot_v1:txn_transfer_hotspot()
             | blockchain_txn_state_channel_close_v1:txn_state_channel_close()
             | blockchain_txn_rewards_v2:txn_rewards_v2()
             | blockchain_txn_assert_location_v2:txn_assert_location()
             | blockchain_txn_gen_validator_v1:txn_gen_validator()
             | blockchain_txn_stake_validator_v1:txn_stake_validator()
             | blockchain_txn_transfer_validator_stake_v1:txn_transfer_validator_stake()
             | blockchain_txn_unstake_validator_v1:txn_unstake_validator()
             | blockchain_txn_unstake_validator_v1:txn_validator_heartbeat()
             | blockchain_txn_transfer_hotspot_v2:txn_transfer_hotspot_v2().

-type before_commit_callback() :: fun((blockchain:blockchain(), blockchain_block:hash()) -> ok | {error, any()}).
-type txns() :: [txn()].
-export_type([hash/0, txn/0, txns/0]).

-callback fee(txn()) -> non_neg_integer().
-callback fee_payer(txn(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
-callback json_type() -> binary() | atom().
-callback hash(State::any()) -> hash().
-callback sign(txn(), libp2p_crypto:sig_fun()) -> txn().
-callback is_valid(txn(), blockchain:blockchain()) -> ok | {error, any()}.
-callback absorb(txn(),  blockchain:blockchain()) -> ok | {error, any()}.
-callback print(txn()) -> iodata().
-callback print(txn(), boolean()) -> iodata().
-callback calculate_fee(txn(), blockchain:blockchain()) -> non_neg_integer().
-callback calculate_staking_fee(txn(), blockchain:blockchain()) -> non_neg_integer().
-callback rescue_absorb(txn(),  blockchain:blockchain()) -> ok | {error, any()}.

-optional_callbacks([calculate_fee/2, calculate_staking_fee/2, rescue_absorb/2, print/2]).

-behavior(blockchain_json).

-export([
    block_delay/0,
    hash/1,
    fee/1,
    fee_payer/2,
    validate/2, validate/3,
    absorb/2,
    print/1, print/2,
    sign/2,
    absorb_and_commit/3, absorb_and_commit/4,
    unvalidated_absorb_and_commit/4,
    absorb_block/2, absorb_block/3,
    absorb_txns/3,
    absorb_delayed/2,
    sort/2,
    type/1,
    serialize/1,
    deserialize/1,
    wrap_txn/1,
    unwrap_txn/1,
    is_valid/2,
    validate_fields/1,
    depends_on/2,
    json_type/1,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(BLOCK_DELAY, 50).
-define(ORDER, [
    {blockchain_txn_rewards_v1, 1},
    {blockchain_txn_vars_v1, 2},
    {blockchain_txn_consensus_group_v1, 3},
    {blockchain_txn_coinbase_v1, 4},
    {blockchain_txn_security_coinbase_v1, 5},
    {blockchain_txn_dc_coinbase_v1, 6},
    {blockchain_txn_gen_gateway_v1, 7},
    {blockchain_txn_token_burn_exchange_rate_v1, 8},
    {blockchain_txn_oui_v1, 9},
    {blockchain_txn_routing_v1, 10},
    {blockchain_txn_create_htlc_v1, 11},
    {blockchain_txn_payment_v1, 12},
    {blockchain_txn_security_exchange_v1, 13},
    {blockchain_txn_add_gateway_v1, 14},
    {blockchain_txn_assert_location_v1, 15},
    {blockchain_txn_redeem_htlc_v1, 16},
    {blockchain_txn_routing_v1, 17},
    {blockchain_txn_poc_request_v1, 18},
    {blockchain_txn_poc_receipts_v1, 19},
    {blockchain_txn_payment_v2, 20},
    {blockchain_txn_state_channel_open_v1, 21},
    {blockchain_txn_update_gateway_oui_v1, 22},
    {blockchain_txn_price_oracle_v1, 23},
    {blockchain_txn_state_channel_close_v1, 24},
    {blockchain_txn_token_burn_v1, 25},
    {blockchain_txn_transfer_hotspot_v1, 26},
    {blockchain_txn_rewards_v2, 27},
    {blockchain_txn_assert_location_v2, 28},
    {blockchain_txn_gen_validator_v1, 29},
    {blockchain_txn_stake_validator_v1, 30},
    {blockchain_txn_transfer_validator_stake_v1, 31},
    {blockchain_txn_unstake_validator_v1, 32},
    {blockchain_txn_validator_heartbeat_v1, 33},
    {blockchain_txn_gen_price_oracle_v1, 34},
    {blockchain_txn_consensus_group_failure_v1, 35},
    {blockchain_txn_transfer_hotspot_v2, 36}
]).

block_delay() ->
    ?BLOCK_DELAY.

hash(Txn) ->
    (type(Txn)):hash(Txn).

fee(Txn) ->
    (type(Txn)):fee(Txn).

fee_payer(Txn, Ledger) ->
    (type(Txn)):fee_payer(Txn, Ledger).

sign(Txn, SigFun) ->
    (type(Txn)):sign(Txn, SigFun).

-spec serialize(txn()) -> binary().
serialize(Txn) ->
    blockchain_txn_pb:encode_msg(wrap_txn(Txn)).

-spec deserialize(binary()) -> txn().
deserialize(Bin) ->
    unwrap_txn(blockchain_txn_pb:decode_msg(Bin, blockchain_txn_pb)).

-spec json_type(txn()) -> binary() | atom().
json_type(Txn) ->
    (type(Txn)):json_type().

-spec to_json(txn(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, Opts) ->
    (type(Txn)):to_json(Txn, Opts).

%% Since the proto file for the transaction union includes the
%% definitions of the underlying protobufs for each transaction we
%% break encapsulation here and do not use the txn modules themselves.
-spec wrap_txn(blockchain_txn:txn()) -> #blockchain_txn_pb{}.
wrap_txn(#blockchain_txn_assert_location_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={assert_location, Txn}};
wrap_txn(#blockchain_txn_payment_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={payment, Txn}};
wrap_txn(#blockchain_txn_security_exchange_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={security_exchange, Txn}};
wrap_txn(#blockchain_txn_create_htlc_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={create_htlc, Txn}};
wrap_txn(#blockchain_txn_redeem_htlc_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={redeem_htlc, Txn}};
wrap_txn(#blockchain_txn_add_gateway_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={add_gateway, Txn}};
wrap_txn(#blockchain_txn_coinbase_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={coinbase, Txn}};
wrap_txn(#blockchain_txn_security_coinbase_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={security_coinbase, Txn}};
wrap_txn(#blockchain_txn_consensus_group_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={consensus_group, Txn}};
wrap_txn(#blockchain_txn_poc_request_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={poc_request, Txn}};
wrap_txn(#blockchain_txn_poc_receipts_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={poc_receipts, Txn}};
wrap_txn(#blockchain_txn_gen_gateway_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={gen_gateway, Txn}};
wrap_txn(#blockchain_txn_oui_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={oui, Txn}};
wrap_txn(#blockchain_txn_routing_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={routing, Txn}};
wrap_txn(#blockchain_txn_vars_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={vars, Txn}};
wrap_txn(#blockchain_txn_rewards_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={rewards, Txn}};
wrap_txn(#blockchain_txn_token_burn_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={token_burn, Txn}};
wrap_txn(#blockchain_txn_dc_coinbase_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={dc_coinbase, Txn}};
wrap_txn(#blockchain_txn_token_burn_exchange_rate_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={token_burn_exchange_rate, Txn}};
wrap_txn(#blockchain_txn_payment_v2_pb{}=Txn) ->
    #blockchain_txn_pb{txn={payment_v2, Txn}};
wrap_txn(#blockchain_txn_bundle_v1_pb{transactions=Txns}=Txn) ->
    #blockchain_txn_pb{txn={bundle, Txn#blockchain_txn_bundle_v1_pb{transactions=lists:map(fun wrap_txn/1, Txns)}}};
wrap_txn(#blockchain_txn_state_channel_open_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={state_channel_open, Txn}};
wrap_txn(#blockchain_txn_update_gateway_oui_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={update_gateway_oui, Txn}};
wrap_txn(#blockchain_txn_state_channel_close_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={state_channel_close, Txn}};
wrap_txn(#blockchain_txn_price_oracle_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={price_oracle_submission, Txn}};
wrap_txn(#blockchain_txn_gen_price_oracle_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={gen_price_oracle, Txn}};
wrap_txn(#blockchain_txn_transfer_hotspot_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={transfer_hotspot, Txn}};
wrap_txn(#blockchain_txn_rewards_v2_pb{}=Txn) ->
    #blockchain_txn_pb{txn={rewards_v2, Txn}};
wrap_txn(#blockchain_txn_assert_location_v2_pb{}=Txn) ->
    #blockchain_txn_pb{txn={assert_location_v2, Txn}};
wrap_txn(#blockchain_txn_gen_validator_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={gen_validator, Txn}};
wrap_txn(#blockchain_txn_stake_validator_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={stake_validator, Txn}};
wrap_txn(#blockchain_txn_transfer_validator_stake_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={transfer_val_stake, Txn}};
wrap_txn(#blockchain_txn_unstake_validator_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={unstake_validator, Txn}};
wrap_txn(#blockchain_txn_validator_heartbeat_v1_pb{} = Txn) ->
    #blockchain_txn_pb{txn={val_heartbeat, Txn}};
wrap_txn(#blockchain_txn_consensus_group_failure_v1_pb{} = Txn) ->
    #blockchain_txn_pb{txn={consensus_group_failure, Txn}};
wrap_txn(#blockchain_txn_transfer_hotspot_v2_pb{}=Txn) ->
    #blockchain_txn_pb{txn={transfer_hotspot_v2, Txn}}.

-spec unwrap_txn(#blockchain_txn_pb{}) -> blockchain_txn:txn().
unwrap_txn(#blockchain_txn_pb{txn={bundle, #blockchain_txn_bundle_v1_pb{transactions=Txns} = Bundle}}) ->
    Bundle#blockchain_txn_bundle_v1_pb{transactions=lists:map(fun unwrap_txn/1, Txns)};
unwrap_txn(#blockchain_txn_pb{txn={_, Txn}}) ->
    Txn.

%%--------------------------------------------------------------------
%% @doc
%% Called in the miner
%% @end
%%--------------------------------------------------------------------
-spec validate(txns(), blockchain:blockchain()) -> {blockchain_txn:txns(), blockchain_txn:txns()}.
validate(Transactions, Chain) ->
    validate(Transactions, Chain, false).

-spec validate(txns(), blockchain:blockchain(), boolean()) ->
                      {blockchain_txn:txns(), [{txn(), atom()}]}.
validate(Transactions, _Chain, true) ->
    {Transactions, []};
validate(Transactions, Chain0, false) ->
    Ledger0 = blockchain:ledger(Chain0),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger0),
    Chain1 = blockchain:ledger(Ledger1, Chain0),
    validate(Transactions, [], [], undefined, [], Chain1).

validate([], Valid, Invalid, PType, PBuf, Chain) ->
    {Valid1, Invalid1} =
        case PType of
            undefined ->
                {Valid, Invalid};
            _ ->
                Res = blockchain_utils:pmap(
                        fun(T) ->
                                Start = erlang:monotonic_time(millisecond),
                                Type = ?MODULE:type(T),
                                Ret = (catch Type:is_valid(T, Chain)),
                                maybe_log_duration(Type, Start),
                                {T, Ret}
                        end, lists:reverse(PBuf)),
                separate_res(Res, Chain, Valid, Invalid)
        end,
    Ledger = blockchain:ledger(Chain),
    blockchain_ledger_v1:delete_context(Ledger),
    lager:info("valid: ~p, invalid: ~p", [types(Valid1), types(Invalid1)]),
    {lists:reverse(Valid1), Invalid1};
validate([Txn | Tail] = Txns, Valid, Invalid, PType, PBuf, Chain) ->
    Type = ?MODULE:type(Txn),
    case Type of
        blockchain_txn_poc_request_v1 when PType == undefined orelse PType == Type ->
            validate(Tail, Valid, Invalid, Type, [Txn | PBuf], Chain);
        blockchain_txn_poc_receipts_v1 when PType == undefined orelse PType == Type ->
            validate(Tail, Valid, Invalid, Type, [Txn | PBuf], Chain);
        _Else when PType == undefined ->
            Start = erlang:monotonic_time(millisecond),
            case catch Type:is_valid(Txn, Chain) of
                ok ->
                    case ?MODULE:absorb(Txn, Chain) of
                        ok ->
                            maybe_log_duration(type(Txn), Start),
                            validate(Tail, [Txn|Valid], Invalid, PType, PBuf, Chain);
                        {error, {bad_nonce, {_NonceType, Nonce, LedgerNonce}}} when Nonce > LedgerNonce + 1 ->
                            %% we don't have enough context to decide if this transaction is valid yet, keep it
                            %% but don't include it in the block (so it stays in the buffer)
                            validate(Tail, Valid, Invalid, PType, PBuf, Chain);
                        {error, {InvalidReason, _Details}} = Error ->
                            lager:warning("invalid txn while absorbing ~p : ~p / ~s", [Type, Error, print(Txn)]),
                            validate(Tail, Valid, [{Txn, InvalidReason} | Invalid], PType, PBuf, Chain);
                        {error, InvalidReason} = Error->
                            lager:warning("invalid txn while absorbing ~p : ~p / ~s", [Type, Error, print(Txn)]),
                            validate(Tail, Valid, [{Txn, InvalidReason} | Invalid], PType, PBuf, Chain)
                    end;
                {error, {bad_nonce, {_NonceType, Nonce, LedgerNonce}}} when Nonce > LedgerNonce + 1 ->
                    %% we don't have enough context to decide if this transaction is valid yet, keep it
                    %% but don't include it in the block (so it stays in the buffer)
                    validate(Tail, Valid, Invalid, PType, PBuf, Chain);
                {error, {InvalidReason, _Details}} = Error ->
                    lager:warning("invalid txn ~p : ~p / ~s", [Type, Error, print(Txn)]),
                    %% any other error means we drop it
                    validate(Tail, Valid, [{Txn, InvalidReason} | Invalid], PType, PBuf, Chain);
                {error, InvalidReason}=Error when is_atom(InvalidReason) ->
                    lager:warning("invalid txn ~p : ~p / ~s", [Type, Error, print(Txn)]),
                    %% any other error means we drop it
                    validate(Tail, Valid, [{Txn, InvalidReason} | Invalid], PType, PBuf, Chain);
                Error ->
                    lager:warning("invalid txn ~p : ~p / ~s", [Type, Error, print(Txn)]),
                    %% any other error means we drop it
                    %% this error is unexpected and could be a crash report or some other weirdness
                    %% we will use a generic error reason
                    validate(Tail, Valid, [{Txn, validation_failed} | Invalid], PType, PBuf, Chain)
            end;
        _Else ->
            Res = blockchain_utils:pmap(
                    fun(T) ->
                            Start = erlang:monotonic_time(millisecond),
                            Ty = ?MODULE:type(T),
                            Ret = (catch Ty:is_valid(T, Chain)),
                            maybe_log_duration(Ty, Start),
                            {T, Ret}
                    end, lists:reverse(PBuf)),
            {Valid1, Invalid1} = separate_res(Res, Chain, Valid, Invalid),
            validate(Txns, Valid1, Invalid1, undefined, [], Chain)
    end.

separate_res([], _Chain, V, I) ->
    {V, I};
separate_res([{T, ok} | Rest], Chain, V, I) ->
    case ?MODULE:absorb(T, Chain) of
        ok ->
            separate_res(Rest, Chain, [T|V], I);
        {error, {Reason, _Details}} = Error ->
            lager:warning("invalid txn while absorbing ~p : ~p / ~s", [type(T), Error, print(T)]),
            separate_res(Rest, Chain, V, [{T, Reason} | I]);
        {error, Reason} ->
            lager:warning("invalid txn while absorbing ~p : ~p / ~s", [type(T), Reason, print(T)]),
            separate_res(Rest, Chain, V, [{T, Reason} | I])
    end;

separate_res([{T, Err} | Rest], Chain, V, I) ->
    case Err of
        {error, {bad_nonce, {_NonceType, Nonce, LedgerNonce}}} when Nonce > LedgerNonce + 1 ->
            separate_res(Rest, Chain, V, I);
        {error, {InvalidReason, _Details}} = Error ->
            lager:warning("invalid txn ~p : ~p / ~s", [type(T), Error, print(T)]),
            %% any other error means we drop it
            separate_res(Rest, Chain, V, [{T, InvalidReason} | I]);
        {error, InvalidReason} = Error ->
            lager:warning("invalid txn ~p : ~p / ~s", [type(T), Error, print(T)]),
            %% any other error means we drop it
            separate_res(Rest, Chain, V, [{T, InvalidReason} | I]);
        {'EXIT', {{_Why,{error, CrashReason}}, Stack}} when is_atom(CrashReason)->
            lager:warning("crashed txn ~p : ~p / ~s - ~p", [type(T), CrashReason, print(T), Stack]),
            %% any other error means we drop it
            separate_res(Rest, Chain, V, [{T, CrashReason} | I]);
        {'EXIT', CrashReason} when is_atom(CrashReason)->
            lager:warning("crashed txn ~p : ~p / ~s", [type(T), CrashReason, print(T)]),
            %% any other error means we drop it
            separate_res(Rest, Chain, V, [{T, CrashReason} | I]);
        Error->
            %% since this is critical code, always make sure we have a catchall clause just in case
            %% Any log events hitting here should be reviewed and considered if we need to add
            %% specific handling for any such error msg, ensuring we return meaningful error msgs where possible
            lager:warning("BUG: unexpected txn validation error format ~p : ~p / ~s", [type(T), Error, print(T)]),
            separate_res(Rest, Chain, V, [{T, txn_failed} | I])
    end.

maybe_log_duration(Type, Start) ->
    case application:get_env(blockchain, log_validation_times, false) of
        true ->
            End = erlang:monotonic_time(millisecond),
            lager:info("~p took ~p ms", [Type, End - Start]);
        _ -> ok
    end.

types(L) ->
    L1 = lists:map(fun
                       ({Txn, _}) when is_tuple(Txn) -> type(Txn);
                       (Txn)-> type(Txn)
                   end, L),
    M = lists:foldl(
          fun(T, Acc) ->
                  maps:update_with(T, fun(X) -> X + 1 end, 1, Acc)
          end,
          #{},
          L1),
    maps:to_list(M).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb_and_commit(blockchain_block:block(), blockchain:blockchain(), before_commit_callback()) ->
                               ok | {error, any()}.
absorb_and_commit(Block, Chain0, BeforeCommit) ->
    absorb_and_commit(Block, Chain0, BeforeCommit, false).

-spec absorb_and_commit(blockchain_block:block(), blockchain:blockchain(), before_commit_callback(), boolean()) ->
                               ok | {error, any()}.
absorb_and_commit(Block, Chain0, BeforeCommit, Rescue) ->
    Ledger0 = blockchain:ledger(Chain0),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger0),
    Chain1 = blockchain:ledger(Ledger1, Chain0),
    Height = blockchain_block:height(Block),

    Transactions0 = blockchain_block:transactions(Block),
    Transactions = lists:sort(fun sort/2, (Transactions0)),
    Start = erlang:monotonic_time(millisecond),
    case ?MODULE:validate(Transactions, Chain1, Rescue) of
        {_ValidTxns, []} ->
            End = erlang:monotonic_time(millisecond),
            AbsordDelayedRef = absorb_delayed_async(Block, Chain0),
            case ?MODULE:absorb_block(Block, Rescue, Chain1) of
                {ok, Chain2} ->
                    Ledger2 = blockchain:ledger(Chain2),
                    Hash = blockchain_block:hash_block(Block),
                    case BeforeCommit(Chain2, Hash) of
                        ok ->
                            ok = blockchain_ledger_v1:commit_context(Ledger2),
                            End2 = erlang:monotonic_time(millisecond),
                            ok = handle_absorb_delayed_result(AbsordDelayedRef),
                            case absorb_aux(Block, Chain0) of
                                ok -> ok;
                                Err ->
                                    lager:info("aux absorb failed with: ~p", [Err]),
                                    ok
                            end,
                            End3 = erlang:monotonic_time(millisecond),
                            lager:info("validation took ~p absorb took ~p post took ~p ms for block height ~p",
                                       [End - Start, End2 - End, End3 - End2, Height]),
                            ok;
                        Any ->
                            Any
                    end;
                Error ->
                    blockchain_ledger_v1:delete_context(Ledger1),
                    Error
            end;
        {_ValidTxns, InvalidTxns} ->
            blockchain_ledger_v1:delete_context(Ledger1),
            lager:error("found invalid transactions: ~p", [InvalidTxns]),
            {error, invalid_txns}
    end.

-spec unvalidated_absorb_and_commit(blockchain_block:block(), blockchain:blockchain(), before_commit_callback(), boolean()) ->
                               ok | {error, any()}.
unvalidated_absorb_and_commit(Block, Chain0, BeforeCommit, Rescue) ->
    Ledger0 = blockchain:ledger(Chain0),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger0),
    Chain1 = blockchain:ledger(Ledger1, Chain0),
    Height = blockchain_block:height(Block),

    Transactions0 = blockchain_block:transactions(Block),
    %% chain vars must always be validated so we don't accidentally sync past a change we don't understand
    Transactions =
         lists:filter(
           fun(T) -> Ty = ?MODULE:type(T),
                     Ty == blockchain_txn_vars_v1
           end, (Transactions0)),
    Start = erlang:monotonic_time(millisecond),
    case ?MODULE:validate(Transactions, Chain1, Rescue) of
        {_ValidTxns, []} ->
            End = erlang:monotonic_time(millisecond),
            AbsordDelayedRef = absorb_delayed_async(Block, Chain0),
            case ?MODULE:absorb_block(Block, Rescue, Chain1) of
                {ok, Chain2} ->
                    Ledger2 = blockchain:ledger(Chain2),
                    Hash = blockchain_block:hash_block(Block),
                    case BeforeCommit(Chain2, Hash) of
                        ok ->
                            ok = blockchain_ledger_v1:commit_context(Ledger2),
                            End2 = erlang:monotonic_time(millisecond),
                            ok = handle_absorb_delayed_result(AbsordDelayedRef),
                            absorb_aux(Block, Chain0),
                            End3 = erlang:monotonic_time(millisecond),
                            lager:info("validation took ~p absorb took ~p post took ~p ms height ~p",
                                       [End - Start, End2 - End, End3 - End2, Height]),
                            ok;
                        Any ->
                            Any
                    end;
                Error ->
                    blockchain_ledger_v1:delete_context(Ledger1),
                    Error
            end;
        {_ValidTxns, InvalidTxns} ->
            blockchain_ledger_v1:delete_context(Ledger1),
            lager:error("found invalid transactions: ~p", [InvalidTxns]),
            {error, invalid_txns}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb_block(blockchain_block:block(), blockchain:blockchain()) ->
                          {ok, blockchain:blockchain()} | {error, any()}.
absorb_block(Block, Chain) ->
    absorb_block(Block, false, Chain).

-spec absorb_block(blockchain_block:block(), boolean(), blockchain:blockchain()) ->
                          {ok, blockchain:blockchain()} | {error, any()}.
absorb_block(Block, Rescue, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Transactions0 = blockchain_block:transactions(Block),
    Transactions = lists:sort(fun sort/2, (Transactions0)),
    Height = blockchain_block:height(Block),
    case absorb_txns(Transactions, Rescue, Chain) of
        ok ->
            ok = blockchain_ledger_v1:increment_height(Block, Ledger),
            ok = blockchain_ledger_v1:process_delayed_actions(Height, Ledger, Chain),
            {ok, Chain};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn(),blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Type = ?MODULE:type(Txn),
    Start = erlang:monotonic_time(millisecond),

    try Type:absorb(Txn, Chain) of
        {error, _Reason}=Error ->
            lager:info("failed to absorb ~p ~p ~s",
                       [Type, _Reason, ?MODULE:print(Txn)]),
            Error;
        ok ->
            End = erlang:monotonic_time(millisecond),
            Slow = application:get_env(blockchain, slow_txn_log_threshold, 25), % in ms
            case (End - Start) >= Slow of
                true ->
                    lager:info("took ~p ms to absorb ~p", [End - Start, Type]),
                    ok;
                _ -> ok
            end
    catch
        What:Why:Stack ->
            lager:warning("crash during absorb: ~p ~p", [Why, Stack]),
            {error, {Type, What, {Why, Stack}}}
    end.

print(Txn) ->
    print(Txn, false).

print(Txn, Verbose) ->
    Type = ?MODULE:type(Txn),
    case erlang:function_exported(Type, print, 1) of
        true ->
            case erlang:function_exported(Type, print, 2) of
                true ->
                    Type:print(Txn, Verbose);
                false ->
                    Type:print(Txn)
            end;
        false ->
            io_lib:format("~p", [Txn])
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Type = ?MODULE:type(Txn),
    case lists:keysearch(Type, 1, ?ORDER) of
        {value, _} ->
            try Type:is_valid(Txn, Chain) of
                Res ->
                    Res
            catch
                What:Why:Stack ->
                    lager:warning("crash during validation: ~p ~p", [Why, Stack]),
                    {error, {Type, What, {Why, Stack}}}
            end;
        false ->
            lager:warning("txn not in sort order ~p : ~s", [Type, print(Txn)]),
            {error, missing_sort_order}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sort(txn(), txn()) -> boolean().
sort(TxnA, TxnB) ->
    {type_order(TxnA), actor(TxnA), nonce(TxnA)} =< {type_order(TxnB), actor(TxnB), nonce(TxnB)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec type(txn()) -> atom().
type(#blockchain_txn_assert_location_v1_pb{}) ->
    blockchain_txn_assert_location_v1;
type(#blockchain_txn_payment_v1_pb{}) ->
    blockchain_txn_payment_v1;
type(#blockchain_txn_security_exchange_v1_pb{}) ->
    blockchain_txn_security_exchange_v1;
type(#blockchain_txn_create_htlc_v1_pb{}) ->
    blockchain_txn_create_htlc_v1;
type(#blockchain_txn_redeem_htlc_v1_pb{}) ->
    blockchain_txn_redeem_htlc_v1;
type(#blockchain_txn_add_gateway_v1_pb{}) ->
    blockchain_txn_add_gateway_v1;
type(#blockchain_txn_coinbase_v1_pb{}) ->
    blockchain_txn_coinbase_v1;
type(#blockchain_txn_security_coinbase_v1_pb{}) ->
    blockchain_txn_security_coinbase_v1;
type(#blockchain_txn_consensus_group_v1_pb{}) ->
    blockchain_txn_consensus_group_v1;
type(#blockchain_txn_consensus_group_failure_v1_pb{}) ->
    blockchain_txn_consensus_group_failure_v1;
type(#blockchain_txn_poc_request_v1_pb{}) ->
    blockchain_txn_poc_request_v1;
type(#blockchain_txn_poc_receipts_v1_pb{}) ->
    blockchain_txn_poc_receipts_v1;
type(#blockchain_txn_gen_gateway_v1_pb{}) ->
    blockchain_txn_gen_gateway_v1;
type(#blockchain_txn_oui_v1_pb{}) ->
    blockchain_txn_oui_v1;
type(#blockchain_txn_routing_v1_pb{}) ->
    blockchain_txn_routing_v1;
type(#blockchain_txn_vars_v1_pb{}) ->
    blockchain_txn_vars_v1;
type(#blockchain_txn_rewards_v1_pb{}) ->
    blockchain_txn_rewards_v1;
type(#blockchain_txn_token_burn_v1_pb{}) ->
    blockchain_txn_token_burn_v1;
type(#blockchain_txn_dc_coinbase_v1_pb{}) ->
    blockchain_txn_dc_coinbase_v1;
type(#blockchain_txn_token_burn_exchange_rate_v1_pb{}) ->
    blockchain_txn_token_burn_exchange_rate_v1;
type(#blockchain_txn_bundle_v1_pb{}) ->
    blockchain_txn_bundle_v1;
type(#blockchain_txn_payment_v2_pb{}) ->
    blockchain_txn_payment_v2;
type(#blockchain_txn_state_channel_open_v1_pb{}) ->
    blockchain_txn_state_channel_open_v1;
type(#blockchain_txn_update_gateway_oui_v1_pb{}) ->
    blockchain_txn_update_gateway_oui_v1;
type(#blockchain_txn_state_channel_close_v1_pb{}) ->
    blockchain_txn_state_channel_close_v1;
type(#blockchain_txn_price_oracle_v1_pb{}) ->
    blockchain_txn_price_oracle_v1;
type(#blockchain_txn_gen_price_oracle_v1_pb{}) ->
    blockchain_txn_gen_price_oracle_v1;
type(#blockchain_txn_transfer_hotspot_v1_pb{}) ->
    blockchain_txn_transfer_hotspot_v1;
type(#blockchain_txn_rewards_v2_pb{}) ->
    blockchain_txn_rewards_v2;
type(#blockchain_txn_assert_location_v2_pb{}) ->
    blockchain_txn_assert_location_v2;
type(#blockchain_txn_gen_validator_v1_pb{}) ->
    blockchain_txn_gen_validator_v1;
type(#blockchain_txn_stake_validator_v1_pb{}) ->
     blockchain_txn_stake_validator_v1;
type(#blockchain_txn_unstake_validator_v1_pb{}) ->
    blockchain_txn_unstake_validator_v1;
type(#blockchain_txn_transfer_validator_stake_v1_pb{}) ->
    blockchain_txn_transfer_validator_stake_v1;
type(#blockchain_txn_validator_heartbeat_v1_pb{}) ->
    blockchain_txn_validator_heartbeat_v1;
type(#blockchain_txn_transfer_hotspot_v2_pb{}) ->
    blockchain_txn_transfer_hotspot_v2.

-spec validate_fields([{{atom(), iodata() | undefined},
                        {binary, pos_integer()} |
                        {binary, pos_integer(), pos_integer()} |
                        {is_integer, non_neg_integer()} |
                        {member, list()} |
                        {address, libp2p}}]) -> ok | {error, any()}.
validate_fields([]) ->
    ok;
validate_fields([{{Name, Field}, {binary, Length}}|Tail]) when is_binary(Field) ->
    case byte_size(Field) == Length of
        true ->
            validate_fields(Tail);
        false ->
            {error, {field_wrong_size, {Name, Length, byte_size(Field)}}}
    end;
validate_fields([{{Name, Field}, {binary, Min, Max}}|Tail]) when is_binary(Field) ->
    case byte_size(Field) =< Max andalso byte_size(Field) >= Min of
        true ->
            validate_fields(Tail);
        false ->
            {error, {field_wrong_size, {Name, {Min, Max}, byte_size(Field)}}}
    end;
validate_fields([{{Name, Field}, {address, libp2p}}|Tail]) when is_binary(Field) ->
    try libp2p_crypto:bin_to_pubkey(Field) of
        _ ->
            validate_fields(Tail)
    catch
        _:_ ->
            {error, {invalid_address, Name}}
    end;
validate_fields([{{Name, Field}, {member, List}}|Tail]) when is_list(List),
                                                            is_binary(Field) ->
    case lists:member(Field, List) of
        true ->
            validate_fields(Tail);
        false ->
            {error, {not_a_member, {Name, Field, List}}}
    end;
validate_fields([{{_Name, Field}, {is_integer, Min}}|Tail]) when is_integer(Field)
                                                         andalso Field >= Min ->
    validate_fields(Tail);
validate_fields([{{Name, Field}, {is_integer, Min}}|_Tail]) when is_integer(Field)
                                                         andalso Field < Min ->
    {error, {integer_too_small, {Name, Field, Min}}};
validate_fields([{{Name, Field}, {is_integer, _Min}}|_Tail]) ->
    {error, {not_an_integer, {Name, Field}}};
validate_fields([{{Name, undefined}, _}|_Tail]) ->
    {error, {missing_field, Name}};
validate_fields([{{Name, _Field}, _Validation}|_Tail]) ->
    {error, {malformed_field, Name}}.
%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec type_order(txn()) -> non_neg_integer().
type_order(Txn) ->
    Type = type(Txn),
    case lists:keyfind(Type, 1, ?ORDER) of
        {Type, Index} -> Index;
        %% don't implicitly order unknown transactions
        false -> error(unknown_transaction)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb_txns(txns(), boolean(), blockchain:blockchain()) ->
                         ok | {error, any()}.
absorb_txns([], _Rescue, _Chain) ->
    ok;
absorb_txns([Txn|Txns], Rescue, Chain) ->
    Type = ?MODULE:type(Txn),
    code:ensure_loaded(Type),
    case Rescue andalso
        erlang:function_exported(Type, rescue_absorb, 2) of
        true ->
            case Type:rescue_absorb(Txn, Chain) of
                ok -> absorb_txns(Txns, Rescue, Chain);
                {error, _} = E -> E
            end;
        false ->
            case ?MODULE:absorb(Txn, Chain) of
                {error, _Reason}=Error -> Error;
                ok -> absorb_txns(Txns, Rescue, Chain)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb_delayed(blockchain_block:block(), blockchain:blockchain()) -> ok | {error, any()}.
absorb_delayed(Block0, Chain0) ->
    {Pid, Ref, MonitorRef} = absorb_delayed_async(Block0, Chain0),
    handle_absorb_delayed_result({Pid, Ref, MonitorRef}).

handle_absorb_delayed_result({Pid, Ref, MonitorRef}) ->
    receive
        {'ETS-TRANSFER', _, _, _} ->
            %% we don't need this message, just discard it
            handle_absorb_delayed_result({Pid, Ref, MonitorRef});
        {Ref, ok} ->
            %% nothing to absorb
            erlang:demonitor(MonitorRef, [flush]),
            ok;
        {Ref, {ok, Ledger}} ->
            erlang:demonitor(MonitorRef, [flush]),
            blockchain_ledger_v1:commit_context(Ledger),
            ok;
        {Ref, Other} ->
            erlang:demonitor(MonitorRef, [flush]),
            Other;
        {'DOWN', MonitorRef, process, Pid, Reason} when Reason /= normal ->
            {error, Reason}
    end.

absorb_delayed_async(Block0, Chain0) ->
    Ref = make_ref(),
    Parent = self(),
    {Pid, MonitorRef} =
    spawn_monitor(fun() ->
                          Ledger0 = blockchain:ledger(Chain0),
                          DelayedLedger0 = blockchain_ledger_v1:mode(delayed, Ledger0),
                          DelayedLedger1 = blockchain_ledger_v1:new_context(DelayedLedger0),
                          Chain1 = blockchain:ledger(DelayedLedger1, Chain0),
                          case blockchain_ledger_v1:current_height(Ledger0) of
                              % This is so it absorbs genesis
                              {ok, H} when H < 2 ->
                                  plain_absorb_(Block0, Chain1),
                                  blockchain_ledger_v1:give_context(DelayedLedger1, Parent),
                                  Parent ! {Ref, {ok, DelayedLedger1}};
                              {ok, CurrentHeight} ->
                                  {ok, DelayedHeight} = blockchain_ledger_v1:current_height(DelayedLedger1),
                                  % Then we absorb if minimum limit is there
                                  case CurrentHeight - DelayedHeight > ?BLOCK_DELAY of
                                      false ->
                                          Parent ! {Ref, ok};
                                      true ->
                                          %% bound the number of blocks we do at a time
                                          Lag = min(?BLOCK_DELAY, CurrentHeight - DelayedHeight - ?BLOCK_DELAY),
                                          Res = lists:foldl(fun(H, ok) ->
                                                                    {ok, Block1} = blockchain:get_block(H, Chain0),
                                                                    plain_absorb_(Block1, Chain1);
                                                               (_, Acc) ->
                                                                    Acc
                                                            end,
                                                            ok,
                                                            lists:seq(DelayedHeight+1, DelayedHeight + Lag)),
                                          case Res of
                                              ok ->
                                                  blockchain_ledger_v1:give_context(DelayedLedger1, Parent),
                                                  Parent ! {Ref, {ok, DelayedLedger1}};
                                              Error ->
                                                  Parent ! {Ref, Error}
                                          end
                                  end;
                              Any ->
                                  Parent ! {Ref, Any}
                          end
                  end),
    {Pid, Ref, MonitorRef}.


-spec absorb_aux(blockchain_block:block(), blockchain:blockchain()) -> ok | {error, any()}.
absorb_aux(Block0, Chain0) ->
    Ledger0 = blockchain:ledger(Chain0),
    case blockchain_ledger_v1:has_aux(Ledger0) of
        true ->
            AuxLedger0 = blockchain_ledger_v1:mode(aux, Ledger0),
            AuxLedger1 = blockchain_ledger_v1:new_context(AuxLedger0),
            Chain1 = blockchain:ledger(AuxLedger1, Chain0),
            case blockchain_ledger_v1:current_height(Ledger0) of
                % This is so it absorbs genesis
                {ok, H} when H < 2 ->
                    plain_absorb_(Block0, Chain1),
                    ok = blockchain_ledger_v1:commit_context(AuxLedger1);
                {ok, CurrentHeight} ->
                    {ok, AuxHeight} = blockchain_ledger_v1:current_height(AuxLedger1),
                    %% don't do too many blocks at once do we don't get timeout killed
                    End = min(AuxHeight + 100, CurrentHeight - 1),
                    Res = lists:foldl(fun(H, ok) ->
                                              {ok, Block1} = blockchain:get_block(H, Chain0),
                                              plain_absorb_(Block1, Chain1);
                                         (_, Acc) ->
                                              Acc
                                      end,
                                      ok,
                                      lists:seq(AuxHeight+1, End)),
                    case Res of
                        ok ->
                            ok = blockchain_ledger_v1:commit_context(AuxLedger1);
                        Error ->
                            lager:info("AUX absorb failed ~p", [Error]),
                            Error
                    end;
                _Any ->
                    _Any
            end;
        false ->
            ok
    end.

plain_absorb_(Block, Chain0) ->
    case ?MODULE:absorb_block(Block, Chain0) of
        {ok, _} ->
            %% Hash = blockchain_block:hash_block(Block),
            Ledger0 = blockchain:ledger(Chain0),
            ok = blockchain_ledger_v1:maybe_gc_pocs(Chain0, Ledger0),
            ok = blockchain_ledger_v1:maybe_gc_scs(Chain0, Ledger0),
            ok = blockchain_ledger_v1:maybe_gc_h3dex(Ledger0),
            %% ok = blockchain_ledger_v1:refresh_gateway_witnesses(Hash, Ledger0),
            ok = blockchain_ledger_v1:maybe_recalc_price(Chain0, Ledger0),
            ok;
        Error ->
            Ledger = blockchain:ledger(Chain0),
            blockchain_ledger_v1:delete_context(Ledger),
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(txn()) -> integer().
nonce(Txn) ->
    case ?MODULE:type(Txn) of
        blockchain_txn_assert_location_v1 ->
            blockchain_txn_assert_location_v1:nonce(Txn);
        blockchain_txn_payment_v1 ->
            blockchain_txn_payment_v1:nonce(Txn);
        blockchain_txn_security_exchange_v1 ->
            blockchain_txn_security_exchange_v1:nonce(Txn);
        blockchain_txn_token_burn_v1 ->
            blockchain_txn_token_burn_v1:nonce(Txn);
        blockchain_txn_payment_v2 ->
            blockchain_txn_payment_v2:nonce(Txn);
        blockchain_txn_state_channel_open_v1 ->
            blockchain_txn_state_channel_open_v1:nonce(Txn);
        blockchain_txn_state_channel_close_v1 ->
            SC = blockchain_txn_state_channel_close_v1:state_channel(Txn),
            case blockchain_state_channel_v1:amount(SC) > 0 of
                true ->
                    %% we want higher nonces to sort first in this case
                    1 - blockchain_state_channel_v1:nonce(blockchain_txn_state_channel_close_v1:state_channel(Txn));
                false ->
                    -1
            end;
        blockchain_txn_routing_v1 ->
            blockchain_txn_routing_v1:nonce(Txn);
        blockchain_txn_assert_location_v2 ->
            blockchain_txn_assert_location_v2:nonce(Txn);
        _ ->
            -1 %% other transactions sort first
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec actor(txn()) -> libp2p_crypto:pubkey_bin() | <<>>.
actor(Txn) ->
    case ?MODULE:type(Txn) of
        blockchain_txn_assert_location_v1 ->
            blockchain_txn_assert_location_v1:gateway(Txn);
        blockchain_txn_payment_v1 ->
            blockchain_txn_payment_v1:payer(Txn);
        blockchain_txn_security_exchange_v1 ->
            blockchain_txn_security_exchange_v1:payer(Txn);
        blockchain_txn_create_htlc_v1 ->
            blockchain_txn_create_htlc_v1:payer(Txn);
        blockchain_txn_redeem_htlc_v1 ->
            blockchain_txn_redeem_htlc_v1:payee(Txn);
        blockchain_txn_poc_request_v1 ->
            blockchain_txn_poc_request_v1:challenger(Txn);
        blockchain_txn_add_gateway_v1 ->
            blockchain_txn_add_gateway_v1:owner(Txn);
        blockchain_txn_coinbase_v1 ->
            blockchain_txn_coinbase_v1:payee(Txn);
        blockchain_txn_security_coinbase_v1 ->
            blockchain_txn_security_coinbase_v1:payee(Txn);
        blockchain_txn_poc_receipts_v1 ->
            blockchain_txn_poc_receipts_v1:challenger(Txn);
        blockchain_txn_oui_v1 ->
            blockchain_txn_oui_v1:owner(Txn);
        blockchain_txn_routing_v1 ->
            blockchain_txn_routing_v1:owner(Txn);
        blockchain_txn_token_burn_v1 ->
            blockchain_txn_token_burn_v1:payer(Txn);
        blockchain_txn_dc_coinbase_v1 ->
            blockchain_txn_dc_coinbase_v1:payee(Txn);
        blockchain_txn_payment_v2 ->
            blockchain_txn_payment_v2:payer(Txn);
        blockchain_txn_state_channel_open_v1 ->
            blockchain_txn_state_channel_open_v1:owner(Txn);
        blockchain_txn_state_channel_close_v1 ->
            %% group by owner
            blockchain_txn_state_channel_close_v1:state_channel_owner(Txn);
        blockchain_txn_assert_location_v2 ->
            blockchain_txn_assert_location_v2:gateway(Txn);
        _ ->
            <<>>
    end.


-spec depends_on(txn(), [blockchain_txn_mgr:cached_txn_type()]) -> [blockchain_txn_mgr:cached_txn_type()].
depends_on(ThisTxn, CachedTxns) ->
    case type(ThisTxn) of
        Type when Type == blockchain_txn_payment_v1;
                  Type == blockchain_txn_create_htlc_v1;
                  Type == blockchain_txn_token_burn_v1;
                  Type == blockchain_txn_payment_v2 ->
            Actor = actor(ThisTxn),
            Nonce = nonce(ThisTxn),
            Types = [blockchain_txn_payment_v1, blockchain_txn_create_htlc_v1, blockchain_txn_token_burn_v1, blockchain_txn_payment_v2],
            lists:filter(fun({_TxnKey, Txn, _TxnData}) ->
                                 ThisNonce = nonce(Txn),
                                 lists:member(type(Txn), Types) andalso actor(Txn) == Actor andalso ThisNonce < Nonce
                         end, CachedTxns);
        blockchain_txn_assert_location_v1 ->
            Actor = actor(ThisTxn),
            Nonce = nonce(ThisTxn),
            lists:filter(fun({_TxnKey, Txn, _TxnData}) ->
                                 (type(Txn) == blockchain_txn_assert_location_v1 andalso actor(Txn) == Actor andalso nonce(Txn) < Nonce) orelse
                                 (type(Txn) == blockchain_txn_add_gateway_v1 andalso blockchain_txn_add_gateway_v1:gateway(Txn) == Actor)
                         end, CachedTxns);
        blockchain_txn_security_exchange_v1 ->
            Actor = actor(ThisTxn),
            Nonce = nonce(ThisTxn),
            lists:filter(fun({_TxnKey, Txn, _TxnData}) ->
                                 ThisNonce = nonce(Txn),
                                 type(Txn) == blockchain_txn_security_exchange_v1 andalso actor(Txn) == Actor andalso ThisNonce < Nonce
                         end, CachedTxns);
        blockchain_txn_vars_v1 ->
            Nonce = nonce(ThisTxn),
            lists:filter(fun({_TxnKey, Txn, _TxnData}) ->
                                 type(Txn) == blockchain_txn_vars_v1 andalso blockchain_txn_vars_v1:nonce(Txn) < Nonce
                         end, CachedTxns);
        blockchain_txn_state_channel_open_v1 ->
            Actor = actor(ThisTxn),
            Nonce = nonce(ThisTxn),
            OUI = blockchain_txn_state_channel_open_v1:oui(ThisTxn),
            lists:filter(fun({_TxnKey, Txn, _TxnData}) ->
                                 (type(Txn) == blockchain_txn_oui_v1 andalso lists:member(Actor, blockchain_txn_oui_v1:addresses(Txn))) orelse
                                 (type(Txn) == blockchain_txn_state_channel_open_v1 andalso actor(Txn) == Actor andalso nonce(Txn) < Nonce) orelse
                                 (type(Txn) == blockchain_txn_routing_v1 andalso blockchain_txn_routing_v1:oui(Txn) == OUI) orelse
                                 (type(Txn) == blockchain_txn_token_burn_v1 andalso blockchain_txn_token_burn_v1:payee(Txn) == Actor)
                         end, CachedTxns);
        blockchain_txn_state_channel_close_v1 ->
            Actor = actor(ThisTxn),
            SC = blockchain_txn_state_channel_close_v1:state_channel(ThisTxn),
            SCCloseID = blockchain_state_channel_v1:id(SC),
            lists:filter(fun({_TxnKey, Txn, _TxnData}) ->
                                 type(Txn) == blockchain_txn_state_channel_open_v1 andalso
                                 blockchain_txn_state_channel_open_v1:owner(Txn) == Actor andalso
                                 blockchain_txn_state_channel_open_v1:id(Txn) == SCCloseID
                         end,
                         CachedTxns);
        blockchain_txn_routing_v1 ->
            Actor = actor(ThisTxn),
            Nonce = nonce(ThisTxn),
            lists:filter(fun({_TxnKey, Txn, _TxnData}) ->
                                 (type(Txn) == blockchain_txn_oui_v1 andalso blockchain_txn_oui_v1:owner(Txn) == Actor) orelse
                                 (type(Txn) == blockchain_txn_routing_v1 andalso actor(Txn) == Actor andalso nonce(Txn) < Nonce)
                         end,
                         CachedTxns);
        blockchain_txn_assert_location_v2 ->
            Actor = actor(ThisTxn),
            Nonce = nonce(ThisTxn),
            lists:filter(fun({_TxnKey, Txn, _TxnData}) ->
                                 (type(Txn) == blockchain_txn_assert_location_v2 andalso actor(Txn) == Actor andalso nonce(Txn) < Nonce) orelse
                                 (type(Txn) == blockchain_txn_add_gateway_v1 andalso blockchain_txn_add_gateway_v1:gateway(Txn) == Actor)
                         end, CachedTxns);
        %% TODO: token exchange rate txn when it's time
        _ ->
            []
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

%% txn fee data
-define(DC_PAYLOAD_SIZE, 24).
-define(TXN_MULTIPLIER, 5000).
-define(USD_TO_DC, 100000).

%% staking fees in DC data ( should be using same values as defined in chain vars )
-define(OUI_STAKING_FEE, 100 * ?USD_TO_DC).
-define(ADD_GW_STAKING_FEE, 40 * ?USD_TO_DC).
-define(ASSERT_LOC_STAKING_FEE, 10 * ?USD_TO_DC).
-define(OUI_PER_ADDRESS_STAKING_FEE, 100 * ?USD_TO_DC).

%% the various router vars below do not have equiv chain vars, added sep here to differentiate
%% request subnet ( which does have a staking fee ) from the rest
-define(ROUTER_UPDATE_ROUTER_STAKING_FEE, 0).
-define(ROUTER_NEW_XOR_STAKING_FEE, 0).
-define(ROUTER_UPDATE_XOR_STAKING_FEE, 0).
-define(ROUTER_REQUEST_SUBNET_STAKING_FEE, ?OUI_PER_ADDRESS_STAKING_FEE).

%% misc
-define(TEST_LOCATION, 631210968840687103).
-define(ADDRESS_KEY1, <<0,105,110,41,229,175,44,3,221,73,181,25,27,184,120,84,
               138,51,136,194,72,161,94,225,240,73,70,45,135,23,41,96,78>>).
-define(ADDRESS_KEY2, <<1,72,253,248,131,224,194,165,164,79,5,144,254,1,168,254,
                111,243,225,61,41,178,207,35,23,54,166,116,128,38,164,87,212>>).
-define(ADDRESS_KEY3, <<1,124,37,189,223,186,125,185,240,228,150,61,9,164,28,75,
                44,232,76,6,121,96,24,24,249,85,177,48,246,236,14,49,80>>).
-define(ADDRESS_KEY4, <<0,201,24,252,94,154,8,151,21,177,201,93,234,97,223,234,
                109,216,141,189,126,227,92,243,87,8,134,107,91,11,221,179,190>>).


depends_on_test() ->
    #{secret := PrivKey1, public := PubKey1} = libp2p_crypto:generate_keys(ecc_compact),
    Payer = libp2p_crypto:pubkey_to_bin(PubKey1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey1),

    #{secret := _PrivKey2, public := PubKey2} = libp2p_crypto:generate_keys(ecc_compact),
    Recipient = libp2p_crypto:pubkey_to_bin(PubKey2),
    Txns = lists:map(fun(Nonce) ->
                      case rand:uniform(2) of
                          1 ->
                              %% need to create a payload same as blockchain_txn_mgr:cached_txn() to pass to depends_on
                              {Nonce, blockchain_txn_payment_v1:sign(blockchain_txn_payment_v1:new(Payer, Recipient, 1, Nonce), SigFun), []};
                          2 ->
                              %% need to create a payload same as blockchain_txn_mgr:cached_txn() to pass to depends_on
                              {Nonce, blockchain_txn_payment_v2:sign(blockchain_txn_payment_v2:new(Payer, [blockchain_payment_v2:new(Recipient, 1)], Nonce), SigFun), []}
                      end
              end, lists:seq(1, 50)),

    ok = nonce_check(Txns),

    ok.

sc_depends_on_test() ->
    [{Payer, SigFun}] = gen_payers(1),
    [{Payer1, SigFun1}] = gen_payers(1),

    {Filter, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),
    {Filter1, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),

    %% oui for payer
    O0 = blockchain_txn_oui_v1:sign(blockchain_txn_oui_v1:new(1, Payer, [Payer], Filter, 8), SigFun),
    O0TxnData = {1, O0, []}, %% emulate blockchain_txn_mgr:cached_txn()
    %% oui for payer1
    O1 = blockchain_txn_oui_v1:sign(blockchain_txn_oui_v1:new(2, Payer1, [Payer1], Filter1, 8), SigFun1),
    O1TxnData = {2, O1, []}, %% emulate blockchain_txn_mgr:cached_txn()

    %% routing for payer
    RT1 = blockchain_txn_routing_v1:sign(blockchain_txn_routing_v1:update_router_addresses(1, Payer, gen_pubkeys(3), 1), SigFun),
    RT1TxnData = {3, RT1, []}, %% emulate blockchain_txn_mgr:cached_txn()
    %% routing for payer1
    RT2 = blockchain_txn_routing_v1:sign(blockchain_txn_routing_v1:update_router_addresses(2, Payer1, gen_pubkeys(3), 1), SigFun),
    RT2TxnData = {4, RT2, []}, %% emulate blockchain_txn_mgr:cached_txn()

    %% sc opens for payer
    SC1 = blockchain_txn_state_channel_open_v1:sign(blockchain_txn_state_channel_open_v1:new(crypto:strong_rand_bytes(24), Payer, 10, 1, 1, 0), SigFun),
    SC1TxnData = {5, SC1, []}, %% emulate blockchain_txn_mgr:cached_txn()
    SC2 = blockchain_txn_state_channel_open_v1:sign(blockchain_txn_state_channel_open_v1:new(crypto:strong_rand_bytes(24), Payer, 20, 1, 2, 0), SigFun),
    SC2TxnData = {6, SC2, []}, %% emulate blockchain_txn_mgr:cached_txn()
    SC3 = blockchain_txn_state_channel_open_v1:sign(blockchain_txn_state_channel_open_v1:new(crypto:strong_rand_bytes(24), Payer, 30, 1, 3, 0), SigFun),
    SC3TxnData = {7, SC3, []}, %% emulate blockchain_txn_mgr:cached_txn()

    %% sc open for payer1
    SC4 = blockchain_txn_state_channel_open_v1:sign(blockchain_txn_state_channel_open_v1:new(crypto:strong_rand_bytes(24), Payer1, 30, 2, 1, 0), SigFun),

    ?assertEqual(lists:sort([O0TxnData, RT1TxnData, SC1TxnData, SC2TxnData]), lists:sort(depends_on(SC3, blockchain_utils:shuffle([O0TxnData, RT1TxnData, SC1TxnData, SC2TxnData])))),
    ?assertEqual(lists:sort([O0TxnData, RT1TxnData, SC1TxnData, SC2TxnData]), lists:sort(depends_on(SC3, blockchain_utils:shuffle([O0TxnData, O1TxnData, RT1TxnData, SC1TxnData, SC2TxnData])))),
    ?assertEqual(lists:sort([O0TxnData, RT1TxnData, SC1TxnData]), lists:sort(depends_on(SC2, blockchain_utils:shuffle([O0TxnData, O1TxnData, RT1TxnData, SC1TxnData, SC2TxnData, SC3TxnData])))),
    ?assertEqual(lists:sort([O1TxnData, RT2TxnData]), lists:sort(depends_on(SC4, blockchain_utils:shuffle([O0TxnData, O1TxnData, RT1TxnData, RT2TxnData, SC1TxnData, SC2TxnData, SC3TxnData])))),

    ok.

nonce_check(Txns) ->
    lists:foreach(fun(_) ->
                      Shuffled = blockchain_utils:shuffle(Txns),
                      {_TxnKey, Txn, _TxnData} = lists:last(Shuffled),
                      Dependencies = depends_on(Txn, Shuffled),
                      io:format("~nTxn: ~p~nDependencies: ~p~n", [Txn, Dependencies]),
                      ?assert(lists:all(fun({_TKey, T, _TData}) -> nonce(T) < nonce(Txn) end, Dependencies))
              end, lists:seq(1, 10)).

gen_pubkeys(Count) ->
    lists:foldl(fun(_, Acc) ->
                        #{secret := _, public := PubKey} = libp2p_crypto:generate_keys(ecc_compact),
                        Addr = libp2p_crypto:pubkey_to_bin(PubKey),
                        [Addr | Acc]
                end, [], lists:seq(1, Count)).

gen_payers(Count) ->
    lists:foldl(fun(_, Acc) ->
                        #{secret := PrivKey, public := PubKey} = libp2p_crypto:generate_keys(ecc_compact),
                        PubkeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
                        SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
                        [{PubkeyBin, SigFun} | Acc]
                end, [], lists:seq(1, Count)).


txn_fees_payment_v1_test() ->
    [{Payer, PayerSigFun}] = gen_payers(1),
    [RandomPubKey1] = gen_pubkeys(1),

    %% create a new payment txn, and confirm expected fee size
    Txn00 = blockchain_txn_payment_v1:new(Payer, RandomPubKey1, 100000, 1),
    Txn00Fee = blockchain_txn_payment_v1:calculate_fee(Txn00, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(35000, Txn00Fee),
    %% set the fee value of the txn, sign it and confirm the fee remains the same and unaffected by signature of fee values
    Txn01 = blockchain_txn_payment_v1:fee(Txn00, Txn00Fee),
    Txn02 = blockchain_txn_payment_v1:sign(Txn01, PayerSigFun),
    Txn02Fee = blockchain_txn_payment_v1:calculate_fee(Txn02, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(35000, Txn02Fee),
    ok.

txn_fees_payment_v2_test() ->
    [{Payer, PayerSigFun}] = gen_payers(1),
    [RandomPubKey1] = gen_pubkeys(1),
    V2Payment = blockchain_payment_v2:new(RandomPubKey1, 2500),

    %% create a new payment txn, and confirm expected fee size
    Txn00 = blockchain_txn_payment_v2:new(Payer, [V2Payment], 1),
    Txn00Fee = blockchain_txn_payment_v2:calculate_fee(Txn00, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(35000, Txn00Fee),
    %% set the fee value of the txn, sign it and confirm the fee remains the same and unaffected by signature of fee values
    Txn01 = blockchain_txn_payment_v2:fee(Txn00, Txn00Fee),
    Txn02 = blockchain_txn_payment_v2:sign(Txn01, PayerSigFun),
    Txn02Fee = blockchain_txn_payment_v2:calculate_fee(Txn02, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(35000, Txn02Fee),
    ok.

txn_fees_add_gateway_v1_test() ->
    [{Payer, PayerSigFun}] = gen_payers(1),
    #{public := GWPubKey, secret := GWPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    GWPubkeyBin = libp2p_crypto:pubkey_to_bin(GWPubKey),
    GWSigFun = libp2p_crypto:mk_sig_fun(GWPrivKey),
    #{public := OwnerPubKey, secret := OwnerPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    OwnerPubkeyBin = libp2p_crypto:pubkey_to_bin(OwnerPubKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),

    %% create new txn, and confirm expected txn and staking fee
    Txn00 = blockchain_txn_add_gateway_v1:new(GWPubkeyBin, OwnerPubkeyBin, Payer),
    Txn00Fee = blockchain_txn_add_gateway_v1:calculate_fee(Txn00, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    Txn00StakingFee = blockchain_txn_add_gateway_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ADD_GW_STAKING_FEE, [], true),
    Txn00LegacyStakingFee = blockchain_txn_add_gateway_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ADD_GW_STAKING_FEE, [], false),
    ?assertEqual(65000, Txn00Fee),
    ?assertEqual(4000000, Txn00StakingFee),
    ?assertEqual(1, Txn00LegacyStakingFee),

    %% set the fee values of the txn, sign it and confirm the fees remains the same and unaffected by signatures
    Txn01 = blockchain_txn_add_gateway_v1:fee(Txn00, Txn00Fee),
    Txn02 = blockchain_txn_add_gateway_v1:staking_fee(Txn01, Txn00StakingFee),
    Txn03 = blockchain_txn_add_gateway_v1:sign_request(Txn02, GWSigFun),
    Txn04 = blockchain_txn_add_gateway_v1:sign(Txn03, OwnerSigFun),
    Txn05 = blockchain_txn_add_gateway_v1:sign_payer(Txn04, PayerSigFun),
    Txn05Fee = blockchain_txn_add_gateway_v1:calculate_fee(Txn05, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    Txn05StakingFee = blockchain_txn_add_gateway_v1:calculate_staking_fee(Txn05, ignore_ledger, ?ADD_GW_STAKING_FEE, [], true),
    Txn05LegacyStakingFee = blockchain_txn_add_gateway_v1:calculate_staking_fee(Txn05, ignore_ledger, ?ADD_GW_STAKING_FEE, [], false),
    ?assertEqual(65000, Txn05Fee),
    ?assertEqual(4000000, Txn05StakingFee),
    ?assertEqual(1, Txn05LegacyStakingFee),
    ok.

txn_fees_assert_location_v1_test() ->
    [{Payer, PayerSigFun}] = gen_payers(1),
    #{public := GWPubKey, secret := GWPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    GWPubkeyBin = libp2p_crypto:pubkey_to_bin(GWPubKey),
    GWSigFun = libp2p_crypto:mk_sig_fun(GWPrivKey),
    #{public := OwnerPubKey, secret := OwnerPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    OwnerPubkeyBin = libp2p_crypto:pubkey_to_bin(OwnerPubKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),

    %% create new txn, and confirm expected txn and staking fee
    Txn00 = blockchain_txn_assert_location_v1:new(GWPubkeyBin, OwnerPubkeyBin, Payer, ?TEST_LOCATION, 1),
    Txn00Fee = blockchain_txn_assert_location_v1:calculate_fee(Txn00, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    Txn00StakingFee = blockchain_txn_assert_location_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ASSERT_LOC_STAKING_FEE, [], true),
    Txn00LegacyStakingFee = blockchain_txn_assert_location_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ASSERT_LOC_STAKING_FEE, [], false),
    ?assertEqual(70000, Txn00Fee),
    ?assertEqual(1000000, Txn00StakingFee),
    ?assertEqual(1, Txn00LegacyStakingFee),

    %% set the fee values of the txn, sign it and confirm the fees remains the same and unaffected by signatures
    Txn01 = blockchain_txn_assert_location_v1:fee(Txn00, Txn00Fee),
    Txn02 = blockchain_txn_assert_location_v1:staking_fee(Txn01, Txn00StakingFee),
    Txn03 = blockchain_txn_assert_location_v1:sign_request(Txn02, GWSigFun),
    Txn04 = blockchain_txn_assert_location_v1:sign(Txn03, OwnerSigFun),
    Txn05 = blockchain_txn_assert_location_v1:sign_payer(Txn04, PayerSigFun),
    Txn05Fee = blockchain_txn_assert_location_v1:calculate_fee(Txn05, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    Txn05StakingFee = blockchain_txn_assert_location_v1:calculate_staking_fee(Txn05, ignore_ledger, ?ASSERT_LOC_STAKING_FEE, [], true),
    Txn05LegacyStakingFee = blockchain_txn_assert_location_v1:calculate_staking_fee(Txn05, ignore_ledger, ?ASSERT_LOC_STAKING_FEE, [], false),
    ?assertEqual(70000, Txn05Fee),
    ?assertEqual(1000000, Txn05StakingFee),
    ?assertEqual(1, Txn05LegacyStakingFee),
    ok.

txn_fees_create_htlc_v1_test() ->
    [{Payer, PayerSigFun}] = gen_payers(1),
    [RandomPubKey1, RandomPubKey2] = gen_pubkeys(2),
    Hashlock = <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>,

    %% create new txn, and confirm expected fee size
    Txn00 = blockchain_txn_create_htlc_v1:new(Payer, RandomPubKey1, RandomPubKey2, Hashlock, 0, 10000, 1),
    Txn00Fee = blockchain_txn_create_htlc_v1:calculate_fee(Txn00, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(55000, Txn00Fee),
    %% set the fee value of the txn, sign it and confirm the fee remains the same and unaffected by signature of fee values
    Txn01 = blockchain_txn_create_htlc_v1:fee(Txn00, Txn00Fee),
    Txn02 = blockchain_txn_create_htlc_v1:sign(Txn01, PayerSigFun),
    Txn02Fee = blockchain_txn_create_htlc_v1:calculate_fee(Txn02, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(55000, Txn02Fee),
    ok.

txn_fees_redeem_htlc_v1_test() ->
    [{Payee, PayeeSigFun}] = gen_payers(1),
    [RandomPubKey1] = gen_pubkeys(1),

    %% create new txn, and confirm expected fee size
    Txn00 = blockchain_txn_redeem_htlc_v1:new(Payee, RandomPubKey1, <<"yolo">>),
    Txn00Fee = blockchain_txn_redeem_htlc_v1:calculate_fee(Txn00, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(35000, Txn00Fee),
    %% set the fee value of the txn, sign it and confirm the fee remains the same and unaffected by signature of fee values
    Txn01 = blockchain_txn_redeem_htlc_v1:fee(Txn00, Txn00Fee),
    Txn02 = blockchain_txn_redeem_htlc_v1:sign(Txn01, PayeeSigFun),
    Txn02Fee = blockchain_txn_redeem_htlc_v1:calculate_fee(Txn02, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(35000, Txn02Fee),
    ok.

txn_fees_oui_test() ->
    OUI = 1,
    [{Payer, PayerSigFun}] = gen_payers(1),
    #{public := OwnerPubKey, secret := OwnerPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    OwnerPubkeyBin = libp2p_crypto:pubkey_to_bin(OwnerPubKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),
    {Filter, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),

    %% create new txn, and confirm expected fee size
    Txn00 = blockchain_txn_oui_v1:new(OUI, OwnerPubkeyBin, [?ADDRESS_KEY1], Filter, 32, Payer),
    Txn00Fee = blockchain_txn_oui_v1:calculate_fee(Txn00, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    Txn00StakingFee = blockchain_txn_oui_v1:calculate_staking_fee(Txn00, ignore_ledger, ?OUI_STAKING_FEE, [{per_address, ?OUI_PER_ADDRESS_STAKING_FEE}], true),
    Txn00LegacyStakingFee = blockchain_txn_oui_v1:calculate_staking_fee(Txn00, ignore_ledger, ?OUI_STAKING_FEE, [{per_address, ?OUI_PER_ADDRESS_STAKING_FEE}], false),
    ?assertEqual(70000, Txn00Fee),
    ?assertEqual(330000000, Txn00StakingFee),
    ?assertEqual(1, Txn00LegacyStakingFee),

    %% set the fee values of the txn, sign it and confirm the fees remains the same and unaffected by signatures
    Txn01 = blockchain_txn_oui_v1:fee(Txn00, Txn00Fee),
    Txn02 = blockchain_txn_oui_v1:staking_fee(Txn01, Txn00StakingFee),
    Txn03 = blockchain_txn_oui_v1:sign(Txn02, OwnerSigFun),
    Txn04 = blockchain_txn_oui_v1:sign_payer(Txn03, PayerSigFun),
    Txn04Fee = blockchain_txn_oui_v1:calculate_fee(Txn04, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    Txn04StakingFee = blockchain_txn_oui_v1:calculate_staking_fee(Txn04, ignore_ledger, ?OUI_STAKING_FEE, [{per_address, ?OUI_PER_ADDRESS_STAKING_FEE}], true),
    Txn04LegacyStakingFee = blockchain_txn_oui_v1:calculate_staking_fee(Txn04, ignore_ledger, ?OUI_STAKING_FEE, [{per_address, ?OUI_PER_ADDRESS_STAKING_FEE}], false),
    ?assertEqual(70000, Txn04Fee),
    ?assertEqual(330000000, Txn04StakingFee),
    ?assertEqual(1, Txn04LegacyStakingFee),
    ok.

txn_fees_routing_update_router_test() ->
    [{Owner, OwnerSigFun}] = gen_payers(1),

    %% create new txn, and confirm expected fee size
    Txn00 = blockchain_txn_routing_v1:update_router_addresses(0, Owner, [?ADDRESS_KEY1, ?ADDRESS_KEY2],  1),
    Txn00Fee = blockchain_txn_routing_v1:calculate_fee(Txn00, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    Txn00StakingFee = blockchain_txn_routing_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ROUTER_UPDATE_ROUTER_STAKING_FEE, [], true),
    Txn00LegacyStakingFee = blockchain_txn_routing_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ROUTER_UPDATE_ROUTER_STAKING_FEE, [], false),
    ?assertEqual(40000, Txn00Fee),
    ?assertEqual(0, Txn00StakingFee),
    ?assertEqual(0, Txn00LegacyStakingFee),

    %% set the fee values of the txn, sign it and confirm the fees remains the same and unaffected by signatures
    Txn01 = blockchain_txn_routing_v1:fee(Txn00, Txn00Fee),
    Txn02 = blockchain_txn_routing_v1:staking_fee(Txn01, Txn00StakingFee),
    Txn03 = blockchain_txn_routing_v1:sign(Txn02, OwnerSigFun),
    Txn03Fee = blockchain_txn_routing_v1:calculate_fee(Txn03, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    Txn03StakingFee = blockchain_txn_routing_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ROUTER_UPDATE_ROUTER_STAKING_FEE, [], true),
    Txn03LegacyStakingFee = blockchain_txn_routing_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ROUTER_UPDATE_ROUTER_STAKING_FEE, [], false),
    ?assertEqual(40000, Txn03Fee),
    ?assertEqual(0, Txn03StakingFee),
    ?assertEqual(0, Txn03LegacyStakingFee),
    ok.

txn_fees_routing_new_xor_test() ->
    [{Owner, OwnerSigFun}] = gen_payers(1),
    {Filter, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),

    %% create new txn, and confirm expected fee size
    Txn00 = blockchain_txn_routing_v1:new_xor(1, Owner, Filter,  1),
    Txn00Fee = blockchain_txn_routing_v1:calculate_fee(Txn00, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    Txn00StakingFee = blockchain_txn_routing_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ROUTER_NEW_XOR_STAKING_FEE, [], true),
    Txn00LegacyStakingFee = blockchain_txn_routing_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ROUTER_NEW_XOR_STAKING_FEE, [], false),
    ?assertEqual(40000, Txn00Fee),
    ?assertEqual(0, Txn00StakingFee),
    ?assertEqual(0, Txn00LegacyStakingFee),

    %% set the fee values of the txn, sign it and confirm the fees remains the same and unaffected by signatures
    Txn01 = blockchain_txn_routing_v1:fee(Txn00, Txn00Fee),
    Txn02 = blockchain_txn_routing_v1:staking_fee(Txn01, Txn00StakingFee),
    Txn03 = blockchain_txn_routing_v1:sign(Txn02, OwnerSigFun),
    Txn03Fee = blockchain_txn_routing_v1:calculate_fee(Txn03, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    Txn03StakingFee = blockchain_txn_routing_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ROUTER_NEW_XOR_STAKING_FEE, [], true),
    Txn03LegacyStakingFee = blockchain_txn_routing_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ROUTER_NEW_XOR_STAKING_FEE, [], false),
    ?assertEqual(40000, Txn03Fee),
    ?assertEqual(0, Txn03StakingFee),
    ?assertEqual(0, Txn03LegacyStakingFee),
    ok.

txn_fees_routing_update_xor_test() ->
    [{Owner, OwnerSigFun}] = gen_payers(1),
    {Filter, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),

    %% create new txn, and confirm expected fee size
    Txn00 = blockchain_txn_routing_v1:update_xor(1, Owner, 0, Filter,  1),
    Txn00Fee = blockchain_txn_routing_v1:calculate_fee(Txn00, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    Txn00StakingFee = blockchain_txn_routing_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ROUTER_UPDATE_XOR_STAKING_FEE, [], true),
    Txn00LegacyStakingFee = blockchain_txn_routing_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ROUTER_UPDATE_XOR_STAKING_FEE, [], false),
    ?assertEqual(45000, Txn00Fee),
    ?assertEqual(0, Txn00StakingFee),
    ?assertEqual(0, Txn00LegacyStakingFee),

    %% set the fee values of the txn, sign it and confirm the fees remains the same and unaffected by signatures
    Txn01 = blockchain_txn_routing_v1:fee(Txn00, Txn00Fee),
    Txn02 = blockchain_txn_routing_v1:staking_fee(Txn01, Txn00StakingFee),
    Txn03 = blockchain_txn_routing_v1:sign(Txn02, OwnerSigFun),
    Txn03Fee = blockchain_txn_routing_v1:calculate_fee(Txn03, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    Txn03StakingFee = blockchain_txn_routing_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ROUTER_UPDATE_XOR_STAKING_FEE, [], true),
    Txn03LegacyStakingFee = blockchain_txn_routing_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ROUTER_UPDATE_XOR_STAKING_FEE, [], false),
    ?assertEqual(45000, Txn03Fee),
    ?assertEqual(0, Txn03StakingFee),
    ?assertEqual(0, Txn03LegacyStakingFee),
    ok.

txn_fees_routing_request_subnet_test() ->
    [{Owner, OwnerSigFun}] = gen_payers(1),

    %% create new txn, and confirm expected fee size
    Txn00 = blockchain_txn_routing_v1:request_subnet(1, Owner, 16,  1),
    Txn00Fee = blockchain_txn_routing_v1:calculate_fee(Txn00, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    Txn00StakingFee = blockchain_txn_routing_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ROUTER_REQUEST_SUBNET_STAKING_FEE, [], true),
    Txn00LegacyStakingFee = blockchain_txn_routing_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ROUTER_REQUEST_SUBNET_STAKING_FEE, [], false),
    ?assertEqual(25000, Txn00Fee),
    ?assertEqual(160000000, Txn00StakingFee),
    ?assertEqual(0, Txn00LegacyStakingFee),

    %% set the fee values of the txn, sign it and confirm the fees remains the same and unaffected by signatures
    Txn01 = blockchain_txn_routing_v1:fee(Txn00, Txn00Fee),
    Txn02 = blockchain_txn_routing_v1:staking_fee(Txn01, Txn00StakingFee),
    Txn03 = blockchain_txn_routing_v1:sign(Txn02, OwnerSigFun),
    Txn03Fee = blockchain_txn_routing_v1:calculate_fee(Txn03, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    Txn03StakingFee = blockchain_txn_routing_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ROUTER_REQUEST_SUBNET_STAKING_FEE, [], true),
    Txn03LegacyStakingFee = blockchain_txn_routing_v1:calculate_staking_fee(Txn00, ignore_ledger, ?ROUTER_REQUEST_SUBNET_STAKING_FEE, [], false),
    ?assertEqual(25000, Txn03Fee),
    ?assertEqual(160000000, Txn03StakingFee),
    ?assertEqual(0, Txn03LegacyStakingFee),
    ok.

txn_fees_security_exchange_v1_test() ->
    [{Payer, PayerSigFun}] = gen_payers(1),
    [RandomPubKey1, _RandomPubKey2] = gen_pubkeys(2),

    %% create a new payment txn, and confirm expected fee size
    Txn00 = blockchain_txn_security_exchange_v1:new(Payer, RandomPubKey1, 100000, 1),
    Txn00Fee = blockchain_txn_security_exchange_v1:calculate_fee(Txn00, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(35000, Txn00Fee),
    %% set the fee value of the txn, sign it and confirm the fee remains the same and unaffected by signature of fee values
    Txn01 = blockchain_txn_security_exchange_v1:fee(Txn00, Txn00Fee),
    Txn02 = blockchain_txn_security_exchange_v1:sign(Txn01, PayerSigFun),
    Txn02Fee = blockchain_txn_security_exchange_v1:calculate_fee(Txn02, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(35000, Txn02Fee),
    ok.

txn_fees_state_channel_open_v1_test() ->
    [{Owner, OwnerSigFun}] = gen_payers(1),

    %% create a sc open txn, and confirm expected fee size
    SCAmount = 10,
    Txn00 = blockchain_txn_state_channel_open_v1:new(<<"state_channel_test_id">>, Owner, 10, 1, 1, SCAmount),
    Txn00Fee = blockchain_txn_state_channel_open_v1:calculate_fee(Txn00, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(30000, Txn00Fee),
    %% set the fee value of the txn, sign it and confirm the fee remains the same and unaffected by signature of fee values
    Txn01 = blockchain_txn_state_channel_open_v1:fee(Txn00, Txn00Fee),
    Txn02 = blockchain_txn_state_channel_open_v1:sign(Txn01, OwnerSigFun),
    Txn02Fee = blockchain_txn_state_channel_open_v1:calculate_fee(Txn02, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(30000, Txn02Fee),
    ok.

txn_fees_state_channel_close_v1_test() ->
    [{Owner, OwnerSigFun}] = gen_payers(1),
    [RandomPubKey1] = gen_pubkeys(1),
    SCAmount = 10,
    SC = blockchain_state_channel_v1:new(<<"state_channel_test_id">>, Owner, SCAmount),

    %% create a new sc close txn, and confirm expected fee size
    Txn00 = blockchain_txn_state_channel_close_v1:new(SC, RandomPubKey1),
    Txn00Fee = blockchain_txn_state_channel_close_v1:calculate_fee(Txn00, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(0, Txn00Fee),
    %% set the fee value of the txn, sign it and confirm the fee remains the same and unaffected by signature of fee values
    Txn01 = blockchain_txn_state_channel_close_v1:fee(Txn00, Txn00Fee),
    Txn02 = blockchain_txn_state_channel_close_v1:sign(Txn01, OwnerSigFun),
    Txn02Fee = blockchain_txn_state_channel_close_v1:calculate_fee(Txn02, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(0, Txn02Fee),
    ok.


txn_fees_token_burn_v1_test() ->
    [{Payer, PayerSigFun}] = gen_payers(1),
    [RandomPubKey1] = gen_pubkeys(1),

    %% create a new payment txn, and confirm expected fee size
    Txn00 = blockchain_txn_token_burn_v1:new(Payer, RandomPubKey1, 100000, 1),
    Txn00Fee = blockchain_txn_token_burn_v1:calculate_fee(Txn00, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(35000, Txn00Fee),
    %% set the fee value of the txn, sign it and confirm the fee remains the same and unaffected by signature of fee values
    Txn01 = blockchain_txn_token_burn_v1:fee(Txn00, Txn00Fee),
    Txn02 = blockchain_txn_token_burn_v1:sign(Txn01, PayerSigFun),
    Txn02Fee = blockchain_txn_token_burn_v1:calculate_fee(Txn02, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(35000, Txn02Fee),
    ok.

txn_fees_update_gateway_oui_v1_test() ->
    [{GW, GWSigFun}] = gen_payers(1),

    %% create a new payment txn, and confirm expected fee size
    Txn00 = blockchain_txn_update_gateway_oui_v1:new(GW, 1, 1),
    Txn00Fee = blockchain_txn_update_gateway_oui_v1:calculate_fee(Txn00, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(40000, Txn00Fee),
    %% set the fee value of the txn, sign it and confirm the fee remains the same and unaffected by signature of fee values
    Txn01 = blockchain_txn_update_gateway_oui_v1:fee(Txn00, Txn00Fee),
    Txn02 = blockchain_txn_update_gateway_oui_v1:sign(Txn01, GWSigFun),
    Txn02Fee = blockchain_txn_update_gateway_oui_v1:calculate_fee(Txn02, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    ?assertEqual(40000, Txn02Fee),
    ok.

txn_fees_assert_location_v2_test() ->
    [{Payer, PayerSigFun}] = gen_payers(1),
    #{public := GWPubKey, secret := _GWPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    GWPubkeyBin = libp2p_crypto:pubkey_to_bin(GWPubKey),
    #{public := OwnerPubKey, secret := OwnerPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    OwnerPubkeyBin = libp2p_crypto:pubkey_to_bin(OwnerPubKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),

    %% create new txn, and confirm expected txn and staking fee
    Txn00 = blockchain_txn_assert_location_v2:new(GWPubkeyBin, OwnerPubkeyBin, Payer, ?TEST_LOCATION, 1),
    Txn00Fee = blockchain_txn_assert_location_v2:calculate_fee(Txn00, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    Txn00StakingFee = blockchain_txn_assert_location_v2:calculate_staking_fee(Txn00, ignore_ledger, ?ASSERT_LOC_STAKING_FEE, [], true),
    Txn00LegacyStakingFee = blockchain_txn_assert_location_v2:calculate_staking_fee(Txn00, ignore_ledger, ?ASSERT_LOC_STAKING_FEE, [], false),
    ?assertEqual(55000, Txn00Fee),
    ?assertEqual(?ASSERT_LOC_STAKING_FEE, Txn00StakingFee),
    ?assertEqual(1, Txn00LegacyStakingFee),

    %% set the fee values of the txn, sign it and confirm the fees remains the same and unaffected by signatures
    Txn01 = blockchain_txn_assert_location_v2:fee(Txn00, Txn00Fee),
    Txn02 = blockchain_txn_assert_location_v2:staking_fee(Txn01, Txn00StakingFee),
    Txn03 = blockchain_txn_assert_location_v2:sign(Txn02, OwnerSigFun),
    Txn04 = blockchain_txn_assert_location_v2:sign_payer(Txn03, PayerSigFun),
    Txn04Fee = blockchain_txn_assert_location_v2:calculate_fee(Txn04, ignore_ledger, ?DC_PAYLOAD_SIZE, ?TXN_MULTIPLIER, true),
    Txn04StakingFee = blockchain_txn_assert_location_v2:calculate_staking_fee(Txn04, ignore_ledger, ?ASSERT_LOC_STAKING_FEE, [], true),
    Txn04LegacyStakingFee = blockchain_txn_assert_location_v2:calculate_staking_fee(Txn04, ignore_ledger, ?ASSERT_LOC_STAKING_FEE, [], false),
    ?assertEqual(55000, Txn04Fee),
    ?assertEqual(?ASSERT_LOC_STAKING_FEE, Txn04StakingFee),
    ?assertEqual(1, Txn04LegacyStakingFee),
    ok.

-endif.
