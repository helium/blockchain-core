%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Validator Heartbeat ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_validator_heartbeat_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_validator_heartbeat_v1_pb.hrl").

-export([
         new/3,
         new/5,
         hash/1,
         address/1,
         height/1,
         signature/1,
         version/1,
         fee/1,
         fee_payer/2,
         poc_key_proposals/1,
         reactivated_gws/1,
         sign/2,
         is_valid/2,
         absorb/2,
         print/1,
         json_type/0,
         to_json/2,

         proposal_length/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_validator_heartbeat() :: #blockchain_txn_validator_heartbeat_v1_pb{}.
-export_type([txn_validator_heartbeat/0]).

-spec new(libp2p_crypto:pubkey_bin(), pos_integer(), pos_integer()) ->
    txn_validator_heartbeat().
new(Address, Height, Version) ->
    new(Address, Height, Version, [], []).

-spec new(libp2p_crypto:pubkey_bin(), pos_integer(), pos_integer(), [binary()], [libp2p_crypto:pubkey_bin()]) ->
          txn_validator_heartbeat().
new(Address, Height, Version, POCKeyProposals, ReactivatedGWs) ->
    #blockchain_txn_validator_heartbeat_v1_pb{
       address = Address,
       height = Height,
       version = Version,
       poc_key_proposals = POCKeyProposals,
       reactivated_gws = ReactivatedGWs
    }.

-spec hash(txn_validator_heartbeat()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_validator_heartbeat_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_validator_heartbeat_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec address(txn_validator_heartbeat()) -> libp2p_crypto:pubkey_bin().
address(Txn) ->
    Txn#blockchain_txn_validator_heartbeat_v1_pb.address.

-spec height(txn_validator_heartbeat()) -> pos_integer().
height(Txn) ->
    Txn#blockchain_txn_validator_heartbeat_v1_pb.height.

-spec version(txn_validator_heartbeat()) -> pos_integer().
version(Txn) ->
    Txn#blockchain_txn_validator_heartbeat_v1_pb.version.

-spec poc_key_proposals(txn_validator_heartbeat()) -> [binary()].
poc_key_proposals(Txn) ->
    Txn#blockchain_txn_validator_heartbeat_v1_pb.poc_key_proposals.

-spec reactivated_gws(txn_validator_heartbeat()) -> [libp2p_crypto:pubkey_bin()].
reactivated_gws(Txn) ->
    Txn#blockchain_txn_validator_heartbeat_v1_pb.reactivated_gws.

-spec signature(txn_validator_heartbeat()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_validator_heartbeat_v1_pb.signature.

-spec fee(txn_validator_heartbeat()) -> 0.
fee(_Txn) ->
    0.

-spec fee_payer(txn_validator_heartbeat(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

-spec sign(txn_validator_heartbeat(), libp2p_crypto:sig_fun()) -> txn_validator_heartbeat().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_validator_heartbeat_v1_pb{signature= <<>>},
    EncodedTxn = blockchain_txn_validator_heartbeat_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_validator_heartbeat_v1_pb{signature=SigFun(EncodedTxn)}.

-spec is_valid_sig(txn_validator_heartbeat()) -> boolean().
is_valid_sig(#blockchain_txn_validator_heartbeat_v1_pb{address=PubKeyBin,
                                                       signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_validator_heartbeat_v1_pb{signature= <<>>},
    EncodedTxn = blockchain_txn_validator_heartbeat_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

-spec is_valid(txn_validator_heartbeat(), blockchain:blockchain()) ->
          ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Validator = address(Txn),
    Version = version(Txn),
    TxnHeight = height(Txn),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    Proposals = poc_key_proposals(Txn),
    Reactivated = reactivated_gws(Txn),
    case is_valid_sig(Txn) of
        false ->
            {error, bad_signature};
        _ ->
            try
                case blockchain:config(?validator_version, Ledger) of
                    {ok, Vers} when Vers >= 1 ->
                        ok;
                    _ -> throw(unsupported_txn)
                end,
                %% make sure that this validator exists and is staked
                case blockchain_ledger_v1:get_validator(Validator, Ledger) of
                    {ok, V} ->
                        {ok, Interval} = blockchain_ledger_v1:config(?validator_liveness_interval, Ledger),
                        Status = blockchain_ledger_validator_v1:status(V),
                        HB = blockchain_ledger_validator_v1:last_heartbeat(V),
                        case Status == staked
                            andalso TxnHeight >= (Interval + HB)
                            andalso TxnHeight =< Height of
                            true -> ok;
                            _ -> throw({bad_height, prev, HB, height, Height, got, TxnHeight})
                        end;
                    {error, not_found} -> throw(nonexistent_validator);
                    {error, Reason} -> throw({validator_fetch_error, Reason})
                end,
                case valid_version(Version)  of
                    true -> ok;
                    false -> throw({bad_version, Version})
                end,
                {TargetLen, ReactivationLimit} =
                    case blockchain_ledger_v1:config(?poc_challenger_type, Ledger) of
                        {ok, validator} ->
                            {ok, RL} = blockchain_ledger_v1:config(?validator_hb_reactivation_limit, Ledger),
                            PL = proposal_length(Ledger),
                            {PL, RL};
                        _ ->
                            {0, 0}
                    end,
                case length(Proposals) == TargetLen of
                    true -> ok;
                    false ->
                        throw({bad_proposal_length, TargetLen, length(Proposals)})
                end,
                case length(Reactivated) =< ReactivationLimit of
                    true -> ok;
                    false ->
                        throw({bad_reactivation_length, ReactivationLimit, length(Reactivated)})
                end,
                ok
            catch throw:Cause ->
                    {error, Cause}
            end
    end.

%% oh dialyzer
valid_version(V) when is_integer(V) andalso V > 0 ->
    true;
valid_version(_) ->
    false.

-spec absorb(txn_validator_heartbeat(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Validator = address(Txn),
    Version = version(Txn),
    TxnHeight = height(Txn),

    %% Get ledger current height and block hash to submit poc proposal
    {ok, LedgerHt} = blockchain_ledger_v1:current_height(Ledger),
    {ok, LedgerHash} = blockchain:get_block_hash(LedgerHt, Chain),

    case blockchain_ledger_v1:get_validator(Validator, Ledger) of
        {ok, V} ->
            V1 = blockchain_ledger_validator_v1:last_heartbeat(TxnHeight, V),
            V2 = blockchain_ledger_validator_v1:version(Version, V1),
            {ok, ConsensusAddrs} = blockchain_ledger_v1:consensus_members(Ledger),
            case lists:member(Validator, ConsensusAddrs) of
                true ->
                    %% gateway reactivation must be processed regardless of consensus status
                    %% see GH#1357
                    case blockchain_ledger_v1:config(?poc_always_process_reactivations, Ledger) of
                        {ok, true} ->
                            maybe_reactivate_gateways(Txn, Ledger),
                            ok;
                        _ -> ok
                    end;
                false ->
                    case blockchain:config(poc_challenger_type, Ledger) of
                        {ok, validator} ->
                            POCKeyProposals = poc_key_proposals(Txn),
                            blockchain_ledger_v1:save_poc_proposals(POCKeyProposals, Validator, LedgerHash, LedgerHt, Ledger);
                        _ ->
                            ok
                    end,
                    maybe_reactivate_gateways(Txn, Ledger)
            end,
            blockchain_ledger_v1:update_validator(Validator, V2, Ledger);
        Err -> Err
    end.

-spec print(txn_validator_heartbeat()) -> iodata().
print(undefined) -> <<"type=validator_heartbeat, undefined">>;
print(#blockchain_txn_validator_heartbeat_v1_pb{
         address = Val,
         height = H,
         version = V}) ->
    io_lib:format("type=validator_heartbeat, validator=~p, height=~p, version=~p",
                  [?TO_ANIMAL_NAME(Val), H, V]).

json_type() ->
    <<"validator_heartbeat_v1">>.

-spec to_json(txn_validator_heartbeat(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      address => ?BIN_TO_B58(address(Txn)),
      height => height(Txn),
      signature => ?BIN_TO_B64(signature(Txn)),
      version => version(Txn),
      poc_key_proposals => [?BIN_TO_B64(K) || K <- poc_key_proposals(Txn)],
      reactivated_gws => [?BIN_TO_B58(GW) || GW <- reactivated_gws(Txn)]
     }.

maybe_reactivate_gateways(Txn, Ledger) ->
    %% process the reactivated GW list submitted in the heartbeat
    %%
    %% these are GWs which have fallen outside of the max activity span
    %% and thus wont be selected for POC
    %%
    %% to get on this reactivated list the GW must have connected
    %% to a validator over GRPC and subscribed to the poc stream
    %% as such it may have come back to life
    %%
    %% so update the last activity tracking and allow it to be
    %% reselected for POC

    TxnHeight = height(Txn),
    case blockchain:config(poc_activity_filter_enabled, Ledger) of
        {ok, true} ->
            ReactivatedGWs = reactivated_gws(Txn),
            reactivate_gws(ReactivatedGWs, TxnHeight, Ledger);
        _ ->
            ok
    end.

reactivate_gws(GWAddrs, Height, Ledger) ->
    lists:foreach(
        fun(GWAddr) ->
            case blockchain_ledger_v1:find_gateway_info(GWAddr, Ledger) of
                {error, _} ->
                    {error, no_active_gateway};
                {ok, GW0} ->
                    blockchain_ledger_v1:reactivate_gateway(Height, GW0, GWAddr, Ledger)
            end
        end, GWAddrs).

-spec proposal_length(blockchain:ledger()) -> non_neg_integer().
proposal_length(Ledger) ->
    %% generate the size for a set of ephemeral keys for POC usage. the count is based on the num of
    %% active validators and the target challenge rate. we also have to consider that key proposals
    %% are submitted by validators as part of their heartbeats which are only submitted periodically
    %% so we need to ensure we have sufficient count of key proposals submitted per HB. to help with
    %% this we reduce the number of val count by 20% so that we have surplus keys being submitted
    case blockchain_ledger_v1:validator_count(Ledger) of
        {ok, NumVals} when NumVals > 0 ->
            {ok, ChallengeRate} = blockchain_ledger_v1:config(?poc_challenge_rate, Ledger),
            {ok, ValCtScale} = blockchain_ledger_v1:config(?poc_validator_ct_scale, Ledger),
            {ok, HBInterval} = blockchain_ledger_v1:config(?validator_liveness_interval, Ledger),
            round((ChallengeRate / (NumVals * ValCtScale)) * HBInterval);
        _ ->
            0
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

to_json_test() ->
    Tx = new(<<"validator_address">>, 20000, 1, [<<"poc_key_proposal">>], [<<"reactivated_gateway_addr1">>]),
    Json = to_json(Tx, []),
    ?assertEqual(lists:sort(maps:keys(Json)),
                 lists:sort([type, hash] ++ record_info(fields, blockchain_txn_validator_heartbeat_v1_pb))).


-endif.
