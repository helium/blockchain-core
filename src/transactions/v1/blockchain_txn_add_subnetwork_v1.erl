%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Txn to add a subnetwork token to the network ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_add_subnetwork_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").

-include_lib("helium_proto/include/blockchain_txn_add_subnetwork_v1_pb.hrl").

-export([
    new/4,
    token_type/1,
    subnetwork_key/1,
    reward_server_keys/1,
    premine/1,
    network_signature/1,
    subnetwork_signature/1,
    sign/2,
    sign_subnetwork/2,
    hash/1,

    fee/1,
    fee_payer/2,
    is_valid/2,
    absorb/2,

    print/1,
    json_type/0,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_add_subnetwork() :: #blockchain_txn_add_subnetwork_v1_pb{}.
-export_type([txn_add_subnetwork/0]).

-spec new(
    TT :: blockchain_token_v1:type(),
    SubnetworkKey :: libp2p_crypto:pubkey_bin(),
    RewardServerKeys :: [libp2p_crypto:pubkey_bin()],
    PremineAmt :: non_neg_integer()
) -> txn_add_subnetwork().
new(TT, SubnetworkKey, RewardServerKeys, PremineAmt) when PremineAmt >= 0 ->
    #blockchain_txn_add_subnetwork_v1_pb{
        token_type = TT,
        subnetwork_key = SubnetworkKey,
        reward_server_keys = lists:sort(RewardServerKeys),
        premine = PremineAmt,
        network_signature = <<>>,
        subnetwork_signature = <<>>
    }.

-spec token_type(Txn :: txn_add_subnetwork()) -> blockchain_token_v1:token_type().
token_type(Txn) ->
    Txn#blockchain_txn_add_subnetwork_v1_pb.token_type.

-spec subnetwork_key(Txn :: txn_add_subnetwork()) -> libp2p_crypto:pubkey_bin().
subnetwork_key(Txn) ->
    Txn#blockchain_txn_add_subnetwork_v1_pb.subnetwork_key.

-spec reward_server_keys(Txn :: txn_add_subnetwork()) -> [libp2p_crypto:pubkey_bin()].
reward_server_keys(Txn) ->
    Txn#blockchain_txn_add_subnetwork_v1_pb.reward_server_keys.

-spec premine(Txn :: txn_add_subnetwork()) -> non_neg_integer().
premine(Txn) ->
    Txn#blockchain_txn_add_subnetwork_v1_pb.premine.

-spec network_signature(txn_add_subnetwork()) -> binary().
network_signature(Txn) ->
    Txn#blockchain_txn_add_subnetwork_v1_pb.network_signature.

-spec subnetwork_signature(txn_add_subnetwork()) -> binary().
subnetwork_signature(Txn) ->
    Txn#blockchain_txn_add_subnetwork_v1_pb.subnetwork_signature.

-spec sign(txn_add_subnetwork(), libp2p_crypto:sig_fun()) -> txn_add_subnetwork().
sign(Txn, SigFun) ->
    BaseTxn = unset_signatures(Txn),
    EncodedTxn = blockchain_txn_add_subnetwork_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_add_subnetwork_v1_pb{network_signature = SigFun(EncodedTxn)}.

-spec sign_subnetwork(txn_add_subnetwork(), libp2p_crypto:sig_fun()) -> txn_add_subnetwork().
sign_subnetwork(Txn, SigFun) ->
    BaseTxn = unset_signatures(Txn),
    EncodedTxn = blockchain_txn_add_subnetwork_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_add_subnetwork_v1_pb{subnetwork_signature = SigFun(EncodedTxn)}.

-spec hash(txn_add_subnetwork()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = unset_signatures(Txn),
    EncodedTxn = blockchain_txn_add_subnetwork_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec fee(Txn :: txn_add_subnetwork()) -> non_neg_integer().
fee(_Txn) ->
    0.

-spec fee_payer(Txn :: txn_add_subnetwork(), Ledger :: blockchain_ledger_v1:ledger()) -> undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

-spec is_valid(Txn :: txn_add_subnetwork(), Chain :: blockchain:blockchain()) ->
    ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case token_type(Txn) of
        hst ->
            {error, invalid_token_hst};
        hnt ->
            {error, invalid_token_hnt};
        TT ->
            case lists:member(TT, blockchain_token_v1:supported_tokens()) of
                false ->
                    {error, {unsupported_token, TT}};
                true ->
                    case blockchain:config(?allowed_num_reward_server_keys, Ledger) of
                        {ok, 1} ->
                            Artifact = create_artifact(Txn),
                            SubnetworkKey = ?MODULE:subnetwork_key(Txn),
                            SubnetworkSig = ?MODULE:subnetwork_signature(Txn),
                            case verify_key(Artifact, SubnetworkKey, SubnetworkSig) of
                                true ->
                                    {ok, MasterKey} = blockchain_ledger_v1:master_key(Ledger),
                                    NetworkSig = ?MODULE:network_signature(Txn),
                                    case verify_key(Artifact, MasterKey, NetworkSig) of
                                        true ->
                                            ok;
                                        _ ->
                                            {error, invalid_network_signature}
                                    end;
                                _ ->
                                    {error, invalid_subnetwork_signature}
                            end;
                        _ ->
                            %% NOTE: Update here when more than one reward server keys are allowed
                            {error, invalid_reward_server_key_length}
                    end
            end
    end.

-spec absorb(Txn :: txn_add_subnetwork(), Chain :: blockchain:blockchain()) -> ok | {error, atom()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    TT = ?MODULE:token_type(Txn),
    PremineAmt = ?MODULE:premine(Txn),
    HNTTreasury = 0,
    SubnetworkKey = ?MODULE:subnetwork_key(Txn),
    RewardServerKeys = ?MODULE:reward_server_keys(Txn),
    blockchain_ledger_v1:add_subnetwork(
        TT,
        PremineAmt,
        HNTTreasury,
        SubnetworkKey,
        RewardServerKeys,
        Ledger
    ).

-spec print(txn_add_subnetwork()) -> iodata().
print(undefined) ->
    <<"type=add_subnetwork_v1, undefined">>;
print(
    #blockchain_txn_add_subnetwork_v1_pb{
        token_type = TT,
        subnetwork_key = SKey,
        reward_server_keys = RKeys,
        premine = PremineAmt,
        network_signature = NS,
        subnetwork_signature = SS
    }
) ->
    io_lib:format(
        "type=add_subnetwork_v1, token_type=~p, premine=~p, subnetwork_key=~p, reward_server_keys=~p, network_signature=~s~n subnetwork_signature: ~s",
        [
            atom_to_list(TT),
            PremineAmt,
            ?TO_B58(SKey),
            [?TO_B58(RKey) || RKey <- RKeys],
            ?TO_B58(NS),
            ?TO_B58(SS)
        ]
    ).

json_type() ->
    <<"add_subnetwork_v1">>.

-spec to_json(txn_add_subnetwork(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
        type => ?MODULE:json_type(),
        hash => ?BIN_TO_B64(hash(Txn)),
        token_type => ?MAYBE_ATOM_TO_BINARY(token_type(Txn)),
        subnetwork_key => ?BIN_TO_B58(subnetwork_key(Txn)),
        reward_server_keys => [?BIN_TO_B58(K) || K <- reward_server_keys(Txn)]
    }.

create_artifact(Txn) ->
    Txn1 = unset_signatures(Txn),
    blockchain_txn_add_subnetwork_v1_pb:encode_msg(Txn1).

unset_signatures(Txn) ->
    Txn#blockchain_txn_add_subnetwork_v1_pb{network_signature = <<>>, subnetwork_signature = <<>>}.

verify_key(_Artifact, _Key, <<>>) ->
    throw({error, no_signature});
verify_key(Artifact, Key, Signature) ->
    libp2p_crypto:verify(Artifact, Signature, libp2p_crypto:bin_to_pubkey(Key)).


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

to_json_test() ->
    T = new(mobile, <<"subnetwork_key">>, [<<"reward_server_key">>], 10000),
    Json = to_json(T, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, token_type, subnetwork_key, reward_server_keys])),
    ?assertEqual(<<"mobile">>, maps:get(token_type, Json)),
    ok.

-endif.
