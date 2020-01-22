%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Response ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_response_v1).

-export([
    accepted/2,
    rejected/1,
    accepted/1,
    req_hash/1,
    state_channel_update/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/src/pb/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type response() :: #blockchain_state_channel_response_v1_pb{}.
-export_type([response/0]).

-spec accepted(binary(), blockchain_state_channel_update_v1:state_channel_update()) -> response().
accepted(Hash, SCUpdate) -> 
    #blockchain_state_channel_response_v1_pb{
        accepted=true,
        req_hash=Hash,
        state_channel_update=SCUpdate
    }.

-spec rejected(binary()) -> response().
rejected(Hash) -> 
    #blockchain_state_channel_response_v1_pb{
        accepted=false,
        req_hash=Hash,
        state_channel_update=undefined
    }.

-spec accepted(response()) -> boolean().
accepted(#blockchain_state_channel_response_v1_pb{accepted=Bool}) ->
    Bool.

-spec req_hash(response()) -> skewed:hash().
req_hash(#blockchain_state_channel_response_v1_pb{req_hash=Hash}) ->
    Hash.

-spec state_channel_update(response()) -> blockchain_state_channel_update_v1:state_channel_update().
state_channel_update(#blockchain_state_channel_response_v1_pb{state_channel_update=SCUpdate}) ->
    SCUpdate.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

accepted2_test() ->
    SC = blockchain_state_channel_v1:new(<<1>>, <<2>>),
    SCUpdate = #blockchain_state_channel_update_v1_pb{
        state_channel=SC,
        previous_hash= <<"prev_hash">>
    },
    ReqHash = <<"req_hash">>,
    Resp = #blockchain_state_channel_response_v1_pb{
        accepted=true,
        req_hash=ReqHash,
        state_channel_update=SCUpdate
    },
    ?assertEqual(Resp, accepted(ReqHash, SCUpdate)).

rejected_test() ->
    ReqHash = <<"req_hash">>,
    Resp = #blockchain_state_channel_response_v1_pb{
        accepted=false,
        req_hash=ReqHash,
        state_channel_update=undefined
    },
    ?assertEqual(Resp, rejected(ReqHash)).

accepted1_test() ->
    SC = blockchain_state_channel_v1:new(<<1>>, <<2>>),
    SCUpdate = #blockchain_state_channel_update_v1_pb{
        state_channel=SC,
        previous_hash= <<"prev_hash">>
    },
    ReqHash = <<"req_hash">>, 
    ?assert(accepted(accepted(ReqHash, SCUpdate))),
    ?assertNot(accepted(rejected(ReqHash))).

req_hash_test() ->
    ReqHash = <<"req_hash">>, 
    ?assertEqual(ReqHash, req_hash(rejected(ReqHash))).

state_channel_update_test() ->
    SC = blockchain_state_channel_v1:new(<<1>>, <<2>>),
    SCUpdate = #blockchain_state_channel_update_v1_pb{
        state_channel=SC,
        previous_hash= <<"prev_hash">>
    },
    ReqHash = <<"req_hash">>, 
    ?assertEqual(SCUpdate, state_channel_update(accepted(ReqHash, SCUpdate))),
    ?assertEqual(undefined, state_channel_update(rejected(ReqHash))).

-endif.