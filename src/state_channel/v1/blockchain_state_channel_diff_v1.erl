%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Diff ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_diff_v1).

-export([
]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-type diff() :: #blockchain_state_channel_diff_v1_pb{}.
-export_type([diff/0]).


%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").


-endif.
