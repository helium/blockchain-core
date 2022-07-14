-module(blockchain_ledger_state_channel).

-export_type([
    state_channel/0,
    vsn/0
]).

-export([
    vsn/1
]).

-type state_channel() ::
      blockchain_ledger_state_channel_v1:state_channel()
    | blockchain_ledger_state_channel_v2:state_channel()
    .

-type vsn() :: v1 | v2.

-spec vsn(state_channel()) -> vsn().
vsn(SC) ->
    IsV1 = blockchain_ledger_state_channel_v1:is_v1(SC),
    IsV2 = blockchain_ledger_state_channel_v2:is_v2(SC),
    case {IsV1, IsV2} of
        {true, false} -> v1;
        {false, true} -> v2
    end.
