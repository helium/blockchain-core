%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Routing ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_routing_v1).

-export([
    new/4,
    owner/1, owner/2,
    oui/1, oui/2,
    addresses/1, addresses/2,
    nonce/1, nonce/2,
    serialize/1, deserialize/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(routing_v1, {
    owner :: binary(),
    oui :: binary(),
    addresses :: [binary()],
    nonce :: non_neg_integer()
}).

-type routing() :: #routing_v1{}.

-export_type([routing/0]).

-spec new(binary(), binary(), [binary()], non_neg_integer()) -> routing().
new(Owner, OUI, Addresses, Nonce) when Owner /= undefined andalso
                                       OUI /= undefined andalso
                                       Addresses /= undefined andalso
                                       Nonce /= undefined ->
    #routing_v1{
        owner=Owner,
        oui=OUI,
        addresses=Addresses,
        nonce=Nonce
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner(routing()) -> binary().
owner(#routing_v1{owner=Owner}) ->
    Owner.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner(binary(), routing()) -> routing().
owner(Owner, Entry) ->
    Entry#routing_v1{owner=Owner}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec oui(routing()) -> binary().
oui(#routing_v1{oui=OUI}) ->
    OUI.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec oui(binary(), routing()) -> routing().
oui(OUI, Entry) ->
    Entry#routing_v1{oui=OUI}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec addresses(routing()) -> [binary()].
addresses(#routing_v1{addresses=Addresses}) ->
    Addresses.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec addresses([binary()], routing()) -> routing().
addresses(Addresses, Entry) ->
    Entry#routing_v1{addresses=Addresses}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(routing()) -> non_neg_integer().
nonce(#routing_v1{nonce=Nonce}) ->
    Nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(non_neg_integer(), routing()) -> routing().
nonce(Nonce, Entry) ->
    Entry#routing_v1{nonce=Nonce}.

%%--------------------------------------------------------------------
%% @doc
%% Version 1
%% @end
%%--------------------------------------------------------------------
-spec serialize(routing()) -> binary().
serialize(Entry) ->
    BinEntry = erlang:term_to_binary(Entry),
    <<1, BinEntry/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Later _ could becomre 1, 2, 3 for different versions.
%% @end
%%--------------------------------------------------------------------
-spec deserialize(binary()) -> routing().
deserialize(<<_:1/binary, Bin/binary>>) ->
    erlang:binary_to_term(Bin).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Routing = #routing_v1{
        owner = <<"owner">>,
        oui = <<"oui">>,
        addresses = [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>],
        nonce = 0
    },
    ?assertEqual(Routing, new(<<"owner">>, <<"oui">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 0)).

owner_test() ->
    Routing = new(<<"owner">>, <<"oui">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 0),
    ?assertEqual(<<"owner">>, owner(Routing)),
    ?assertEqual(<<"owner2">>, owner(owner(<<"owner2">>, Routing))).

oui_test() ->
    Routing = new(<<"owner">>, <<"oui">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 0),
    ?assertEqual(<<"oui">>, oui(Routing)),
    ?assertEqual(<<"oui2">>, oui(oui(<<"oui2">>, Routing))).

nonce_test() ->
    Routing = new(<<"owner">>, <<"oui">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 0),
    ?assertEqual(0, nonce(Routing)),
    ?assertEqual(1, nonce(nonce(1, Routing))).

addresses_test() ->
    Routing = new(<<"owner">>, <<"oui">>, "", 0),
    ?assertEqual("", addresses(Routing)),
    ?assertEqual([<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], addresses(addresses([<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], Routing))).

-endif.
