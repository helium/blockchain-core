%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Routing ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_routing_v1).

-export([
    new/6,
    owner/1, owner/2,
    oui/1, oui/2,
    addresses/1,
    filters/1,
    subnets/1,
    update/3,
    nonce/1, nonce/2,
    serialize/1, deserialize/1,
    subnet_mask_to_size/1, subnet_size_to_mask/1,
    is_routing/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(BITS_23, 8388607). %% biggest unsigned number in 23 bits

-record(routing_v1, {
    oui :: non_neg_integer(),
    owner :: libp2p_crypto:pubkey_bin(),
    router_addresses = [] :: [libp2p_crypto:pubkey_bin()],
    filters = [] :: [binary()],
    subnets = [] :: [<<_:48>>],
    nonce :: non_neg_integer()
}).

-type routing() :: #routing_v1{}.

-export_type([routing/0]).

-spec new(non_neg_integer(),  binary(), [binary()], binary(), <<_:48>>, non_neg_integer()) -> routing().
new(OUI, Owner, Addresses, Filter, Subnet, Nonce) when Owner /= undefined andalso
                                       OUI /= undefined andalso
                                       Addresses /= undefined andalso
                                       Nonce /= undefined ->
    #routing_v1{
        oui=OUI,
        owner=Owner,
        router_addresses=Addresses,
        filters=[Filter],
        subnets=[Subnet],
        nonce=Nonce
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec oui(routing()) -> non_neg_integer().
oui(#routing_v1{oui=OUI}) ->
    OUI.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec oui(non_neg_integer(), routing()) -> routing().
oui(OUI, Entry) ->
    Entry#routing_v1{oui=OUI}.

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
-spec addresses(routing()) -> [libp2p_crypto:pubkey_bin()].
addresses(#routing_v1{router_addresses=Addresses}) ->
    Addresses.

-spec filters(routing()) -> [binary()].
filters(#routing_v1{filters=Filters}) ->
    Filters.

-spec subnets(routing()) -> [binary()].
subnets(#routing_v1{subnets=Subnets}) ->
    Subnets.

-spec update(routing(), term(), pos_integer()) -> routing().
update(Routing0, Action, Nonce) ->
    Routing = Routing0#routing_v1{nonce=Nonce},
    case Action of
        {update_routers, Routers} ->
            Routing#routing_v1{router_addresses=Routers};
        {new_xor, Filter} ->
            Routing#routing_v1{filters=Routing#routing_v1.filters++[Filter]};
        {update_xor, Index, Filter} ->
            Routing#routing_v1{filters=replace(Index, Filter, Routing#routing_v1.filters)};
        {request_subnet, Subnet} when is_binary(Subnet), byte_size(Subnet) == 6 ->
            %% subnet was already allocated prior to getting here
            Routing#routing_v1{subnets=Routing#routing_v1.subnets++[Subnet]}
    end.

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
    BinEntry = erlang:term_to_binary(Entry, [compressed]),
    <<1, BinEntry/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Later _ could becomre 1, 2, 3 for different versions.
%% @end
%%--------------------------------------------------------------------
-spec deserialize(binary()) -> routing().
deserialize(<<_:1/binary, Bin/binary>>) ->
    erlang:binary_to_term(Bin).


-spec subnet_mask_to_size(integer()) -> integer().
subnet_mask_to_size(Mask) ->
    (((Mask bxor ?BITS_23) bsl 2) + 2#11) + 1.

-spec subnet_size_to_mask(integer()) -> integer().
subnet_size_to_mask(Size) ->
    ?BITS_23 bxor ((Size bsr 2) - 1).

-spec is_routing(any()) -> boolean().
is_routing(#routing_v1{}) ->
    true;
is_routing(_) ->
    false.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

replace(Index, Element, List) ->
    {Head, [_ToRemove|Tail]} = lists:split(Index, List),
    Head ++ [Element] ++ Tail.

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


new_test() ->
    Routing = #routing_v1{
        owner = ?KEY2,
        oui = 1,
        router_addresses = [?KEY1],
        filters=[<<>>],
        subnets=[<<>>],
        nonce = 0
    },
    ?assertEqual(Routing, new(1, ?KEY2, [?KEY1], <<>>, <<>>, 0)).

oui_test() ->
    Routing = new(1, ?KEY2, [?KEY1], <<>>, <<>>, 0),
    ?assertEqual(1, oui(Routing)),
    ?assertEqual(2, oui(oui(2, Routing))).

owner_test() ->
    Routing = new(1, ?KEY2, [?KEY1], <<>>, <<>>, 0),
    ?assertEqual(?KEY2, owner(Routing)),
    ?assertEqual(?KEY3, owner(owner(?KEY3, Routing))).

nonce_test() ->
    Routing = new(1, ?KEY2, [?KEY1], <<>>, <<>>, 0),
    ?assertEqual(0, nonce(Routing)),
    ?assertEqual(1, nonce(nonce(1, Routing))).

addresses_test() ->
    Routing = new(1, ?KEY2, [], <<>>, <<>>, 0),
    ?assertEqual([], addresses(Routing)),
    ?assertEqual([?KEY3], addresses(update(Routing, {update_routers, [?KEY3]}, 1))).

replace_test() ->
    List = lists:seq(1, 5),
    ?assertEqual([6, 2, 3, 4, 5], replace(0, 6, List)),
    ?assertEqual([1, 6, 3, 4, 5], replace(1, 6, List)),
    ?assertEqual([1, 2, 3, 6, 5], replace(3, 6, List)),
    ?assertEqual([1, 2, 3, 4, 6], replace(4, 6, List)),
    ok.

is_routing_test() ->
    Routing = new(1, ?KEY2, [], <<>>, <<>>, 0),
    ?assert(is_routing(Routing)),
    SomeOtherThing1 = "yolo",
    ?assertNot(is_routing(SomeOtherThing1)),
    SomeOtherThing2 = ["not", "routing"],
    ?assertNot(is_routing(SomeOtherThing2)).

-endif.
