%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Implicit Burn ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_implicit_burn).

-export([
    new/2,
    fee/1, fee/2,
    payer/1, payer/2,
    serialize/1, deserialize/1,
    to_json/2
]).

-record(implicit_burn, {
    fee :: non_neg_integer(),
    payer :: libp2p_crypto:pubkey_bin()
}).

-type implicit_burn() :: #implicit_burn{}.

-export_type([implicit_burn/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------

-spec new(non_neg_integer(), libp2p_crypto:pubkey_bin()) -> implicit_burn().
new(Fee, Payer) ->
    #implicit_burn{fee=Fee, payer=Payer}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(implicit_burn()) -> non_neg_integer().
fee(#implicit_burn{fee=Fee}) ->
    Fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(non_neg_integer(), implicit_burn()) -> implicit_burn().
fee(Fee, ImplicitBurn) ->
    ImplicitBurn#implicit_burn{fee=Fee}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payer(implicit_burn()) -> libp2p_crypto:pubkey_bin().
payer(#implicit_burn{payer=Payer}) ->
    Payer.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payer(libp2p_crypto:pubkey_bin(), implicit_burn()) -> implicit_burn().
payer(Payer, ImplicitBurn) ->
    ImplicitBurn#implicit_burn{payer=Payer}.

%%--------------------------------------------------------------------
%% @doc
%% Version 1
%% @end
%%--------------------------------------------------------------------
-spec serialize(implicit_burn()) -> binary().
serialize(ImplicitBurn) ->
    BinEntry = erlang:term_to_binary(ImplicitBurn, [compressed]),
    <<1, BinEntry/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Later _ could becomre 1, 2, 3 for different versions.
%% @end
%%--------------------------------------------------------------------
-spec deserialize(binary()) -> implicit_burn().
deserialize(<<_:1/binary, Bin/binary>>) ->
    erlang:binary_to_term(Bin).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------

-spec to_json(implicit_burn(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(ImplicitBurn, _Opts) ->
    #{
      fee => fee(ImplicitBurn),
      payer => ?BIN_TO_B58(payer(ImplicitBurn))
    }.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
% -ifdef(TEST).

% new_test() ->
%     ImplicitBurn0 = #implicit_burn{
%         fee = 0
%     },
%     ?assertEqual(ImplicitBurn0, new()),
%     ImplicitBurn1 = #implicit_burn{
%         fee = 1
%     },
%     ?assertEqual(ImplicitBurn1, new(1)).

% fee_test() ->
%     ImplictiBurn = new(),
%     ?assertEqual(0, fee(ImplictiBurn)),
%     ?assertEqual(1, fee(fee(1, ImplicitBurn))).

% -endif.
