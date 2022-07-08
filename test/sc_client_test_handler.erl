-module(sc_client_test_handler).

-export([handle_response/1, handle_purchase/1]).

handle_response(Resp) ->
    F = application:get_env(blockchain, sc_client_handle_response_fun, fun(_Resp) -> ok end),
    lager:info("Resp: ~p", [Resp]),
    F(Resp).

handle_purchase(Purchase) ->
    F = application:get_env(blockchain, sc_client_handle_purchase_fun, fun(_Resp) -> ok end),
    lager:info("Purchase: ~p", [Purchase]),
    F(Purchase).
