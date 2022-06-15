-module(blockchain_txn_mgr_SUITE).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_txn_payment_v1_pb.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    init_per_testcase/2,
    end_per_testcase/2,
    all/0
]).

-export([
    submit_and_query_test/1,
    grpc_submit_and_query_test/1
]).

%% Setup --------------------------------------------------------

all() ->
    [
        submit_and_query_test,
        grpc_submit_and_query_test
    ].

init_per_testcase(TestCase, Cfg0) ->
    Src = t_user:new(),
    SrcBalance = 5000,

    case TestCase of
        grpc_submit_and_query_test ->
            GrpcConfig = [#{grpc_opts => #{service_protos => [transaction_pb],
                                           services => #{'helium.transaction' => helium_transaction_service}},
                            transport_opts => #{ssl => false},
                            listen_opts => #{port => 10001, ip => {0,0,0,0}},
                            pool_opts => #{size => 2},
                            server_opts => #{header_table_size => 4096,
                                             enable_push => 1,
                                             max_concurrent_streams => unlimited,
                                             initial_window_size => 65535,
                                             max_frame_size => 16384,
                                             max_header_list_size => unlimited}}],
            application:load(grpcbox),
            application:set_env(grpcbox, server, GrpcConfig),
            application:ensure_all_started(grpcbox);
        _ -> ok
    end,

    Cfg = t_chain:start(Cfg0, #{users_with_hnt => [{Src, SrcBalance}]}),

    [{src_user, Src} | Cfg].

end_per_testcase(_TestCase, _Cfg) ->
    t_chain:stop().

%% Cases --------------------------------------------------------

submit_and_query_test(Cfg) ->
    [CG1, CG2 | _Group] = ?config(users_in_consensus, Cfg),
    Chain = ?config(chain, Cfg),
    Src = ?config(src_user, Cfg),
    Dst = t_user:new(),

    Txn = t_txn:pay(Src, Dst, 1000, 1),

    ok = test_utils:wait_until(fun() -> {ok, 1} =:= blockchain:height(Chain) end),

    {ok, Key, Height} = blockchain_txn_mgr:grpc_submit(Txn),
    ct:pal("Received ~p from txn submitted at height ~p", [Key, Height]),

    ?assert(is_integer(binary_to_integer(Key))),
    ?assertEqual(1, Height),

    Accept = {CG1, 2, 3, 4},
    Reject = {CG2, 2, fail},

    [{Key, Txn, {txn_data, Callbk, RecHt, _, _, Dials}}] = ets:lookup(txn_cache, Key),
    ets:insert(txn_cache, {Key, Txn, {txn_data, Callbk, RecHt, [Accept], [Reject], Dials}}),
    ets:insert(height_cache, {cur_height, 2}),

    {ok, pending, #{key := Key,
                    recv_block_height := RecvHeight,
                    height := CurHeight,
                    acceptors := [Acc],
                    rejectors := [Rej]}} = blockchain_txn_mgr:txn_status(Key),

    ct:pal("Accepted by : ~p, Rejected by : ~p", [Acc, Rej]),
    ?assertEqual(1, RecvHeight),
    ?assertEqual(2, CurHeight),
    ?assertEqual(CG1, element(1, Acc)),
    ?assertEqual(fail, element(3, Rej)).

grpc_submit_and_query_test(Cfg) ->
    [_CG1 | _Group] = ?config(users_in_consensus, Cfg),
    Chain = ?config(chain, Cfg),
    Src = ?config(src_user, Cfg),
    Dst = t_user:new(),

    Amount = 1000,
    #blockchain_txn_payment_v1_pb{payer = SrcPub,
                                  payee = DstPub,
                                  amount = Amount,
                                  fee = Fee,
                                  nonce = Nonce,
                                  signature = Sig} = t_txn:pay(Src, Dst, Amount, 1),
    TxnMap = #{payer => SrcPub,
               payee => DstPub,
               amount => Amount,
               fee => Fee,
               nonce => Nonce,
               signature => Sig},

    ok = test_utils:wait_until(fun() -> {ok, 1} =:= blockchain:height(Chain) end),

    Config = application:get_env(grpcbox, server),
    ct:pal("GRPC BOX CONFIG ~p", [Config]),
    {ok, GrpcConn} = grpc_client:connect(tcp, "127.0.0.1", 10001),
    %% {ok, #{http_status := 200,
    %%        result := #txn_submit_resp_v1_pb{recv_height = Height, key = Key}}} =
    Result = grpc_client:unary(GrpcConn,
                          #{txn => #{txn => {payment, TxnMap}}},
                          'helium.transaction',
                          submit,
                          transaction_client_pb,
                          [{timeout, 1000}]),
    ct:pal("RESULT ~p", [Result]),
    %% ct:pal("Received ~p from txn submitted at height ~p", [Key, Height]),

    %% ?assert(is_integer(binary_to_integer(Key))),
    %% ?assertEqual(1, Height).
    ct:fail("Boom").
