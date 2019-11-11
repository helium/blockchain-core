-module(path_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([prop_path_check/0]).

prop_path_check() ->
    ?FORALL({AlphaBetas,
             Hash,
             Index,
             UseTarget,
             UsePathLimit,
             PathLimit}, {gen_scores(),
                          gen_hash(),
                          gen_target_index(),
                          gen_use_target(),
                          gen_use_path_limit(),
                          gen_path_limit()},
            begin
                ScoredIndices = lists:zip(indices(), AlphaBetas),
                BaseDir = tmp_dir("path_eqc"),

                Ledger = case UsePathLimit of
                             false ->
                                 %% path limit is not set
                                 build_fake_ledger(BaseDir, ScoredIndices, 3, 60, not_found);
                             true ->
                                 %% path limit is set to whatever
                                 build_fake_ledger(BaseDir, ScoredIndices, 3, 60, PathLimit)
                         end,

                ActiveGateways = blockchain_ledger_v1:active_gateways(Ledger),

                {HasLocalGeo, {PathFound, Path}} = case UseTarget of
                                                       false ->
                                                           %% use a known target, testing neighbors code path
                                                           indexed_target(Hash, Index, ScoredIndices, ActiveGateways, Ledger);
                                                       true ->
                                                           %% calculate a target and use that for building a path
                                                           %% to test select_target and relevant code path
                                                           calculated_target(Hash, ActiveGateways, Ledger)
                                                   end,

                Check = case {HasLocalGeo, PathFound, Path} of
                            {false, false, undefined} ->
                                %% no local geography, no path
                                true;
                            {true, false, undefined} ->
                                %% local geography found but no path
                                true;
                            {true, true, P0} when P0 /= undefined ->
                                %% local geography and path found
                                case UsePathLimit of
                                    true ->
                                        %% honor path limit if set
                                        length(P0) =< PathLimit;
                                    false ->
                                        true
                                end;
                            _ ->
                                %% fail in all other scenarios
                                false
                        end,
                unload_meck(),
                ?WHENFAIL(begin
                              io:format("UseTarget: ~p~n", [UseTarget]),
                              io:format("UsePathLimit: ~p~n", [UsePathLimit]),
                              io:format("HasLocalGeo: ~p~n", [HasLocalGeo]),
                              io:format("PathLimit: ~p~n", [PathLimit]),
                              io:format("PathFound: ~p~n", [PathFound])
                          end,
                          conjunction([{verify_path, Check}])
                         )
            end).

gen_beta() ->
    ?SUCHTHAT(S, real(), S > 1.0 andalso S < 200.0).

gen_alpha() ->
    ?SUCHTHAT(S, real(), S > 1.0 andalso S < 200.0).

gen_scores() ->
    vector(length(indices()), {gen_alpha(), gen_beta()}).

gen_hash() ->
    binary(32).

gen_target_index() ->
    ?SUCHTHAT(S, nat(), S < length(indices())).

gen_use_target() ->
    bool().

gen_use_path_limit() ->
    bool().

gen_path_limit() ->
    elements([3, 4, 5, 6, 7]).

indices() ->
    %% currently known hotspot indices
    [631188755337926143, 631210968850123263, 631713493642392063, 631210968893205503,
     631781457161018367, 631781457224905727, 631210968941675007, 631210968926349823,
     631246149780204543, 631781457279630847, 631246129121579519, 631210968858365439,
     631781457456030719, 631211393196942335, 631246145933764095, 631781457713455615,
     631781489504348159, 631781457454618623, 631781456876811263, 631781452262810111,
     631210935151315455, 631781457763585023, 631781456868852735, 631781453294608895,
     631781456867264511, 631781452707118591, 631210968859298303, 631781456876615167,
     631781457688216063, 631211239308628479, 631781456964693503, 631211236220012543,
     631781456921777151, 631781456964279295, 631210979476728831, 631781457511789055,
     631781456971303423, 631781452656629247, 631781456964709375, 631781456934687231,
     631781452838965759, 631781456967153663, 631781456964278783, 631210973951547903,
     631781456967539711, 631781457696611839, 631210935151441407, 631781457186806271,
     631781456868233727, 631210968931850239, 631246145768164351, 631781457343694335,
     631781456934824959, 631781306195382783, 631210968874634239, 631781456977271807,
     631246145771745791, 631210971081944063, 631781085142063615, 631781456969993727,
     631781452590400511, 631246145920316415, 631781457071769599, 631246145747784703,
     631781457606236671, 631713493675559935, 631781457144914431, 631246145674683391,
     631781456916921855, 631210979481525759, 631781457337432575, 631781456966936575,
     631210973986758143, 631210968909003263, 631246145808026111, 631211240316661247,
     631210968859489791, 631210971082016255, 631246145131561471, 631210968892119551,
     631210973812670975, 631781457088743935, 631781456954056191, 631781456905644031,
     631713488795188735, 631781456971519999, 631781456923379711, 631781456977093119,
     631211239530874879, 631781457455685119, 631781456930480639, 631210968943528447,
     631781457759244799, 631714412089447423, 631246149761936895, 631262208947602431,
     631781456930474495, 631210971081835007, 631781456964401663, 631781084347550207,
     631781489299048447, 631781456914771455, 631781457691817983, 631781457223911423,
     631781457411359231, 631246145900140031, 631781457713456127, 631781456964692479,
     631781050465723903, 631781453286509055, 631781457275432447, 631781453181051903,
     631781456964402175, 631210968892960255, 631210971014478335, 631781457687437823,
     631713492825613823, 631781456964707327, 631713524677463039, 631781457727431167,
     631211239379920383, 631781457558420479, 631781456971383295, 631781457442303487,
     631781456970146815, 631210968843172863, 631781457377743871, 631781457159542783,
     631211118533098495, 631781457321090047, 631781457143452159, 631781457136991231,
     631246145565408767, 631246145837870591, 631781456971375615, 631781489265239039,
     631781457456161279, 631246130373814783, 631246145927713279, 631781457720940543,
     631713524583007231, 631781456970010111, 631781456964391935, 631781457107162623,
     631713492414399487, 631210968874529791, 631210968928781823, 631211239341083135,
     631781457687492095, 631781455454984703, 631211118525831679, 631210972276239871,
     631781456923206143, 631210968874051071, 631210971081847807, 631246145668592127,
     631781457215930879, 631210968876178431, 631246145387142143, 631246145652436479,
     631781489263578623, 631781456973153791, 631781456967539711, 631246145105349119,
     631246363884633087, 631781452924144639, 631781456970562559, 631188755339337215,
     631246145614039039, 631781489219023871, 631781456970193919, 631781457275414015,
     631713492825613823, 631707651682763263, 631210979481736703, 631210983218633727,
     631781456967592959, 631781457221702143, 631781457226945023, 631211239308072959,
     631781456954446847, 631781457316596223, 631246145763611647, 631246145377593855,
     631713492395972607, 631713492785310719, 631781451722596863, 631781456970015743,
     631781457435921919, 631246145606797823, 631781457312549887, 631781456964602879,
     631781457776167935, 631211239494330367, 631781085185109503, 631211239536516095,
     631210971058584063, 631781457190207999, 631781457224870399, 631246145601144831,
     631210968840456191, 631781489256018943, 631781456961180671, 631781089167934463,
     631210966330693119, 631781456967572479, 631781452243101183, 631781456971393535,
     631210968880540671, 631210968859299327, 631210968867706367, 631246129121736703,
     631210979481525759, 631210970153275903, 631210971074031103, 631210968840452607,
     631781457227376639, 631781084613314047, 631246145700792831].

build_fake_ledger(TestDir, ScoredIndices, ExclusionRingDist, MaxGridDist, PathLimit) ->
    Ledger = blockchain_ledger_v1:new(TestDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    meck:new(blockchain, [passthrough]),
    meck:expect(blockchain,
                config,
                fun(min_score, _) ->
                        {ok, 0.2};
                   (h3_exclusion_ring_dist, _) ->
                        {ok, ExclusionRingDist};
                   (h3_max_grid_distance, _) ->
                        {ok, MaxGridDist};
                   (h3_neighbor_res, _) ->
                        {ok, 12};
                   (alpha_decay, _) ->
                        {ok, 0.007};
                   (beta_decay, _) ->
                        {ok, 0.0005};
                   (max_staleness, _) ->
                        {ok, 100000};
                   (poc_version, _) ->
                        {ok, 2};
                   (poc_challenge_sync_interval, _) ->
                        {error, not_found};
                   (poc_path_limit, _) ->
                        case PathLimit of
                            not_found ->
                                {error, not_found};
                            L ->
                                {ok, L}
                        end
                end),
    meck:new(blockchain_score_cache, [passthrough]),
    meck:expect(blockchain_score_cache, fetch, fun(_, Fun) -> Fun() end),
    N = length(ScoredIndices),
    OwnerAndGateways = [{O, G} || {{O, _}, {G, _}} <- lists:zip(generate_keys(N), generate_keys(N))],

    lists:foreach(fun({{Owner, Address}, {Index, {Alpha, Beta}}}) ->
                          ok = blockchain_ledger_v1:add_gateway(Owner, Address, Index, 0, Ledger1),
                          ok = blockchain_ledger_v1:update_gateway_score(Address, {Alpha, Beta}, Ledger1)
                  end, lists:zip(OwnerAndGateways, ScoredIndices)),
    ok = blockchain_ledger_v1:commit_context(Ledger1),
    Ledger.

generate_keys(N) ->
    generate_keys(N, ecc_compact).

generate_keys(N, Type) ->
    lists:foldl(
      fun(_, Acc) ->
              #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(Type),
              SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
              [{libp2p_crypto:pubkey_to_bin(PubKey), {PubKey, PrivKey, SigFun}}|Acc]
      end,
      [],
      lists:seq(1, N)).

tmp_dir() ->
    nonl(os:cmd("mktemp -d")).

tmp_dir(Dir) ->
    filename:join(tmp_dir(), Dir).

nonl([$\n|T]) -> nonl(T);
nonl([H|T]) -> [H|nonl(T)];
nonl([]) -> [].

unload_meck() ->
    ?assert(meck:validate(blockchain)),
    meck:unload(blockchain),
    ?assert(meck:validate(blockchain_score_cache)),
    meck:unload(blockchain_score_cache).


indexed_target(Hash, Index, ScoredIndices, ActiveGateways, Ledger) ->
    {TargetIndex, {_A, _B}} = lists:nth(Index + 1, ScoredIndices),

    {TargetGwBin, _} = hd(lists:filter(fun({_K, V}) ->
                                               blockchain_ledger_gateway_v2:location(V) == TargetIndex
                                       end,
                                       maps:to_list(ActiveGateways))),

    HasLocalGeo0 = case blockchain_poc_path:neighbors(TargetGwBin, ActiveGateways, Ledger) of
                       {error, _} ->
                           false;
                       N when length(N) > 1 ->
                           true;
                       _ ->
                           false
                   end,

    PathFound0 = case HasLocalGeo0 of
                     false ->
                         {false, undefined};
                     true ->
                         case blockchain_poc_path:build(Hash, TargetGwBin, ActiveGateways, 1, Ledger) of
                             {error, _} ->
                                 {false, undefined};
                             {ok, P} ->
                                 {true, P}
                         end
                 end,
    {HasLocalGeo0, PathFound0}.

calculated_target(Hash, ActiveGateways, Ledger) ->
    Challenger = hd(maps:keys(ActiveGateways)),
    case blockchain_poc_path:target(Hash, Ledger, Challenger) of
        no_target ->
            {false, {false, undefined}};
        {T, Gs} ->
            case blockchain_poc_path:build(Hash, T, Gs, 1, Ledger) of
                {error, _} ->
                    {true, {false, undefined}};
                {ok, P2} ->
                    {true, {true, P2}}
            end
    end.
