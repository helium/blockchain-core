%% Utility macros
-define(normalize_float(Float, Vars),
        case maps:get(poc_version, Vars, undefined) of
            V when is_integer(V), V > 4 ->
                blockchain_utils:normalize_float(Float);
            _ ->
                Float
        end).

-define(TO_B58(X), libp2p_crypto:bin_to_b58(X)).
-define(TO_ANIMAL_NAME(X), element(2, erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(X)))).
