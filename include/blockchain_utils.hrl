%% Utility macros
-define(normalize_float(Float, Vars),
        case maps:get(poc_version, Vars, undefined) of
            V when V > 4 ->
                blockchain_utils:normalize_float(Float);
            _ ->
                Float
        end).

