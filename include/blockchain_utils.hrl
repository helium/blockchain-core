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

-define(SNR_CURVE, #{12 => {-90,-35},
                     4 => {-115,-112},
                     -4 => {-125,-125},
                     16 => {-90,-35},
                     -15 => {-124,-124},
                     -6 => {-124,-124},
                     -1 => {-125,-125},
                     -2 => {-125,-125},
                     5 => {-115,-100},
                     14 => {-90,-35},
                     -11 => {-125,-125},
                     9 => {-95,-45},
                     10 => {-90,-40},
                     -12 => {-125,-125},
                     -7 => {-123,-123},
                     -10 => {-125,-125},
                     -14 => {-125,-125},
                     2 => {-117,-112},
                     6 => {-113,-100},
                     -5 => {-125,-125},
                     3 => {-115,-112},
                     -3 => {-125,-125},
                     1 => {-120,-117},
                     7 => {-108,-45},
                     8 => {-105,-45},
                     -8 => {-125,-125},
                     0 => {-125,-125},
                     13 => {-90,-35},
                     -16 => {-123,-123},
                     -17 => {-123,-123},
                     -18 => {-123,-123},
                     -19 => {-123,-123},
                     -20 => {-123,-123},
                     15 => {-90,-35},
                     -9 => {-125,-125},
                     -13 => {-125,-125},
                     11 => {-90,-35}}).

%% default values for gateways
-define(DEFAULT_GAIN, 12). %% as dBi * 10
-define(DEFAULT_ELEVATION, 0). %% as AGL (above ground level) in meters

-define(SC_MAX_ACTORS, 1100).

-define(MHzToHzMultiplier, 1000000). %% Hz -> MHz multiplier
