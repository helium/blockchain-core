%%%
%%% https://www.erlang.org/doc/apps/erts/erl_ext_dist.html
%%%

-define(ETF_VERSION, 131).

-define(ETF_TAG_COMPRESSED         ,  80).

%% integer()
-define(ETF_TAG_SMALL_INTEGER_EXT  ,  97).
-define(ETF_TAG_INTEGER_EXT        ,  98).
-define(ETF_TAG_SMALL_BIG_EXT      , 110).
-define(ETF_TAG_LARGE_BIG_EXT      , 111).

%% float()
-define(ETF_TAG_FLOAT_EXT          ,  99).
-define(ETF_TAG_NEW_FLOAT_EXT      ,  70).

%% atom()
-define(ETF_TAG_ATOM_EXT           , 100). % deprecated
-define(ETF_TAG_ATOM_CACHE_REF     ,  82).
-define(ETF_TAG_ATOM_UTF8_EXT      , 118).
-define(ETF_TAG_SMALL_ATOM_UTF8_EXT, 119).
-define(ETF_TAG_SMALL_ATOM_EXT     , 115). % deprecated

%% tuple()
-define(ETF_TAG_SMALL_TUPLE_EXT    , 104).
-define(ETF_TAG_LARGE_TUPLE_EXT    , 105).

%% string()
-define(ETF_TAG_STRING_EXT         , 107).

%% list()
-define(ETF_TAG_NIL_EXT            , 106).
-define(ETF_TAG_LIST_EXT           , 108).

%% binary()
-define(ETF_TAG_BINARY_EXT         , 109).
-define(ETF_TAG_BIT_BINARY_EXT     ,  77).

%% map()
-define(ETF_TAG_MAP_EXT            , 116).

%% port()
-define(ETF_TAG_PORT_EXT           , 102).
-define(ETF_TAG_NEW_PORT_EXT       ,  89).
-define(ETF_TAG_V4_PORT_EXT        , 120).

%% pid()
-define(ETF_TAG_PID_EXT            , 103).
-define(ETF_TAG_NEW_PID_EXT        ,  88).

%% reference()
-define(ETF_TAG_REFERENCE_EXT      , 101). % deprecated
-define(ETF_TAG_NEW_REFERENCE_EXT  , 114).
-define(ETF_TAG_NEWER_REFERENCE_EXT,  90).

%% fun()
-define(ETF_TAG_FUN_EXT            , 117).
-define(ETF_TAG_NEW_FUN_EXT        , 112).

%% fun M:F/A
-define(ETF_TAG_EXPORT_EXT         , 113).
