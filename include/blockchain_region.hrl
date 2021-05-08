-define(SUPPORTED_REGIONS, [
    "as923_1",
    "as923_2",
    "as923_3",
    "au915",
    "cn470",
    "eu433",
    "eu868",
    "in865",
    "kr920",
    "ru864",
    "us915"

    %% We will not support cn779
]).

%% TODO: Left the last ones out, can add if needed
%% https://www.thethingsnetwork.org/docs/lorawan/frequency-plans/

-define(REGION_PARAMS_US915, [
    [
        {<<"channel_frequency">>, 903900000},                                   % in hz
        {<<"bandwidth">>, 125000},                                              % in hz
        {<<"max_eirp">>, 360},                                                  % dBi x 10
        {<<"spreading">>, [<<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}        % will be converted to atoms
    ],
    [
        {<<"channel_frequency">>, 904100000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 360},
        {<<"spreading">>, [<<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 904300000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 360},
        {<<"spreading">>, [<<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 904500000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 360},
        {<<"spreading">>, [<<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 904700000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 360},
        {<<"spreading">>, [<<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 904900000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 360},
        {<<"spreading">>, [<<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 905100000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 360},
        {<<"spreading">>, [<<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 905300000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 360},
        {<<"spreading">>, [<<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ]
]).

-define(REGION_PARAMS_EU868, [
    [
        {<<"channel_frequency">>, 868100000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 868300000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 868500000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 868500000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 867100000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 867300000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 867500000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 867700000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 867900000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ]
]).

-define(REGION_PARAMS_AU915, [
    [
        {<<"channel_frequency">>, 916800000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 300},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 917000000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 300},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 917200000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 300},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 917400000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 300},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 917600000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 300},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 917800000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 300},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 918000000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 300},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 918200000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 300},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ]
]).

-define(REGION_PARAMS_AS923_1, [
    [
        {<<"channel_frequency">>, 923200000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 923400000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 922200000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 922400000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 922600000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 922800000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 923000000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 922000000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ]
]).

-define(REGION_PARAMS_AS923_2, [
    [
        {<<"channel_frequency">>, 923200000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 923400000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 923600000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 923800000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 924000000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 924200000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 924400000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 924600000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ]
]).

-define(REGION_PARAMS_AS923_3, [
    [
        {<<"channel_frequency">>, 916600000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 916800000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ]
    %% This is incomplete...
]).

-define(REGION_PARAMS_RU864, [
    [
        {<<"channel_frequency">>, 868900000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 869100000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 160},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ]
]).

-define(REGION_PARAMS_CN470, [
    [
        {<<"channel_frequency">>, 486300000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 191},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 486500000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 191},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 486700000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 191},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 486900000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 191},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 487100000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 191},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 487300000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 191},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 487500000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 191},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 487700000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 191},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ]
]).

-define(REGION_PARAMS_IN865, [
    [
        {<<"channel_frequency">>, 865062500},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 300},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 865985000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 300},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 865402500},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 300},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ]
]).

-define(REGION_PARAMS_KR920, [
    [
        {<<"channel_frequency">>, 922100000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 922300000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 922500000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 922700000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 922900000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 923100000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 923300000},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ]
]).

-define(REGION_PARAMS_EU433, [
    [
        {<<"channel_frequency">>, 43317500},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 121},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 43337500},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 121},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 43357500},
        {<<"bandwidth">>, 125000},
        {<<"max_eirp">>, 121},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
]).

%% -define(REGION_PARAMS_CN779, [
%%     [
%%         {<<"channel_frequency">>, 779500000},
%%         {<<"bandwidth">>, 125000},
%%         {<<"max_eirp">>, 121},
%%         {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
%%     ],
%%     [
%%         {<<"channel_frequency">>, 779700000},
%%         {<<"bandwidth">>, 125000},
%%         {<<"max_eirp">>, 121},
%%         {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
%%     ],
%%     [
%%         {<<"channel_frequency">>, 779900000},
%%         {<<"bandwidth">>, 125000},
%%         {<<"max_eirp">>, 121},
%%         {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
%%     ]
%% ]).
