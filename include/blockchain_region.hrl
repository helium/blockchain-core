-define(SUPPORTED_REGIONS, [
    "as923_1",
    "as923_2",
    "as923_3",
    "au915",
    "cn779",
    "eu433",
    "eu868",
    "in865",
    "kr920",
    "ru864",
    "us915"
]).

-define(REGION_PARAMS_US915, [
    [
        {<<"channel_frequency">>, 903900000},                                   % in hz
        {<<"bandwidth">>, 125000},                                              % in hz
        {<<"max_power">>, 360},                                                 % dBi x 10
        {<<"spreading">>, [<<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}        % will be converted to atoms
    ],
    [
        {<<"channel_frequency">>, 904100000},
        {<<"bandwidth">>, 125000},
        {<<"max_power">>, 360},
        {<<"spreading">>, [<<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 904300000},
        {<<"bandwidth">>, 125000},
        {<<"max_power">>, 360},
        {<<"spreading">>, [<<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 904500000},
        {<<"bandwidth">>, 125000},
        {<<"max_power">>, 360},
        {<<"spreading">>, [<<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 904700000},
        {<<"bandwidth">>, 125000},
        {<<"max_power">>, 360},
        {<<"spreading">>, [<<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 904900000},
        {<<"bandwidth">>, 125000},
        {<<"max_power">>, 360},
        {<<"spreading">>, [<<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 905100000},
        {<<"bandwidth">>, 125000},
        {<<"max_power">>, 360},
        {<<"spreading">>, [<<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 905300000},
        {<<"bandwidth">>, 125000},
        {<<"max_power">>, 360},
        {<<"spreading">>, [<<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ]
]).

%% TODO: EU can transmit on a single channel at 27 dBi apparently ¯\_(ツ)_/¯
-define(REGION_PARAMS_EU868, [
    [
        {<<"channel_frequency">>, 868100000},
        {<<"bandwidth">>, 125000},
        {<<"max_power">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 868300000},
        {<<"bandwidth">>, 125000},
        {<<"max_power">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ],
    [
        {<<"channel_frequency">>, 868500000},
        {<<"bandwidth">>, 125000},
        {<<"max_power">>, 140},
        {<<"spreading">>, [<<"sf12">>, <<"sf11">>, <<"sf10">>, <<"sf9">>, <<"sf8">>, <<"sf7">>]}
    ]
]).
