-define(APPEUI, <<0, 0, 0, 2, 0, 0, 0, 1>>).
-define(DEVEUI, <<0, 0, 0, 0, 0, 0, 0, 1>>).
-define(APPKEY,
    <<16#2B, 16#7E, 16#15, 16#16, 16#28, 16#AE, 16#D2, 16#A6, 16#AB, 16#F7, 16#15, 16#88, 16#09,
        16#CF, 16#4F, 16#3C>>
).
-define(JOIN_REQUEST, 2#000).

%% NOTE: Only used for testing
%% Each of these is used to download a serialized copy of h3 region set
-define(region_as923_1_url,
    "https://github.com/helium/lorawan-h3/blob/main/serialized/AS923-1.res7.h3idx?raw=true"
).

-define(region_as923_2_url,
    "https://github.com/helium/lorawan-h3/blob/main/serialized/AS923-2.res7.h3idx?raw=true"
).

-define(region_as923_3_url,
    "https://github.com/helium/lorawan-h3/blob/main/serialized/AS923-3.res7.h3idx?raw=true"
).

-define(region_as923_4_url,
    "https://github.com/helium/lorawan-h3/blob/main/serialized/AS923-4.res7.h3idx?raw=true"
).

-define(region_au915_url,
    "https://github.com/helium/lorawan-h3/blob/main/serialized/AU915.res7.h3idx?raw=true"
).

-define(region_cn470_url,
    "https://github.com/helium/lorawan-h3/blob/main/serialized/CN470.res7.h3idx?raw=true"
).

-define(region_eu433_url,
    "https://github.com/helium/lorawan-h3/blob/main/serialized/EU433.res7.h3idx?raw=true"
).

-define(region_eu868_url,
    "https://github.com/helium/lorawan-h3/blob/main/serialized/EU868.res7.h3idx?raw=true"
).

-define(region_in865_url,
    "https://github.com/helium/lorawan-h3/blob/main/serialized/IN865.res7.h3idx?raw=true"
).

-define(region_kr920_url,
    "https://github.com/helium/lorawan-h3/blob/main/serialized/KR920.res7.h3idx?raw=true"
).

-define(region_ru864_url,
    "https://github.com/helium/lorawan-h3/blob/main/serialized/RU864.res7.h3idx?raw=true"
).

-define(region_us915_url,
    "https://github.com/helium/lorawan-h3/blob/main/serialized/US915.res7.h3idx?raw=true"
).

-define(regulatory_region_bin_str,
    <<"region_as923_1,region_as923_2,region_as923_3,region_as923_4,region_au915,region_cn470,region_eu433,region_eu868,region_in865,region_kr920,region_ru864,region_us915">>
).
