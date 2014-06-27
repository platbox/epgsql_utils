%%
%% epgsql_utils_orm_tests

-module(epgsql_utils_orm_tests).

-include_lib("eunit/include/eunit.hrl").

pack_test() ->
    FixtureData = [
        {json, #{}, <<"{}">>},
        {
            json,
            #{people => [
                #{
                    name => <<"Tommy">>,
                    job => <<"Developer">>
                },
                #{
                    name => <<"Alice">>,
                    job => <<"Housewife">>
                }
            ]},
            <<"{\"people\":[{\"name\":\"Tommy\",\"job\":\"Developer\"},{\"name\":\"Alice\",\"job\":\"Housewife\"}]}">>
        }

    ],
    lists:foreach(
        fun({Type, Before, After}) ->
            ?assertEqual(After, epgsql_utils_orm:pack({epgsql_utils_orm, Type}, Before))
        end,
        FixtureData
    ).

unpack_test() ->
    FixtureData = [
        {json, <<"{}">>, #{}},
        {
            json,
            <<"{\"foo\":[\"bing\",2.3,true,{\"message\":\"ok\"}]}">>,
            #{
                <<"foo">> => [
                    <<"bing">>,
                    2.3,
                    true,
                    #{
                        <<"message">> => <<"ok">>
                    }
                ]
            }
        }

    ],
    lists:foreach(
        fun({Type, Before, After}) ->
            ?assertEqual(After, epgsql_utils_orm:unpack({epgsql_utils_orm, Type}, Before))
        end,
        FixtureData
    ).
