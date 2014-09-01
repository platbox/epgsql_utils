%%
%% epgsql_utils_orm_tests

-module(epgsql_utils_orm_tests).
-compile({parse_transform, epgsql_utils_model}).

-include_lib("eunit/include/eunit.hrl").
-include_lib("epgsql_utils/include/epgsql_utils_model.hrl").

-export([pack/2]).
-export([unpack/2]).

-record(harassment, {
    id          :: key(pos_integer()),
    type        :: ?MODULE:htype(),
    offender    :: binary(),
    penalty     :: non_neg_integer()
}).

-record(nonsence, {
    foo         :: key(datetime()),
    bar         :: key(binary())
}).

-type htype() :: sexual | other.

-export_record_model([
    {harassment, [{table, {?MODULE, harassments}}]},
    {nonsence  , [{table, {?MODULE, nonsences}}]}
]).

pack(_, undefined) -> null;
pack(htype, V) -> atom_to_binary(V, utf8);
pack(T, V) -> pack_record(T, V).

unpack(_, null) -> undefined;
unpack(htype, V) -> binary_to_atom(V, utf8);
unpack(T, V) -> unpack_record(T, V).

%%

db_test_() ->
    {setup, spawn,
        fun () ->
            Apps = [lager],
            _ = [application:start(A) || A <- Apps],
            _ = lager:set_loglevel(lager_console_backend, debug),
            {ok, Pid} = epgsql_utils_supervisor2:start_link(epgsql_utils_conn_pool, {?MODULE, options()}),
            {Apps, Pid}
        end,
        fun ({Apps, Pid}) ->
            _ = [application:stop(A) || A <- Apps],
            _ = exit(Pid, normal)
        end,
        fun (_) ->
            [
                ?_test(begin
                    epgsql_utils_querying:do(?MODULE, fun () ->
                        _ = epgsql_utils_schema:drop(?MODULE_STRING),
                        _ = epgsql_utils_schema:prepare(?MODULE_STRING, [{1, fun prepare_schema/0}])
                    end),
                    epgsql_utils_querying:do(?MODULE, T)
                end) || T <- [
                    fun test_create/0,
                    fun test_update/0,
                    fun test_fields/0,
                    fun test_batch/0
                ]
            ]
        end
    }.

prepare_schema() ->
    epgsql_utils_querying:do_query(<<"create table ", ?MODULE_STRING, $., "harassments (",
        "id bigserial primary key,"
        "type varchar(64) not null,"
        "offender text not null,"
        "penalty integer"
    ");">>),
    epgsql_utils_querying:do_query(<<"create table ", ?MODULE_STRING, $., "nonsences (",
        "foo timestamp without time zone not null,"
        "bar varchar(64) not null,"
        "primary key (foo, bar)"
    ");">>).

options() ->
    [].

%%

test_create() ->
    Type = {?MODULE, harassment},
    Type2 = {?MODULE, nonsence},
    ?assertEqual(1 , epgsql_utils_orm:create(Type, #harassment{type = sexual, offender = <<"Lil' Schukovsky">>})),
    ?assertEqual(42, epgsql_utils_orm:create(Type, #harassment{id = 42, type = other, offender = <<"MC Banking">>, penalty = 1337})),
    Now = calendar:local_time(),
    ?assertEqual([Now, <<"BloodyKey">>], epgsql_utils_orm:create(Type2, #nonsence{foo = Now, bar = <<"BloodyKey">>})).

test_update() ->
    Type = {?MODULE, harassment},
    ID = epgsql_utils_orm:create(Type, #harassment{type = sexual, offender = <<"Lil' Schukovsky">>}),
    ?assertEqual(ID, epgsql_utils_orm:create_or_update(Type, ID, #harassment{id = ID, type = other, offender = <<"Lil' B Sides">>})),
    ?assertEqual(1, epgsql_utils_orm:update_fields(Type, ID, [{penalty, 42}])),
    ?assertEqual([#harassment{id = ID, type = other, offender = <<"Lil' B Sides">>, penalty = 42}], epgsql_utils_orm:get(Type, ID)).

test_fields() ->
    Type = {?MODULE, harassment},
    _   = epgsql_utils_orm:create(Type, #harassment{type = sexual, offender = N1 = <<"Lil' Schukovsky">>}),
    ID2 = epgsql_utils_orm:create(Type, #harassment{type = other , offender =      <<"Dr Poppler">>}),
    _   = epgsql_utils_orm:create(Type, #harassment{type = sexual, offender = N3 = <<"Dick Dickerson">>}),
    ?assertMatch([[{_, N1}], [{_, N3}]], epgsql_utils_orm:get_fields_by_cond(Type, {type, '=' , sexual}, [offender])),
    ?assertMatch([[{_, ID2}]]          , epgsql_utils_orm:get_fields_by_cond(Type, {type, '!=', sexual}, [id])).

test_batch() ->
    Type = {?MODULE, nonsence},
    Objects = [#nonsence{foo = calendar:local_time(), bar = integer_to_binary(N)} || N <- lists:seq(1, 20)],
    ?assertEqual(20, epgsql_utils_orm:batch_create(Type, Objects, 3)),
    ?assertEqual(20, epgsql_utils_orm:count(Type)).

%%

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
