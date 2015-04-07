%%
%% epgsql_utils_model tests

-module(epgsql_utils_model_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("epgsql_utils/include/epgsql_utils_model.hrl").
-compile({parse_transform, epgsql_utils_model}).

-export([pack/2]).
-export([unpack/2]).

-record(dancer, {
    id::key(pos_integer()),
    name::string(),
    aliases::[string()],
    age::non_neg_integer(),
    hobby::?MODULE:hobby()
}).

-type hobby() :: {atom(), string()}.

-export_record_model([
    {dancer, [{table, {test, dancers}}]}
]).

%%
pack(hobby, {Type, Value}) ->
     [atom_to_list(Type), ":", Value];
pack(T, V) ->
    pack_record(T, V).

unpack(hobby, V) ->
    V1 = iolist_to_binary(V),
    [H1, H2] = binary:split(V1, <<":">>),
    {binary_to_atom(H1, utf8), H2};
unpack(T, V) ->
    unpack_record(T, V).
%%

struct_info_test() ->
    ?assertEqual({test, dancers}, ?MODULE:struct_info({dancer, table})),
    ?assertEqual([id], ?MODULE:struct_info({dancer, keys})),
    ?assertEqual({epgsql_utils_orm, non_neg_integer},
                  proplists:get_value(age, ?MODULE:struct_info({dancer, fields}))
    ).

pack_external_type_test() ->
    Record = #dancer{id = 42, hobby = {hooker, <<"Tracey">>}, aliases = [<<"Mr.">>, <<"Nicolas">>, <<"Cage">>]},
    Packed = epgsql_utils_orm:pack({?MODULE, dancer}, Record),
    ?assertEqual(Record, epgsql_utils_orm:unpack({?MODULE, dancer}, Packed)).
