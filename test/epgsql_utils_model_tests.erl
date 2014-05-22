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
    Record = #dancer{id = 42, hobby = {hooker, <<"Tracey">>}},
    Packed = epgsql_utils_orm:pack({?MODULE, dancer}, Record),
    ?assertEqual(Record, epgsql_utils_orm:unpack({?MODULE, dancer}, Packed)).

match_field_nums_test() ->
    Fields = ?MODULE:struct_info({dancer, fields}),
    IDFieldNames = [id, name, age, hobby],
    Matched = epgsql_utils_orm:match_field_nums(1, IDFieldNames, Fields),
    ?assertEqual(1, proplists:get_value(id, Matched)),
    ?assertEqual(2, proplists:get_value(name, Matched)),
    ?assertEqual(3, proplists:get_value(age, Matched)),
    ?assertEqual(4, proplists:get_value(hobby, Matched)).


get_record_keys_test() ->
    Record = #dancer{id = 99, hobby = games},
    Keys = epgsql_utils_orm:get_record_keys(?MODULE, Record),
    ?assertEqual([{id, 99}], Keys).