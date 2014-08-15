%%
%% epgsql_utils_model tests

-module(epgsql_utils_model_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("epgsql_utils/include/epgsql_utils_model.hrl").
-compile({parse_transform, epgsql_utils_model}).

-export([pack/3]).
-export([unpack/3]).

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
pack(hobby, {Type, Value}, context) ->
     [atom_to_list(Type), ":", Value];
pack(T, V, context) ->
    pack_record(T, V, context).

unpack(hobby, V, context) ->
    V1 = iolist_to_binary(V),
    [H1, H2] = binary:split(V1, <<":">>),
    {binary_to_atom(H1, utf8), H2};
unpack(T, V, context) ->
    unpack_record(T, V, context).
%%

struct_info_test() ->
    ?assertEqual({test, dancers}, ?MODULE:struct_info({dancer, table})),
    ?assertEqual([id], ?MODULE:struct_info({dancer, keys})),
    ?assertEqual({epgsql_utils_orm, non_neg_integer},
                  proplists:get_value(age, ?MODULE:struct_info({dancer, fields}))
    ).

pack_external_type_test() ->
    Record = #dancer{id = 42, hobby = {hooker, <<"Tracey">>}},
    Packed = epgsql_utils_orm:pack({?MODULE, dancer}, Record, context),
    ?assertEqual(Record, epgsql_utils_orm:unpack({?MODULE, dancer}, Packed, context)).
