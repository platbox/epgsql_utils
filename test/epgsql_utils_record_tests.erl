%%
%% epgsql_utils_record tests

-module(epgsql_utils_record_tests).

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

get_keys_test() ->
    Record = #dancer{id = 99, hobby = games},
    Keys = epgsql_utils_record:get_keys({?MODULE, dancer}, Record),
    ?assertEqual([99], Keys).
