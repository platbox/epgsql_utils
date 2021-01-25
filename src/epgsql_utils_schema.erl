-module(epgsql_utils_schema).

-export([prepare/2]).
-export([upgrade/2]).
-export([drop   /1]).


%% TODO:
%%  - downgrade;
%%  - orm or sql dsl using insead of plain sql;
%%  - min version > 1;
%%  - ability to have a intervals between versions (1,2,3,5,6...) (???).

%% schema managment API
prepare(Name, MigrateMFAs) ->
    case is_exist(Name) of
        false -> q([<<"CREATE SCHEMA ">>, Name, <<";">>]);
        true  -> ok
    end,
    case is_initialized(Name) of
        false -> init(Name);
        true -> ok
    end,
    upgrade(Name, MigrateMFAs).

drop(Name) ->
    case is_exist(Name) of
        false -> ok;
        true  -> q([<<"DROP SCHEMA ">>, Name, <<" CASCADE;">>])
    end.

upgrade(Name, MigrateMFAs) ->
    upgrade(Name, MigrateMFAs, last_rev(MigrateMFAs)).
upgrade(Name, MigrateMFAs, N) ->
    case get_rev(Name) of
        N ->
            ok;
        CurrentRev ->
            [upgrade_(MigrateMFAs, Rev) || Rev <- lists:seq(CurrentRev + 1, N)],
            set_rev(Name, N)
    end.


%%
%% local
%%
is_exist(Name) ->
    Q = [<<"SELECT true FROM information_schema.schemata WHERE schema_name = '">>, Name, <<"';">>],
    case q(Q) of
        [{true}] -> true;
        [      ] -> false
    end.

init(Name) ->
    q([<<"CREATE TABLE ">>, Name, <<".schema_rev(rev integer);">>]),
    q([<<"INSERT INTO ">>, Name, <<".schema_rev(rev) VALUES(0);">>]).

is_initialized(Name) ->
    Q = [<<"SELECT true FROM pg_tables WHERE tablename = 'schema_rev' AND schemaname = '">>, Name, <<"';">>],
    case q(Q) of
        [{true}] -> true;
        [      ] -> false
    end.

get_rev(Name) ->
    case q([<<"SELECT rev FROM ">>, Name, <<".schema_rev;">>]) of
        [{Rev}] ->
            Rev;
        [] ->
            1 = q([<<"INSERT INTO ">>, Name, <<".schema_rev(rev) VALUES(0);">>]),
            0
    end.


set_rev(Name, Rev) ->
    1 = q([<<"UPDATE ">>, Name, <<".schema_rev SET rev=$1;">>], [Rev]).

upgrade_(MigrateMFAs, N) ->
    MFA = get_mfa(MigrateMFAs, N),
    apply_mfa(MFA).

get_mfa(MigrateMFAs, N) ->
    proplists:get_value(N, MigrateMFAs).

apply_mfa({M, F, A})                    -> erlang:apply(M, F, A);
apply_mfa(Fun) when is_function(Fun, 0) -> Fun().

last_rev(MigrateMFAs) ->
    lists:max(element(1, lists:unzip(MigrateMFAs))).

q(Q) ->
    q(Q, []).
q(Q, A) ->
    epgsql_utils_querying:do_query(Q, A).
