%%
%% epgsql_utils_setup_tests

-module(epgsql_utils_setup_tests).
-compile({parse_transform, epgsql_utils_model}).

-include_lib("eunit/include/eunit.hrl").
-include_lib("epgsql_utils/include/epgsql_utils_model.hrl").

-export([db_setup_fun/2]).

%%

db_setup_test_() ->
    {setup, 
        spawn,
        fun () ->
            Apps = [lager],
            _ = [application:ensure_all_started(A) || A <- Apps],
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
                ?_test(begin epgsql_utils_querying:do(?MODULE, T) end) || T <- [fun test_setup/0]
            ]
        end
    }.


options() -> 
    [{conn_params, [
        {db_setup_fun, {?MODULE, db_setup_fun, []}}
    ]}].

db_setup_fun(Connection, Args) ->
    pgsql:squery(Connection, "set timezone to 'Pacific/Kiritimati';").

test_setup() ->
    [{<<"Pacific/Kiritimati">>}] = epgsql_utils_querying:do_query("SELECT current_setting('TIMEZONE') TZ;").



