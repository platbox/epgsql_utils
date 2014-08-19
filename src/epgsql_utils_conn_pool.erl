-module(epgsql_utils_conn_pool).

%% API
-export([child_spec  /2]).
-export([acquire_conn/1]).
-export([release_conn/2]).
-export_types([options/0]).

%% Supervisor callbacks
-export([init/1]).

%% poolboy worker callbacks
-export([start_link/1]).

%%
%% API
%%
-type conn_param() ::
      {hostname, string()}
    | {username, string()}
    | {password, string()}
    | {database, string()}
.
-type conn_params() :: list(conn_param()).

-type option() ::
      {size        , pos_integer()}
    | {max_overflow, pos_integer()}
    | {conn_params , conn_params()}
.
-type options() :: list(option()).


-spec child_spec(atom() ,options()) -> supervisor:child_spec().
child_spec(PoolName, Options) ->
    {epgsql_utils_supervisor2, {epgsql_utils_supervisor2, start_link, [?MODULE, {PoolName, Options}]},
        permanent, infinity, supervisor, [epgsql_utils_supervisor2]}.

acquire_conn(PoolName) ->
    poolboy:checkout(PoolName).

release_conn(PoolName, C) ->
    poolboy:checkin(PoolName, C).

%%
%% Supervisor callbacks
%%
init({PoolName, Options}) ->
    Size        = proplists:get_value(size        , Options, 10),
    MaxOverflow = proplists:get_value(max_overflow, Options, 5 ),
    ConnParams  = proplists:get_value(conn_params , Options, []),
    PoolArgs  = [
        {name         , {local, PoolName}},
        {worker_module, ?MODULE            },
        {size         , Size               },
        {max_overflow , MaxOverflow        }
    ],
    PoolSpec = {_Ref, _, Strategy, _, _, _} = poolboy:child_spec(PoolName, PoolArgs, ConnParams),
    TunedPoolSpec = setelement(3, PoolSpec, {Strategy, 0}),
    {ok, {{one_for_one, 2, 2}, [TunedPoolSpec]}}.


%%
%% Poolboy worker callbacks
%%
start_link(ConnParams) ->
    R = pgsql:connect(
            proplists:get_value(hostname, ConnParams, "localhost"),
            proplists:get_value(username, ConnParams, "test"     ),
            proplists:get_value(password, ConnParams, "test"     ),
            [
                {port, proplists:get_value(port, ConnParams, 5432)},
                {database, proplists:get_value(database, ConnParams, "test")}
            ]
        ),
    case R of
        {ok, Pid} ->
            %% TODO callback here
            {ok, Pid};
        Error={error, _} ->
            Error
    end.
