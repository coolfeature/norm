
-module(norm_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
  Dbs = norm_utls:get_config(dbs), 
  Pools = if Dbs /= undefined -> start_pools(Dbs);
    true -> []
  end,
  InitResults = run_init(Dbs),
  norm_log:log_term(debug,InitResults),
  {ok,{{one_for_one, 5,10},Pools}}.

%% @doc Starts connection pool

start_pools(Dbs) ->
  lists:foldl(fun({Name,_Config},Acc) -> 
    Pools = norm_utls:get_db_config(Name,pools),
    if is_list(Pools) =:= true ->
      io:fwrite("Starting pool for ~p~n",[Name]),
      Acc ++ start_db_pools(Name,Pools);
    true -> Acc ++ [] end
  end,[],Dbs).

start_db_pools(Db,Pools) ->
  WorkerName = list_to_atom(atom_to_list(norm) ++ "_" 
    ++ atom_to_list(Db) ++ "_worker"),
  lists:map(fun({Name,Args}) ->
    SizeArgs = norm_utls:get_value(pool_size,Args,undefined),
    WorkerArgs = norm_utls:get_value(worker_args,Args,undefined),
    PoolArgs = [{name, {local, Name}},
      {worker_module, WorkerName}] ++ SizeArgs,
      poolboy:child_spec(Name,PoolArgs,WorkerArgs)
    end,Pools).

run_init(Dbs) ->
  lists:foldl(fun({Name,Config},Acc) ->
    InitResult = case norm_utls:get_value(init,Config,false) of
      true -> norm:init(Name);
      _ -> {Name,no_init}
    end,  
    Acc ++ [InitResult]
  end,[],Dbs).

