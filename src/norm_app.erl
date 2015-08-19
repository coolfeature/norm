-module(norm_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, ensure_started/1]).

%% ============================================================================
%% Application callbacks
%% ============================================================================

start(_StartType, _StartArgs) ->
  ensure_started([asn1,crypto,public_key,ssl,epgsql]),
  Return = norm_sup:start_link(),
  Dbs = norm_utls:get_config(dbs), 
  InitResults = run_init(Dbs),
  norm_log:log_term(debug,InitResults),
  Return.

stop(_State) ->
  ok.

%% ----------------------------------------------------------------------------

ensure_started(Apps) when is_list(Apps) ->
  lists:map(fun(App) -> ensure_started(App) end,Apps);
ensure_started(App) when is_atom(App) ->
  case application:start(App) of
    ok -> ok;
    {error,{already_started,App}} -> ok
  end.

run_init(undefined) ->
  {error,no_dbs};
run_init(Dbs) ->
  lists:foldl(fun({Name,Config},Acc) ->
    InitResult = case norm_utls:get_value(init,Config,false) of
      true -> norm:init(Name);
      _ -> {Name,no_init}
    end,  
    Acc ++ [InitResult]
  end,[],Dbs).

