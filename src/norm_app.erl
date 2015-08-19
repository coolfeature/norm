-module(norm_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, ensure_started/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
  ensure_started([asn1,crypto,public_key,ssl,epgsql]),
  norm_sup:start_link().

stop(_State) ->
  ok.

ensure_started(Apps) when is_list(Apps) ->
  lists:map(fun(App) -> ensure_started(App) end,Apps);
ensure_started(App) when is_atom(App) ->
  case application:start(App) of
    ok -> ok;
    {error,{already_started,App}} -> ok
  end.
