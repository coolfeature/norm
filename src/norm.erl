-module(norm).

-export([
  init/0
  ,new/1
  ,save/1
  ,find/2
  ,remove/1
  ,models/0
]).

-export([
  init/1
  ,new/2
  ,save/2
  ,find/3
  ,remove/2
  ,models/1
]).

-behaviour(norm_behaviour).

%% ----------------------------------------------------------------------------
%% -------------------------- BEHAVIOUR CALLBACKS -----------------------------
%% ----------------------------------------------------------------------------

init() ->
  init(norm_utls:get_config(default_db)).

new(Name) ->
  new(norm_utls:get_config(default_db),Name).

save(Model) ->
  save(norm_utls:get_config(default_db),Model).

find(Name,Predicates) ->
  find(norm_utls:get_config(default_db),Name,Predicates).

remove(Model) ->
  remove(norm_utls:get_config(default_db),Model).

models() ->
  models(norm_utls:get_config(default_db)).

%% ----------------------------------------------------------------------------

init(DbName) ->
  M = norm_utls:get_module(DbName),
  M:init().

new(DbName,Name) ->
  M = norm_utls:get_module(DbName),
  M:new(Name).

save(DbName,Model) ->
  case norm_utls:maybe_add_meta(Model,DbName) of
    {ok,FullModel} ->
      M = norm_utls:get_module(DbName),
      M:save(FullModel);
    {error,Model} -> 
      {error,<<"Missing __meta__ data.">>}
  end.

find(DbName,Name,Predicates) ->
  M = norm_utls:get_module(DbName),
  M:find(Name,Predicates).

remove(DbName,Model) ->
  case norm_utls:maybe_add_meta(Model,DbName) of
    {ok,FullModel} ->
      M = norm_utls:get_module(DbName),
      M:remove(FullModel);
    {error,Model} ->
      {error,<<"Missing __meta__ data.">>}
  end.

models(DbName) ->
  M = norm_utls:get_module(DbName),
  M:models().

 
