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
  M = norm_utls:get_module(),
  init(M).

new(Name) ->
  M = norm_utls:get_module(),
  M:new(Name).

save(Model) ->
  FullModel = norm_utls:maybe_add_meta(Model),
  M = norm_utls:get_module(),
  M:save(FullModel).

find(Name,Predicates) ->
  M = norm_utls:get_module(),
  M:find(Name,Predicates).

remove(Model) ->
  FullModel = norm_utls:maybe_add_meta(Model),
  M = norm_utls:get_module(),
  M:remove(FullModel).

models() ->
  M = norm_utls:get_module(),
  M:models().

%% ----------------------------------------------------------------------------

init(DbName) ->
  M = norm_utls:get_module(DbName),
  M:init().

new(DbName,Name) ->
  M = norm_utls:get_module(DbName),
  M:new(Name).

save(DbName,Model) ->
  FullModel = norm_utls:maybe_add_meta(Model,DbName),
  M = norm_utls:get_module(DbName),
  M:save(FullModel).

find(DbName,Name,Predicates) ->
  M = norm_utls:get_module(DbName),
  M:find(Name,Predicates).

remove(DbName,Model) ->
  FullModel = norm_utls:maybe_add_meta(Model,DbName),
  M = norm_utls:get_module(DbName),
  M:remove(FullModel).

models(DbName) ->
  M = norm_utls:get_module(DbName),
  M:models().

 
