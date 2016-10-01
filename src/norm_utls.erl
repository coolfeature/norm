-module(norm_utls).

%%-export([
%%  get_module/0
%%  ,get_module/1
%%  ,get_db_config/2
%%  ,get_db_config/3
%%  ,get_config/1
%%  ,get_value/3
%%  ,remove_dups/1
%%  ,format_time/2
%%  ,format_date/2
%%  ,format_datetime/2
%%  ,date_to_erlang/2
%%  ,time_to_erlang/2
%%  ,datetime_to_erlang/2
%%  ,bin_to_num/1
%%  ,num_to_bin/1
%%  ,root_dir/0
%%  ,enabled_dbs/0
%%  ,format_calltime/1
%%]).

-compile(export_all).
-define(APP,norm).

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

%% -------
%% -- NORM  
%% -------

root_dir() ->
  {ok,Path} = file:get_cwd(),
  Path.

get_module(Db) ->
  Prefix = atom_to_list(?APP) ++ "_",
  case string:str(atom_to_list(Db),Prefix) of
    0 -> common_utils:ensure_atom(Prefix ++ atom_to_list(Db));
    1 -> Db;
    _ -> unknown_module
  end.

get_module() ->
  M = case get_config(default_db) of
    undefined -> {Db,_} = lists:nth(1,get_config(dbs)), Db;
    Module -> Module
  end,
  common_utils:ensure_atom(atom_to_list(?APP) ++ "_" ++ atom_to_list(M)).

enabled_dbs() ->
  lists:foldl(fun({Name,_Conf},Acc) -> 
    Acc ++ [Name] 
  end, [], norm_utils:get_config(dbs)).


models(Db) ->
  ModelsModule = get_config(models),
  try ModelsModule:Db() catch _:_ -> #{} end.

get_db_config(Db,Key,Default) ->
  case get_db_config(Db,Key) of
    undefined -> Default;
    Val -> Val
  end.

get_db_config(Db,Key) ->
  DbConfig = common_utils:get_values(Db,get_config(dbs),[]),
  common_utils:get_values(Key,DbConfig,undefined).

set_db_config(Db,Key,Val) ->
  DbConfig = common_utils:get_values(Db,get_config(dbs),[]),
  DbConfigUpdated = lists:keyreplace(tablespace,1,DbConfig,{tablespace,Val}), 
  NewDbs = lists:keyreplace(Key,1,DbConfig,{Key,DbConfigUpdated}),
  application:set_env(?APP,dbs,NewDbs).

get_config(Key) ->
  case application:get_env(?APP,Key) of
    {ok,Value} -> Value;
    undefined -> undefined 
  end.

model_name(Model) ->
  Meta = maps:get(<<"__meta__">>,Model),
  maps:get(<<"name">>,Meta).

model_keys(Model) ->
  Keys = maps:keys(Model),
  lists:delete(<<"__meta__">>,Keys). 

model_type(Field,Model) ->
  Meta = maps:get(<<"__meta__">>,Model),
  MetaFields = maps:get(<<"fields">>,Meta),
  FieldMeta = maps:get(Field,MetaFields),
  maps:get(<<"type">>,FieldMeta).

maybe_add_meta(Model) ->
  maybe_add_meta(Model,get_module()).
maybe_add_meta(Model,DbName) ->
  Meta = maps:get(<<"__meta__">>,Model,#{}),
  Name = maps:get(<<"name">>,Meta,undefined),
  case Name of
    undefined -> {error,Model};
    _ -> 
      Fields = maps:get(<<"fields">>,Meta,undefined),
      if Fields =:= undefined -> 
        DbModels = models(DbName),
        case maps:get(Name,DbModels,undefined) of
          undefined -> {error,Model};
          ModelSpec ->
            FieldSpec = maps:get(<<"fields">>,ModelSpec,undefined), 
            if FieldSpec =:= undefined -> {error,Model};
            true ->
              NewMeta = maps:put(<<"fields">>,FieldSpec,Meta),
              {ok,maps:put(<<"__meta__">>,NewMeta,Model)}
            end
        end;
      true -> {ok,Model} end
  end.
  
%% ------------
%% -- UTILITIES 
%% ------------

remove_dups([]) -> 
  [];
remove_dups([H|T]) -> 
  [H | [X || X <- remove_dups(T), X /= H]].

%% -------------
%% -- CONVERTERS 
%% -------------

bin_to_num(Bin) ->
  N = binary_to_list(Bin),
  case string:to_float(N) of
    {error,no_float} -> list_to_integer(N);
    {F,_Rest} -> F
  end.

num_to_bin(Num) when is_float(Num) ->
  float_to_binary(Num);
num_to_bin(Num) when is_integer(Num) ->
  list_to_binary(integer_to_list(Num)).

val_to_bin(V) when is_atom(V) andalso V /= undefined ->
  atom_to_bin(V);
val_to_bin(V) when is_float(V) ->
  val_to_bin({V,2});
val_to_bin({V,Dec}) when is_float(V) ->
  float_to_binary(V,[{decimals,Dec}]);
val_to_bin(V) when is_integer(V) ->
  num_to_bin(V);
val_to_bin(V) when is_list(V) ->
  list_to_binary(V);
val_to_bin(V) when is_binary(V) ->
  V.

concat_bin(List) ->
  erlang:iolist_to_binary(List).

bin_to_atom(Bin) ->
  binary_to_atom(Bin,'utf8').

atom_to_bin(Atom) ->
  atom_to_binary(Atom,'utf8').

%% -------------
%% -- FORMATTERS 
%% -------------

format_calltime(Time) ->
  Float = Time / 1000000,
  float_to_list(Float,[{decimals,3},compact]).

quote(Bin) ->
  concat_bin([<<"'">>,Bin,<<"'">>]).

%% @todo Get rid of strings - consider using dh_date

format_time({_,{Hour,Min,Sec}},'iso8601') ->
  Str = lists:flatten(io_lib:format("~2..0B:~2..0B:~2..0B", [Hour,Min,Sec])),
  val_to_bin(Str).

%% @todo Get rid of strings - consider using dh_date

format_date({{Year,Month,Day},_},'iso8601') ->
  Str = lists:flatten(io_lib:format("~4..0B-~2..0B-~2..0B", [Year,Month,Day])),
  val_to_bin(Str).

%% @todo Get rid of strings - consider using dh_date

format_datetime(DateTime,Format) ->
  Date = format_date(DateTime,Format),
  Time = format_time(DateTime,Format),
  val_to_bin(concat_bin([Date," ",Time])).

%% @todo Get rid of strings - consider using dh_date

date_to_erlang(Date,'iso8601') ->
  [Y,M,D] = re:split(Date,"-",[{return,list}]),
  {list_to_integer(Y),list_to_integer(M),list_to_integer(D)}.
   
%% @todo Get rid of strings - consider using dh_date

time_to_erlang(Time,'iso8601') ->
  HMS = case string:rchr(Time, $.) of 0 -> Time; _Dotted ->
  [HhMmSs,_SS] = re:split(Time,"\\.",[{return,list}]),HhMmSs end,
  [H,M,S] = re:split(HMS,":",[{return,list}]),
  {list_to_integer(H),list_to_integer(M),list_to_integer(S)}.

%% @todo Get rid of strings - consider using dh_date

datetime_to_erlang(DateTimeBin,Format) ->
  [Date,Time] = re:split(DateTimeBin," ",[{return,list}]),
  {date_to_erlang(Date,Format),time_to_erlang(Time,Format)}.
