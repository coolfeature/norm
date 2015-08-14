-module(norm_pgsql).

%-export([
%  init/0
%  ,models/0
%  ,save/1
%  ,delete/1
%]).
%
%-export([
%  squery/1
%  ,squery/2
%  ,equery/2
%  ,equery/3
%]).
%
%-export([
%  transaction/0
%  ,commit/0
%  ,rollback/0
%]).
%
%-export([
%  get_schema/0
%  ,get_pool/0
%]).

-compile(export_all).
-include("include/pgsql.hrl").

-define(SQUERY(Sql),squery(Sql)).
-define(EQUERY(Sql,Args),equery(Sql,Args)).
-define(SCHEMA,get_schema()).
-define(POOL,get_pool()).
-define(LOG(Level,Term),norm_log:log_term(Level,Term)).


%% ----------------------------------------------------------------------------
%% --------------------------------- API --------------------------------------
%% ----------------------------------------------------------------------------

save(Model) ->  
  Model.

delete(Model) ->
  Model.

init() ->
  try
    {ok,start} = transaction()
    %%,drop_schema(?SCHEMA)
    ,{ok,commit} = commit()
  catch Error:Reason ->
    Rollback = rollback(),
    ?LOG(info,[Error,Reason,Rollback])
  end.

models() -> 
  PgSql = #'pgsql'{},
  maps:keys(PgSql#'pgsql'.'models').

%% -------------------------------- TABLESPACE --------------------------------

%% @doc Drop tablespace schema.

drop_schema(Schema) ->
  case schema_exists(Schema) of {ok,true} -> 
    case ?SQUERY(sql_drop_schema(Schema)) of
      {ok,_,_} -> {ok,{Schema,dropped}}; 
      Error -> {error,{Schema,Error}}
    end; 
  {ok,false} -> {ok,{Schema,not_exists}} end.

sql_drop_schema(Schema) ->
  sql_drop_schema(Schema,[{cascade,true}]).
sql_drop_schema(Schema,Opts) ->
  concat([<<"DROP SCHEMA ">>,options_to_sql({options,ifexists},Opts),<<" ">>,
  Schema,<<" ">>,options_to_sql({options,cascade},Opts),<<";">>]).

%% @doc Create tablespace schema.

create_schema(Schema) ->
  case schema_exists(Schema) of {ok,false} -> 
    case ?SQUERY(sql_create_schema(Schema)) of
      {ok,_,_} -> {ok,{Schema,created}}; 
      Error -> {error,{Schema,Error}}
    end; 
  {ok,true} -> {ok,{Schema,exists}} end.
    
sql_create_schema(Schema) ->
  sql_create_schema(Schema,[]).
sql_create_schema(Schema,Op) ->
  concat([ <<"CREATE SCHEMA ">>,options_to_sql({options,ifexists},Op),
    <<" ">>,Schema,<<";">> ]).

schema_exists(Schema) ->
  case ?SQUERY(sql_schema_exists(Schema)) of
    {ok,_ColInfo,[{SchemaBin}]} when is_binary(SchemaBin) -> {ok,true};
    {ok,_ColInfo,[]} -> {ok,false};
    Error -> {error,Error}
  end.

sql_schema_exists(Schema) ->
  concat([ <<"SELECT schema_name FROM information_schema.schemata WHERE schema"
  "_name = '">>,Schema,<<"';">> ]).

%% ---------------------------------- CREATE ----------------------------------

create_tables() ->
  PgSql = #'pgsql'{}, 
  ModelMap = PgSql#'pgsql'.'models',
  Results = lists:foldl(fun(Key,Acc) -> 
    Acc ++ [create_table(Key,maps:get(Key,ModelMap))]
  end,[],maps:keys(ModelMap)),
  {ok_error(Results),Results}.

create_table(Name,Spec) when is_map(Spec) ->
  case ?SQUERY(sql_create_table(Name,Spec)) of
    {ok,_,_} -> {ok,{Name,created}};
    Error -> {error,{Name,Error}}
  end.

sql_create_table(Name,Spec) ->
  sql_create_table(Name,Spec,[{ifexists,false}]).

sql_create_table(Name,Spec,Options) ->
  Fields = field_to_sql(Spec),
  Constraints = constraint_to_sql(Spec),
  concat([<<"CREATE TABLE ">>,options_to_sql({options,ifexists},Options),
    atom_to_binary(Name),<<" ( ">>,Fields,Constraints,<<" )\n">>]).
 
field_to_sql(TableSpec) ->
  lists:foldl(fun(Key,Acc) ->
    FieldSpec = maps:get(Key,TableSpec),
    FldSqlLine = field_to_sql(Key,FieldSpec),     
    concat([ Acc,FldSqlLine,<<"\n">> ]) 
  end,<<"">>,maps:keys(TableSpec)).

 %% @doc Get something like 'customer varchar(50) not null'

field_to_sql(Name,FldSpec) ->
  Type = maps:get('type',FldSpec,undefined),
  TypeLine = case Type of
    'varchar' ->
      concat([<<" VARCHAR(">>,maps:get('length',FldSpec,50),<<")">>]);
    'bigserial' ->
      <<" BIGSERIAL ">>;
    'date' ->
      <<" DATE ">>;
    'bigint' -> 
      <<" BIGINT ">>;
    undefined -> 
      <<"">>
  end,
  Null = case maps:get('null',FldSpec,undefined) of
    undefined -> <<" NULL ">>;
    false -> <<" NOT NULL ">>;
    true -> <<" NULL ">>
  end,
  concat([atom_to_binary(Name),<<" ">>,TypeLine,Null,<<",\n">>]).

%% @doc

constraint_to_sql(TableSpec) ->
  TableConstraints = maps:get('constraints',TableSpec,#{}),
  lists:foldl(fun(Key,Acc) ->
    Constraints = maps:get(Key,TableConstraints,undefined),
    ConstraintSql = constraint_to_sql(Key,Constraints),
    concat([ Acc,ConstraintSql ])
  end,<<"">>,maps:keys(TableConstraints)).

constraint_to_sql('pk',Constraints) ->
  lists:foldl(fun(Constraint,Acc) ->
   Name = maps:get('name',Constraint,undefined),
   ConName = if Name =:= undefined -> <<"">>; true -> atom_to_binary(Name) end,
   Fields = lists:foldl(fun(Field,AccF) ->
       concat([ AccF,atom_to_binary(Field),<<",">>])
     end,<<"">>,maps:get('fields',Constraint,[])),
   concat([ Acc,<<"CONSTRAINT ">>,ConName,<<" PRIMARY KEY (">>,Fields,<<")\n">> ])
  end,<<"">>,Constraints);

constraint_to_sql('fk',Constraints) ->
  lists:foldl(fun(Constraint,Acc) ->
    Name = maps:get('name',Constraint,<<"">>),
    Fields = lists:foldl(fun(Field,AccF) ->
        concat([ AccF,atom_to_binary(Field),<<",">>])
      end,<<"">>,maps:get('fields',Constraint,[])),
    References = maps:get('references',Constraint,[]),
    RefTable = atom_to_binary(maps:get('table',References)),
    RefFields = lists:foldl(fun(Field,AccRF) ->
        concat([ AccRF,atom_to_binary(Field),<<",">>])
      end,<<"">>,maps:get('fields',References,[])),
    Options = maps:get('options',Constraint,<<"">>),
    concat([ Acc,<<"CONSTRAINT ">>,Name,<<" FOREIGN KEY (">>,Fields,
      <<") REFERENCES ">>,RefTable,<<" (">>,RefFields,<<") ">>,Options,
      <<"\n">> ])
  end,<<"">>,Constraints).


%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

squery(Sql) ->
  squery(?POOL,Sql).
squery(PoolName,Sql) ->
  ?LOG(debug,Sql).
%  poolboy:transaction(PoolName,fun(Worker) ->
%    gen_server:call(Worker,{squery,Sql})
%  end).

equery(Sql,Params) ->
  equery(?POOL,Sql,Params).
equery(PoolName,Sql,Params) ->
  ?LOG(debug,[Sql,Params]),
  poolboy:transaction(PoolName,fun(Worker) ->
    gen_server:call(Worker,{equery,Sql,Params})
  end).


%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

transaction() ->
  {ok,start}.

commit() ->
  {ok,commit}.

rollback() ->
  {ok,rollback}.

%% ----------------------------------------------------------------------------

options_to_sql({options,cascade},true) ->
  <<"CASCADE">>;
options_to_sql({options,ifexists},true) ->
  <<"IF EXISTS">>; 
options_to_sql({options,ifexists},false) ->
  <<"IF NOT EXISTS">>;
options_to_sql({options,Key},Options) when is_list(Options) ->
  options_to_sql({options,Key},proplists:get_value(Key,Options));
options_to_sql({options,_},undefined) ->
  [].

%% ----------------------------------------------------------------------------
%% ----------------------------- UTILITIES ------------------------------------
%% ----------------------------------------------------------------------------

get_schema() ->
  case norm_utls:get_db_config(pgsql,tablespace) of
    undefined -> norm;
    Schema -> Schema
  end.

get_pool() ->
  Pools = norm_utls:get_db_config(pgsql,pools),
  {Name,_} = lists:nth(1,Pools),
  Name.

concat(List) ->
  erlang:iolist_to_binary(List).

atom_to_binary(Atom) ->
  atom_to_binary(Atom,'utf8').

ok_error([{ok,_}|Results]) ->
  ok_error(Results);
ok_error([]) ->
  ok;
ok_error([{error,_}|_Results]) ->
  error.
