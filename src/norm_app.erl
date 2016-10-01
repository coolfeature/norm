-module(norm_app).

-behaviour(application).

%% Application callbacks
-export([
	start/2
	, stop/1
]).

%% ============================================================================
%% Application callbacks
%% ============================================================================

start(_StartType, _StartArgs) ->
	common_utils:ensure_started([
		asn1
		, crypto
		, public_key
		, ssl
		, epgsql
	]),
	Return = norm_sup:start_link(),
	Dbs = norm_utls:get_config(dbs), 
	InitResults = run_init(Dbs),
	log:debug("DB init results ~p~n",[InitResults]),
	Return.

stop(_State) ->
	ok.

%% ----------------------------------------------------------------------------

run_init(undefined) ->
	{error,no_dbs};
run_init(Dbs) ->
	lists:foldl(fun({Name,Config},Acc) ->
		InitResult = case common_utils:get_value(init, Config, false) of
			true -> 
				norm:init(Name);
			_ -> 
				{Name, no_init}
		end,  
 		Acc ++ [InitResult]
	end,[],Dbs).

