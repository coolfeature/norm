-module(norm_behaviour).

%% ----------------------------------------------------------------------------

%% @doc Sets up the environment and starts the database.
-callback init() -> 'ok' | { 'error', Reason :: any() }.

%% @doc Returns a map representing a model.
-callback new( Name :: atom() ) -> 'undefined' | map().

%% @doc Does an upset - inserts Model or updates existing record with the new 
%% Model.
-callback save( Model :: map() ) -> { 'ok', any() } | { 'error', any() }.

%% @doc Finds and returns a record.
-callback find( Name :: atom(), Predicates :: any() ) -> list(). 

%% @doc Deletes record using an id.
-callback remove( Model :: map() ) -> { 'ok', any() } | { 'error', any() }.

%% @doc Return a list of model names.
-callback models() -> 'undefined' | list().
