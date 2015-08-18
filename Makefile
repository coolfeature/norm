REBAR = ./rebar
DEPS = ./deps/*/ebin

.PHONY: all get-deps test clean compile build-plt dialyze

all: deps compile

compile:
	$(REBAR) compile

test:
	export ERL_FLAGS="-config test/norm_test"; $(REBAR) skip_deps=true eunit

clean:
	@$(REBAR) clean

get-deps: $(REBAR)
	@$(REBAR) get-deps
	@$(REBAR) compile

deps: 
	@$(REBAR) get-deps
	@$(REBAR) compile

build-plt:
	@$(REBAR) build-plt

dialyze: compile
	@$(REBAR) dialyze

