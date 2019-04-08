.PHONY: compile test typecheck

REBAR=./rebar3

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

test: compile
	$(REBAR) as test do eunit, ct,xref && $(REBAR) dialyzer

ci: compile
	$(REBAR) as test do eunit,ct,xref && $(REBAR) dialyzer 2>&1 | tee build.log | sed 's/^\(\x1b\[[0-9;]*m\)*>>>/---/'

typecheck:
	$(REBAR) dialyzer

cover:
	$(REBAR) cover
