.PHONY: compile test typecheck

REBAR=./rebar3

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

test: compile
	$(REBAR) as test do eunit, ct,xref && $(REBAR) dialyzer

ci: compile
	$(REBAR) do dialyzer,xref && $(REBAR) as test do eunit,ct

typecheck:
	$(REBAR) dialyzer

cover:
	$(REBAR) cover
