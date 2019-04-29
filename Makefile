.PHONY: compile test typecheck ci

REBAR=./rebar3

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

test: compile
	$(REBAR) as test do eunit, ct,xref && $(REBAR) dialyzer

ci:
	$(REBAR) dialyzer && $(REBAR) as test do eunit,ct,cover
	$(REBAR) covertool generate
	codecov -f _build/test/covertool/blockchain.covertool.xml

typecheck:
	$(REBAR) dialyzer

cover:
	$(REBAR) cover
