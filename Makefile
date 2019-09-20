.PHONY: compile test typecheck ci

REBAR=./rebar3

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

test: compile
	$(REBAR) as test do eunit, ct,xref && $(REBAR) dialyzer

typecheck:
	$(REBAR) dialyzer

cover:
	$(REBAR) cover

ci:
	$(REBAR) dialyzer && $(REBAR) as test do eunit,ct,cover
	$(REBAR) covertool generate
	codecov --required -f _build/test/covertool/blockchain.covertool.xml

ci-nightly:
	$(REBAR) as test do eunit,ct,eqc -t 600,cover
	cp _build/eqc/cover/eqc.coverdata _build/test/cover/
	$(REBAR) covertool generate
	codecov --required -f _build/test/covertool/blockchain.covertool.xml
