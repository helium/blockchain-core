.PHONY: compile test typecheck ci
grpc_services_directory=src/grpc/autogen

REBAR=./rebar3

compile: | $(grpc_services_directory)
	$(REBAR) compile

clean:
	rm -rf $(grpc_services_directory)
	$(REBAR) clean

test: compile
	$(REBAR) as test do eunit, ct,xref && $(REBAR) dialyzer

test_eunit_txns:
	$(REBAR) as test do eunit --module=$(shell find src/transactions/{v1,v2} -name 'blockchain_txn*.erl' -exec grep -l '-behavior(blockchain_txn)' '{}' \; -print0 | xargs -0 -I% basename -s .erl % | xargs | sed 's/\s\+/,/g')

typecheck:
	$(REBAR) dialyzer

cover:
	$(REBAR) cover

ci: | $(grpc_services_directory)
	$(REBAR) dialyzer && $(REBAR) do eunit, ct
	$(REBAR) do cover,covertool generate
	codecov --required -f _build/test/covertool/blockchain.covertool.xml

ci-nightly: | $(grpc_services_directory)
	$(REBAR) do eunit,ct,eqc -t 600
	cp -f _build/eqc/cover/eqc.coverdata _build/test/cover/
	$(REBAR) do cover,covertool generate
	codecov --required -f _build/test/covertool/blockchain.covertool.xml

grpc: | $(grpc_services_directory)
	@echo "generating grpc services"
	REBAR_CONFIG="config/grpc_server_gen.config" $(REBAR) grpc gen
	REBAR_CONFIG="config/grpc_client_gen.config" $(REBAR) grpc gen

clean_grpc:
	@echo "cleaning blockchain core grpc services"
	rm -rf $(grpc_services_directory)

$(grpc_services_directory):
	@echo "blockchain core grpc service directory $(directory) does not exist"
	$(REBAR) get-deps

