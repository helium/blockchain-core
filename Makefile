.PHONY: compile test typecheck ci
grpc_services_directory=src/grpc/autogen

REBAR=./rebar3

compile: | $(grpc_services_directory)
	$(REBAR) compile

clean:
	$(REBAR) clean

test: compile
	$(REBAR) as test do eunit, ct,xref && $(REBAR) dialyzer

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

grpc:
	REBAR_CONFIG="config/grpc_server_gen.config" $(REBAR) grpc gen; \
	REBAR_CONFIG="config/grpc_client_gen.config" $(REBAR) grpc gen

$(grpc_services_directory):
	@echo "grpc service directory $(directory) does not exist, will generate services"
	$(REBAR) get-deps;mkdir -p _build/default/lib/blockchain/ebin;$(MAKE) grpc
