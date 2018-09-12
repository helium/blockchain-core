FROM erlang:21.0

# Install deps
WORKDIR /tmp
RUN apt-get update
RUN apt-get install -y flex bison libgmp-dev cmake

WORKDIR /opt/blockchain-core

## Fetch / Compile deps
ADD rebar.config rebar.config
ADD rebar.lock rebar.lock
ADD rebar3 rebar3
ADD Makefile Makefile
RUN make

## Add / Compile source
ADD src/ src/
ADD test/ test/
RUN make

CMD ["rebar3", "shell"]
