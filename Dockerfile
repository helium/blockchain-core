FROM erlang:21.0

# Install deps
WORKDIR /tmp
ENV LD_LIBRARY_PATH /usr/local/lib
RUN apt-get update
RUN apt-get install -y flex bison libgmp-dev cmake
RUN git clone -b stable https://github.com/jedisct1/libsodium.git
RUN cd libsodium && ./configure && make check && make install && cd ..

WORKDIR /opt/blockchain-core

## Fetch / Compile deps
ADD rebar.config rebar.config
ADD rebar.lock rebar.lock
ADD rebar3 rebar3
ADD Makefile Makefile
RUN make

## Add / Compile source
ADD include/ include/
ADD src/ src/
ADD test/ test/
RUN make

CMD ["rebar3", "shell"]
