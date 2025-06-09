build_dir := "build"
example_bin := "bin/server"

build:
    rm -rf {{ build_dir }} && \
    mkdir -p {{ build_dir }} && \
    cd {{ build_dir }} && \
    cmake .. && \
    make

quick:
    cd {{ build_dir }} && \
    make

runs:
    cd {{ build_dir }} && \
    ./{{ example_bin }} --port 10000

runc:
    cd {{ build_dir }} && \
    ./{{ example_bin }} --server 127.0.0.1 --port 10000

clean:
    rm -rf {{ build_dir }}