build_dir := "build"
example_bin := "bin/tensor_transfer_example"

build:
    rm -rf {{ build_dir }} && \
    mkdir -p {{ build_dir }} && \
    cd {{ build_dir }} && \
    cmake .. && \
    make

quick:
    cd {{ build_dir }} && \
    make

run:
    cd {{ build_dir }} && \
    ./{{ example_bin }} --port=33123

run-client:
    cd {{ build_dir }} && \
    ./{{ example_bin }} --server=127.0.0.1 --port=33123

clean:
    rm -rf {{ build_dir }}