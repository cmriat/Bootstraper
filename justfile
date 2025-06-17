build_dir := "build"
example_bin := "bin/tensor_transfer_demo"

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
    ./{{ example_bin }} --port 10000 --cuda-device 0

runc:
    cd {{ build_dir }} && \
    ./{{ example_bin }} --server 127.0.0.1 --port 10000 --cuda-device 1

clean:
    rm -rf {{ build_dir }}

killcs:
    ps -ef | grep {{ example_bin }} | grep -v grep | awk '{print $2}' | xargs -r kill -9