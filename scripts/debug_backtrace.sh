#!/bin/bash

EXECUTABLE="./build/bin/tensor_transfer_demo"

for addr in 0x61d9b6 0x61f632 0x61fe03 0x62b169 0x4251f 0x2e3d3 0x1803c 0x1b679 0x1acc5 0x1a363 0x258f0 0x25850 0x257c0; do
    echo "address $addr:"
    addr2line -e "$EXECUTABLE" -f -C "$addr" 2>/dev/null || echo "  cannot resolve"
    echo ""
done

objdump -t "$EXECUTABLE" | grep -E "(coroutine|rdma|connect|await)" | head -10
