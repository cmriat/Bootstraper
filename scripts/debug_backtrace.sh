#!/bin/bash

EXECUTABLE="./build/bin/tensor_transfer_demo"

for addr in 0x4251f 0x969fb 0x42475 0x287f2 0x61f632 0x61fe03 0x62b169 0x4251f 0x3e625 0x21586 0x250f3 0x2473f 0x23da2; do
    echo "address $addr:"
    addr2line -e "$EXECUTABLE" -f -C "$addr" 2>/dev/null || echo "  cannot resolve"
    echo ""
done

objdump -t "$EXECUTABLE" | grep -E "(coroutine|rdma|connect|await)" | head -10
