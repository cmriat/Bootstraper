#!/bin/bash

# 解析 Seastar 调用栈的脚本
EXECUTABLE="./build/bin/simple_rpc"

echo "=== 解析 std::bad_alloc 异常的调用栈 ==="
echo "地址列表："
echo "0x2fc74"
echo "0x25211" 
echo "0x30b69"
echo "0x26a8f"
echo "0x2e4d4"
echo "0x30c4e"
echo "0x26b58"
echo ""

echo "=== 使用 addr2line 解析 ==="
for addr in 0x2fc74 0x25211 0x30b69 0x26a8f 0x2e4d4 0x30c4e 0x26b58; do
    echo "地址 $addr:"
    addr2line -e "$EXECUTABLE" -f -C "$addr" 2>/dev/null || echo "  无法解析"
    echo ""
done

echo "=== 使用 objdump 查看符号表 ==="
echo "查找相关函数符号..."
objdump -t "$EXECUTABLE" | grep -E "(coroutine|rdma|connect|await)" | head -10
