# Buffer Allocation Benchmark

这个基准测试使用Google Benchmark框架来比较不同内存分配方法的性能，包括UCXX的allocateBuffer、C++标准分配方法、RMM buffer和CUDA分配。

## 测试的分配器

### Host Memory (主机内存)
- **malloc**: 标准C库的malloc/free
- **new_delete**: C++的new/delete操作符
- **std_vector**: std::vector容器
- **UCXX_Host**: UCXX的Host BufferType

### GPU Memory (GPU内存，需要CUDA支持)
- **UCXX_RMM**: UCXX的RMM (RAPIDS Memory Manager) BufferType
- **UCXX_CUDA**: UCXX的CUDA BufferType
- **cudaMalloc**: 直接使用CUDA的cudaMalloc/cudaFree
- **cudaMallocManaged**: CUDA统一内存分配

## 基准测试类型

### 1. Allocation Benchmark (分配性能)
- 测试纯分配和释放的性能
- 不包含内存拷贝操作
- 适合比较分配器的开销

### 2. Memory Copy Benchmark (内存拷贝性能)
- 测试从host内存拷贝到分配的buffer的性能
- 对于GPU内存，使用cudaMemcpy
- 对于host内存，使用std::memcpy

### 3. Combined Benchmark (综合性能)
- 测试分配 + 拷贝 + 释放的完整流程
- 最接近实际使用场景的性能测试

## 测试的Buffer大小

- 1KB (1,024 bytes)
- 1MB (1,048,576 bytes)
- 10MB (10,485,760 bytes)
- 100MB (104,857,600 bytes)
- 1GB (1,073,741,824 bytes)

## 构建和运行

### 1. 安装依赖

首先确保已经安装了Google Benchmark库：

```bash
# 激活pixi环境
pixi shell

# 更新依赖（benchmark已添加到pixi.toml）
pixi install
```

### 2. 构建

```bash
# 创建build目录并构建
mkdir -p build
cd build
cmake ..
make buffer_allocation_benchmark
```

### 3. 运行基准测试

```bash
# 运行所有基准测试
./bin/buffer_allocation_benchmark

# 运行特定的基准测试（例如只测试分配性能）
./bin/buffer_allocation_benchmark --benchmark_filter="BM_Allocation"

# 运行特定大小的测试（例如只测试1MB）
./bin/buffer_allocation_benchmark --benchmark_filter="1048576"

# 输出详细信息
./bin/buffer_allocation_benchmark --benchmark_display_aggregates_only=false

# 输出为JSON格式
./bin/buffer_allocation_benchmark --benchmark_format=json --benchmark_out=results.json

# 输出为CSV格式
./bin/buffer_allocation_benchmark --benchmark_format=csv --benchmark_out=results.csv
```

### 4. 常用命令行选项

```bash
# 设置运行时间（每个测试至少运行3秒）
./bin/buffer_allocation_benchmark --benchmark_min_time=3

# 设置重复次数
./bin/buffer_allocation_benchmark --benchmark_repetitions=5

# 显示统计信息（平均值、标准差等）
./bin/buffer_allocation_benchmark --benchmark_report_aggregates_only=true

# 只运行UCXX相关的测试
./bin/buffer_allocation_benchmark --benchmark_filter="ucxx"

# 只运行CUDA相关的测试
./bin/buffer_allocation_benchmark --benchmark_filter="cuda"
```

## 输出解释

基准测试的输出包含以下信息：

- **Benchmark**: 测试名称
- **Time**: 每次迭代的平均时间（微秒）
- **CPU**: CPU时间
- **Iterations**: 迭代次数
- **Bytes/s**: 吞吐量（字节/秒）
- **Label**: 分配器名称

## 性能分析

### 预期结果

1. **分配性能**: malloc通常最快，UCXX和CUDA分配器可能有额外开销
2. **内存拷贝**: GPU内存的拷贝性能取决于PCIe带宽
3. **综合性能**: 需要权衡分配开销和拷贝性能

### 关键指标

- **延迟**: 单次操作的时间
- **吞吐量**: 每秒处理的字节数
- **可扩展性**: 不同buffer大小下的性能变化

## 故障排除

### CUDA相关错误

如果遇到CUDA相关错误：

1. 确保系统有NVIDIA GPU
2. 确保CUDA驱动已正确安装
3. 检查CUDA_AVAILABLE宏是否正确定义

### RMM相关错误

如果RMM分配失败：

1. 确保GPU内存充足
2. 检查RMM是否正确初始化
3. 可能需要设置CUDA_VISIBLE_DEVICES

### 编译错误

如果遇到编译错误：

1. 确保所有依赖都已安装
2. 检查CMake配置
3. 确保使用C++20标准

## 扩展

要添加新的分配器：

1. 继承`BufferAllocator`类
2. 实现`allocate`、`deallocate`和`name`方法
3. 在`InitializeAllocators`中创建实例
4. 添加相应的BENCHMARK_CAPTURE调用
