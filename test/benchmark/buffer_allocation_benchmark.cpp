#include <glog/export.h>
#include <benchmark/benchmark.h>
#include <ucxx/buffer.h>
#include <glog/logging.h>
#include <memory>
#include <vector>
#include <cstring>
#include <string>

#ifdef CUDA_AVAILABLE
#include <cuda_runtime.h>
#include <cuda.h>
#endif

// Buffer allocator interface
class BufferAllocator {
public:
    virtual ~BufferAllocator() = default;
    virtual void* allocate(size_t size) = 0;
    virtual void deallocate(void* ptr, size_t size) = 0;
    virtual std::string name() const = 0;
    virtual bool is_gpu_memory() const { return false; }
};

// UCXX Host Buffer Allocator
class UCXXHostAllocator : public BufferAllocator {
public:
    void* allocate(size_t size) override {
        buffer = ucxx::allocateBuffer(ucxx::BufferType::Host, size);
        return buffer->data();
    }
    
    void deallocate(void* ptr, size_t size) override {
        buffer.reset();
    }
    
    std::string name() const override { return "UCXX_Host"; }
    
private:
    std::shared_ptr<ucxx::Buffer> buffer;
};

// UCXX RMM Buffer Allocator
class UCXXRMMAllocator : public BufferAllocator {
public:
    void* allocate(size_t size) override {
        try {
            buffer = ucxx::allocateBuffer(ucxx::BufferType::RMM, size);
            return buffer->data();
        } catch (const std::exception& e) {
            return nullptr;
        }
    }
    
    void deallocate(void* ptr, size_t size) override {
        buffer.reset();
    }
    
    std::string name() const override { return "UCXX_RMM"; }
    bool is_gpu_memory() const override { return true; }
    
private:
    std::shared_ptr<ucxx::Buffer> buffer;
};

// Standard malloc allocator
class MallocAllocator : public BufferAllocator {
public:
    void* allocate(size_t size) override {
        return std::malloc(size);
    }
    
    void deallocate(void* ptr, size_t size) override {
        std::free(ptr);
    }
    
    std::string name() const override { return "malloc"; }
};

// Standard new/delete allocator
class NewDeleteAllocator : public BufferAllocator {
public:
    void* allocate(size_t size) override {
        return new char[size];
    }
    
    void deallocate(void* ptr, size_t size) override {
        delete[] static_cast<char*>(ptr);
    }
    
    std::string name() const override { return "new_delete"; }
};

// std::vector allocator
class VectorAllocator : public BufferAllocator {
public:
    void* allocate(size_t size) override {
        buffer = std::make_unique<std::vector<char>>(size);
        return buffer->data();
    }
    
    void deallocate(void* ptr, size_t size) override {
        buffer.reset();
    }
    
    std::string name() const override { return "std_vector"; }
    
private:
    std::unique_ptr<std::vector<char>> buffer;
};

#ifdef CUDA_AVAILABLE
// CUDA malloc allocator
class CudaMallocAllocator : public BufferAllocator {
public:
    void* allocate(size_t size) override {
        void* ptr;
        cudaError_t err = cudaMalloc(&ptr, size);
        if (err != cudaSuccess) {
            return nullptr;
        }
        return ptr;
    }
    
    void deallocate(void* ptr, size_t size) override {
        if (ptr) {
            cudaFree(ptr);
        }
    }
    
    std::string name() const override { return "cudaMalloc"; }
    bool is_gpu_memory() const override { return true; }
};

// CUDA managed memory allocator
class CudaManagedAllocator : public BufferAllocator {
public:
    void* allocate(size_t size) override {
        void* ptr;
        cudaError_t err = cudaMallocManaged(&ptr, size);
        if (err != cudaSuccess) {
            return nullptr;
        }
        return ptr;
    }
    
    void deallocate(void* ptr, size_t size) override {
        if (ptr) {
            cudaFree(ptr);
        }
    }
    
    std::string name() const override { return "cudaMallocManaged"; }
    bool is_gpu_memory() const override { return true; }
};

// UCXX CUDA Buffer Allocator
class UCXXCudaAllocator : public BufferAllocator {
public:
    void* allocate(size_t size) override {
        try {
            buffer = ucxx::allocateBuffer(ucxx::BufferType::RMM, size);
            return buffer->data();
        } catch (const std::exception& e) {
            return nullptr;
        }
    }
    
    void deallocate(void* ptr, size_t size) override {
        buffer.reset();
    }
    
    std::string name() const override { return "UCXX_CUDA"; }
    bool is_gpu_memory() const override { return true; }
    
private:
    std::shared_ptr<ucxx::Buffer> buffer;
};
#endif

// Global allocators for benchmarks
static std::unique_ptr<BufferAllocator> g_malloc_allocator;
static std::unique_ptr<BufferAllocator> g_new_delete_allocator;
static std::unique_ptr<BufferAllocator> g_vector_allocator;
static std::unique_ptr<BufferAllocator> g_ucxx_host_allocator;
static std::unique_ptr<BufferAllocator> g_ucxx_rmm_allocator;

#ifdef CUDA_AVAILABLE
static std::unique_ptr<BufferAllocator> g_cuda_malloc_allocator;
static std::unique_ptr<BufferAllocator> g_cuda_managed_allocator;
static std::unique_ptr<BufferAllocator> g_ucxx_cuda_allocator;
#endif

// Initialize allocators
void InitializeAllocators() {
    g_malloc_allocator = std::make_unique<MallocAllocator>();
    g_new_delete_allocator = std::make_unique<NewDeleteAllocator>();
    g_vector_allocator = std::make_unique<VectorAllocator>();
    g_ucxx_host_allocator = std::make_unique<UCXXHostAllocator>();
    g_ucxx_rmm_allocator = std::make_unique<UCXXRMMAllocator>();

#ifdef CUDA_AVAILABLE
    // Check CUDA availability
    int device_count = 0;
    cudaError_t err = cudaGetDeviceCount(&device_count);
    if (err == cudaSuccess && device_count > 0) {
        g_cuda_malloc_allocator = std::make_unique<CudaMallocAllocator>();
        g_cuda_managed_allocator = std::make_unique<CudaManagedAllocator>();
        g_ucxx_cuda_allocator = std::make_unique<UCXXCudaAllocator>();
    }
#endif
}

// Benchmark allocation performance
template<typename AllocatorPtr>
void BM_Allocation(benchmark::State& state, AllocatorPtr allocator) {
    if (!allocator) {
        state.SkipWithError("Allocator not available");
        return;
    }
    
    const size_t buffer_size = state.range(0);
    
    for (auto _ : state) {
        void* ptr = allocator->allocate(buffer_size);
        if (!ptr) {
            state.SkipWithError("Allocation failed");
            return;
        }
        benchmark::DoNotOptimize(ptr);
        allocator->deallocate(ptr, buffer_size);
    }
    
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * buffer_size);
    state.SetLabel(allocator->name());
}

// Benchmark memory copy performance
template<typename AllocatorPtr>
void BM_MemoryCopy(benchmark::State& state, AllocatorPtr allocator) {
    if (!allocator) {
        state.SkipWithError("Allocator not available");
        return;
    }
    
    const size_t buffer_size = state.range(0);
    
    // Allocate source and destination buffers
    void* dst = allocator->allocate(buffer_size);
    if (!dst) {
        state.SkipWithError("Destination allocation failed");
        return;
    }
    
    std::vector<char> src(buffer_size, 0x42);
    
    for (auto _ : state) {
#ifdef CUDA_AVAILABLE
        if (allocator->is_gpu_memory()) {
            cudaMemcpy(dst, src.data(), buffer_size, cudaMemcpyHostToDevice);
            cudaDeviceSynchronize();
        } else {
            std::memcpy(dst, src.data(), buffer_size);
        }
#else
        std::memcpy(dst, src.data(), buffer_size);
#endif
        benchmark::DoNotOptimize(dst);
    }
    
    allocator->deallocate(dst, buffer_size);
    
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * buffer_size);
    state.SetLabel(allocator->name() + "_copy");
}

// Benchmark allocation + copy + deallocation combined
template<typename AllocatorPtr>
void BM_AllocCopyDealloc(benchmark::State& state, AllocatorPtr allocator) {
    if (!allocator) {
        state.SkipWithError("Allocator not available");
        return;
    }
    
    const size_t buffer_size = state.range(0);
    std::vector<char> src(buffer_size, 0x42);
    
    for (auto _ : state) {
        void* ptr = allocator->allocate(buffer_size);
        if (!ptr) {
            state.SkipWithError("Allocation failed");
            return;
        }
        
#ifdef CUDA_AVAILABLE
        if (allocator->is_gpu_memory()) {
            cudaMemcpy(ptr, src.data(), buffer_size, cudaMemcpyHostToDevice);
            cudaDeviceSynchronize();
        } else {
            std::memcpy(ptr, src.data(), buffer_size);
        }
#else
        std::memcpy(ptr, src.data(), buffer_size);
#endif
        
        benchmark::DoNotOptimize(ptr);
        allocator->deallocate(ptr, buffer_size);
    }
    
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * buffer_size);
    state.SetLabel(allocator->name() + "_full");
}

// Buffer sizes to test (1KB to 1GB)
static const std::vector<int64_t> buffer_sizes = {
    1024,                    // 1KB
    1024 * 1024,            // 1MB
    10 * 1024 * 1024,       // 10MB
    100 * 1024 * 1024,      // 100MB
    1024 * 1024 * 1024      // 1GB
};

// Register allocation benchmarks
BENCHMARK_CAPTURE(BM_Allocation, malloc, g_malloc_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_Allocation, new_delete, g_new_delete_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_Allocation, std_vector, g_vector_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_Allocation, ucxx_host, g_ucxx_host_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_Allocation, ucxx_rmm, g_ucxx_rmm_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

#ifdef CUDA_AVAILABLE
BENCHMARK_CAPTURE(BM_Allocation, cuda_malloc, g_cuda_malloc_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_Allocation, cuda_managed, g_cuda_managed_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_Allocation, ucxx_cuda, g_ucxx_cuda_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);
#endif

// Register memory copy benchmarks
BENCHMARK_CAPTURE(BM_MemoryCopy, malloc, g_malloc_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_MemoryCopy, new_delete, g_new_delete_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_MemoryCopy, std_vector, g_vector_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_MemoryCopy, ucxx_host, g_ucxx_host_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_MemoryCopy, ucxx_rmm, g_ucxx_rmm_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

#ifdef CUDA_AVAILABLE
BENCHMARK_CAPTURE(BM_MemoryCopy, cuda_malloc, g_cuda_malloc_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_MemoryCopy, cuda_managed, g_cuda_managed_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_MemoryCopy, ucxx_cuda, g_ucxx_cuda_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);
#endif

// Register combined benchmarks
BENCHMARK_CAPTURE(BM_AllocCopyDealloc, malloc, g_malloc_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_AllocCopyDealloc, new_delete, g_new_delete_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_AllocCopyDealloc, std_vector, g_vector_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_AllocCopyDealloc, ucxx_host, g_ucxx_host_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_AllocCopyDealloc, ucxx_rmm, g_ucxx_rmm_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

#ifdef CUDA_AVAILABLE
BENCHMARK_CAPTURE(BM_AllocCopyDealloc, cuda_malloc, g_cuda_malloc_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_AllocCopyDealloc, cuda_managed, g_cuda_managed_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_AllocCopyDealloc, ucxx_cuda, g_ucxx_cuda_allocator.get())
    ->ArgsProduct({buffer_sizes})
    ->Unit(benchmark::kMicrosecond);
#endif

int main(int argc, char** argv) {
    // Initialize glog
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    // Initialize allocators
    InitializeAllocators();

    // Initialize and run benchmarks
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;

    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();

    return 0;
}
