/*
 * RPC Benchmark Utilities for Seastar
 *
 * This file contains utility functions for RPC benchmarking.
 */
#pragma once

#include <algorithm>
#include <vector>
#include <unordered_map>
#include <fmt/core.h>

namespace rpc_benchmark {

// Calculate percentiles from a vector of latencies
inline std::unordered_map<std::string, uint64_t> calculate_percentiles(std::vector<uint64_t>& latencies) {
    if (latencies.empty()) {
        return {{"p50", 0}, {"p90", 0}, {"p99", 0}, {"p9999", 0}, {"p100", 0}};
    }

    std::sort(latencies.begin(), latencies.end());

    size_t total = latencies.size();
    std::unordered_map<std::string, uint64_t> results;

    results["p50"] = latencies[total * 50 / 100];
    results["p90"] = latencies[total * 90 / 100];
    results["p99"] = latencies[total * 99 / 100];
    results["p9999"] = latencies[total * 9999 / 10000];
    results["p100"] = latencies[total - 1]; // max value

    return results;
}

// Print benchmark results in a nice format
inline void print_benchmark_results(size_t payload_size, const std::unordered_map<std::string, uint64_t>& percentiles) {
    fmt::print("\n{} byte payload\tlatency (microseconds)\n", payload_size);
    fmt::print("p50\t{}\n", percentiles.at("p50"));
    fmt::print("p90\t{}\n", percentiles.at("p90"));
    fmt::print("p99\t{}\n", percentiles.at("p99"));
    fmt::print("p9999\t{}\n", percentiles.at("p9999"));
    fmt::print("p100\t{}\n", percentiles.at("p100"));
}

} // namespace rpc_benchmark
