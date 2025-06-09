#pragma once

#include <glog/export.h>
#include <glog/logging.h>
#include <fmt/core.h>
#include <string>
#include <vector>

namespace btsp {

class LogWrapper {
public:
    static void init(const char* program_name) {
        google::InitGoogleLogging(program_name);
        FLAGS_logtostderr = true;
        FLAGS_colorlogtostderr = true;
    }
    
    static void set_verbose_level(int level) {
        FLAGS_v = level;
    }
    
    static void set_log_dir(const std::string& dir) {
        FLAGS_log_dir = dir;
        FLAGS_logtostderr = false;
    }

    template<typename... Args>
    static void info(const std::string& format, Args&&... args) {
        if constexpr (sizeof...(args) > 0) {
            try {
                LOG(INFO) << fmt::vformat(format, fmt::make_format_args(args...));
            } catch (const std::exception& e) {
                LOG(INFO) << format << " [fmt error: " << e.what() << "]";
            }
        } else {
            LOG(INFO) << format;
        }
    }

    template<typename... Args>
    static void warning(const std::string& format, Args&&... args) {
        if constexpr (sizeof...(args) > 0) {
            try {
                LOG(WARNING) << fmt::vformat(format, fmt::make_format_args(args...));
            } catch (const std::exception& e) {
                LOG(WARNING) << format << " [fmt error: " << e.what() << "]";
            }
        } else {
            LOG(WARNING) << format;
        }
    }

    template<typename... Args>
    static void error(const std::string& format, Args&&... args) {
        if constexpr (sizeof...(args) > 0) {
            try {
                LOG(ERROR) << fmt::vformat(format, fmt::make_format_args(args...));
            } catch (const std::exception& e) {
                LOG(ERROR) << format << " [fmt error: " << e.what() << "]";
            }
        } else {
            LOG(ERROR) << format;
        }
    }

    template<typename... Args>
    static void debug(int verbose_level, const std::string& format, Args&&... args) {
        if constexpr (sizeof...(args) > 0) {
            try {
                VLOG(verbose_level) << fmt::vformat(format, fmt::make_format_args(args...));
            } catch (const std::exception& e) {
                VLOG(verbose_level) << format << " [fmt error: " << e.what() << "]";
            }
        } else {
            VLOG(verbose_level) << format;
        }
    }
    
    template<typename... Args>
    static void info_if(bool condition, const std::string& format, Args&&... args) {
        if constexpr (sizeof...(args) > 0) {
            try {
                LOG_IF(INFO, condition) << fmt::vformat(format, fmt::make_format_args(args...));
            } catch (const std::exception& e) {
                LOG_IF(INFO, condition) << format << " [fmt error: " << e.what() << "]";
            }
        } else {
            LOG_IF(INFO, condition) << format;
        }
    }

    template<typename... Args>
    static void warning_if(bool condition, const std::string& format, Args&&... args) {
        if constexpr (sizeof...(args) > 0) {
            try {
                LOG_IF(WARNING, condition) << fmt::vformat(format, fmt::make_format_args(args...));
            } catch (const std::exception& e) {
                LOG_IF(WARNING, condition) << format << " [fmt error: " << e.what() << "]";
            }
        } else {
            LOG_IF(WARNING, condition) << format;
        }
    }

    template<typename... Args>
    static void info_every_n(int n, const std::string& format, Args&&... args) {
        if constexpr (sizeof...(args) > 0) {
            try {
                LOG_EVERY_N(INFO, n) << fmt::vformat(format, fmt::make_format_args(args...));
            } catch (const std::exception& e) {
                LOG_EVERY_N(INFO, n) << format << " [fmt error: " << e.what() << "]";
            }
        } else {
            LOG_EVERY_N(INFO, n) << format;
        }
    }
    
    template<typename T>
    static std::string vector_to_string(const std::vector<T>& vec, size_t max_elements = 5) {
        if (vec.empty()) return "[]";
        
        std::string result = "[";
        size_t count = std::min(vec.size(), max_elements);
        
        for (size_t i = 0; i < count; ++i) {
            if (i > 0) result += ", ";
            result += std::to_string(vec[i]);
        }
        
        if (vec.size() > max_elements) {
            result += fmt::format(", ... ({} more)", vec.size() - max_elements);
        }
        
        result += "]";
        return result;
    }
};

} // namespace btsp

#define BTSP_LOG_INFO(format, ...) btsp::LogWrapper::info(format, ##__VA_ARGS__)
#define BTSP_LOG_WARNING(format, ...) btsp::LogWrapper::warning(format, ##__VA_ARGS__)
#define BTSP_LOG_ERROR(format, ...) btsp::LogWrapper::error(format, ##__VA_ARGS__)
#define BTSP_LOG_DEBUG(level, format, ...) btsp::LogWrapper::debug(level, format, ##__VA_ARGS__)

#define BTSP_LOG_INFO_IF(condition, format, ...) btsp::LogWrapper::info_if(condition, format, ##__VA_ARGS__)
#define BTSP_LOG_WARNING_IF(condition, format, ...) btsp::LogWrapper::warning_if(condition, format, ##__VA_ARGS__)

#define BTSP_LOG_INFO_EVERY_N(n, format, ...) btsp::LogWrapper::info_every_n(n, format, ##__VA_ARGS__)

#define RDMA_LOG_INFO(format, ...) BTSP_LOG_INFO("[RDMA] " format, ##__VA_ARGS__)
#define RDMA_LOG_WARNING(format, ...) BTSP_LOG_WARNING("[RDMA] " format, ##__VA_ARGS__)
#define RDMA_LOG_ERROR(format, ...) BTSP_LOG_ERROR("[RDMA] " format, ##__VA_ARGS__)
#define RDMA_LOG_DEBUG(level, format, ...) BTSP_LOG_DEBUG(level, "[RDMA] " format, ##__VA_ARGS__)

#define RPC_LOG_INFO(format, ...) BTSP_LOG_INFO("[RPC] " format, ##__VA_ARGS__)
#define RPC_LOG_WARNING(format, ...) BTSP_LOG_WARNING("[RPC] " format, ##__VA_ARGS__)
#define RPC_LOG_ERROR(format, ...) BTSP_LOG_ERROR("[RPC] " format, ##__VA_ARGS__)
#define RPC_LOG_DEBUG(level, format, ...) BTSP_LOG_DEBUG(level, "[RPC] " format, ##__VA_ARGS__)

#define GLOG_INFO LOG(INFO)
#define GLOG_WARNING LOG(WARNING)
#define GLOG_ERROR LOG(ERROR)
#define GLOG_FATAL LOG(FATAL)
#define GLOG_VLOG(level) VLOG(level)

#define GLOG_CHECK(condition) CHECK(condition)
#define GLOG_CHECK_EQ(a, b) CHECK_EQ(a, b)
#define GLOG_CHECK_NE(a, b) CHECK_NE(a, b)
#define GLOG_CHECK_GT(a, b) CHECK_GT(a, b)
#define GLOG_CHECK_LT(a, b) CHECK_LT(a, b)
#define GLOG_CHECK_GE(a, b) CHECK_GE(a, b)
#define GLOG_CHECK_LE(a, b) CHECK_LE(a, b)
