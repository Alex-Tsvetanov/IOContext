#pragma once

#include <boost/stacktrace.hpp>
#include <iostream>
#include <mutex>
#include <ostream>
#include <unordered_map>
#include <thread>
#include <sstream>

namespace ts_std
{
  class Logger
  {
  private:
    std::mutex mutex_;
    std::unordered_map<std::thread::id, std::stringstream> thread_logs_;
    std::ostream& out;

    using manip_fn = std::ostream& (*) (std::ostream&);

  public:
    explicit Logger(std::ostream& output)
      : out(output)
    {}

    // Generic insertion stays as-is.
    template <typename T> Logger& operator<<(const T& message)
    {
      std::lock_guard<std::mutex> lock(mutex_);
      thread_logs_[std::this_thread::get_id()] << message;
      return *this;
    }

    // Overload for iostream manipulators like std::endl, std::flush, std::ends…
    Logger& operator<<(manip_fn m)
    {
      std::lock_guard<std::mutex> lock(mutex_);

      // Handle the common ones specially
      if (m == static_cast<manip_fn>(std::endl<char, std::char_traits<char>>))
      {
        flush_unlocked(/*with_newline=*/true); // writes newline + flushes
      }
      else if (m == static_cast<manip_fn>(std::flush<char, std::char_traits<char>>))
      {
        flush_unlocked(/*with_newline=*/false); // just flushes
      }
      else
      {
        // For other no-arg ostream manipulators, preserve ordering:
        flush_unlocked(/*with_newline=*/false);
        m(out); // forward to the underlying stream
      }
      return *this;
    }

  private:
    void flush_unlocked(bool with_newline)
    {
      const auto tid = std::this_thread::get_id();
      auto& ss = thread_logs_[tid];

      out << "[Thread " << tid << "]\n" << boost::stacktrace::stacktrace() << "\n[Thread " << tid << "] " << ss.str();
      if (with_newline)
      {
        out << std::endl; // newline + flush
      }
      else
      {
        out << std::flush;
      }

      // Clear the per-thread buffer so we don't reprint old content
      ss.str(std::string{});
      ss.clear();
    }

  public:
    // Optional: public flush if you ever want to call it directly
    void flush()
    {
      std::lock_guard<std::mutex> lock(mutex_);
      flush_unlocked(false);
    }
  };

  inline Logger cout(std::cout);
  inline Logger cerr(std::cerr);
} // namespace ts_std
