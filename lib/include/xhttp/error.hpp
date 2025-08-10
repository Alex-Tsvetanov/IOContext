#pragma once

#include <type_traits>
#include <utility>
#include <new>

namespace std
{
  template <class E> class unexpected
  {
  public:
    constexpr explicit unexpected(const E& e)
      : e_(e)
    {}
    constexpr explicit unexpected(E&& e)
      : e_(std::move(e))
    {}
    constexpr const E& error() const& { return e_; }
    constexpr E& error() & { return e_; }
    constexpr E&& error() && { return std::move(e_); }

  private:
    E e_;
  };

  template <class E> [[nodiscard]] constexpr unexpected<E> make_unexpected(E e)
  {
    return unexpected<E>(std::move(e));
  }

  template <class T, class E> class expected
  {
    static_assert(!std::is_reference_v<T>, "T must not be a reference");

  public:
    constexpr expected(const T& v)
      : has_(true)
    {
      ::new (static_cast<void*>(&storage_.val)) T(v);
    }
    constexpr expected(T&& v)
      : has_(true)
    {
      ::new (static_cast<void*>(&storage_.val)) T(std::move(v));
    }
    constexpr expected(unexpected<E> u)
      : has_(false)
    {
      ::new (static_cast<void*>(&storage_.err)) E(std::move(u.error()));
    }

    expected(const expected& other)
      : has_(other.has_)
    {
      if (has_)
        ::new (static_cast<void*>(&storage_.val)) T(other.storage_.val);
      else
        ::new (static_cast<void*>(&storage_.err)) E(other.storage_.err);
    }
    expected(expected&& other) noexcept(std::is_nothrow_move_constructible_v<T> &&
                                        std::is_nothrow_move_constructible_v<E>)
      : has_(other.has_)
    {
      if (has_)
        ::new (static_cast<void*>(&storage_.val)) T(std::move(other.storage_.val));
      else
        ::new (static_cast<void*>(&storage_.err)) E(std::move(other.storage_.err));
    }

    expected& operator=(const expected& other)
    {
      if (this == &other)
        return *this;
      this->~expected();
      has_ = other.has_;
      if (has_)
        ::new (static_cast<void*>(&storage_.val)) T(other.storage_.val);
      else
        ::new (static_cast<void*>(&storage_.err)) E(other.storage_.err);
      return *this;
    }
    expected& operator=(expected&& other) noexcept(std::is_nothrow_move_constructible_v<T> &&
                                                   std::is_nothrow_move_constructible_v<E>)
    {
      if (this == &other)
        return *this;
      this->~expected();
      has_ = other.has_;
      if (has_)
        ::new (static_cast<void*>(&storage_.val)) T(std::move(other.storage_.val));
      else
        ::new (static_cast<void*>(&storage_.err)) E(std::move(other.storage_.err));
      return *this;
    }

    ~expected()
    {
      if (has_)
        storage_.val.~T();
      else
        storage_.err.~E();
    }

    constexpr bool has_value() const noexcept { return has_; }
    constexpr explicit operator bool() const noexcept { return has_; }

    constexpr T& value() & { return storage_.val; }
    constexpr const T& value() const& { return storage_.val; }
    constexpr T&& value() && { return std::move(storage_.val); }

    constexpr E& error() & { return storage_.err; }
    constexpr const E& error() const& { return storage_.err; }
    constexpr E&& error() && { return std::move(storage_.err); }

    constexpr T& operator*() & { return storage_.val; }
    constexpr const T& operator*() const& { return storage_.val; }
    constexpr T* operator->() { return &storage_.val; }
    constexpr const T* operator->() const { return &storage_.val; }

  private:
    bool has_;
    union Storage
    {
      T val;
      E err;
      Storage() {}
      ~Storage() {}
    } storage_;
  };

  template <class E> class expected<void, E>
  {
  public:
    constexpr expected()
      : has_(true)
    {}
    constexpr expected(unexpected<E> u)
      : has_(false)
    {
      ::new (static_cast<void*>(&err_)) E(std::move(u.error()));
    }

    expected(const expected& other)
      : has_(other.has_)
    {
      if (!has_)
        ::new (static_cast<void*>(&err_)) E(other.err_);
    }
    expected(expected&& other) noexcept(std::is_nothrow_move_constructible_v<E>)
      : has_(other.has_)
    {
      if (!has_)
        ::new (static_cast<void*>(&err_)) E(std::move(other.err_));
    }
    expected& operator=(const expected& other)
    {
      if (this == &other)
        return *this;
      this->~expected();
      has_ = other.has_;
      if (!has_)
        ::new (static_cast<void*>(&err_)) E(other.err_);
      return *this;
    }
    expected& operator=(expected&& other) noexcept(std::is_nothrow_move_constructible_v<E>)
    {
      if (this == &other)
        return *this;
      this->~expected();
      has_ = other.has_;
      if (!has_)
        ::new (static_cast<void*>(&err_)) E(std::move(other.err_));
      return *this;
    }
    ~expected()
    {
      if (!has_)
        err_.~E();
    }

    constexpr bool has_value() const noexcept { return has_; }
    constexpr explicit operator bool() const noexcept { return has_; }
    constexpr void value() const noexcept {}

    constexpr E& error() & { return err_; }
    constexpr const E& error() const& { return err_; }
    constexpr E&& error() && { return std::move(err_); }

  private:
    bool has_;
    union
    {
      E err_;
    };
  };
} // namespace std

#include <string_view>

namespace xhttp
{

  enum class Error
  {
    None = 0,
    WouldBlock,
    Again = WouldBlock,
    Timeout,
    ConnectionClosed,
    SyscallFailure,
    ParseError,
    Overflow,
    Unknown
  };

  inline std::string_view to_string(Error e)
  {
    switch (e)
    {
    case Error::None:
      return "none";
    case Error::WouldBlock:
      return "would_block";
    case Error::Timeout:
      return "timeout";
    case Error::ConnectionClosed:
      return "connection_closed";
    case Error::SyscallFailure:
      return "syscall_failure";
    case Error::ParseError:
      return "parse_error";
    case Error::Overflow:
      return "overflow";
    default:
      return "unknown";
    }
  }

  template <class T> using expected = std::expected<T, Error>;

} // namespace xhttp
