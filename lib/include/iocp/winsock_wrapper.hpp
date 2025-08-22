// Windows socket wrapper
#ifdef _WIN32

struct WinSockWrapper
{
  static inline LPFN_ACCEPTEX AcceptExPtr = nullptr;

  WinSockWrapper();
  ~WinSockWrapper();

  [[noreturn]] static void die(const char* msg);
  static void load_extensions(SOCKET s);
  static SOCKET make_listen_socket(uint16_t port);
};

extern WinSockWrapper wsa_;

#endif