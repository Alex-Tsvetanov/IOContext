#ifdef WIN32
#  include "server/worker.hpp"

Worker::Worker(fd_t listener, const WorkerConfig& cfg, HANDLE iocp)
  : listener_(listener)
  , cfg_(cfg)
  , iocp_(iocp)
{}

void Worker::run()
{
  // Worker thread logic here
}
#endif