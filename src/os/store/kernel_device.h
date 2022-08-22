#ifndef ARIES_KERNEL_DEVICE_H
#define ARIES_KERNEL_DEVICE_H

#include <string>

#include "buffer.h"

struct Aio {
     

  Aio(Aio &&aio);


  int set_io_type();

};


// Used to track inflight IO 
class IoContext {
 public:
  std::list<Aio> pending_ios;


};


class KernelDevice {
 public:
  KernelDevice();

  int Open(const std::string &path);

  int Close();

  int AioWrite(uint64_t off, Buffer *buffer, IoContext *io_ctx);

 private:
  int fd_;
};

#endif
