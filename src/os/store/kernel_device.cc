#include "kernel_device.h"

#include <fcntl.h>
#include <unistd.h>

KernelDevice::KernelDevice() {

}

int KernelDevice::Open(const std::string &path) {
  int ret = 0;

  // O_DIRECT is a hint that you want your I/O to bypass the Linux kernel's caches
  // File I/O is done directly to/from
  // user-space buffers.  The O_DIRECT flag on its own makes an
  // effort to transfer data synchronously, but does not give
  // the guarantees of the O_SYNC flag that data and necessary
  // metadata are transferred.  To guarantee synchronous I/O,
  // O_SYNC must be used in addition to O_DIRECT
  //
  int fd = ::open(path.c_str(), O_RDWR | O_DIRECT);
  if (fd < 0) {
    return fd;
  }

  fd_ = fd;

  return ret;
}

int KernelDevice::Close() {
  return ::close(fd_);
}

int KernelDevice::AioWrite(uint64_t off, Buffer &buffer, IoContext *io_ctx) {
  Aio aio; 

  
  
  io_ctx.emplace_back(aio);

  return 0;
}
