#ifndef ARIES_KERNEL_BUFFER_H
#define ARIES_KERNEL_BUFFER_H

class Buffer {
 public:
  Buffer(const char *data, size_t size);

 private:
  const char *data_;
  const size_t size_;
};



#endif
