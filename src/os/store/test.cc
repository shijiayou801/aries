#include <cassert>

#include "kernel_device.h"

int main() {
  KernelDevice kernel_device;

  assert(kernel_device.Open("/root/Aries/src/os/store/kernel_device.file") == 0);
  assert(kernel_device.Close() == 0);

  return 0;
}
