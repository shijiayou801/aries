#include <cstring>
#include <limits>
#include <random>

#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/tmp_file.hh>
#include <iostream>

constexpr size_t kAlignedSize = 4096;

seastar::future<> verify_data_file(seastar::file &f, 
    seastar::temporary_buffer<char> &rbuf,
    seastar::temporary_buffer<char> &wbuf) {
  return f.dma_read(0, rbuf.get_write(), kAlignedSize).then(
    [&rbuf, &wbuf] (size_t count) {
      assert(count == kAlignedSize);
      assert(memcmp(rbuf.get(), wbuf.get(), kAlignedSize) == 0);
    });
}

/*
future<file> open_data_file(sstring meta_filename, 
    temporary_buffer<char>& rbuf) {
  return with_file(open_file_dma(meta_filename, open_flags::ro), [&rbuf] (file& f) {
    return f.dma_read(0, rbuf.get_write(), aligned_size).then([&rbuf] (size_t count) {
      assert(count == aligned_size);
      auto data_filename = sstring(rbuf.get());
      fmt::print("    opening {}\n", data_filename);
      return open_file_dma(data_filename, open_flags::ro);
    });
  });
}*/

seastar::future<> open_file(const std::string &filename) {
  using namespace seastar;

  fmt::print("Opening file {0}\n", filename);

  // do_with_thread using a seastar::thread object which comes 
  // with its own stack
  //
  // tmp_dir creates a temporary directory
  return tmp_dir::do_with_thread([] (tmp_dir &t) {
    fmt::print("temp directory path={0}\n", t.get_path());

    sstring meta_filename = (t.get_path() / "meta_file").native();
    sstring data_filename = (t.get_path() / "data_file").native();

    fmt::print("meta_filename={0}\ndata_filename={1}\n",
      meta_filename, data_filename);

    auto write_to_file = [] (const sstring filename, 
                             temporary_buffer<char> &wbuf) {
      auto count = with_file(
        open_file_dma(filename, open_flags::rw | open_flags::create),
        [&wbuf] (file &f) {
          return f.dma_write(0, wbuf.get(), kAlignedSize);
        }).get0();
      assert(count == kAlignedSize);
    };

    auto wbuf = temporary_buffer<char>::aligned(kAlignedSize, kAlignedSize);
    std::fill(wbuf.get_write(), wbuf.get_write() + kAlignedSize, 0);
    std::copy(data_filename.cbegin(), data_filename.cend(), wbuf.get_write());
    write_to_file(meta_filename, wbuf);

    auto rnd = std::mt19937(std::random_device()());
    auto dist = std::uniform_int_distribution<char>(
      0, std::numeric_limits<char>::max());
    std::generate(wbuf.get_write(), wbuf.get_write() + kAlignedSize, 
      [&dist, &rnd] { return dist(rnd); });
    write_to_file(data_filename, wbuf);

    auto rbuf = temporary_buffer<char>::aligned(kAlignedSize, kAlignedSize);

    with_file(open_file_dma(data_filename, open_flags::ro),
      [&rbuf, &wbuf] (file &f) {
        return verify_data_file(f, rbuf, wbuf);
      }).get();
  });
}

int main(int argc, char **argv) {
  seastar::app_template app;

  app.run(argc, argv, [] {
    std::cout << seastar::smp::count << "\n";

    return open_file("file1");
  });
}