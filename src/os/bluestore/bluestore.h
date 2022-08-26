// Dividing the entire device into a lot of bit-range, each bit-range covers some bits.
// Each bit-range is stored in a key in a KeyValueDB.
class BitmapFreelistManager: public FreelistManager {
 public:

 private:
  
};

class StupidAllocator: public Allocator {


};




struct bluestore_blob_t {

 private:
  // A vector of <offset, len> pairs
  // Usually one blob is one extent, but sometimes it could be one blob
  // equal to multiple extents
  PExtentVector extents;
  

};




// To know how many clones are referencing the same bytes
struct bluestore_extent_ref_map_t {
  struct record_t {
    uint32_t length;
    uint32_t refs;
  };

  mempool::bluestore_cache_other::map<uint64_t, record_t> ref_map;

};


// Multiple clones of the same object that are referencing the same blob
struct bluestore_shared_blob_t {
  uint64_t sbid;  // shared blob id
  bluestore_extent_ref_map_t ref_map; // shared blbo extents
};


// Metadata for a single object
struct bluestore_onode_t {
  uint64_t nid;  // unique inode number
  uint64_t size; // object size

  map<mempool::bluestore_cache_other::string, bufferptr> attrs; // attributes

  // Each object is sharded
  struct shard_info {
    uint32_t offset;
    uint32_t bytes;
  };
  std::vector<shard_info> extent_map_shards; 

};


class BlueStore {
 public:

  // Syncrhonous writes
  int read(Collection *c,
           const GenHashObjectId &oid,
           uint64_t offset,
           size_t len,
           Buffer *buffer);

 private:
  KeyValueDB *db;
  
  BlockDeivce *bdev;

  Allocator *alloc;
  
  FreelistManager *fm;

};

int BlueStore::read(Collection *c,
    const GenHashObjectId &oid,
    uint64_t offset,
    size_t len,
    Buffer *buffer) {
  int r;

  Onode onode;
  r = c->get_onode(oid, false, &onode);
  if (r < 0) {
    return r;
  }

  r = do_read(c, onode, offset, len, buffer);
  if (r < 0) {
    return r;
  }

  return 0;
}

int BlueStore::do_read(Collection *c,
    const Onode &onode,
    uint64_t offset,
    size_t len,
    Buffer *buffer) {

  IOContext ioc;

  // Put aio_read into pending list of ioc
  r = bdev->aio_read(offset, length, &bl, &ioc);

  // Time to submit the pending aio
  if (ioc.has_pending_aios()) {
    bdev->aio_submit(&ioc);
    ioc.aio_wait();
    r = ioc.get_return_value();
    if (r < 0) {
      return r;
    }
  }

  return 0;
}
