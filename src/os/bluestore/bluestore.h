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
  // Usually one blob is one extent, but sometimes it could be multiple extents
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


// On disk type of Metadata for a single object
struct bluestore_onode_t {
  uint64_t nid;  // unique inode number
  uint64_t size; // object size

  map<mempool::bluestore_cache_other::string, bufferptr> attrs; // attributes

  // Each object is sharded
  struct shard_info {
    uint32_t offset; // logical offset for start of a shard
    uint32_t bytes;
  };
  std::vector<shard_info> extent_map_shards; 

};


// A sharded extent map, mapping offsets to lextents to blobs
struct ExtentMap {
  extent_map_t extent_map; // boost::intrusive::set<Extent>

  struct Shard {
    bluestore_onode_t::shard_info *shard_info = nullptr;

    unsigned extents = 0;  // count of extents in this shard
    bool loaded = false; // true if shard is loaded
    bool dirty = false; // true if shard is dirty and needs reencoding
  };
};

// Load shard (metadata) from KeyValueDB
void ExtentMap::fault_range(KeyValueDB *db, uint32_t offset, uint32_t length) {
  int start_shard_index = seek_shard(offset);
  int last_shard_index = seek_shard(offset + length);

  
  for (int shard_index = start_shard; shard_index <= last_shard_index; ++shard_index) {
    Shard shard = &shards[start_shard_index];

    if (shard.loaded == false) {
      bufferlist buffer;

      generate_extent_shard_key_and_apply(
        ...,
        db->get(PREFIX_OBJ, final_key, &buffer));

      p->extents = decode_some(buffer);
      p->loaded = true;
    }
  }
}



// In-memory object type
struct Onode {
  bluestore_onode_t onode;  // metadata stored as value in kv store
  bool exists; // true if object logically exists

  ExtentMap extent_map;

};




// on disk PG type
struct bluestore_cnode_t {
  bits
};

struct Collection: public CollectionImpl {



  OnodeSpace onode_map;

  OnodeRef get_onode(const ghobject_t &oid, bool create);


};

OnodeRef Collection::get_onode(const ghobject_t &oid, bool create) {
  OnodeRef o = onode_map.lookup(oid);
  if (o)
    return o;

  bufferlist buffer;
  int r = store->db->get(PREFIX_OBJ, key.c_str(), key.size(), &buffer);

  Onode *onode = nullptr;
  if (buffer.length() == 0) {
    onode = new Onode(this, oid, key);
  } else {
    onode = Onode::decode(this, oid, key, v);
  }
  o.reset(onode);

  return onode_map.add(oid, o);
}


// A vector of physical blocks
struct bluestore_blob_t {
  PExtentVector extents; // raw data position on device
  uint32_t logical_length = 0;  // original length of data stored in the blob

};


struct BufferSpace {
  mempool::bluestore_cache_other::map<uint32_t, std::unique_ptr<Buffer>>
    buffer_map;
};

struct SharedBlob {
  CollectionRef coll;

  Bufferspace bc; // buffer cache


};


// A vector of physical blocks
struct Blob {

  mutable bluestore_blob_t blob;

  SharedBlobRef shared_blob;

};


// A cache (shard) of onodes and buffers
struct Cache {
  std::recursive_mutex lock;

  struct LRUCache: public Cache {
    ...
  }

  struct TwoQCache: public Cache {
    ...
  }
};



struct TransContext final: public AioContext {

  uint64_t seq = 0;
};

// Extent is logically continuous data (not necessarily continuous on disk)
// Blob represents the physical blocks
struct Extent: public ExtentBase {   // Sorted based on logical offset

  uint32_t logical_offset = 0; // logical offset

  uint32_t blob_offset = 0; // 
  uint32_t length = 0;  
  BlobRef blob; // the blob with our data


};

struct ExtentMap {
  extent_map-t extent_map;   // map of extents to blobs


};


class BlueStore {
 public:

  int queue_transactions();


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

int Bluestore::queue_transactions(
  CollectionHandle &ch,
  vector<Transaction> &tls,
  TrackedOpRef op,
  ThreadPool::TPHandle *handle) {

  Collection *c = static_cast<Collection *>(ch.get());
  OpSequencer *osr = c->osr.get();

  // Create a transaction context
  TransContext *txc = new TransContext();
  txc->t = db->get_transaction();  // return std::make_shared<RocksDBTransactionImpl>(this)
  osd->queue_new(txc);

  // 1. 
  // 2. Place dirty nodes into TransContext
  for (vector<Transaction>::iterator p = tls.begin(); p != tls.end(); ++p) {
    txc->bytes += (*p).get_num_bytes();
    _txc_add_transaction(txc, &(*p));
  }
  
  // Write all dirty onodes by putting them in the RocksDB transaction
  _txc_write_onodes(txc, txc->t);

  if (txc->deferred_txn) {
    // TODO:
  }
 
  // Updates the freelist manager 
  _txc_finalize_kv(txc, txc->t);

  if (txc->deferred_txn) {
    // TODO:
  }

  // A simple state machine
  _txc_state_proc(txc);
}

// If the blocker layer finishes the aio, it triggers a callback
// to execute this function again
void BlueStore::_txc_state_proc(TransContext *txc)
{
  while (true) {
    case TransContext::STATE_PREPARE:
      if (txc->ioc.has_pending_aios())
      {
        txc->state = TransContext::STATE_AIO_WAIT;
        txc->had_ios = true;
        _txc_aio_submit(txc); // bdev->aio_submit(&txc->ioc);
        return;
      }

    case TransContext::STATE_AIO_WAIT:
      _txc_finish_io(txc); // may trigger blocked txc's too
      return;

    case TransContext::STATE_IO_DONE:
      txc->state = TransContext::STATE_KV_QUEUED;
      
      {
        std::lock_guard l(kv_lock);
        kv_queue.push_back(txc); 
        kv_cond.notify_one();  // Wake up kv sync thread to commit them
      }

    case TransContext::STATE_KV_SUBMITTED:
      _txc_committed_kv(txc);

    case TransContext::STATE_KV_DONE:   
      if (txc->deferred_txn)
      {
        txc->state = TransContext::STATE_DEFERRED_QUEUED;
        _deferred_queue(txc);
        return;
      }
      txc->state = TransContext::STATE_FINISHING;

    case TransContext::STATE_FINISHING:
      _txc_finish(txc);
      return;
  }
}

void BlueStore::_txc_finish(TransContext *txc) {
  for (auto &sb : txc->shared_blobs_written)
  {
    // b->state = Buffer::STATE_CLEAN
    sb->finish_write(txc->seq);
  }
  txc->shared_blobs_written.clear();

  
}

void BlueStore::_txc_committed_kv(TransContext *txc)
{
  {
    std::lock_guard l(txc->osr->qlock);

    txc->state = TransContext::STATE_KV_DONE;

    if (txc->ch->commit_queue)
    {
      txc->ch->commit_queue->queue(txc->oncommits);
    }
    else
    {
      finisher.queue(txc->oncommits);
    }  
   
  }
}

void BlueStore::_kv_sync_thread() {
  while (true) {
    kv_committing.swap(kv_queue)
  
    if (force_flush)
    {
      // Block waiting
      bdev->flush();
    }

    // we will use one final transaction to force a sync
    KeyValueDB::Transaction synct = db->get_transaction();

    for (auto txc : kv_committing)
    {
      int r = db->submit_transaction(txc->t);

      txc->state = TransContext::STATE_KV_SUBMITTED;

      // TODO: Notify a bunch of stuffs 
      _txc_applied_kv(txc);
    }

    int r = db->submit_transaction_sync(synct);

    // _kv_finalize_thread will process these
    {
      std::unique_lock m(kv_finalize_lock);
      if (kv_committing_to_finalize.empty())
      {
        kv_committing_to_finalize.swap(kv_committing);
      }
      else
      {
        kv_committing_to_finalize.insert(
              kv_committing_to_finalize.end(),
              kv_committing.begin(),
              kv_committing.end());
        kv_committing.clear();
    }
  }

}

void BlueStore::_kv_finalize_thread() {
  while (true)
  {
    kv_committed.swap(kv_committing_to_finalize);

    while (!kv_committed.empty())
    {
      TransContext *txc = kv_committed.front();
      ceph_assert(txc->state == TransContext::STATE_KV_SUBMITTED);
      _txc_state_proc(txc);
      kv_committed.pop_front();
    }
  }
}

void BlueStore::_txc_finish_io(TransContext *txc)
{
  std::lock_guard l(osr->qlock);

  // Change state
  txc->state = TransContext::STATE_IO_DONE;

  OpSequencer::q_list_t::iterator p = osr->q.iterator_to(*txc);
  while (p != osr->q.begin())
  {
    --p;

    // If there is any txn submitted before this, 
    if (p->state < TransContext::STATE_IO_DONE)
      return;

    // TODO:
    if (p->state > TransContext::STATE_IO_DONE) {
      ++p;
      break;
    }
  }

  do
  {
    _txc_state_proc(&*p++);
  } while (p != osr->q.end() &&
           p->state == TransContext::STATE_IO_DONE);

  if (osr->kv_submitted_waiters)
  {
    osr->qcond.notify_all();
  }
}

void BlueStore::_txc_finalize_kv(TransContext *txc, KeyValueDB::Transaction t) {
  // Handle the overlapping sets, that is, handling the case that a same region 
  // is allocated and deallocated
  if (!txc->allocated.empty() && !txc->released.empty()) {
    overlap.intersection_of(txc->allocated, txc->released);
 
    tmp_allocated.subtract(overlap);
    tmp_released.subtract(overlap);

    pallocated = &tmp_allocated;
    preleased = &tmp_released;
  }

  // Update freelist with non-overlap sets
  for (interval_set<uint64_t>::iterator p = pallocated->begin();
       p != pallocated->end();
       ++p)
  {
    fm->allocate(p.get_start(), p.get_len(), t);
  }

  for (interval_set<uint64_t>::iterator p = preleased->begin();
       p != preleased->end();
       ++p)
  {
    fm->release(p.get_start(), p.get_len(), t);
  }

  _txc_update_store_statfs(txc);
}

void BlueStore::_txc_write_nodes(TransContext *txc, KeyValueDB::Transaction t) {
  // finalize onodes
  for (auto o : txc->onodes)
  {
    _record_onode(o, t);
    o->flushing_count++;
  }

  // finalize shared blob
}

void BlueStore::_record_onode(OnodeRef &o, KeyValueDB::Transaction &txn) {
  ...

  txn->set(PREFIX_OBJ, o->key.c_str(), o->key.size(), bl);
}

void BlueStore::_txc_add_transaction(TransContext *txc, Transaction *t) {
  // Get all PGs related to this OSD transaction, usually 1 or 2
  Transaction::iterator i = t->begin();
  vector<CollectionRef> cvec(i.colls.size());
  unsigned j = 0;
  for (vector<coll_t>::iterator p = i.colls.begin(); p != i.colls.end();
       ++p, ++j)
  {
    cvec[j] = _get_collection(*p);
  }

  // Loop through each op inside this OSD transaction
  for (int pos = 0; i.have_op(); ++pos) {
    // these operations implicity create the object
    bool create = false;
    if (op->op == Transaction::OP_TOUCH ||
        op->op == Transaction::OP_WRITE ||
        op->op == Transaction::OP_ZERO)
    {
      create = true;
    }

    // Get Onode
    o = c->get_onode(oid, create);

    switch (op->op) {
      case Transaction::OP_TOUCH: {
        r = _touch(txc, c, o);
        break;
      }

      case Transaction::OP_WRITE: {
        uint64_t off = op->off;
        uint64_t len = op->len;
        bufferlist bl;
        i.decode_bl(bl);

        r = _write(txc, c, o, off, len, bl, fadvise_flags);
        break;
      }

      case Transaction::OP_CLONE: {
        OnodeRef &no = ovec[op->dest_oid];
        if (!no)
        {
          const ghobject_t &noid = i.get_oid(op->dest_oid);
          no = c->get_onode(noid, true);
        }
        r = _clone(txc, c, o, no);
        break;
      }

      case Transaction::OP_CLONERANGE2: {
        // TODO:
      }
    }
  }
}

int BlueStore::_touch(TransContext *txc,
                      CollectionRef &c,
                      OnodeRef &o)
{
  // Assign an onode id, monotonically incremented, unique within a bluestore instance
  _assign_nid(txc, o);

  // Add to the dirty inodes of this transaction
  txc->write_onode(o);
}

int BlueStore::_write() {
  _assign_nid(txc, o);

  // TODO:
  r = _do_write(txc, c, o, offset, length, bl, fadvise_flags);

  txc->write_onode(o);
}

struct WriteContext {
  struct WriteItem {
    
  }; 
  
  vector<WriteItem> writes;
};

int BlueStore::_do_write() {
  WriteContext wctx;

  // Read up the specified region of io metadata
  o->extent_map.fault_range(db, offset, length);

  // Place writes into WriteContext
  _do_write_data(txc, c, o, offset, length, bl, &wctx);

  // 
  r = _do_alloc_write(txc, c, o, &wctx);

  _wctx_finish(txc, c, o, &wctx_gc);

  return 0;
}

void BlueStore::_wctx_finish() {

}

int BlueStore::_do_alloc_write() {
  // Allocate space to write these data
  PExtentVector prealloc;
  prealloc.reserve(2 * wctx->writes.size());
  int64_t prealloc_left = 0;
  prealloc_left = alloc->allocate(
      need, min_alloc_size, need,
      0, &prealloc);

  for (auto &wi : wctx->writes)
  {
    PExtentVector extents;
    int64_t left = final_length;
    while (left > 0)
    {
      ceph_assert(prealloc_left > 0);
      if (prealloc_pos->length <= left)
      {
        prealloc_left -= prealloc_pos->length;
        left -= prealloc_pos->length;
        txc->statfs_delta.allocated() += prealloc_pos->length;
        extents.push_back(*prealloc_pos);
        ++prealloc_pos;
      }
      else
      {
        extents.emplace_back(prealloc_pos->offset, left);
        prealloc_pos->offset += left;
        prealloc_pos->length -= left;
        prealloc_left -= left;
        txc->statfs_delta.allocated() += left;
        left = 0;
        break;
      }
    }

    for (auto &p : extents)
    {
      txc->allocated.insert(p.offset, p.length);
    }
    dblob.allocated(p2align(b_off, min_alloc_size), final_length, extents);

    // Update the extent_map to point to these new blobs
    Extent *le = o->extent_map.set_lextent(coll, wi.logical_offset,
                                       b_off + (wi.b_off0 - wi.b_off),
                                       wi.length0,
                                       wi.b,
                                       nullptr);
    // Even if this write is not buffered write, we still do this in order to
    // track the inflight buffer writes to the disk
    _buffer_cache_write(txc, wi.b, b_off, wi.bl,
                        wctx->buffered ? 0 : Buffer::FLAG_NOCACHE);

    // Queue the IO
    //
    // Deferred IO
    if (l->length() <= prefer_deferred_size.load())
    {
      bluestore_deferred_op_t *op = _get_deferred_op(txc, o);
      op->op = bluestore_deferred_op_t::OP_WRITE;
      int r = b->get_blob().map(
          b_off, l->length(),
          [&](uint64_t offset, uint64_t length)
          {
            op->extents.emplace_back(bluestore_pextent_t(offset, length));
            return 0;
          });
    }
    else // Queue the IO directly
    {
      b->get_blob().map_bl(
          b_off, *l,
          [&](uint64_t offset, bufferlist &t)
          {
            bdev->aio_write(offset, t, &txc->ioc, false);
          });
    }
  }

  return 0;
}

int BlueStore::_do_write_data() {
  uint64_t end = offset + length;
  bufferlist::iterator p = bl.begin();

  // If the write falls in the same block
  // min_alloc_size=64K(HDD), 4K(SSD)
  if (offset / min_alloc_size == (end - 1) / min_alloc_size &&
      (length != min_alloc_size))
  {
    _do_write_small(txc, c, o, offset, length, p, wctx);
  }
  else
  {
    uint64_t head_offset, head_length;
    uint64_t middle_offset, middle_length;
    uint64_t tail_offset, tail_length;

    head_offset = offset;
    head_length = p2nphase(offset, min_alloc_size);

    tail_offset = p2align(end, min_alloc_size);
    tail_length = p2phase(end, min_alloc_size);

    middle_offset = head_offset + head_length;
    middle_length = length - head_length - tail_length;

    if (head_length)
    {
      _do_write_small(txc, c, o, head_offset, head_length, p, wctx);
    }

    if (middle_length)
    {
      // Write to a completely new part of a device
      _do_write_big(txc, c, o, middle_offset, middle_length, p, wctx);
    }

    if (tail_length)
    {
      _do_write_small(txc, c, o, tail_offset, tail_length, p, wctx);
    }
  }
}

void BlueStore::_do_write_big(
    WriteContext *wctx) {

  while (length > 0) {
    BlobRef b;

    // look for an existing mutable blob we can reuse
    do {

    } while (b == nullptr && any_change);

    // If it's a new extent, we use a new blob
    if (b == nullptr)
    {
      b = c->new_blob();
      b_off = 0;
      new_blob = true;
    }

    wctx->write(offset, b, l, b_off, t, b_off, l, false, new_blob);
  }
}

void BlueStore::_do_write_small() {
  // try to reuse existing blobs 
  do {

    // direct write into unused blocks of an existing mutable blob?
    if ((b_off % chunk_size == 0 && b_len % chunk_size == 0) &&
        b->get_blob().get_ondisk_length() >= b_off + b_len &&
        b->get_blob().is_unused(b_off, b_len) &&
        b->get_blob().is_allocated(b_off, b_len))
    {
      _apply_padding(head_pad, tail_pad, bl);

      _buffer_cache_write(txc, b, b_off, bl,
                          wctx->buffered ? 0 : Buffer::FLAG_NOCACHE);

      Extent *le = o->extent_map.set_lextent(c, offset, b_off + head_pad, length,
                                             b,
                                             &wctx->old_extents);

      b->dirty_blob().mark_used(le->blob_offset, le->length);
      return;
    }

    // RMW
    // requires reading in the old blob and overwrite some bytes in the middle
    // so sometimes it will block
    //
    // read some data to fill out the chunk?
    uint64_t head_read = p2phase(b_off, chunk_size);
    uint64_t tail_read = p2nphase(b_off + b_len, chunk_size);
    if ((head_read || tail_read) &&
        (b->get_blob().get_ondisk_length() >= b_off + b_len + tail_read) &&
        head_read + tail_read < min_alloc_size)
    {
      b_off -= head_read;
      b_len += head_read + tail_read;
    }
    else
    {
      head_read = tail_read = 0;
    }

  } while (any_change);

  
  // No blob can be reused, we use new blob
  BlobRef b = c->new_blob()nt64_t b_off = p2phase<uint64_t>(offset, alloc_len);
  uint64_t b_off0 = b_off;
  _pad_zeros(&bl, &b_off0, block_size);
  o->extent_map.punch_hole(c, offset, length, &wctx->old_extents);
  wctx->write(offset, b, alloc_len, b_off0, bl, b_off, length,
              min_alloc_size != block_size, // use 'unused' bitmap when alloc granularity
                                            // doesn't match disk one only
              true);;
}

// TODO:
int BlueStore::_clone() {

}

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

  r = do_read(c, &onode, offset, len, buffer);
  if (r < 0) {
    return r;
  }

  return 0;
}

int BlueStore::do_read(Collection *c,
    Onode *onode,
    uint64_t offset,
    size_t len,
    Buffer *buffer) {
  
  // Metadata of this io range is loaded into the memory
  // To be more precise, Shards' offset and length are loaded
  onode.extent_map.fault_range(db, offset, length);


  blobs2read_t blobs2read; // The blobs that we need to read from disk
  unsigned left = length;
  uint64_t pos = offset;
  // seek to the first lextent including or after offset (>= offset)
  extent_map_t::iterator lp = o->extent_map.seek_lextent(offset);
  while (left > 0 && lp != o->extent_map.extent_map.end()) {
    BlobRef &blob = lp->blob;

    map<uint64_t, bufferlist> cache_result;
    map<uint64_t, bufferlist> ready_regions;

    // Try to read from buffer cache first
    blob->shared_blob->bc.read(..., cache_result, ...);

    auto pc = cache_result.begin();
    while (b_len > 0) {
      if (/* cache hit */) {
        // Put into ready regions
        ready_regions[pos].claim(pc->second);
      } else {
        // 
        blobs2read[blob].emplace_back(read_req_t(...));
      }
    }
  }


  // Start read blobs from disk (if there is any that requires disk reading)
  IOContext ioc;
  for (auto &p: blobs2read) {
    // Put aio_read into pending list of ioc
    bdev->aio_read(offset, length, &reg.bl, &ioc);
  }

  // Time to submit the pending aio
  if (ioc.has_pending_aios()) {
    bdev->aio_submit(&ioc);

    ioc.aio_wait();
    r = ioc.get_return_value();
    if (r < 0) {
      return r;
    }
  }

  // Generate resulting buffer
  auto pr = ready_regions.begin();
  while (pos < length) {
    if (...) {
      buffer.claim_append(pr->second);
    } else {
      buffer.append_zero(l);
    }
  }

  return buffer.length();
}
