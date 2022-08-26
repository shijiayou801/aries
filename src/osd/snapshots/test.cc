#include <iostream>
#include <vector>
#include <map>

#define READ 0
#define WRITE 1

typedef uint64_t snapid_t;

template <typename T>
class IntervalSet {
 public:
  bool contains(snapid_t snap_id) { return true; }
};



class OSDMap {

  /// queue of snaps to remove
  map<int64_t, IntervalSet> removed_snaps_queue;

  /// removed_snaps additions this epoch
  map<int64_t, IntervalSet> new_removed_snaps;

  /// removed_snaps removals this epoch
  map<int64_t, IntervalSet> new_purged_snaps;

};


struct ObjectInfo {};



class hobject_t {};

class MOSDOp {
 public:
  hobject_t oid;

  int operation;

  snapid_t seq;
  std::vector<snapid_t> snaps;

};


// Encoded into the 'snapset' attr of the head object
// Cached in the OSD as a SnapSetContext
struct SnapSet {
  snapid_t seq;  // The highest snap id the OSD has seen
  std::vector<snapid_t> snaps;  // Descending
  std::vector<snapid_t> clones; // Ascending, snap id of the clone objects
  
  std::map<snapid_t, IntervalSet<uint64_t>> clone_overlap;
  std::map<snapid_t, uint64_t> clone_size;
  std::map<snapid_t, std::vector<snapid_t>> clone_snaps; // Descending
};

// Per-Object Snapshot related information
struct SnapSetContext {
  hobject_t oid;
  SnapSet snapset;

};

struct SnapContext {
  snapid_t seq;
  std::vector<snapid_t> snaps; // Existent snaps, in descending order
};

struct ObjectState {
  ObjectInfo oi;
  bool exists;
};

struct ObjectContext {
  ObjectState obs;

  SnapSetContext *ssc;

};

struct OpContext {
  const ObjectState *obs; // Old ObjectState
  const SnapSet *snapset; // Old SnapSet

  ObjectState new_obs; // Resulting ObjectState
  SnapSet new_snapset; // Resulting SnapSet

  SnapContext snapc; // Writer snap context

  ObjectContext *obc;

  OpContext(ObjectContext *obc):
      obc(obc) {
    if (obc->ssc != nullptr) {
      new_snapset = obc->ssc->snapset;
      snapset = &obc->ssc->snapset;
    }
  }
};





class OSD {
 public:
  OSD();
  ~OSD();


  int do_op(const MOSDOp &op);

  void snap_trim();

 private:
  
  int find_object_context(const hobject_t &oid, ObjectContext *obc);

  void write(OpContext *op_ctx);

  void read(OpContext *op_ctx);

  void filter_snapc(std::vector<snapid_t> *snaps);

  int get_next_objects_to_trim();

  IntervalSet<snapid_t> snap_trim_queue;
  IntervalSet<snapid_t> purged_snaps;
  std::map<hobject_t, SnapSetContext *> snapset_contexts;
};

OSD::OSD() {

}

OSD::~OSD() {

}

void OSD::snap_trim(snapid_t snap) {
  std::vector<hobject_t> objects;
  int ret = get_next_objects_to_trim(snap, &objects);
  if (ret < 0) {
    // TODO:
  }
  
  
}

// TODO:
int OSD::get_next_objects_to_trim(snapid_t snap, std::vector<hobject_t> *objects) {
  return 0;
}

int OSD::do_op(const MOSDOp &op) {
  // Get object's metadata either from cache or disk (if not on cache)
  ObjectContext obc;
  int ret = find_object_context(op.oid, &obc);
  if (ret < 0) {
    return ret;
  }

  OpContext op_ctx(&obc);

  // Initialize OpContext's writer's snapshot info with writer's snapshot information
  op_ctx.snapc.seq = op.seq;
  op_ctx.snapc.snaps = op.snaps;

  // Filter writer's snaps
  filter_snapc(&op_ctx.snapc.snaps);

  if (op.operation == WRITE) {
    write(&op_ctx);
  } else {
    read(&op_ctx);
  }

  return 0;
}

int OSD::find_object_context(const hobject_t &oid, ObjectContext *obc) {
  int ret = 0;

  // Normal write logic will go here
  if (oid.snap == NOSNAP) {
    ret = get_object_context(oid, obc);
    if (ret < 0) {

    }
    return ret;
  }


  // Read logic
  SnapSetContext ssc;
  ret = get_snapset_context(oid, &ssc);
  if (ret < 0) {
    // TODO:
  }


  // Find the first clone whose snap_id >= target snap_id
  uint32_t k = 0;
  while (k < ssc->snapset.clones.size()
         && ssc->snapset.clones[k] < oid.snap) {
    ++k;
  }

  // Not found
  if (k == ssc->snapset.clones.size()) {
    return -ENOENT;
  }

  // Now we read that clone's metadata
  ObjectContext clone_obc;
  ret = get_object_context(soid, false, &clone_obc);
  if (ret < 0) {
    return ret;
  }

  auto p = obc->ssc->snapset.clone_snaps.find(soid.snap);
  if (p->second.empty()) {
    // TODO: 
  }

  // clone_snaps is descending
  snapid_t first = p->second.back();
  if (first <= oid.snap) {
    *obc = clone_obc;
    return 0;
  }

  return 0;
}

int OSD::get_object_context(const hobject_t &oid, ObjectContext *obc) {
  return 0; 
}

// Filter all trimming or trimmed snap ids out
void OSD::filter_snapc(std::vector<snapid_t> *snaps) {
  std::vector<snapid_t> new_snaps;

  for (snapid_t snap_id: *snaps) {
    if (snap_trim_queue.contains(snap_id) || purged_snaps.contains(snap_id)) {
      continue;
    }
    new_snaps.push_back(snap_id);
  }

  snaps->swap(new_snaps);
}

void OSD::write(OpContext *op_ctx) {
  const hobject_t &soid = ctx->obs->oi.soid;
  SnapContext &snapc = op_ctx->snapc;

  if (op_ctx->obs->exists  // Head object exists
      && !snapc.snaps.empty() // There are snapshots
      && snapc.snaps[0] > op_ctx->new_snapset.seq) { // Writer's largest living
                                                     // snap id is greater than
                                                     // what this object has 
                                                     // seen
     hobject_t coid = soid;
     coid.snap = snapc.seq;


     // Get all the snaps this OSD has never seen
     std::vector<snapid_t> snaps;
     for (uint32_t snap_id: snapc.snaps) {
       if (snap_id <= op_ctx->new_snapset.seq) {
         break;
       }
       snaps.push_back(snap_id);
     }

     // TODO: fill up ctx->clone_obc
     //


    ctx->pg_txc->clone(soid, coid);

    replicated_backend->submit_transaction(op_ctx);
  }
}

void OSD::read(OpContext *op_ctx) {
  
}


class ReplicatedBackend {
 public:

 private:

};

void ReplicatedBackend::submit_transaction(OpContext *op_ctx) {
  ObjectStore::Transaction os_txc;

  os_txc->clone(); // TODO:

  log_operation(os_txc);
  
  queue_transactions(os_txc);
}

int main() {
  OSD osd;

  return 0;
}
