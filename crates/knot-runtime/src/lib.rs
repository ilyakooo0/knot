//! Knot runtime library.
//!
//! Provides C-ABI functions for value management, relation operations,
//! and SQLite-backed persistence. This crate is compiled as a static
//! library and linked into every compiled Knot program.

pub mod log;
mod tui;

use rusqlite::types::ValueRef;
use rusqlite::Connection;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ffi::c_void;
use std::slice;
#[cfg(feature = "gc-stats")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, Weak};
use std::time::Duration;

// ── Arena allocator ──────────────────────────────────────────────
//
// Three-tier GC with bump allocation:
//
//  1. **Bump chunks** — values are allocated by bumping a pointer in fixed-size
//     chunks (no per-value malloc). Chunks are stable; pointers never move.
//  2. **Mark / reset** — `mark()` snapshots the position; `reset_to()` runs
//     destructors on values allocated since the mark and rewinds the bump
//     pointer.  Used for per-iteration cleanup in do-block loops.
//  3. **Promote (selective)** — before `reset_to`, yielded values are
//     deep-cloned into a Box-allocated `pinned` list.  Only chunk-resident
//     values are cloned; singletons, text-cache, and parent-frame values are
//     reused by pointer (zero-copy for safe values).
//  4. **Frame stack** — `push_frame` / `pop_frame` isolate function calls.
//     `pop_frame_promote` deep-clones the return value into the parent,
//     using chunk-range checks instead of a HashSet.

use std::mem::MaybeUninit;

/// Number of `Value` slots per chunk.
///
/// `sizeof(Value) = 40` (verified by `_size_tests::report_value_size`), so
/// 512 × 40 B = 20 KB per chunk.  This fits comfortably in L1D on every
/// modern CPU (Intel Skylake: 32 KB, Apple M-series P-core: 128 KB), so
/// promotion scans stay cache-resident.  Smaller caps (128, 256) trade
/// worse allocation throughput (more `take_chunk` calls) for marginal
/// cache wins we don't need.  Larger caps (1024+) exceed Intel L1D and
/// waste memory for short-running programs.
const CHUNK_CAP: usize = 512;

/// Cache-line aligned backing buffer for a chunk.
///
/// Forcing 64-byte alignment on the whole backing array means the
/// chunk's first slot lands at a cache-line boundary, which keeps
/// sequential row scans (e.g. `promote`, `clone_into_pinned`) from
/// straddling cache lines on their first few values and avoids false
/// sharing when two thread-local arenas happen to allocate chunks
/// whose heads would otherwise share a line (rare but possible with
/// glibc's per-arena pools).  `MaybeUninit` lets us leave the buffer
/// bit-untouched until `Chunk::alloc` writes into a slot.
#[repr(align(64))]
struct ChunkData([MaybeUninit<Value>; CHUNK_CAP]);

/// A contiguous block of bump-allocated `Value` slots.
struct Chunk {
    data: Box<ChunkData>,
    len: usize,
}

impl Chunk {
    fn new() -> Self {
        // SAFETY: `ChunkData` is `[MaybeUninit<Value>; CHUNK_CAP]`
        // wrapped in a `#[repr(align(64))]` newtype; `MaybeUninit` is
        // valid for any bit pattern, so the fresh allocation from the
        // global allocator is already in a legal state.  The alignment
        // attribute guarantees `std::alloc::alloc(layout)` hands back a
        // 64-byte-aligned pointer.
        let layout = std::alloc::Layout::new::<ChunkData>();
        let ptr = unsafe { std::alloc::alloc(layout) as *mut ChunkData };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        let data = unsafe { Box::from_raw(ptr) };
        Chunk { data, len: 0 }
    }

    #[inline]
    fn is_full(&self) -> bool { self.len >= CHUNK_CAP }

    /// Bump-allocate a value, returning a stable pointer.
    #[inline]
    fn alloc(&mut self, v: Value) -> *mut Value {
        debug_assert!(!self.is_full());
        let slot = &mut self.data.0[self.len];
        let ptr = slot.as_mut_ptr();
        unsafe { ptr.write(v); }
        self.len += 1;
        ptr
    }

    /// Check whether `val` points into this chunk's live region.
    #[inline]
    fn contains(&self, val: *mut Value) -> bool {
        if self.len == 0 { return false; }
        let base = self.data.0.as_ptr() as usize;
        let end = unsafe { self.data.0.as_ptr().add(self.len) as usize };
        let addr = val as usize;
        addr >= base && addr < end
    }

    /// Hint to the OS that this chunk's backing pages can be
    /// reclaimed.  Called when a chunk is about to be dropped because
    /// the pool is at capacity; the allocator's free list may retain
    /// the virtual allocation but madvise lets the kernel drop the
    /// physical pages and keep RSS bounded in long-running processes.
    ///
    /// On Linux: `MADV_DONTNEED` releases pages and zeroes them on next
    /// access.  On macOS/BSD: `MADV_FREE` marks pages as reclaimable
    /// (kernel may drop them under pressure).  Both are advisory — if
    /// the syscall fails there's no correctness impact.
    #[cfg(unix)]
    fn advise_release(&mut self) {
        let addr = self.data.0.as_mut_ptr() as *mut libc::c_void;
        let len = std::mem::size_of::<ChunkData>();
        // SAFETY: addr/len describe a valid, owned allocation; madvise
        // is non-destructive — it only affects paging behaviour.
        #[cfg(target_os = "linux")]
        unsafe { libc::madvise(addr, len, libc::MADV_DONTNEED); }
        #[cfg(any(target_os = "macos", target_os = "ios",
                  target_os = "freebsd", target_os = "openbsd",
                  target_os = "netbsd", target_os = "dragonfly"))]
        unsafe { libc::madvise(addr, len, libc::MADV_FREE); }
    }

    #[cfg(not(unix))]
    fn advise_release(&mut self) {}
}

/// A single frame in the arena's frame stack.
///
/// `chunks` holds bump-allocated "young" values freed by `reset_to` or
/// frame drop.  `pinned_chunks` holds bump-allocated values promoted
/// out of the young chunks (so they survive `reset_to`) but still
/// freed at frame drop or via selective pinned reclamation.
///
/// Pinned values share the chunk-pool machinery — no per-value
/// `Box::into_raw` on the promote path — which eliminates a malloc
/// per yield in do-block loops.  Two parallel structures (`chunks` and
/// `pinned_chunks`) keep the bump positions independent so the two
/// lifetimes don't interfere.
struct Frame {
    chunks: Vec<Chunk>,
    /// Sorted-by-base-address sidetable mapping young-chunk base
    /// addresses to their index in `chunks`.  Enables O(log n)
    /// `owns_in_chunks` lookup.  Maintained by `push_chunk` /
    /// `pop_chunk` helpers; `chunks` retains insertion order (used by
    /// `reset_to`'s mark arithmetic).
    ///
    /// Box allocator doesn't guarantee adjacency between sequential
    /// chunks, so a simple `[min_base, max_end]` bound isn't tight
    /// enough to serve as a fast reject in realistic workloads.
    chunk_index: Vec<(usize, u32)>,
    /// Chunks reserved for pinned (promoted) values.  These are
    /// bump-allocated just like `chunks`, but survive `reset_to` —
    /// instead of being unwound on every iteration, they're only
    /// truncated to the pinned mark.
    pinned_chunks: Vec<Chunk>,
    /// Parallel sidetable to `chunk_index` for `pinned_chunks`, used
    /// by `owns_in_pinned_chunks` to do the same O(log n) range check.
    pinned_chunk_index: Vec<(usize, u32)>,
}

impl Frame {
    /// Build an empty frame.  The arena supplies chunks on first alloc via
    /// `Arena::take_chunk`, so this doesn't allocate.
    fn empty() -> Self {
        Frame {
            chunks: Vec::new(),
            chunk_index: Vec::new(),
            pinned_chunks: Vec::new(),
            pinned_chunk_index: Vec::new(),
        }
    }

    /// Flat position across `chunks` (chunk_index * CHUNK_CAP + slot).
    fn mark_chunks(&self) -> usize {
        if self.chunks.is_empty() { return 0; }
        (self.chunks.len() - 1) * CHUNK_CAP + self.chunks.last().unwrap().len
    }

    /// Append a young chunk and update the sorted sidetable.
    fn push_chunk(&mut self, c: Chunk) {
        let base = c.data.0.as_ptr() as usize;
        let idx = self.chunks.len() as u32;
        self.chunks.push(c);
        let pos = self
            .chunk_index
            .binary_search_by_key(&base, |&(b, _)| b)
            .unwrap_or_else(|p| p);
        self.chunk_index.insert(pos, (base, idx));
    }

    /// Pop the last young chunk and update the sorted sidetable.
    fn pop_chunk(&mut self) -> Option<Chunk> {
        let chunk = self.chunks.pop()?;
        let base = chunk.data.0.as_ptr() as usize;
        if let Ok(pos) = self.chunk_index.binary_search_by_key(&base, |&(b, _)| b) {
            self.chunk_index.remove(pos);
        }
        Some(chunk)
    }

    /// Append a pinned chunk and update the sorted sidetable.
    fn push_pinned_chunk(&mut self, c: Chunk) {
        let base = c.data.0.as_ptr() as usize;
        let idx = self.pinned_chunks.len() as u32;
        self.pinned_chunks.push(c);
        let pos = self
            .pinned_chunk_index
            .binary_search_by_key(&base, |&(b, _)| b)
            .unwrap_or_else(|p| p);
        self.pinned_chunk_index.insert(pos, (base, idx));
    }

    /// Check whether `val` lives in one of this frame's young chunks.
    /// Uses binary search on `chunk_index` for O(log n) lookup.
    ///
    /// Fast path: many functions only ever allocate into a single chunk
    /// (the frame's first one), so the binary-search machinery is pure
    /// overhead there.  Short-circuit before touching `chunk_index`.
    fn owns_in_chunks(&self, val: *mut Value) -> bool {
        match self.chunks.len() {
            0 => false,
            1 => self.chunks[0].contains(val),
            _ => {
                let addr = val as usize;
                // Find the chunk whose base is the largest one ≤ addr.
                let pos = match self.chunk_index.binary_search_by_key(&addr, |&(b, _)| b) {
                    Ok(p) => p,        // addr == base of chunk `p`
                    Err(0) => return false,  // addr precedes all chunks
                    Err(p) => p - 1,   // chunk p-1 is the greatest base ≤ addr
                };
                let (_, chunk_idx) = self.chunk_index[pos];
                self.chunks[chunk_idx as usize].contains(val)
            }
        }
    }

    /// Check whether `val` lives in one of this frame's pinned chunks.
    /// Same O(log n) structure as `owns_in_chunks`, but against the
    /// pinned-chunk sidetable.  The common case (no pinned values yet)
    /// returns immediately.
    fn owns_in_pinned_chunks(&self, val: *mut Value) -> bool {
        match self.pinned_chunks.len() {
            0 => false,
            1 => self.pinned_chunks[0].contains(val),
            _ => {
                let addr = val as usize;
                let pos = match self.pinned_chunk_index.binary_search_by_key(&addr, |&(b, _)| b) {
                    Ok(p) => p,
                    Err(0) => return false,
                    Err(p) => p - 1,
                };
                let (_, chunk_idx) = self.pinned_chunk_index[pos];
                self.pinned_chunks[chunk_idx as usize].contains(val)
            }
        }
    }

    /// Check whether `val` is owned by this frame (young or pinned),
    /// probing pinned first.  Used by `clone_from_child` where the
    /// traversed graph is a deep-pinned spine promoted across prior
    /// iterations — the pinned-chunk range check typically hits fast.
    fn owns_pinned_first(&self, val: *mut Value) -> bool {
        if self.owns_in_pinned_chunks(val) { return true; }
        self.owns_in_chunks(val)
    }

    /// Drop all live values in young chunks (called from
    /// `Arena::drop_frame_contents`).  Leaves chunks zero-length but
    /// retained so `pop_chunk` can harvest them into the pool.
    fn drop_chunks(&mut self) {
        for chunk in &mut self.chunks {
            for si in 0..chunk.len {
                unsafe { std::ptr::drop_in_place(chunk.data.0[si].as_mut_ptr()); }
            }
            chunk.len = 0;
        }
    }

    /// Drop all live values in pinned chunks.  Symmetric to
    /// `drop_chunks`; leaves pinned chunks zero-length for harvest.
    fn drop_pinned_chunks(&mut self) {
        for chunk in &mut self.pinned_chunks {
            for si in 0..chunk.len {
                unsafe { std::ptr::drop_in_place(chunk.data.0[si].as_mut_ptr()); }
            }
            chunk.len = 0;
        }
    }

}

impl Drop for Frame {
    fn drop(&mut self) {
        // Safety net: if a Frame is dropped without going through
        // Arena::pop_frame (e.g. Arena::drop on thread exit), free its
        // contents here.  Chunks that have already been harvested into
        // the pool will have `len == 0` and contribute no work.
        self.drop_chunks();
        self.drop_pinned_chunks();
    }
}

/// A frame-stack arena for `Value` allocations.
///
/// Values are bump-allocated in per-frame chunks (no per-value malloc).
/// The frame stack enables isolation across function call boundaries:
/// - `push_frame()` / `pop_frame()` for call-site isolation
/// - `mark()` / `reset_to()` for per-iteration cleanup within a frame
/// - `promote()` selectively clones values into the pinned set, and those
///   pinned values survive `reset_to` (they're only freed on frame drop);
///   do-block codegen wraps each do-block in `push_frame`/`pop_frame_promote`
///   so that accumulated pinned yields get freed when the block ends
/// - `pop_frame_promote()` deep-clones a value to the parent frame
///
/// `free_chunks` is a process-global-per-thread pool of empty chunks shared
/// across all frames.  When `reset_to` or `pop_frame` releases chunks they
/// go here instead of being freed, so hot loops reuse backing memory.
struct Arena {
    frames: Vec<Frame>,
    free_chunks: Vec<Chunk>,
    /// Transient per-call deduplication cache used by `promote` and
    /// `pop_frame_promote` to share cloned subtrees when the same source
    /// pointer is reached via multiple paths (DAG structure).  Emptied
    /// after every top-level call so it never holds stale pointers.
    promote_cache: HashMap<*mut Value, *mut Value>,
}

// ── GC telemetry ─────────────────────────────────────────────────
//
// Cross-thread `AtomicU64` counters surfacing allocator behaviour for
// profiling.  All counters use `Ordering::Relaxed`: we only need
// monotonic updates, not happens-before, so the ordering is a single
// non-fenced add on every architecture we care about (basically free).
//
// The stats are accumulated process-wide (not per-thread) so the
// snapshot reflects aggregate activity across all Knot threads; this
// matches how a user would ask "how much churn is my program doing"
// rather than per-worker introspection.  A future extension could
// sharde by thread if we need to attribute costs.

/// Snapshot of all GC counters.  Stable layout for FFI: every field is
/// a `u64` at a fixed offset so consumers written against this struct
/// can read it as a plain byte blob.
#[repr(C)]
pub struct GcStatsSnapshot {
    pub allocs: u64,
    pub chunks_allocated: u64,
    pub chunks_pool_hits: u64,
    pub chunks_returned: u64,
    pub chunks_dropped: u64,
    pub promotes: u64,
    pub promote_cache_hits: u64,
    pub clone_into_pinned: u64,
    pub frame_pushes: u64,
    pub frame_pops: u64,
    pub peak_frame_depth: u64,
    pub relation_pool_hits: u64,
    pub relation_pool_misses: u64,
    pub text_cache_hits: u64,
    pub text_cache_misses: u64,
    pub pinned_allocs: u64,
    pub resets: u64,
}

#[cfg(feature = "gc-stats")]
struct GcStats {
    allocs: AtomicU64,
    chunks_allocated: AtomicU64,
    chunks_pool_hits: AtomicU64,
    chunks_returned: AtomicU64,
    chunks_dropped: AtomicU64,
    promotes: AtomicU64,
    promote_cache_hits: AtomicU64,
    clone_into_pinned: AtomicU64,
    frame_pushes: AtomicU64,
    frame_pops: AtomicU64,
    peak_frame_depth: AtomicU64,
    relation_pool_hits: AtomicU64,
    relation_pool_misses: AtomicU64,
    text_cache_hits: AtomicU64,
    text_cache_misses: AtomicU64,
    pinned_allocs: AtomicU64,
    resets: AtomicU64,
}

#[cfg(feature = "gc-stats")]
impl GcStats {
    const fn new() -> Self {
        GcStats {
            allocs: AtomicU64::new(0),
            chunks_allocated: AtomicU64::new(0),
            chunks_pool_hits: AtomicU64::new(0),
            chunks_returned: AtomicU64::new(0),
            chunks_dropped: AtomicU64::new(0),
            promotes: AtomicU64::new(0),
            promote_cache_hits: AtomicU64::new(0),
            clone_into_pinned: AtomicU64::new(0),
            frame_pushes: AtomicU64::new(0),
            frame_pops: AtomicU64::new(0),
            peak_frame_depth: AtomicU64::new(0),
            relation_pool_hits: AtomicU64::new(0),
            relation_pool_misses: AtomicU64::new(0),
            text_cache_hits: AtomicU64::new(0),
            text_cache_misses: AtomicU64::new(0),
            pinned_allocs: AtomicU64::new(0),
            resets: AtomicU64::new(0),
        }
    }

    #[inline]
    fn bump(&self, field: &AtomicU64) {
        field.fetch_add(1, Ordering::Relaxed);
    }

    /// Raise `peak_frame_depth` if `depth` exceeds the current peak.
    /// `fetch_max` is a single CAS-loop per call, but peak updates
    /// happen on `push_frame` (not per allocation) so the cost is
    /// negligible.
    fn record_frame_depth(&self, depth: u64) {
        self.peak_frame_depth.fetch_max(depth, Ordering::Relaxed);
    }

    fn snapshot(&self) -> GcStatsSnapshot {
        GcStatsSnapshot {
            allocs: self.allocs.load(Ordering::Relaxed),
            chunks_allocated: self.chunks_allocated.load(Ordering::Relaxed),
            chunks_pool_hits: self.chunks_pool_hits.load(Ordering::Relaxed),
            chunks_returned: self.chunks_returned.load(Ordering::Relaxed),
            chunks_dropped: self.chunks_dropped.load(Ordering::Relaxed),
            promotes: self.promotes.load(Ordering::Relaxed),
            promote_cache_hits: self.promote_cache_hits.load(Ordering::Relaxed),
            clone_into_pinned: self.clone_into_pinned.load(Ordering::Relaxed),
            frame_pushes: self.frame_pushes.load(Ordering::Relaxed),
            frame_pops: self.frame_pops.load(Ordering::Relaxed),
            peak_frame_depth: self.peak_frame_depth.load(Ordering::Relaxed),
            relation_pool_hits: self.relation_pool_hits.load(Ordering::Relaxed),
            relation_pool_misses: self.relation_pool_misses.load(Ordering::Relaxed),
            text_cache_hits: self.text_cache_hits.load(Ordering::Relaxed),
            text_cache_misses: self.text_cache_misses.load(Ordering::Relaxed),
            pinned_allocs: self.pinned_allocs.load(Ordering::Relaxed),
            resets: self.resets.load(Ordering::Relaxed),
        }
    }
}

/// Zero-cost stub when `gc-stats` feature is disabled.  All counter
/// field accesses resolve to no-op `StatCounter` handles so hot-path
/// call sites like `GC_STATS.bump(&GC_STATS.allocs)` compile down to
/// nothing.  Exposed FFI calls (`knot_gc_stats_snapshot`,
/// `knot_gc_stats_dump`) still succeed; they just report zeros.
#[cfg(not(feature = "gc-stats"))]
struct StatCounter;

#[cfg(not(feature = "gc-stats"))]
#[allow(dead_code)]
struct GcStats {
    allocs: StatCounter,
    chunks_allocated: StatCounter,
    chunks_pool_hits: StatCounter,
    chunks_returned: StatCounter,
    chunks_dropped: StatCounter,
    promotes: StatCounter,
    promote_cache_hits: StatCounter,
    clone_into_pinned: StatCounter,
    frame_pushes: StatCounter,
    frame_pops: StatCounter,
    peak_frame_depth: StatCounter,
    relation_pool_hits: StatCounter,
    relation_pool_misses: StatCounter,
    text_cache_hits: StatCounter,
    text_cache_misses: StatCounter,
    pinned_allocs: StatCounter,
    resets: StatCounter,
}

#[cfg(not(feature = "gc-stats"))]
impl GcStats {
    const fn new() -> Self {
        GcStats {
            allocs: StatCounter,
            chunks_allocated: StatCounter,
            chunks_pool_hits: StatCounter,
            chunks_returned: StatCounter,
            chunks_dropped: StatCounter,
            promotes: StatCounter,
            promote_cache_hits: StatCounter,
            clone_into_pinned: StatCounter,
            frame_pushes: StatCounter,
            frame_pops: StatCounter,
            peak_frame_depth: StatCounter,
            relation_pool_hits: StatCounter,
            relation_pool_misses: StatCounter,
            text_cache_hits: StatCounter,
            text_cache_misses: StatCounter,
            pinned_allocs: StatCounter,
            resets: StatCounter,
        }
    }

    #[inline(always)]
    fn bump(&self, _field: &StatCounter) {}

    #[inline(always)]
    fn record_frame_depth(&self, _depth: u64) {}

    fn snapshot(&self) -> GcStatsSnapshot {
        GcStatsSnapshot {
            allocs: 0, chunks_allocated: 0, chunks_pool_hits: 0, chunks_returned: 0,
            chunks_dropped: 0, promotes: 0, promote_cache_hits: 0, clone_into_pinned: 0,
            frame_pushes: 0, frame_pops: 0, peak_frame_depth: 0, relation_pool_hits: 0,
            relation_pool_misses: 0, text_cache_hits: 0, text_cache_misses: 0,
            pinned_allocs: 0, resets: 0,
        }
    }
}

static GC_STATS: GcStats = GcStats::new();

/// Populate `out` with a snapshot of the GC counters.  Exposed for
/// programs/tests that want to observe allocator behaviour in-process.
/// Safe to call from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn knot_gc_stats_snapshot(out: *mut GcStatsSnapshot) {
    if out.is_null() { return; }
    unsafe { std::ptr::write(out, GC_STATS.snapshot()); }
}

/// Print a human-readable dump of the current GC counters to stderr.
/// Intended for diagnostics (`--gc-stats` style invocations or panics).
#[unsafe(no_mangle)]
pub extern "C" fn knot_gc_stats_dump() {
    let s = GC_STATS.snapshot();
    eprintln!("[gc] allocs={} pinned_allocs={} resets={}", s.allocs, s.pinned_allocs, s.resets);
    eprintln!("[gc] chunks: allocated={} pool_hits={} returned={} dropped={}",
        s.chunks_allocated, s.chunks_pool_hits, s.chunks_returned, s.chunks_dropped);
    eprintln!("[gc] promote: calls={} cache_hits={} clone_into_pinned={}",
        s.promotes, s.promote_cache_hits, s.clone_into_pinned);
    eprintln!("[gc] frames: pushes={} pops={} peak_depth={}",
        s.frame_pushes, s.frame_pops, s.peak_frame_depth);
    eprintln!("[gc] relation_pool: hits={} misses={}", s.relation_pool_hits, s.relation_pool_misses);
    eprintln!("[gc] text_cache: hits={} misses={}", s.text_cache_hits, s.text_cache_misses);
}

/// Cap the chunk pool so the arena doesn't hoard memory after a transient
/// peak.  64 × CHUNK_CAP × sizeof(Value) ≈ a few MB — a reasonable upper
/// bound for steady-state working set.
const CHUNK_POOL_CAP: usize = 64;

/// Drop threshold for returning relation vecs to the pool.  Very large
/// Vecs (e.g. accidentally-full query results) are freed instead of
/// pooled to avoid keeping megabytes around after a single large query.
const RELATION_POOL_MAX_CAPACITY: usize = 4096;

/// Number of size-class bins.  Bin `i` stores vecs with capacity in
/// `[1 << i, 1 << (i+1))`.  We cap at `log2(RELATION_POOL_MAX_CAPACITY)
/// + 1 = 13` so bin 12 covers [4096, 8192) but the reject threshold
/// keeps the tail small.
const RELATION_POOL_BINS: usize = 13;

/// Per-bin capacity cap.  Keeps total pool memory bounded while still
/// providing generous reuse for hot size classes.
const RELATION_POOL_PER_BIN_CAP: usize = 32;

/// Compute the bin index for a given capacity.  Bin `i` holds vecs with
/// capacity in `[1 << i, 1 << (i+1))`.  Capacity 0 or 1 maps to bin 0.
#[inline]
fn relation_pool_bin(cap: usize) -> usize {
    if cap <= 1 {
        0
    } else {
        // floor(log2(cap))
        ((usize::BITS - 1 - cap.leading_zeros()) as usize).min(RELATION_POOL_BINS - 1)
    }
}

thread_local! {
    /// Recycled `Vec<*mut Value>` buffers reused by `knot_relation_*`
    /// constructors.  Organised into size-class bins keyed by
    /// `floor(log2(capacity))` for O(1) best-fit lookup.  Populated by
    /// `Value::drop` when a `Value::Relation` is dropped; drained by
    /// `take_relation_vec`.
    static RELATION_VEC_POOL: RefCell<[Vec<Vec<*mut Value>>; RELATION_POOL_BINS]> =
        RefCell::new(std::array::from_fn(|_| Vec::new()));
}

/// Obtain a `Vec<*mut Value>` from the pool (if any) or allocate a new
/// one.  The returned Vec is always empty; callers may reserve further
/// capacity as needed.  First searches the bin matching `min_capacity`
/// and then larger bins; only falls back to smaller bins (which will
/// grow on push) if nothing bigger is available.
fn take_relation_vec(min_capacity: usize) -> Vec<*mut Value> {
    RELATION_VEC_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        let start = relation_pool_bin(min_capacity);
        // Prefer a bin that already satisfies the request so the caller
        // doesn't pay for a grow.
        for bin in start..RELATION_POOL_BINS {
            if let Some(v) = pool[bin].pop() {
                GC_STATS.bump(&GC_STATS.relation_pool_hits);
                return v;
            }
        }
        // Nothing large enough — accept an under-sized buffer; push()
        // will reallocate on demand.
        for bin in (0..start).rev() {
            if let Some(v) = pool[bin].pop() {
                GC_STATS.bump(&GC_STATS.relation_pool_hits);
                return v;
            }
        }
        GC_STATS.bump(&GC_STATS.relation_pool_misses);
        Vec::with_capacity(min_capacity)
    })
}

/// Return a `Vec<*mut Value>` to the pool if there's space and the
/// capacity is reasonable.  Called from `Value::drop` when a Relation
/// is being destroyed.
///
/// Uses `try_with` because `Value::drop` can be invoked during thread
/// teardown (when `ARENA`'s TLS destructor walks every chunk, dropping
/// each live `Value`).  TLS destructor order is unspecified, so the
/// pool may already be destroyed — if so we fall through and let `v`
/// drop normally (its allocation is freed by the global allocator).
fn return_relation_vec(v: Vec<*mut Value>) {
    let cap = v.capacity();
    if cap == 0 || cap > RELATION_POOL_MAX_CAPACITY {
        return;
    }
    let _ = RELATION_VEC_POOL.try_with(|pool| {
        let mut v = v;
        v.clear();
        let mut pool = pool.borrow_mut();
        let bin = relation_pool_bin(cap);
        if pool[bin].len() < RELATION_POOL_PER_BIN_CAP {
            pool[bin].push(v);
        }
    });
}

// ── Record-field vec pool ───────────────────────────────────────
//
// Records are built by `knot_record_empty(cap) + knot_record_set_field`
// on every row construction: record literals, JSON-deserialised HTTP
// requests, SELECT-derived relation rows, etc.  Each `with_capacity`
// hits the allocator.  Mirror the relation-vec pool here to absorb that
// churn — same binning, same per-bin cap, independent storage.
//
// Records are typically small (fewer fields than relations have rows),
// so a tighter bin count suffices: bin 6 covers caps up to 128, which
// comfortably holds every record we emit in practice.

const RECORD_POOL_BINS: usize = 7;
const RECORD_POOL_MAX_CAPACITY: usize = 128;
const RECORD_POOL_PER_BIN_CAP: usize = 32;

#[inline]
fn record_pool_bin(cap: usize) -> usize {
    if cap <= 1 {
        0
    } else {
        ((usize::BITS - 1 - cap.leading_zeros()) as usize).min(RECORD_POOL_BINS - 1)
    }
}

thread_local! {
    static RECORD_VEC_POOL: RefCell<[Vec<Vec<RecordField>>; RECORD_POOL_BINS]> =
        RefCell::new(std::array::from_fn(|_| Vec::new()));
}

/// Obtain a `Vec<RecordField>` from the pool or allocate a new one.
fn take_record_vec(min_capacity: usize) -> Vec<RecordField> {
    RECORD_VEC_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        let start = record_pool_bin(min_capacity);
        for bin in start..RECORD_POOL_BINS {
            if let Some(v) = pool[bin].pop() {
                return v;
            }
        }
        for bin in (0..start).rev() {
            if let Some(v) = pool[bin].pop() {
                return v;
            }
        }
        Vec::with_capacity(min_capacity)
    })
}

/// Return a `Vec<RecordField>` to the pool.  Called from `Value::drop`.
///
/// Uses `try_with` for the same reason as `return_relation_vec`:
/// `Value::drop` may fire during thread teardown (via `ARENA`'s TLS
/// destructor) after `RECORD_VEC_POOL` has itself been destroyed.
/// On a miss, `v` is dropped normally by the allocator.
fn return_record_vec(v: Vec<RecordField>) {
    let cap = v.capacity();
    if cap == 0 || cap > RECORD_POOL_MAX_CAPACITY {
        return;
    }
    let _ = RECORD_VEC_POOL.try_with(|pool| {
        let mut v = v;
        v.clear();
        let mut pool = pool.borrow_mut();
        let bin = record_pool_bin(cap);
        if pool[bin].len() < RECORD_POOL_PER_BIN_CAP {
            pool[bin].push(v);
        }
    });
}

impl Arena {
    fn new() -> Self {
        Arena {
            frames: vec![Frame::empty()],
            free_chunks: Vec::new(),
            promote_cache: HashMap::new(),
        }
    }

    /// Take a chunk from the pool or allocate a new one.
    #[inline]
    fn take_chunk(&mut self) -> Chunk {
        if let Some(c) = self.free_chunks.pop() {
            GC_STATS.bump(&GC_STATS.chunks_pool_hits);
            c
        } else {
            GC_STATS.bump(&GC_STATS.chunks_allocated);
            Chunk::new()
        }
    }

    /// Return an empty chunk to the pool for reuse.  Capped at
    /// `CHUNK_POOL_CAP` to avoid hoarding after peak memory usage.
    /// When the pool is full, the incoming chunk is dropped — before
    /// dropping, `advise_release` hints to the OS that its physical
    /// pages can be reclaimed, keeping RSS tight in long-running
    /// processes (routes, TUIs) whose pools saturate then idle.
    #[inline]
    fn return_chunk(&mut self, chunk: Chunk) {
        debug_assert_eq!(chunk.len, 0, "arena: pool received non-empty chunk");
        if self.free_chunks.len() < CHUNK_POOL_CAP {
            GC_STATS.bump(&GC_STATS.chunks_returned);
            self.free_chunks.push(chunk);
        } else {
            GC_STATS.bump(&GC_STATS.chunks_dropped);
            let mut chunk = chunk;
            chunk.advise_release();
            drop(chunk);
        }
    }

    #[inline]
    fn current_frame(&mut self) -> &mut Frame {
        self.frames.last_mut().expect("arena: no frames")
    }

    fn alloc(&mut self, v: Value) -> *mut Value {
        GC_STATS.bump(&GC_STATS.allocs);
        let need_new = self
            .frames
            .last()
            .expect("arena: no frames")
            .chunks
            .last()
            .map_or(true, Chunk::is_full);
        if need_new {
            let chunk = self.take_chunk();
            self.current_frame().push_chunk(chunk);
        }
        self.current_frame().chunks.last_mut().unwrap().alloc(v)
    }

    /// Snapshot the current frame's young-chunk allocation frontier.
    ///
    /// Returns a flat slot position `(chunk_idx * CHUNK_CAP + slot)`;
    /// pinned entries are intentionally excluded — they must survive
    /// `reset_to` so yields promoted mid-iteration remain live in the
    /// result relation (see `reset_to`).
    fn mark(&self) -> usize {
        let frame = self.frames.last().expect("arena: no frames");
        frame.mark_chunks()
    }

    fn reset_to(&mut self, mark: usize) {
        GC_STATS.bump(&GC_STATS.resets);
        let mark_chunk = mark / CHUNK_CAP;
        let mark_slot = mark % CHUNK_CAP;
        let mut harvested: Vec<Chunk> = Vec::new();
        {
            let frame = self.current_frame();
            // Drop values in young chunks above the mark.
            for ci in (mark_chunk..frame.chunks.len()).rev() {
                let chunk = &mut frame.chunks[ci];
                let start = if ci == mark_chunk { mark_slot } else { 0 };
                for si in (start..chunk.len).rev() {
                    unsafe { std::ptr::drop_in_place(chunk.data.0[si].as_mut_ptr()); }
                }
                chunk.len = start;
            }
            // Harvest empty young chunks above the mark into the pool.
            while frame.chunks.len() > mark_chunk + 1 {
                // `pop_chunk` keeps the sorted `chunk_index` in sync.
                harvested.push(frame.pop_chunk().unwrap());
            }
            // Pinned values are NOT truncated: compile_do's per-iteration
            // `knot_arena_reset_to` runs with `arena_mark` taken *before*
            // the yield's `knot_arena_promote`, so anything pinned during
            // the iteration (the yielded row pushed into `result`) must
            // survive the reset.  Pinned entries are reclaimed at frame
            // drop via `drop_and_harvest_frame`.
        }
        for chunk in harvested {
            self.return_chunk(chunk);
        }
        // Cache keys are raw pointers into chunks just dropped — they can
        // be reused for different values on the next allocation, so the
        // cache must be cleared.  `HashMap::clear` retains capacity, so
        // repeated cycles reuse the underlying allocation.
        self.promote_cache.clear();
    }

    fn push_frame(&mut self) {
        GC_STATS.bump(&GC_STATS.frame_pushes);
        // Cache keys are relative to the current frame's chunk addresses;
        // a new frame starts with a fresh identity space.
        self.promote_cache.clear();
        self.frames.push(Frame::empty());
        GC_STATS.record_frame_depth(self.frames.len() as u64);
    }

    /// Drop a frame's contents (running value destructors on both
    /// young and pinned chunks) and harvest its empty chunks into
    /// the pool.  The frame itself is consumed.
    fn drop_and_harvest_frame(&mut self, mut frame: Frame) {
        frame.drop_chunks();
        frame.drop_pinned_chunks();
        let mut chunks: Vec<Chunk> = frame.chunks.drain(..).collect();
        chunks.extend(frame.pinned_chunks.drain(..));
        drop(frame);
        for chunk in chunks {
            self.return_chunk(chunk);
        }
    }

    /// Pop the current frame, freeing all its allocations and pinned values.
    /// Empty chunks are harvested into the pool.
    fn pop_frame(&mut self) {
        if self.frames.len() > 1 {
            GC_STATS.bump(&GC_STATS.frame_pops);
            let frame = self.frames.pop().unwrap();
            self.drop_and_harvest_frame(frame);
            // Dropped frame's pointers could collide with future allocations.
            self.promote_cache.clear();
        }
    }

    /// Pop the current frame, deep-clone `val` into the parent frame,
    /// then free the popped frame.  Uses chunk-range ownership checks
    /// instead of building a HashSet.  `promote_cache` dedups shared
    /// subtrees in the value being lifted.
    fn pop_frame_promote(&mut self, val: *mut Value) -> *mut Value {
        if self.frames.len() <= 1 || val.is_null() {
            return val;
        }
        GC_STATS.bump(&GC_STATS.frame_pops);
        // Child frame's pointers are about to be freed — cache must not
        // retain them across the call.  We clear before cloning so the
        // clone_from_child recursion starts with a fresh cache (its keys
        // are child pointers, which are invalid after drop).
        self.promote_cache.clear();
        let child = self.frames.pop().unwrap();
        // Clone into the (now-current) parent frame before dropping the child.
        let promoted = self.clone_from_child(val, &child);
        self.drop_and_harvest_frame(child);
        self.promote_cache.clear();
        promoted
    }

    // ── Promote (selective clone into pinned) ────────────────────

    /// Selectively clone `val` so it survives the upcoming `reset_to`.
    /// Values already safe (singletons, parent-frame, text-cache) are
    /// returned as-is; only chunk-resident values are deep-cloned.
    ///
    /// `promote_cache` dedups shared subtrees during this call and
    /// across subsequent calls within the same frame/mark window — a
    /// second `promote(val)` on the same pointer returns the already-
    /// pinned clone without re-cloning.  The cache is invalidated at
    /// the events that could reuse its keys: `reset_to`, `push_frame`,
    /// `pop_frame`, and `pop_frame_promote`.
    fn promote(&mut self, val: *mut Value) -> *mut Value {
        self.promote_value(val)
    }

    /// `val` is NOT in the current frame's chunks (safe from reset),
    /// but its children might be.  Recurse and copy-on-write if needed.
    ///
    /// For Records and Relations, uses lazy-materialization: the new
    /// backing Vec is only allocated on the first child that actually
    /// changes.  If no child changes, `val` is returned unchanged and
    /// no allocation occurs.  Previously we always built a fresh Vec
    /// of the same length and only threw it away if nothing changed —
    /// pure overhead in the common "already-safe value reached via
    /// recursion" case.
    fn promote_children(&mut self, val: *mut Value) -> *mut Value {
        if val.is_null() { return val; }
        // Tagged pointers encode leaf values inline; they have no
        // heap-resident payload, so there's nothing to walk.
        if is_tagged(val) { return val; }
        match unsafe { &*val } {
            Value::Int(_) | Value::Float(_) | Value::Text(_)
            | Value::Bool(_) | Value::Bytes(_) | Value::Unit => val,

            Value::Record(fields) => {
                let mut new_fields: Option<Vec<RecordField>> = None;
                for (i, f) in fields.iter().enumerate() {
                    let nv = self.promote_value(f.value);
                    if let Some(vec) = new_fields.as_mut() {
                        vec.push(RecordField { name: f.name.clone(), value: nv });
                    } else if nv != f.value {
                        // First divergence: materialize, copy prefix.
                        let mut vec = take_record_vec(fields.len());
                        for prev in &fields[..i] {
                            vec.push(RecordField { name: prev.name.clone(), value: prev.value });
                        }
                        vec.push(RecordField { name: f.name.clone(), value: nv });
                        new_fields = Some(vec);
                    }
                }
                match new_fields {
                    Some(v) => self.alloc_pinned(Value::Record(v)),
                    None => val,
                }
            }
            Value::Relation(rows) => {
                let mut new_rows: Option<Vec<*mut Value>> = None;
                for (i, &r) in rows.iter().enumerate() {
                    let nr = self.promote_value(r);
                    if let Some(vec) = new_rows.as_mut() {
                        vec.push(nr);
                    } else if nr != r {
                        let mut vec = Vec::with_capacity(rows.len());
                        vec.extend_from_slice(&rows[..i]);
                        vec.push(nr);
                        new_rows = Some(vec);
                    }
                }
                match new_rows {
                    Some(v) => self.alloc_pinned(Value::Relation(v)),
                    None => val,
                }
            }
            Value::Constructor(tag, inner) => {
                let ni = self.promote_value(*inner);
                if ni != *inner {
                    self.alloc_pinned(Value::Constructor(tag.clone(), ni))
                } else { val }
            }
            Value::Function(f) => {
                let ne = self.promote_value(f.env);
                if ne != f.env {
                    self.alloc_pinned(Value::Function(Box::new(FunctionInner {
                        fn_ptr: f.fn_ptr,
                        env: ne,
                        source: f.source.clone(),
                    })))
                } else { val }
            }
            Value::IO(fp, env) => {
                let ne = self.promote_value(*env);
                if ne != *env {
                    self.alloc_pinned(Value::IO(*fp, ne))
                } else { val }
            }
            Value::Pair(a, b) => {
                let na = self.promote_value(*a);
                let nb = self.promote_value(*b);
                if na != *a || nb != *b {
                    self.alloc_pinned(Value::Pair(na, nb))
                } else { val }
            }
        }
    }

    /// Top-level recursive promote: dispatch chunk-resident vs safe.
    ///
    /// Uses `promote_cache` to dedup DAG structure — if two parents share
    /// a child pointer, we clone the child once and both parents reference
    /// the same promoted pointer afterward.
    fn promote_value(&mut self, val: *mut Value) -> *mut Value {
        if val.is_null() { return val; }
        GC_STATS.bump(&GC_STATS.promotes);
        if let Some(&cached) = self.promote_cache.get(&val) {
            GC_STATS.bump(&GC_STATS.promote_cache_hits);
            return cached;
        }
        // Lazy promote: if `val` already lives in this frame's pinned
        // chunks it was produced by a prior promote, so its children
        // were walked at that time and everything reachable from it is
        // guaranteed safe.  Skip the recursive descent entirely —
        // otherwise `promote_children` would re-walk the whole subtree
        // only to discover every child returns identity.  Hot on
        // do-blocks that yield the same already-pinned accumulator
        // across iterations.
        let frame = self.current_frame();
        if frame.owns_in_pinned_chunks(val) {
            return val;
        }
        let promoted = if frame.owns_in_chunks(val) {
            self.clone_into_pinned(val)
        } else {
            self.promote_children(val)
        };
        if promoted != val {
            self.promote_cache.insert(val, promoted);
        }
        promoted
    }

    /// Deep-clone a chunk-resident value into the pinned set.
    /// Children are processed with `promote_value` (selective).
    fn clone_into_pinned(&mut self, val: *mut Value) -> *mut Value {
        if val.is_null() { return val; }
        // Tagged pointers carry their full payload inline — they're
        // already safe from any arena reset.  No pinned clone needed.
        if is_tagged(val) { return val; }
        GC_STATS.bump(&GC_STATS.clone_into_pinned);
        let cloned = match unsafe { &*val } {
            Value::Int(n) => Value::Int(*n),
            Value::Float(f) => Value::Float(*f),
            Value::Text(s) => Value::Text(s.clone()),
            Value::Bool(b) => Value::Bool(*b),
            Value::Bytes(b) => Value::Bytes(b.clone()),
            Value::Unit => Value::Unit,
            Value::Record(fields) => {
                let mut new_fields = take_record_vec(fields.len());
                for f in fields.iter() {
                    new_fields.push(RecordField {
                        name: f.name.clone(),
                        value: self.promote_value(f.value),
                    });
                }
                Value::Record(new_fields)
            }
            Value::Relation(rows) => {
                // Fast path: if every row is a leaf primitive that is
                // not owned by the current frame's chunks, `promote_value`
                // would return each pointer unchanged.  Skip the per-row
                // dispatch and bulk-copy the pointer slice.  This hits
                // hard on primitive-valued relations (e.g. `sum [1..n]`,
                // filter/map over a column of Ints).  Use the
                // relation-vec pool so the fresh backing allocation
                // reuses pooled capacity instead of hitting malloc.
                let frame_idx = self.frames.len() - 1;
                let mut new_rows = take_relation_vec(rows.len());
                let all_identity = rows.iter().all(|&r| {
                    if r.is_null() { return true; }
                    // Tagged pointers carry leaf values inline —
                    // always identity-copyable, never chunk-owned.
                    if is_tagged(r) { return true; }
                    let is_leaf = matches!(unsafe { &*r },
                        Value::Int(_) | Value::Float(_)
                        | Value::Bool(_) | Value::Unit | Value::Text(_)
                        | Value::Bytes(_));
                    is_leaf && !self.frames[frame_idx].owns_in_chunks(r)
                });
                if all_identity {
                    new_rows.extend_from_slice(rows);
                } else {
                    for &r in rows.iter() {
                        new_rows.push(self.promote_value(r));
                    }
                }
                Value::Relation(new_rows)
            }
            Value::Constructor(tag, inner) => {
                Value::Constructor(tag.clone(), self.promote_value(*inner))
            }
            Value::Function(f) => {
                Value::Function(Box::new(FunctionInner {
                    fn_ptr: f.fn_ptr,
                    env: self.promote_value(f.env),
                    source: f.source.clone(),
                }))
            }
            Value::IO(fp, env) => {
                Value::IO(*fp, self.promote_value(*env))
            }
            Value::Pair(a, b) => {
                Value::Pair(self.promote_value(*a), self.promote_value(*b))
            }
        };
        self.alloc_pinned(cloned)
    }

    /// Bump-allocate a value into the current frame's pinned chunks.
    ///
    /// Previously this used `Box::into_raw`, one malloc per promoted
    /// value — a measurable cost in yield-heavy do-blocks.  Pinned
    /// chunks share the `free_chunks` pool with young chunks so the
    /// amortised cost is near zero once steady-state reuse kicks in.
    fn alloc_pinned(&mut self, v: Value) -> *mut Value {
        GC_STATS.bump(&GC_STATS.pinned_allocs);
        let need_new = self
            .current_frame()
            .pinned_chunks
            .last()
            .map_or(true, Chunk::is_full);
        if need_new {
            let chunk = self.take_chunk();
            self.current_frame().push_pinned_chunk(chunk);
        }
        self.current_frame()
            .pinned_chunks
            .last_mut()
            .unwrap()
            .alloc(v)
    }

    // ── Pop-frame selective clone ────────────────────────────────

    /// Deep-clone `val` into the parent frame, only cloning values owned
    /// by `child`.  Uses chunk-range + pinned checks (no HashSet).
    /// Consults `promote_cache` to share cloned subtrees.
    fn clone_from_child(&mut self, val: *mut Value, child: &Frame) -> *mut Value {
        if val.is_null() { return val; }
        // Use pinned-first ownership to short-circuit on the deep-pinned
        // spine that dominates values promoted across frame boundaries.
        if !child.owns_pinned_first(val) {
            return val;
        }
        if let Some(&cached) = self.promote_cache.get(&val) {
            return cached;
        }
        let cloned = match unsafe { &*val } {
            Value::Int(n) => Value::Int(*n),
            Value::Float(f) => Value::Float(*f),
            Value::Text(s) => Value::Text(s.clone()),
            Value::Bool(b) => Value::Bool(*b),
            Value::Bytes(b) => Value::Bytes(b.clone()),
            Value::Unit => Value::Unit,
            Value::Record(fields) => {
                let mut new_fields = take_record_vec(fields.len());
                for f in fields.iter() {
                    new_fields.push(RecordField {
                        name: f.name.clone(),
                        value: self.clone_from_child(f.value, child),
                    });
                }
                Value::Record(new_fields)
            }
            Value::Relation(rows) => {
                let mut new_rows = take_relation_vec(rows.len());
                for &r in rows.iter() {
                    new_rows.push(self.clone_from_child(r, child));
                }
                Value::Relation(new_rows)
            }
            Value::Constructor(tag, inner) => {
                Value::Constructor(tag.clone(), self.clone_from_child(*inner, child))
            }
            Value::Function(f) => {
                Value::Function(Box::new(FunctionInner {
                    fn_ptr: f.fn_ptr,
                    env: self.clone_from_child(f.env, child),
                    source: f.source.clone(),
                }))
            }
            Value::IO(fp, env) => {
                Value::IO(*fp, self.clone_from_child(*env, child))
            }
            Value::Pair(a, b) => {
                Value::Pair(
                    self.clone_from_child(*a, child),
                    self.clone_from_child(*b, child),
                )
            }
        };
        let out = self.alloc(cloned);
        self.promote_cache.insert(val, out);
        out
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        self.frames.clear();
    }
}

thread_local! {
    static ARENA: RefCell<Arena> = RefCell::new(Arena::new());
}

// ── Global state for spawn/threads ───────────────────────────────

/// Database path — set in knot_db_open so spawned threads can open their own connections.
static DB_PATH: Mutex<String> = Mutex::new(String::new());

/// Count of live detached `fork`ed threads.  Incremented when a thread is
/// spawned, decremented when it exits (via a drop guard so panics still
/// decrement).  `knot_threads_join` waits on `ACTIVE_FORKS_CVAR` until the
/// count reaches zero.  Using a counter + condvar instead of a `Vec` of
/// `JoinHandle`s means we never accumulate handles across forks — the
/// previous `THREAD_HANDLES: Mutex<Vec<_>>` grew unboundedly in programs
/// that `fork`ed repeatedly.
static ACTIVE_FORKS: AtomicUsize = AtomicUsize::new(0);
static ACTIVE_FORKS_MUTEX: Mutex<()> = Mutex::new(());
static ACTIVE_FORKS_CVAR: Condvar = Condvar::new();

// ── Process-level write serialization ────────────────────────────
//
// SQLite WAL allows only one writer at a time. We serialize writes in Rust
// so threads never contend at the SQLite level.  The lock is reentrant:
// `atomic` blocks acquire it for their full duration, and individual write
// functions (replace, set, etc.) inside the block increment the depth
// without re-acquiring.

static WRITE_LOCKED: AtomicBool = AtomicBool::new(false);

thread_local! {
    static WRITE_LOCK_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// RAII guard returned by `write_lock_guard()`.
struct WriteLockGuard;

impl Drop for WriteLockGuard {
    fn drop(&mut self) {
        write_lock_release();
    }
}

fn write_lock_acquire() {
    let reentrant = WRITE_LOCK_DEPTH.with(|d| {
        let depth = d.get();
        d.set(depth + 1);
        depth > 0
    });
    if !reentrant {
        while WRITE_LOCKED
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::thread::yield_now();
        }
    }
}

fn write_lock_release() {
    let release = WRITE_LOCK_DEPTH.with(|d| {
        let depth = d.get();
        assert!(depth > 0, "write_lock_release without matching acquire");
        d.set(depth - 1);
        depth == 1
    });
    if release {
        WRITE_LOCKED.store(false, Ordering::Release);
    }
}

/// Release any write locks held by the current thread.
/// Used for panic recovery (e.g. in the HTTP handler's catch_unwind)
/// to prevent permanent deadlocks when a panic occurs inside an
/// atomic block.
fn write_lock_force_release() {
    let had_lock = WRITE_LOCK_DEPTH.with(|d| {
        let depth = d.get();
        if depth > 0 {
            d.set(0);
            true
        } else {
            false
        }
    });
    if had_lock {
        WRITE_LOCKED.store(false, Ordering::Release);
    }
}

/// Acquire the write lock, returning an RAII guard that releases on drop.
fn write_lock_guard() -> WriteLockGuard {
    write_lock_acquire();
    WriteLockGuard
}

// ── STM retry support ────────────────────────────────────────────

/// Per-table version counters. RwLock guards the map structure; individual
/// counters are `AtomicU64` so reads and increments avoid the write lock.
static TABLE_VERSIONS: std::sync::LazyLock<RwLock<HashMap<String, Arc<AtomicU64>>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

/// Per-thread wake slot for targeted retry notification.
/// Each retrying thread registers a slot with the tables it read;
/// `notify_relation_changed` wakes only slots watching the changed table.
struct WakeSlot {
    woken: Mutex<bool>,
    cvar: Condvar,
}

impl WakeSlot {
    fn new() -> Self {
        WakeSlot {
            woken: Mutex::new(false),
            cvar: Condvar::new(),
        }
    }
    fn wake(&self) {
        *self.woken.lock().unwrap() = true;
        self.cvar.notify_one();
    }
    fn wait(&self, timeout: Duration) {
        let guard = self.woken.lock().unwrap();
        if *guard {
            return;
        }
        let _ = self.cvar.wait_timeout_while(guard, timeout, |woken| !*woken);
    }
}

/// Registry of wake slots per table. Only touched on retry (register) and
/// write (notify). Dead Weak refs are cleaned up lazily during notification.
static TABLE_WATCHERS: std::sync::LazyLock<Mutex<HashMap<String, Vec<Weak<WakeSlot>>>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

thread_local! {
    /// Set by `knot_stm_retry`, checked by `knot_stm_check_and_clear` after atomic body.
    static STM_RETRY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Set when an atomic IO body skips early via a failed constructor-pattern
    /// bind or false `where` guard. Checked after the body so the surrounding
    /// `atomic` rolls back instead of committing partial writes.
    static STM_SKIP: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Tables read during current atomic block, with version at read time.
    static STM_READ_VERSIONS: RefCell<HashMap<String, u64>> = RefCell::new(HashMap::new());
    /// Tables written during current atomic block (notification deferred to commit).
    static STM_WRITTEN_TABLES: RefCell<HashSet<String>> = RefCell::new(HashSet::new());
    /// Stack of saved (read_versions, written_tables) for nested atomic blocks.
    /// Pushed before inner atomic's loop, popped/merged on inner commit.
    static STM_TRACKING_STACK: RefCell<Vec<(HashMap<String, u64>, HashSet<String>)>> = RefCell::new(Vec::new());
}

/// Save current STM read/write tracking onto a stack (for nested atomics).
/// Called before a nested atomic's retry loop so inner snapshot/retry
/// doesn't destroy outer tracking state.
#[unsafe(no_mangle)]
pub extern "C" fn knot_stm_push() {
    let reads = STM_READ_VERSIONS.with(|rv| rv.borrow().clone());
    let writes = STM_WRITTEN_TABLES.with(|wt| wt.borrow().clone());
    STM_TRACKING_STACK.with(|stack| {
        stack.borrow_mut().push((reads, writes));
    });
}

/// Pop saved STM tracking from the stack and merge inner tracking into it.
/// Called after a nested atomic commits to restore outer read/write sets
/// combined with inner reads/writes.
#[unsafe(no_mangle)]
pub extern "C" fn knot_stm_pop_merge() {
    let (saved_reads, saved_writes) = STM_TRACKING_STACK.with(|stack| {
        stack
            .borrow_mut()
            .pop()
            .expect("knot runtime: STM tracking stack underflow")
    });
    // Merge: outer reads + inner reads (keep earliest version per table).
    // For tables read in both: outer's saved version is older (versions are
    // monotonic), so it wins — the outer commit must retry on any change
    // since the earliest observation.
    STM_READ_VERSIONS.with(|rv| {
        let mut rv = rv.borrow_mut();
        for (table, ver) in saved_reads {
            rv.entry(table)
                .and_modify(|v| {
                    if ver < *v {
                        *v = ver;
                    }
                })
                .or_insert(ver);
        }
    });
    // Merge: outer writes + inner writes
    STM_WRITTEN_TABLES.with(|wt| {
        let mut wt = wt.borrow_mut();
        for table in saved_writes {
            wt.insert(table);
        }
    });
}

/// Notify waiting `retry` callers that a specific relation has changed.
/// Only wakes threads that registered interest in this table.
/// Uses a read lock + atomic increment for existing tables (common case);
/// falls back to a write lock only for the first write to a new table.
fn notify_relation_changed(name: &str) {
    let needs_insert = {
        let versions = TABLE_VERSIONS.read().unwrap();
        if let Some(v) = versions.get(name) {
            v.fetch_add(1, Ordering::Release);
            false
        } else {
            true
        }
    };
    if needs_insert {
        let mut versions = TABLE_VERSIONS.write().unwrap();
        versions
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .fetch_add(1, Ordering::Release);
    }
    let mut watchers = TABLE_WATCHERS.lock().unwrap();
    if let Some(slots) = watchers.get_mut(name) {
        slots.retain(|weak| match weak.upgrade() {
            Some(slot) => {
                slot.wake();
                true
            }
            None => false,
        });
    }
}

/// Record that a table was read inside an atomic block.
/// Captures the version at first-read time as the baseline for retry.
/// Skips the RwLock and allocation if already tracking this table.
/// Exported wrapper for codegen-emitted SQL queries that need STM tracking.
#[unsafe(no_mangle)]
pub extern "C" fn knot_stm_track_read(name_ptr: *const u8, name_len: usize) {
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    stm_track_read(name);
}

fn stm_track_read(name: &str) {
    let already = STM_READ_VERSIONS.with(|rv| rv.borrow().contains_key(name));
    if already {
        return;
    }
    let ver = TABLE_VERSIONS
        .read()
        .unwrap()
        .get(name)
        .map(|v| v.load(Ordering::Acquire))
        .unwrap_or(0);
    STM_READ_VERSIONS.with(|rv| {
        rv.borrow_mut().entry(name.to_string()).or_insert(ver);
    });
}

/// Record that a table was written inside an atomic block.
/// The actual notification is deferred to commit.
/// Skips the allocation if already tracking this table.
fn stm_track_write(name: &str) {
    let already = STM_WRITTEN_TABLES.with(|wt| wt.borrow().contains(name));
    if already {
        return;
    }
    STM_WRITTEN_TABLES.with(|wt| {
        wt.borrow_mut().insert(name.to_string());
    });
}

// ── Debug mode ───────────────────────────────────────────────────

// ── ToJSON dispatcher ────────────────────────────────────────────
//
// Stores the compiled toJson trait dispatcher function pointer so the
// runtime (e.g. HTTP response serialization) can use custom ToJSON impls.
// Written once during program init, read by listen handler threads.

static TO_JSON_FN: AtomicUsize = AtomicUsize::new(0);

fn debug_sql(sql: &str) {
    log_debug!("[SQL] {}", sql);
}

fn debug_sql_params(sql: &str, params: &[rusqlite::types::Value]) {
    if log::debug_enabled() {
        if params.is_empty() {
            log_debug!("[SQL] {}", sql);
        } else {
            log_debug!("[SQL] {} -- params: {:?}", sql, params);
        }
    }
}

// ── CLI constant overrides ───────────────────────────────────────

/// Look up a command-line override for a top-level constant.
///
/// Scans `std::env::args()` for `--{name}=value` or `--{name} value`.
/// `type_tag`: 0=Int, 1=Float, 2=Text, 3=Bool, 4=Maybe Int, 5=Maybe Float, 6=Maybe Text, 7=Maybe Bool.
/// Maybe tags automatically wrap the parsed value in `Just`.
/// Returns a boxed `Value` if found, or null if absent.
/// On parse error, prints a message and exits.
#[unsafe(no_mangle)]
pub extern "C" fn knot_override_lookup(
    name_ptr: *const u8,
    name_len: usize,
    type_tag: i32,
) -> *mut Value {
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let flag = format!("--{}", name);

    let args: Vec<String> = std::env::args().collect();
    let mut value: Option<String> = None;

    let mut i = 1; // skip argv[0]
    while i < args.len() {
        if let Some(v) = args[i].strip_prefix(&flag) {
            if let Some(v) = v.strip_prefix('=') {
                value = Some(v.to_string());
                break;
            } else if v.is_empty() {
                // --name value (two-arg form)
                if i + 1 < args.len() && !args[i + 1].starts_with("--") {
                    value = Some(args[i + 1].clone());
                    break;
                }
            }
        }
        i += 1;
    }

    let val_str = match value {
        Some(v) => v,
        None => return std::ptr::null_mut(),
    };

    // Tags 4-7 are Maybe variants of 0-3
    let base_tag = if type_tag >= 4 { type_tag - 4 } else { type_tag };
    let wrap_maybe = type_tag >= 4;

    let type_name = match base_tag {
        0 => "Int",
        1 => "Float",
        2 => "Text",
        3 => "Bool",
        _ => "unknown",
    };

    let inner = match base_tag {
        0 => {
            // Int
            match val_str.parse::<i64>() {
                Ok(n) => alloc_int(n),
                Err(_) => {
                    eprintln!(
                        "Error: invalid value '{}' for --{} (expected {})",
                        val_str, name, type_name
                    );
                    std::process::exit(1);
                }
            }
        }
        1 => {
            // Float
            match val_str.parse::<f64>() {
                Ok(n) => alloc(Value::Float(n)),
                Err(_) => {
                    eprintln!(
                        "Error: invalid value '{}' for --{} (expected {})",
                        val_str, name, type_name
                    );
                    std::process::exit(1);
                }
            }
        }
        2 => {
            // Text
            alloc(Value::Text(Arc::from(val_str.as_str())))
        }
        3 => {
            // Bool
            match val_str.as_str() {
                "true" | "1" => encode_bool(true),
                "false" | "0" => encode_bool(false),
                _ => {
                    eprintln!(
                        "Error: invalid value '{}' for --{} (expected true or false)",
                        val_str, name
                    );
                    std::process::exit(1);
                }
            }
        }
        _ => return std::ptr::null_mut(),
    };

    if wrap_maybe {
        let tag = intern_str("Just");
        alloc(Value::Constructor(tag, inner))
    } else {
        inner
    }
}

/// Look up a CLI override that *must* be supplied (no in-source default).
/// Behaves like `knot_override_lookup`, but exits with an error if the user
/// did not pass the flag. Used for body-less top-level constant declarations.
#[unsafe(no_mangle)]
pub extern "C" fn knot_override_required_lookup(
    name_ptr: *const u8,
    name_len: usize,
    type_tag: i32,
) -> *mut Value {
    let result = knot_override_lookup(name_ptr, name_len, type_tag);
    if !result.is_null() {
        return result;
    }
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    eprintln!(
        "Error: missing required argument --{}\n  pass --{}=<value> on the command line, or run --help for usage",
        name, name
    );
    std::process::exit(1);
}

/// Run a refinement predicate against a CLI-supplied value. Exits if it fails.
/// `type_label` is the refined type name (or empty for inline refinements).
#[unsafe(no_mangle)]
pub extern "C" fn knot_override_refinement_check(
    db: *mut c_void,
    value: *mut Value,
    predicate: *mut Value,
    name_ptr: *const u8,
    name_len: usize,
    type_label_ptr: *const u8,
    type_label_len: usize,
) -> *mut Value {
    let result = knot_value_call(db, predicate, value);
    match unsafe { as_ref(result) } {
        Value::Bool(true) => value,
        _ => {
            let name = unsafe { str_from_raw(name_ptr, name_len) };
            let label = unsafe { str_from_raw(type_label_ptr, type_label_len) };
            if label.is_empty() {
                eprintln!(
                    "Error: value supplied for --{} does not satisfy its refinement predicate",
                    name
                );
            } else {
                eprintln!(
                    "Error: value supplied for --{} does not satisfy '{}' predicate",
                    name, label
                );
            }
            std::process::exit(1);
        }
    }
}

/// Decode a default-value string emitted by the compiler.
///
/// The compiler escapes `\` as `\\` and `,` as `\c` so the entry
/// separator stays unambiguous. Anything else passes through.
fn decode_default_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\\') => out.push('\\'),
                Some('c') => out.push(','),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Per-entry kind decoded from the help descriptor.
enum DefaultKind {
    /// `name:type:!` — body-less constant; must be supplied on the CLI.
    Required,
    /// `name:type:=<value>` — has an in-source default we can display.
    Default(String),
    /// `name:type` — has a default but it isn't a literal we can render.
    OpaqueDefault,
}

/// Check for `--help` and print available constant overrides.
///
/// `desc` is a compile-time string. Each comma-separated entry is one of:
///   `name:type:!`           (required CLI arg)
///   `name:type:=<value>`    (overridable, default from source)
///   `name:type`             (overridable, default not displayable)
/// If `--help` is found, prints usage and exits.
#[unsafe(no_mangle)]
pub extern "C" fn knot_override_check_help(desc_ptr: *const u8, desc_len: usize) {
    let has_help = std::env::args().any(|a| a == "--help");
    if !has_help {
        return;
    }

    let desc = unsafe { str_from_raw(desc_ptr, desc_len) };
    let exe = std::env::args().next().unwrap_or_else(|| "program".to_string());

    let mut overrides: Vec<(&str, &str, DefaultKind)> = Vec::new();
    if !desc.is_empty() {
        for entry in desc.split(',') {
            let mut parts = entry.splitn(3, ':');
            if let (Some(name), Some(ty)) = (parts.next(), parts.next()) {
                let kind = match parts.next() {
                    Some("!") => DefaultKind::Required,
                    Some(rest) if rest.starts_with('=') => {
                        DefaultKind::Default(decode_default_value(&rest[1..]))
                    }
                    _ => DefaultKind::OpaqueDefault,
                };
                overrides.push((name, ty, kind));
            }
        }
    }
    let has_debug_override = overrides.iter().any(|(n, _, _)| *n == "debug");

    eprintln!("Usage: {} [OPTIONS]", exe);
    eprintln!();
    eprintln!("Options:");
    if !has_debug_override {
        eprintln!("  --debug                    Enable debug output");
    }
    eprintln!("  --help                     Show this help message");
    eprintln!(
        "  --http-max-body-bytes=N    Cap HTTP request and response bodies (suffixes: K, M, G; default 16M)"
    );

    for (name, ty, kind) in &overrides {
        let label = match kind {
            DefaultKind::Required => "(required)".to_string(),
            DefaultKind::Default(v) => format!("(default: {})", v),
            DefaultKind::OpaqueDefault => "(default from source)".to_string(),
        };
        eprintln!("  --{:<24} {} {}", name, ty, label);
    }

    std::process::exit(0);
}

// ── String interner ──────────────────────────────────────────────
//
// Record field names and constructor tags are drawn from a small,
// heavily-repeated vocabulary ("name", "id", "value", "Ok", "Err", etc.).
// Interning them into `Arc<str>` means every field/constructor in the
// program shares a single heap allocation per unique name, and clones
// are atomic-increment-cheap.  The interner is a global Mutex-guarded
// structure; callers hit the `Mutex` once per record construction but
// all subsequent cloning/comparison is lock-free.
//
// Bounded by an LRU so long-running programs with dynamic tag-name
// vocabularies (HTTP handlers synthesizing JSON keys, user-supplied
// record field names via reflection, etc.) don't silently stop
// sharing after the cap is hit.  Eviction is safe because
// `Arc<str>` equality is content-based (not pointer-based) — evicting
// an entry leaves outstanding clones valid and correctly comparable,
// and a subsequent call for the same text will just intern it anew.
//
// Intended as a *sharing* cache: a miss still returns a correct (but
// non-shared) `Arc<str>`, so a low hit rate degrades memory use but
// never correctness.

/// Maximum number of interned strings.  Typical programs have at most
/// a few hundred unique field names / constructor tags; 65536 leaves
/// ample headroom while capping memory at (avg_str_len + 24) * 65536
/// bytes — roughly 2 MB for 8-byte average strings.
const INTERNER_CAP: usize = 65_536;

/// LRU sentinel indicating "no neighbor" in the doubly-linked list.
const INTERNER_NIL: u32 = u32::MAX;

struct InternerEntry {
    key: String,
    val: Arc<str>,
    prev: u32,
    next: u32,
}

/// Bounded LRU cache keyed by string content.  O(1) get and insert.
///
/// `entries` is a slab: removed slots get pushed onto the `free` chain
/// for reuse.  `head` is MRU, `tail` is LRU.  `map` points from the
/// string content to the slab index.
struct StringInterner {
    map: HashMap<String, u32>,
    entries: Vec<InternerEntry>,
    head: u32,
    tail: u32,
    free: u32,
}

impl StringInterner {
    fn new() -> Self {
        StringInterner {
            map: HashMap::new(),
            entries: Vec::new(),
            head: INTERNER_NIL,
            tail: INTERNER_NIL,
            free: INTERNER_NIL,
        }
    }

    fn detach(&mut self, idx: u32) {
        let (prev, next) = {
            let e = &self.entries[idx as usize];
            (e.prev, e.next)
        };
        if prev != INTERNER_NIL {
            self.entries[prev as usize].next = next;
        } else {
            self.head = next;
        }
        if next != INTERNER_NIL {
            self.entries[next as usize].prev = prev;
        } else {
            self.tail = prev;
        }
    }

    fn push_head(&mut self, idx: u32) {
        let old_head = self.head;
        {
            let e = &mut self.entries[idx as usize];
            e.prev = INTERNER_NIL;
            e.next = old_head;
        }
        if old_head != INTERNER_NIL {
            self.entries[old_head as usize].prev = idx;
        } else {
            self.tail = idx;
        }
        self.head = idx;
    }

    /// Return the interned `Arc<str>` for `s` if present, promoting it
    /// to MRU.  Returns `None` on miss.
    fn get(&mut self, s: &str) -> Option<Arc<str>> {
        let idx = *self.map.get(s)?;
        self.detach(idx);
        self.push_head(idx);
        Some(self.entries[idx as usize].val.clone())
    }

    /// Insert `(s, arc)`, evicting the LRU entry if at capacity.
    fn insert(&mut self, s: String, arc: Arc<str>) {
        if self.map.len() >= INTERNER_CAP && self.tail != INTERNER_NIL {
            // Evict LRU and reuse the slot.
            let victim = self.tail;
            self.detach(victim);
            let old_key = std::mem::take(&mut self.entries[victim as usize].key);
            self.map.remove(&old_key);
            self.entries[victim as usize].key = s.clone();
            self.entries[victim as usize].val = arc;
            self.push_head(victim);
            self.map.insert(s, victim);
            return;
        }
        let idx = if self.free != INTERNER_NIL {
            let slot = self.free;
            self.free = self.entries[slot as usize].next;
            self.entries[slot as usize] = InternerEntry {
                key: s.clone(),
                val: arc,
                prev: INTERNER_NIL,
                next: INTERNER_NIL,
            };
            slot
        } else {
            let slot = self.entries.len() as u32;
            self.entries.push(InternerEntry {
                key: s.clone(),
                val: arc,
                prev: INTERNER_NIL,
                next: INTERNER_NIL,
            });
            slot
        };
        self.push_head(idx);
        self.map.insert(s, idx);
    }
}

static STRING_INTERNER: std::sync::LazyLock<Mutex<StringInterner>> =
    std::sync::LazyLock::new(|| Mutex::new(StringInterner::new()));

/// Per-thread L1 cache in front of the Mutex-guarded global interner.
///
/// `intern_str` is called every time a record is built (once per field)
/// and every constructor tag — under fork-parallel workloads the Mutex
/// becomes a contention hotspot even though most lookups hit a tiny
/// vocabulary.  The L1 holds recently-seen `Arc<str>`s (themselves
/// produced by the global interner and therefore still content-shared
/// across threads) and short-circuits the lock on hit.
///
/// Capacity is deliberately small: the live field-name/constructor-tag
/// vocabulary of a typical program is dozens of entries, and keeping
/// the L1 small keeps its HashMap in a handful of cache lines.  On
/// overflow we drop everything and re-seed; this costs one extra
/// Mutex acquisition per stale entry, but the hot working set rebuilds
/// in-place so steady-state hit rate returns immediately.
const INTERN_L1_CAP: usize = 512;

thread_local! {
    static INTERN_L1: RefCell<HashMap<String, Arc<str>>> =
        RefCell::new(HashMap::with_capacity(64));
}

/// Intern a string, returning a shared `Arc<str>`.  Repeated calls with
/// the same contents (that haven't been evicted from the LRU) return
/// the same `Arc` without re-allocating.  When the interner is full
/// the LRU entry is evicted and the new string takes its place —
/// correctness is preserved regardless because `Arc<str>` equality
/// compares by content, not pointer identity.
///
/// Two-tier: thread-local `INTERN_L1` short-circuits without the global
/// Mutex when the string is in the thread's recently-seen set; on miss
/// we consult the global LRU interner, which returns a cross-thread
/// shared `Arc<str>`, and populate the L1 for next time.
fn intern_str(s: &str) -> Arc<str> {
    if let Some(arc) = INTERN_L1.with(|cell| cell.borrow().get(s).cloned()) {
        return arc;
    }
    let arc = {
        let mut table = STRING_INTERNER.lock().unwrap();
        if let Some(existing) = table.get(s) {
            existing
        } else {
            let arc: Arc<str> = Arc::from(s);
            table.insert(s.to_string(), arc.clone());
            arc
        }
    };
    INTERN_L1.with(|cell| {
        let mut m = cell.borrow_mut();
        if m.len() >= INTERN_L1_CAP {
            // Overflow: clear and rebuild.  The hot working set re-
            // populates on next access; stale long-tail entries get
            // evicted.  Cheaper than per-entry LRU bookkeeping for a
            // cache this small.
            m.clear();
        }
        m.insert(s.to_string(), arc.clone());
    });
    arc
}

// ── Pointer tagging (infrastructure) ─────────────────────────────
//
// Real `*mut Value` pointers land in either a chunk slot (aligned to
// at least 8 bytes by `ChunkData`'s `#[repr(align(64))]`) or a `Box`
// allocation (aligned to at least 8 bytes by the system allocator).
// That leaves the low 3 bits free for encoding small immediate values
// directly in the pointer — no allocation, no deref, no cache miss.
//
// Encoding:
//   low 3 bits == 0b000 : real boxed `*mut Value`
//   low 3 bits == 0b001 : `SmallInt` payload in upper 61 bits (signed, sign-extended)
//   low 3 bits == 0b010 : `Bool`; bit 3 holds `b as u8`
//   low 3 bits == 0b011 : `Unit` (the rest of the word is zero)
//   others              : reserved
//
// The encoders below are ready to use, but the runtime as a whole is
// not yet tag-aware: every `unsafe { &*val }` deref site (≈240 across
// `lib.rs`) would need to dispatch through a decoder before tagged
// pointers can safely flow through compiled-code boundaries.  Wiring
// that in universally requires a careful audit — the encoders are
// shipped here so the infrastructure is ready, but `alloc_int`
// et al. still return real pointers.  Activating tagging is a matter
// of (a) returning `encode_smallint(n)` from `alloc_int` when
// `n` fits in 61 bits, (b) teaching `as_ref` to decode, and (c)
// spot-checking that Drop-sensitive variants (`Relation` in particular)
// never receive tagged pointers.

#[allow(dead_code)]
pub(crate) const VALUE_TAG_MASK: usize = 0b111;
#[allow(dead_code)]
pub(crate) const TAG_SMALLINT: usize = 0b001;
#[allow(dead_code)]
pub(crate) const TAG_BOOL: usize = 0b010;
#[allow(dead_code)]
pub(crate) const TAG_UNIT: usize = 0b011;

/// Encode an `i64` as a tagged pointer if it fits in 61 signed bits.
/// Returns `None` otherwise — callers should fall through to a real
/// heap allocation for larger values.
#[inline]
#[allow(dead_code)]
pub(crate) fn encode_smallint(n: i64) -> Option<*mut Value> {
    // i61 range: −2^60 .. 2^60 − 1
    const MIN: i64 = -(1i64 << 60);
    const MAX: i64 = (1i64 << 60) - 1;
    if n < MIN || n > MAX { return None; }
    let raw = ((n as usize) << 3) | TAG_SMALLINT;
    Some(raw as *mut Value)
}

/// Encode a `bool` as a tagged pointer.  The two constants the
/// returned pointer can take (`0b1010` and `0b0010`) cannot collide
/// with any valid allocation thanks to the alignment guarantees.
#[inline]
#[allow(dead_code)]
pub(crate) fn encode_bool(b: bool) -> *mut Value {
    let raw = ((b as usize) << 3) | TAG_BOOL;
    raw as *mut Value
}

/// Encode a `Unit` as a tagged pointer.  All-tagged Units share the
/// same bit pattern, so identity comparisons work.
#[inline]
#[allow(dead_code)]
pub(crate) fn encode_unit() -> *mut Value {
    TAG_UNIT as *mut Value
}

/// Check whether a pointer is tagged (non-zero low bits).
#[inline]
#[allow(dead_code)]
pub(crate) fn is_tagged(p: *mut Value) -> bool {
    (p as usize) & VALUE_TAG_MASK != 0
}

/// Decode a tagged pointer into an owned `Value`.  Panics if `p` is
/// untagged — callers should gate on `is_tagged` first.
#[inline]
#[allow(dead_code)]
pub(crate) fn decode_tagged(p: *mut Value) -> Value {
    let raw = p as usize;
    match raw & VALUE_TAG_MASK {
        TAG_SMALLINT => {
            // Sign-extend from 61-bit field.
            let shifted = (raw as i64) >> 3;
            Value::Int(shifted)
        }
        TAG_BOOL => Value::Bool((raw >> 3) & 1 == 1),
        TAG_UNIT => Value::Unit,
        _ => panic!("knot runtime: decode_tagged on untagged pointer 0x{:x}", raw),
    }
}

// ── Value representation ──────────────────────────────────────────

/// Runtime representation of all Knot values.
///
/// Every Knot expression evaluates to a heap-allocated `Value`.
/// The Cranelift-generated code works exclusively with `*mut Value` pointers.
pub enum Value {
    /// 64-bit signed integer, inline.  Produced by `knot_value_int` /
    /// `alloc_int` for values that aren't already covered by the
    /// small-int singleton cache or the i61-range pointer-tagged path.
    /// Arithmetic uses checked operations and panics on overflow.
    Int(i64),
    Float(f64),
    /// Immutable UTF-8 text.  Stored as `Arc<str>` so `clone()` is a
    /// refcount bump (not a heap allocation) and large strings shared
    /// across threads (via `fork`) avoid deep copies.
    Text(Arc<str>),
    Bool(bool),
    /// Immutable byte buffer.  Stored as `Arc<[u8]>` for the same
    /// reasons as `Text`.
    Bytes(Arc<[u8]>),
    Unit,
    Record(Vec<RecordField>),
    Relation(Vec<*mut Value>),
    /// Constructor tag is interned via `intern_str` so the common vocabulary
    /// (`Ok`, `Err`, `Just`, ...) shares one allocation per name.
    Constructor(Arc<str>, *mut Value),
    /// (fn_ptr, env, source) — fn_ptr has signature: extern "C" fn(db, env, arg) -> *mut Value.
    /// `source` is the Knot expression that produced the function, used in
    /// diagnostics; interned because the same lambda site is stringified
    /// repeatedly.
    ///
    /// Boxed so the three-field tuple doesn't bloat every `Value` slot.
    Function(Box<FunctionInner>),
    /// IO thunk — fn_ptr: extern "C" fn(db: *mut KnotDb, env: *mut Value) -> *mut Value
    IO(*const u8, *mut Value),
    /// Internal two-pointer tuple used to build closure envs without
    /// allocating a `Record` + `Vec<RecordField>` + two `String`s per
    /// construction.  Used by `knot_io_bind`, `knot_io_then`, `knot_io_map`,
    /// etc. — the env doesn't need field names because the thunk knows the
    /// positions statically.  Must not escape to user code; matches on
    /// `Value` outside of arena/GC handling should treat this as an
    /// internal implementation detail.
    Pair(*mut Value, *mut Value),
}

/// Payload for `Value::Function`: boxed to keep `Value` compact.
pub struct FunctionInner {
    pub fn_ptr: *const u8,
    pub env: *mut Value,
    pub source: Arc<str>,
}

pub struct RecordField {
    /// Field name, interned via `intern_str`.  Cloning an interned name
    /// is an atomic increment, and comparison is a pointer-equal fast
    /// path followed by the usual lexical compare.
    pub name: Arc<str>,
    pub value: *mut Value,
}

impl Drop for Value {
    fn drop(&mut self) {
        // Custom Drop intercepts `Value::Relation` / `Value::Record` to
        // return their backing Vec storage to the thread-local pool; all
        // other variants fall back to the compiler-generated field drop.
        match self {
            Value::Relation(rows) => {
                let owned = std::mem::take(rows);
                return_relation_vec(owned);
            }
            Value::Record(fields) => {
                let owned = std::mem::take(fields);
                return_record_vec(owned);
            }
            _ => {}
        }
    }
}

/// SQLite database handle.
pub struct KnotDb {
    pub conn: Connection,
    /// Nesting depth for `atomic` savepoints.
    atomic_depth: std::cell::Cell<usize>,
    /// Tracks which indexes have been created this session to avoid redundant DDL.
    indexed: RefCell<HashSet<String>>,
}

impl KnotDb {
    /// Create an index on `column` of `table` if one hasn't been created yet.
    fn ensure_index(&self, table: &str, column: &str) {
        let key = format!("{}:{}", table, column);
        if self.indexed.borrow().contains(&key) {
            return;
        }
        let idx_name = format!("_knot_auto_{}_{}", table, column);
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} ({});",
            quote_ident(&idx_name),
            quote_ident(table),
            quote_ident(column)
        );
        debug_sql(&sql);
        let _ = self.conn.execute_batch(&sql);
        self.indexed.borrow_mut().insert(key);
    }

    /// Ensure indexes on all columns referenced in a WHERE clause.
    /// Column names in generated SQL are always double-quoted identifiers.
    fn ensure_indexes_for_where(&self, table: &str, where_clause: &str) {
        for col in extract_where_columns(where_clause) {
            self.ensure_index(table, &col);
        }
    }
}

/// Extract column names from a generated SQL WHERE clause.
/// Columns are always double-quoted identifiers (e.g. `"age"`, `"name"`).
fn extract_where_columns(sql: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut columns = Vec::new();
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '"' {
            let mut col = String::new();
            loop {
                match chars.next() {
                    Some('"') => {
                        if chars.peek() == Some(&'"') {
                            col.push('"');
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    Some(ch) => col.push(ch),
                    None => break,
                }
            }
            if seen.insert(col.clone()) {
                columns.push(col);
            }
        }
    }
    columns
}

// ── Helpers ───────────────────────────────────────────────────────

fn alloc(v: Value) -> *mut Value {
    ARENA.with(|a| a.borrow_mut().alloc(v))
}

/// Allocate an `i64` integer.  The singleton cache for `[-128, 127]`
/// is tried first, then pointer tagging for i61-range values
/// (≈ 2.3 × 10^18), falling back to an arena `Int` for the tails of
/// the i64 range.  Tagged pointers carry the integer inline in the
/// pointer's upper 61 bits with a `0b001` tag in the low 3 bits;
/// `as_ref` / callers that dereference route through a tagged-aware
/// scratch ring, so tagged pointers can flow transparently through all
/// existing runtime functions.
#[inline]
fn alloc_int(n: i64) -> *mut Value {
    if n >= SMALL_INT_MIN && n <= SMALL_INT_MAX {
        return SINGLETONS.with(|s| s.small_ints[(n - SMALL_INT_MIN) as usize]);
    }
    if let Some(tagged) = encode_smallint(n) {
        return tagged;
    }
    alloc(Value::Int(n))
}

/// Extract an `i64` from an integer value without allocating.
#[inline]
fn int_as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Int(n) => Some(*n),
        _ => None,
    }
}

/// Extract a `usize` from an integer value.  Returns `None` if the
/// value doesn't fit (e.g., negative or too large).
#[inline]
fn int_as_usize(v: &Value) -> Option<usize> {
    match v {
        Value::Int(n) => usize::try_from(*n).ok(),
        _ => None,
    }
}

/// Return a tagged Bool pointer.  The tag encoding (`0b010` in the
/// low 3 bits, payload in bit 3) produces two globally-unique bit
/// patterns for `true` / `false` — no thread-local lookup, no heap
/// allocation, and identity comparison (`a == b` on the pointers)
/// correctly implies value equality.
#[inline]
fn alloc_bool(b: bool) -> *mut Value {
    encode_bool(b)
}

/// Allocate a float, returning a cached pointer for +0.0 and 1.0.
fn alloc_float(n: f64) -> *mut Value {
    if n.to_bits() == 0.0_f64.to_bits() {
        SINGLETONS.with(|s| s.float_zero)
    } else if n == 1.0 {
        SINGLETONS.with(|s| s.float_one)
    } else {
        alloc(Value::Float(n))
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_arena_mark() -> usize {
    ARENA.with(|a| a.borrow().mark())
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_arena_reset_to(mark: usize) {
    ARENA.with(|a| a.borrow_mut().reset_to(mark));
}

/// Push a new arena frame for call-site isolation.
#[unsafe(no_mangle)]
pub extern "C" fn knot_arena_push_frame() {
    ARENA.with(|a| a.borrow_mut().push_frame());
}

/// Pop the current arena frame, freeing all its allocations.
#[unsafe(no_mangle)]
pub extern "C" fn knot_arena_pop_frame() {
    if log::debug_enabled() {
        ARENA.with(|a| {
            let arena = a.borrow();
            let depth = arena.frames.len();
            log_debug!("[ARENA] pop_frame: depth {} → {}", depth, depth.saturating_sub(1));
        });
    }
    ARENA.with(|a| a.borrow_mut().pop_frame());
}

/// Pop the current arena frame, deep-cloning `val` into the parent frame.
/// Returns the promoted pointer. Used at function return boundaries
/// to preserve the return value while freeing the callee's temporaries.
#[unsafe(no_mangle)]
pub extern "C" fn knot_arena_pop_frame_promote(val: *mut Value) -> *mut Value {
    ARENA.with(|a| a.borrow_mut().pop_frame_promote(val))
}

/// Deep-clone `val` into the current frame's pinned set so it survives
/// `knot_arena_reset_to`. Used before `knot_relation_push` in do-block
/// loops to preserve yielded values across per-iteration resets.
#[unsafe(no_mangle)]
pub extern "C" fn knot_arena_promote(val: *mut Value) -> *mut Value {
    ARENA.with(|a| a.borrow_mut().promote(val))
}

unsafe fn as_ref<'a>(v: *mut Value) -> &'a Value {
    if v.is_null() {
        panic!("knot runtime: null pointer dereference (value is null)");
    }
    if is_tagged(v) {
        // Materialize the tagged pointer into the next slot of a
        // per-thread scratch ring and hand back a reference into it.
        // The ring has `UNPACK_SCRATCH_SLOTS` entries; each `as_ref`
        // call on a tagged pointer advances the cursor, so the common
        // pattern `match (as_ref(a), as_ref(b)) { ... }` sees two
        // distinct slots even if both a and b are tagged.
        //
        // Invariant: a caller must not keep a reference from this
        // function alive across more than `UNPACK_SCRATCH_SLOTS - 1`
        // subsequent `as_ref` calls that could themselves be tagged,
        // or the cursor will wrap around and overwrite the slot.
        // In practice, match arms extract owned data (i64, bool) or
        // refcount-clone (Arc<str>, Arc<[u8]>) immediately, so no
        // pattern-bound reference outlives its arm; the worst nesting
        // we see is 2-deep tuple matches, well within a 8-slot ring.
        UNPACK_SCRATCH.with(|ring| {
            let cursor_cell = &ring.cursor;
            let idx = cursor_cell.get();
            cursor_cell.set((idx + 1) % UNPACK_SCRATCH_SLOTS);
            let slots = ring.slots.get();
            unsafe {
                let ptr = (*slots).as_mut_ptr().add(idx);
                std::ptr::drop_in_place(ptr);
                std::ptr::write(ptr, decode_tagged(v));
                std::mem::transmute::<&Value, &'a Value>(&*ptr)
            }
        })
    } else {
        unsafe { &*v }
    }
}

/// Number of scratch slots in the tagged-pointer decode ring.  Must be
/// large enough that no realistic match-chain (including nested
/// helpers like `type_name`, `brief_value`, and equality recursion)
/// wraps the cursor around while pattern-bound references are still
/// live.  8 is generous — the observed maximum concurrent tagged
/// derefs per call stack is 2 (binary op match patterns).
const UNPACK_SCRATCH_SLOTS: usize = 8;

struct UnpackScratchRing {
    slots: std::cell::UnsafeCell<[Value; UNPACK_SCRATCH_SLOTS]>,
    cursor: std::cell::Cell<usize>,
}

thread_local! {
    static UNPACK_SCRATCH: UnpackScratchRing = UnpackScratchRing {
        slots: std::cell::UnsafeCell::new([
            Value::Unit, Value::Unit, Value::Unit, Value::Unit,
            Value::Unit, Value::Unit, Value::Unit, Value::Unit,
        ]),
        cursor: std::cell::Cell::new(0),
    };
}

unsafe fn str_from_raw(ptr: *const u8, len: usize) -> &'static str {
    let bytes = unsafe { slice::from_raw_parts(ptr, len) };
    match std::str::from_utf8(bytes) {
        Ok(s) => unsafe { &*(s as *const str) },
        Err(e) => panic!("knot runtime: invalid UTF-8 from compiled code at byte {}", e.valid_up_to()),
    }
}

/// Runtime error for missing trait implementations.
#[unsafe(no_mangle)]
pub extern "C" fn knot_trait_no_impl(
    method_ptr: *const u8,
    method_len: usize,
    value: *mut Value,
) -> *mut Value {
    let method = unsafe { str_from_raw(method_ptr, method_len) };
    panic!(
        "knot runtime: no implementation of '{}' for type {}",
        method,
        brief_value(value)
    );
}

fn type_name(v: *mut Value) -> &'static str {
    if v.is_null() {
        return "null";
    }
    match unsafe { as_ref(v) } {
        Value::Int(_) => "Int",
        Value::Float(_) => "Float",
        Value::Text(_) => "Text",
        Value::Bool(_) => "Bool",
        Value::Bytes(_) => "Bytes",
        Value::Unit => "Unit",
        Value::Record(_) => "Record",
        Value::Relation(_) => "Relation",
        Value::Constructor(_, _) => "Constructor",
        Value::Function(_) => "Function",
        Value::IO(_, _) => "IO",
        Value::Pair(_, _) => "Pair",
    }
}

fn brief_value(v: *mut Value) -> String {
    if v.is_null() {
        return "null".to_string();
    }
    match unsafe { as_ref(v) } {
        Value::Int(n) => format!("Int({})", n),
        Value::Float(n) => format!("Float({})", n),
        Value::Text(s) => {
            if s.len() > 30 {
                let truncated: String = s.chars().take(27).collect();
                format!("Text(\"{}...\")", truncated)
            } else {
                format!("Text(\"{}\")", s)
            }
        }
        Value::Bool(b) => format!("Bool({})", b),
        Value::Bytes(b) => format!("Bytes({} bytes)", b.len()),
        Value::Unit => "Unit".to_string(),
        Value::Record(fields) => {
            let names: Vec<&str> = fields.iter().map(|f| f.name.as_ref()).collect();
            format!("Record({{{}}})", names.join(", "))
        }
        Value::Relation(rows) => format!("Relation({} rows)", rows.len()),
        Value::Constructor(tag, _) => format!("Constructor({})", &**tag),
        Value::Function(f) => format!("Function({})", &*f.source),
        Value::IO(_, _) => "IO".to_string(),
        Value::Pair(_, _) => "Pair".to_string(),
    }
}

/// Escape a SQL identifier by wrapping it in double quotes and doubling
/// any internal `"` characters, per the SQL standard.
pub(crate) fn quote_ident(name: &str) -> String {
    if name.contains('"') {
        let mut s = String::with_capacity(name.len() + 2);
        s.push('"');
        for ch in name.chars() {
            if ch == '"' { s.push('"'); }
            s.push(ch);
        }
        s.push('"');
        s
    } else {
        let mut s = String::with_capacity(name.len() + 2);
        s.push('"');
        s.push_str(name);
        s.push('"');
        s
    }
}

// ── Value constructors ────────────────────────────────────────────

// ── Small integer cache ───────────────────────────────────────────

const SMALL_INT_MIN: i64 = -128;
const SMALL_INT_MAX: i64 = 127;

/// Grouped thread-local singletons with Drop so spawned threads reclaim memory.
struct ValueSingletons {
    small_ints: Vec<*mut Value>,
    unit: *mut Value,
    bool_true: *mut Value,
    bool_false: *mut Value,
    float_zero: *mut Value,
    float_one: *mut Value,
}

impl Drop for ValueSingletons {
    fn drop(&mut self) {
        for &ptr in &self.small_ints {
            unsafe { let _ = Box::from_raw(ptr); }
        }
        unsafe {
            let _ = Box::from_raw(self.unit);
            let _ = Box::from_raw(self.bool_true);
            let _ = Box::from_raw(self.bool_false);
            let _ = Box::from_raw(self.float_zero);
            let _ = Box::from_raw(self.float_one);
        }
    }
}

/// Upper bound on cached text literals per thread.  Source-pointer-keyed
/// literals from `.rodata` are typically bounded by program size; this cap
/// protects against pathological callers that pass dynamically-allocated
/// pointers (e.g. generated SQL fragments).
const TEXT_LITERAL_CACHE_CAP: usize = 4096;

/// Sentinel index meaning "no neighbor" in the linked list.
const LRU_NIL: u32 = u32::MAX;

/// One entry in the LRU cache.  Packed representation (u32 indices)
/// keeps each node at 16 bytes (val + prev + next + padding).
struct LruEntry {
    val: *mut Value,
    prev: u32,
    next: u32,
}

/// Doubly-linked-list-based LRU cache for text literals.
///
/// `map` maps source pointers to their index in `entries`.
/// `entries` is a slab; a removed entry's slot is pushed onto `free` for
/// reuse.  `head` is the most-recently-used index, `tail` is the
/// least-recently-used.  All operations (get, insert, evict) are O(1).
struct TextLiteralCache {
    map: HashMap<*const u8, u32>,
    entries: Vec<LruEntry>,
    head: u32,
    tail: u32,
    free: u32,
}

impl TextLiteralCache {
    fn new() -> Self {
        TextLiteralCache {
            map: HashMap::new(),
            entries: Vec::new(),
            head: LRU_NIL,
            tail: LRU_NIL,
            free: LRU_NIL,
        }
    }

    /// Detach `idx` from its current position in the list (if linked).
    fn detach(&mut self, idx: u32) {
        let (prev, next) = {
            let e = &self.entries[idx as usize];
            (e.prev, e.next)
        };
        if prev != LRU_NIL {
            self.entries[prev as usize].next = next;
        } else {
            self.head = next;
        }
        if next != LRU_NIL {
            self.entries[next as usize].prev = prev;
        } else {
            self.tail = prev;
        }
    }

    /// Link `idx` at the head (most-recently-used position).
    fn push_head(&mut self, idx: u32) {
        let old_head = self.head;
        {
            let e = &mut self.entries[idx as usize];
            e.prev = LRU_NIL;
            e.next = old_head;
        }
        if old_head != LRU_NIL {
            self.entries[old_head as usize].prev = idx;
        } else {
            // Empty list — new head is also the tail.
            self.tail = idx;
        }
        self.head = idx;
    }

    /// Look up `key` and, if present, promote it to the head.
    fn get(&mut self, key: *const u8) -> Option<*mut Value> {
        let idx = *self.map.get(&key)?;
        // Promote to head: detach then re-link at front.
        self.detach(idx);
        self.push_head(idx);
        Some(self.entries[idx as usize].val)
    }

    /// Insert a new (key, val). Returns true if the entry was inserted.
    ///
    /// When the cache is full, returns false instead of evicting — the
    /// previous LRU eviction logic freed the victim's `Value`, but the
    /// raw pointer had already been handed out to the runtime, leaving
    /// dangling references on the heap.  Now `knot_value_text_cached`
    /// is responsible for not orphan-leaking the box when insertion is
    /// rejected.
    fn insert(&mut self, key: *const u8, val: *mut Value) -> bool {
        if self.map.len() >= TEXT_LITERAL_CACHE_CAP {
            return false;
        }

        // Grow: reuse a freed slot if available, otherwise append.
        let idx = if self.free != LRU_NIL {
            let slot = self.free;
            self.free = self.entries[slot as usize].next;
            self.entries[slot as usize] = LruEntry {
                val,
                prev: LRU_NIL,
                next: LRU_NIL,
            };
            slot
        } else {
            let slot = self.entries.len() as u32;
            self.entries.push(LruEntry {
                val,
                prev: LRU_NIL,
                next: LRU_NIL,
            });
            slot
        };
        self.push_head(idx);
        self.map.insert(key, idx);
        true
    }
}

impl Drop for TextLiteralCache {
    fn drop(&mut self) {
        // Iterate the map (not entries) so we skip freed slots.
        for &idx in self.map.values() {
            let val = self.entries[idx as usize].val;
            unsafe { let _ = Box::from_raw(val); }
        }
    }
}

thread_local! {
    static SINGLETONS: ValueSingletons = ValueSingletons {
        small_ints: (SMALL_INT_MIN..=SMALL_INT_MAX)
            .map(|n| Box::into_raw(Box::new(Value::Int(n))))
            .collect(),
        unit: Box::into_raw(Box::new(Value::Unit)),
        bool_true: Box::into_raw(Box::new(Value::Bool(true))),
        bool_false: Box::into_raw(Box::new(Value::Bool(false))),
        float_zero: Box::into_raw(Box::new(Value::Float(0.0))),
        float_one: Box::into_raw(Box::new(Value::Float(1.0))),
    };
    /// Cache for text literals keyed by static data pointer.
    /// Values are allocated outside the arena so they survive arena resets.
    static TEXT_LITERAL_CACHE: RefCell<TextLiteralCache> = RefCell::new(TextLiteralCache::new());
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_int(n: i64) -> *mut Value {
    alloc_int(n)
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_int_from_str(ptr: *const u8, len: usize) -> *mut Value {
    let s = unsafe { str_from_raw(ptr, len) };
    let n = s.parse::<i64>().unwrap_or_else(|e| panic!("knot runtime: invalid integer literal '{}': {}", s, e));
    alloc_int(n)
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_float(n: f64) -> *mut Value {
    if n.to_bits() == 0.0_f64.to_bits() {
        SINGLETONS.with(|s| s.float_zero)
    } else if n == 1.0 {
        SINGLETONS.with(|s| s.float_one)
    } else {
        alloc(Value::Float(n))
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_text(ptr: *const u8, len: usize) -> *mut Value {
    let s = unsafe { str_from_raw(ptr, len) };
    alloc(Value::Text(Arc::from(s)))
}

/// Like `knot_value_text` but caches by data pointer, avoiding repeated
/// allocations for the same string literal.  Cached values live outside the
/// arena so they survive `knot_arena_reset_to`.
///
/// If the per-thread cache is at capacity, the cache rejects the insert and
/// we fall back to the arena allocator instead of leaking heap boxes (and,
/// previously, freeing values that the runtime might still reference). The
/// fallback value is only valid until the next arena reset, so the cache cap
/// effectively bounds how many literals we promise to keep alive across
/// resets — pathological dynamic-pointer callers degrade to standard arena
/// lifetime rather than corrupting memory.
#[unsafe(no_mangle)]
pub extern "C" fn knot_value_text_cached(ptr: *const u8, len: usize) -> *mut Value {
    TEXT_LITERAL_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(val) = cache.get(ptr) {
            GC_STATS.bump(&GC_STATS.text_cache_hits);
            return val;
        }
        GC_STATS.bump(&GC_STATS.text_cache_misses);
        let s = unsafe { str_from_raw(ptr, len) };
        let val = Box::into_raw(Box::new(Value::Text(Arc::from(s))));
        if cache.insert(ptr, val) {
            val
        } else {
            // Cache full — drop our box and fall back to the arena.
            // Same observable result for the caller; just no longer
            // "survives arena reset".
            let value = unsafe { *Box::from_raw(val) };
            alloc(value)
        }
    })
}

/// Per-call-site interned text literal.
///
/// `slot` is a compiler-emitted 8-byte zero-initialized data slot,
/// unique to the specific text literal at this source location.  On
/// first use, `*slot` is null and we box-allocate the `Value::Text`,
/// storing the pointer back into the slot.  All subsequent calls
/// observe the non-null slot and return it directly — no hashing, no
/// LRU traversal, no re-allocation.
///
/// Thread-safe via `AtomicPtr::compare_exchange`: the slot is treated
/// as a one-shot init cell. On contention the loser drops its newly
/// allocated box and uses the winner's value, so duplicates never leak.
/// The slot pointer must outlive the program (true for compiled-code
/// statics) and start zeroed.
///
/// Compared to `knot_value_text_cached`, this also frees the runtime
/// LRU from per-call pressure — the LRU remains available for dynamic
/// callers that don't go through the inline slot path, but the common
/// case (compiled-code-emitted literals) stays entirely out of it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn knot_value_text_intern(
    ptr: *const u8,
    len: usize,
    slot: *mut *mut Value,
) -> *mut Value {
    debug_assert!(!slot.is_null(), "knot runtime: null text-literal slot");
    debug_assert_eq!(
        slot as usize % std::mem::align_of::<std::sync::atomic::AtomicPtr<Value>>(),
        0,
        "knot runtime: text-literal slot is not pointer-aligned (codegen bug)"
    );
    let atomic = unsafe { &*(slot as *const std::sync::atomic::AtomicPtr<Value>) };
    let cached = atomic.load(std::sync::atomic::Ordering::Acquire);
    if !cached.is_null() {
        GC_STATS.bump(&GC_STATS.text_cache_hits);
        return cached;
    }
    GC_STATS.bump(&GC_STATS.text_cache_misses);
    let s = unsafe { str_from_raw(ptr, len) };
    let val = Box::into_raw(Box::new(Value::Text(Arc::from(s))));
    match atomic.compare_exchange(
        std::ptr::null_mut(),
        val,
        std::sync::atomic::Ordering::AcqRel,
        std::sync::atomic::Ordering::Acquire,
    ) {
        Ok(_) => val,
        Err(winner) => {
            // Lost the race; drop our box and use the winner's value.
            unsafe { drop(Box::from_raw(val)); }
            winner
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_bool(b: i32) -> *mut Value {
    encode_bool(b != 0)
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_unit() -> *mut Value {
    encode_unit()
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_function(
    fn_ptr: *const u8,
    env: *mut Value,
    src_ptr: *const u8,
    src_len: usize,
) -> *mut Value {
    let source = intern_str(unsafe { str_from_raw(src_ptr, src_len) });
    alloc(Value::Function(Box::new(FunctionInner { fn_ptr, env, source })))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_constructor(
    tag_ptr: *const u8,
    tag_len: usize,
    payload: *mut Value,
) -> *mut Value {
    let tag = intern_str(unsafe { str_from_raw(tag_ptr, tag_len) });
    alloc(Value::Constructor(tag, payload))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_bytes(ptr: *const u8, len: usize) -> *mut Value {
    let bytes: Arc<[u8]> = if ptr.is_null() || len == 0 {
        Arc::from(&[][..])
    } else {
        let slice = unsafe { slice::from_raw_parts(ptr, len) };
        Arc::from(slice)
    };
    alloc(Value::Bytes(bytes))
}

// ── Value accessors ───────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_get_int(v: *mut Value) -> i64 {
    match unsafe { as_ref(v) } {
        Value::Int(n) => *n,
        _ => panic!("knot runtime: expected Int, got {}", brief_value(v)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_get_float(v: *mut Value) -> f64 {
    match unsafe { as_ref(v) } {
        Value::Float(n) => *n,
        _ => panic!("knot runtime: expected Float, got {}", brief_value(v)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_get_bool(v: *mut Value) -> i32 {
    match unsafe { as_ref(v) } {
        Value::Bool(b) => *b as i32,
        _ => panic!("knot runtime: expected Bool, got {}", brief_value(v)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_get_tag(v: *mut Value) -> i32 {
    if v.is_null() {
        return 9; // Nullable none (null pointer)
    }
    match unsafe { as_ref(v) } {
        Value::Int(_) => 0,
        Value::Float(_) => 1,
        Value::Text(_) => 2,
        Value::Bool(_) => 3,
        Value::Unit => 4,
        Value::Record(_) => 5,
        Value::Relation(_) => 6,
        Value::Constructor(_, _) => 7,
        Value::Function(_) => 8,
        Value::Bytes(_) => 10,
        Value::IO(_, _) => 11,
        Value::Pair(_, _) => 12,
    }
}

// ── Record operations ─────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_record_empty(capacity: usize) -> *mut Value {
    alloc(Value::Record(take_record_vec(capacity)))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_record_set_field(
    record: *mut Value,
    key_ptr: *const u8,
    key_len: usize,
    value: *mut Value,
) {
    let name_str = unsafe { str_from_raw(key_ptr, key_len) };
    let rec = unsafe { &mut *record };
    match rec {
        Value::Record(fields) => {
            // Maintain sorted order by field name for O(log n) lookup
            match fields.binary_search_by(|f| (*f.name).cmp(name_str)) {
                Ok(idx) => fields[idx].value = value,
                Err(idx) => fields.insert(idx, RecordField { name: intern_str(name_str), value }),
            }
        }
        _ => panic!("knot runtime: expected Record in set_field, got {}", type_name(record)),
    }
}

/// Batch-construct a record from field pairs.  `data` points to a flat
/// array of triples: [key_ptr, key_len, value, ...] where each element
/// is pointer-sized.
///
/// Codegen guarantees pairs are emitted in sorted order (see the
/// `compiled.sort_by_key` calls in codegen.rs's Record/Lambda/Do
/// lowerings).  `debug_assert` catches invariant violations during
/// development; release builds trust codegen and skip the O(n) scan.
#[unsafe(no_mangle)]
pub extern "C" fn knot_record_from_pairs(data: *const usize, count: usize) -> *mut Value {
    let mut fields = take_record_vec(count);
    for i in 0..count {
        let offset = i * 3;
        let key_ptr = unsafe { *data.add(offset) as *const u8 };
        let key_len = unsafe { *data.add(offset + 1) };
        let value = unsafe { *data.add(offset + 2) as *mut Value };
        let name = intern_str(unsafe { str_from_raw(key_ptr, key_len) });
        fields.push(RecordField { name, value });
    }
    debug_assert!(
        fields.windows(2).all(|w| w[0].name <= w[1].name),
        "knot_record_from_pairs: codegen emitted unsorted field pairs"
    );
    alloc(Value::Record(fields))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_record_field(
    record: *mut Value,
    key_ptr: *const u8,
    key_len: usize,
) -> *mut Value {
    if record.is_null() {
        let name = unsafe { str_from_raw(key_ptr, key_len) };
        panic!("knot runtime: field '{}' access on null (nullable none variant)", name);
    }
    let name = unsafe { str_from_raw(key_ptr, key_len) };
    match unsafe { as_ref(record) } {
        Value::Record(fields) => {
            // Fields are kept sorted by all constructors
            // (`knot_record_from_pairs` sorts defensively, `set_field`
            // uses sorted insert).  Binary search is always correct.
            if let Ok(idx) = fields.binary_search_by(|f| (*f.name).cmp(name)) {
                return fields[idx].value;
            }
            let available: Vec<&str> = fields.iter().map(|f| f.name.as_ref()).collect();
            panic!(
                "knot runtime: field '{}' not found in record\n  available fields: {}",
                name,
                if available.is_empty() { "(none)".to_string() } else { available.join(", ") }
            )
        }
        Value::Constructor(_, payload) => {
            // Delegate to the payload (which should be a record)
            knot_record_field(*payload, key_ptr, key_len)
        }
        Value::Relation(rows) => {
            // After groupBy, field access on a group relation delegates to first element.
            // All elements in a group share the key fields, so this is well-defined.
            if rows.is_empty() {
                return alloc(Value::Relation(vec![]));
            }
            knot_record_field(rows[0], key_ptr, key_len)
        }
        _ => panic!(
            "knot runtime: expected Record in field access, got {}",
            brief_value(record)
        ),
    }
}

/// Direct index-based field access for closure environments.
/// Index corresponds to the field's position in sorted order.
#[unsafe(no_mangle)]
pub extern "C" fn knot_record_field_by_index(record: *mut Value, index: usize) -> *mut Value {
    match unsafe { as_ref(record) } {
        Value::Record(fields) => {
            if index < fields.len() {
                fields[index].value
            } else {
                panic!("knot runtime: field_by_index out of bounds (index {} >= len {})", index, fields.len())
            }
        }
        _ => panic!("knot runtime: expected Record in field_by_index, got {}", type_name(record)),
    }
}

// ── SQLite-backed temp tables for relation operations ─────────────

thread_local! {
    static TEMP_COUNTER: std::cell::Cell<u64> = std::cell::Cell::new(0);
}

fn next_temp_name() -> String {
    TEMP_COUNTER.with(|c| {
        let n = c.get();
        c.set(n + 1);
        format!("_knot_tmp_{}", n)
    })
}

/// Schema for SQLite temp tables, inferred from relation values at runtime.
enum TempSchema {
    /// Records: named columns with SQL types
    Record(Vec<(String, ColType)>),
    /// Scalars (Int, Float, Text, Bool, Bytes): single `_val` column
    Scalar(ColType),
    /// ADT constructors: `_tag TEXT` + nullable fields from all constructors
    Adt {
        constructors: Vec<(String, Vec<(String, ColType)>)>,
        all_fields: Vec<(String, ColType)>,
    },
    /// Unit values
    Unit,
}

/// Infer the SQL column type from a runtime Value.
fn infer_col_type(v: *mut Value) -> Option<ColType> {
    if v.is_null() {
        return Some(ColType::Text);
    }
    match unsafe { as_ref(v) } {
        Value::Int(_) => Some(ColType::Int),
        Value::Float(_) => Some(ColType::Float),
        Value::Text(_) => Some(ColType::Text),
        Value::Bool(_) => Some(ColType::Bool),
        Value::Bytes(_) => Some(ColType::Bytes),
        Value::Unit => None,
        Value::Constructor(_, payload) => {
            // Only treat as Tag when the payload is Unit (nullary constructor).
            // Constructors with fields would lose their payload data if stored as Tag.
            if (*payload).is_null() || matches!(unsafe { as_ref(*payload) }, Value::Unit) {
                Some(ColType::Tag)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Infer a TempSchema from a non-empty slice of values.
/// Returns None if the values contain unsupported types (Function, nested Relation).
fn infer_temp_schema(rows: &[*mut Value]) -> Option<TempSchema> {
    if rows.is_empty() {
        return Some(TempSchema::Unit);
    }
    let first = rows[0];
    if first.is_null() {
        return Some(TempSchema::Scalar(ColType::Text));
    }
    match unsafe { as_ref(first) } {
        Value::Record(fields) => {
            let mut cols = Vec::with_capacity(fields.len());
            for f in fields {
                if !f.value.is_null() {
                    match unsafe { as_ref(f.value) } {
                        Value::Relation(_) | Value::Function(_) => return None,
                        _ => {}
                    }
                }
                let ty = infer_col_type(f.value)?;
                cols.push((f.name.to_string(), ty));
            }
            Some(TempSchema::Record(cols))
        }
        Value::Constructor(_, _) => {
            // Scan all rows to collect all constructor variants
            let mut ctors: Vec<(String, Vec<(String, ColType)>)> = Vec::new();
            let mut seen_tags: HashSet<String> = HashSet::new();
            let mut seen_field_names: HashSet<String> = HashSet::new();
            let mut all_fields: Vec<(String, ColType)> = Vec::new();

            for row in rows {
                if row.is_null() { continue; }
                match unsafe { as_ref(*row) } {
                    Value::Constructor(tag, payload) => {
                        if !seen_tags.insert(tag.to_string()) {
                            continue;
                        }
                        let unit_placeholder = Value::Unit;
                        let payload_ref = if (*payload).is_null() {
                            &unit_placeholder
                        } else {
                            unsafe { as_ref(*payload) }
                        };
                        let ctor_fields = match payload_ref {
                            Value::Unit => Vec::new(),
                            Value::Record(fields) => {
                                let mut cf = Vec::new();
                                for f in fields {
                                    let ty = infer_col_type(f.value)?;
                                    cf.push((f.name.to_string(), ty));
                                    if seen_field_names.insert(f.name.to_string()) {
                                        all_fields.push((f.name.to_string(), ty));
                                    }
                                }
                                cf
                            }
                            _ => return None,
                        };
                        ctors.push((tag.to_string(), ctor_fields));
                    }
                    _ => return None,
                }
            }
            Some(TempSchema::Adt { constructors: ctors, all_fields })
        }
        Value::Unit => Some(TempSchema::Unit),
        Value::Int(_) => Some(TempSchema::Scalar(ColType::Int)),
        Value::Float(_) => Some(TempSchema::Scalar(ColType::Float)),
        Value::Text(_) => Some(TempSchema::Scalar(ColType::Text)),
        Value::Bool(_) => Some(TempSchema::Scalar(ColType::Bool)),
        Value::Bytes(_) => Some(TempSchema::Scalar(ColType::Bytes)),
        _ => None,
    }
}

/// Create a temp table with the given schema.
fn create_temp_table(conn: &Connection, name: &str, schema: &TempSchema) {
    let table = quote_ident(name);
    let col_defs = match schema {
        TempSchema::Record(cols) => {
            if cols.is_empty() {
                "\"_dummy\" INTEGER DEFAULT 0".to_string()
            } else {
                cols.iter()
                    .map(|(name, ty)| format!("{} {}", quote_ident(name), sql_type(*ty)))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        }
        TempSchema::Scalar(ty) => format!("\"_val\" {}", sql_type(*ty)),
        TempSchema::Adt { all_fields, .. } => {
            let mut defs = vec!["\"_tag\" TEXT NOT NULL".to_string()];
            for (name, ty) in all_fields {
                defs.push(format!("{} {}", quote_ident(name), sql_type(*ty)));
            }
            defs.join(", ")
        }
        TempSchema::Unit => "\"_dummy\" INTEGER DEFAULT 0".to_string(),
    };
    let sql = format!("CREATE TEMP TABLE {} ({});", table, col_defs);
    debug_sql(&sql);
    conn.execute_batch(&sql)
        .unwrap_or_else(|e| panic!("knot runtime: failed to create temp table: {}", e));
}

/// Build an INSERT SQL statement for a temp table.
fn temp_insert_sql(name: &str, schema: &TempSchema) -> String {
    let table = quote_ident(name);
    let (col_names, n_cols) = match schema {
        TempSchema::Record(cols) => {
            if cols.is_empty() {
                ("\"_dummy\"".to_string(), 1)
            } else {
                let names: Vec<String> = cols.iter().map(|(n, _)| quote_ident(n)).collect();
                let n = names.len();
                (names.join(", "), n)
            }
        }
        TempSchema::Scalar(_) => ("\"_val\"".to_string(), 1),
        TempSchema::Adt { all_fields, .. } => {
            let mut names = vec!["\"_tag\"".to_string()];
            for (n, _) in all_fields {
                names.push(quote_ident(n));
            }
            let n = names.len();
            (names.join(", "), n)
        }
        TempSchema::Unit => ("\"_dummy\"".to_string(), 1),
    };
    let placeholders: Vec<String> = (1..=n_cols).map(|i| format!("?{}", i)).collect();
    format!("INSERT INTO {} ({}) VALUES ({});", table, col_names, placeholders.join(", "))
}

/// Convert a Value to SQL params for temp table insertion.
fn temp_row_to_params(v: *mut Value, schema: &TempSchema) -> Vec<rusqlite::types::Value> {
    match schema {
        TempSchema::Record(cols) => {
            if cols.is_empty() {
                return vec![rusqlite::types::Value::Integer(0)];
            }
            let fields = match unsafe { as_ref(v) } {
                Value::Record(fields) => fields,
                _ => panic!("knot runtime: expected Record for temp table insert, got {}", type_name(v)),
            };
            cols.iter()
                .map(|(name, ty)| {
                    let field = fields.iter().find(|f| &*f.name == name.as_str());
                    match field {
                        Some(f) => value_to_sqlite(f.value, *ty),
                        None => rusqlite::types::Value::Null,
                    }
                })
                .collect()
        }
        TempSchema::Scalar(ty) => vec![value_to_sqlite(v, *ty)],
        TempSchema::Adt { all_fields, constructors } => {
            match unsafe { as_ref(v) } {
                Value::Constructor(tag, payload) => {
                    let mut params = vec![rusqlite::types::Value::Text(tag.to_string())];
                    let ctor = constructors.iter().find(|(t, _)| t.as_str() == &**tag);
                    for (fname, fty) in all_fields {
                        let has_field = ctor.map_or(false, |(_, fields)| {
                            fields.iter().any(|(n, _)| n == fname)
                        });
                        if has_field {
                            match unsafe { as_ref(*payload) } {
                                Value::Record(fields) => {
                                    let field = fields.iter().find(|f| &*f.name == fname.as_str());
                                    params.push(match field {
                                        Some(f) => value_to_sqlite(f.value, *fty),
                                        None => rusqlite::types::Value::Null,
                                    });
                                }
                                _ => params.push(rusqlite::types::Value::Null),
                            }
                        } else {
                            params.push(rusqlite::types::Value::Null);
                        }
                    }
                    params
                }
                _ => panic!("knot runtime: expected Constructor for ADT temp table"),
            }
        }
        TempSchema::Unit => vec![rusqlite::types::Value::Integer(0)],
    }
}

/// Read a single row from a query result and convert to a Value using TempSchema.
fn read_temp_row(row: &rusqlite::Row, schema: &TempSchema) -> *mut Value {
    match schema {
        TempSchema::Record(cols) => {
            if cols.is_empty() {
                return knot_record_empty(0);
            }
            let record = knot_record_empty(cols.len());
            for (i, (name, ty)) in cols.iter().enumerate() {
                let val = read_sql_column(row, i, *ty);
                let name_bytes = name.as_bytes();
                knot_record_set_field(record, name_bytes.as_ptr(), name_bytes.len(), val);
            }
            record
        }
        TempSchema::Scalar(ty) => read_sql_column(row, 0, *ty),
        TempSchema::Adt { constructors, all_fields } => {
            let tag: String = row.get(0).unwrap();
            let ctor = constructors.iter().find(|(t, _)| t == &tag);
            let payload = if let Some((_, fields)) = ctor {
                if fields.is_empty() {
                    alloc(Value::Unit)
                } else {
                    let field_idx: HashMap<&str, usize> = all_fields.iter().enumerate()
                        .map(|(i, (n, _))| (n.as_str(), i)).collect();
                    let record = knot_record_empty(fields.len());
                    for (fname, fty) in fields {
                        let col_idx = *field_idx.get(fname.as_str()).unwrap_or_else(|| {
                            panic!(
                                "knot runtime: schema mismatch — constructor `{}` field `{}` not found in stored ADT layout (expected one of: {})",
                                tag,
                                fname,
                                all_fields.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>().join(", ")
                            )
                        });
                        let val = read_sql_column(row, col_idx + 1, *fty);
                        let name_bytes = fname.as_bytes();
                        knot_record_set_field(record, name_bytes.as_ptr(), name_bytes.len(), val);
                    }
                    record
                }
            } else {
                alloc(Value::Unit)
            };
            alloc(Value::Constructor(intern_str(&tag), payload))
        }
        TempSchema::Unit => alloc(Value::Unit),
    }
}

/// Read rows from an arbitrary SQL query using a TempSchema.
fn read_query_rows(conn: &Connection, sql: &str, schema: &TempSchema) -> Vec<*mut Value> {
    debug_sql(sql);
    let mut stmt = conn
        .prepare_cached(sql)
        .unwrap_or_else(|e| panic!("knot runtime: temp query error: {}\n  SQL: {}", e, sql));
    let mut result_rows = stmt
        .query([])
        .unwrap_or_else(|e| panic!("knot runtime: temp query exec error: {}\n  SQL: {}", e, sql));

    let mut rows: Vec<*mut Value> = Vec::new();
    while let Some(row) = result_rows
        .next()
        .unwrap_or_else(|e| panic!("knot runtime: temp query fetch error: {}", e))
    {
        rows.push(read_temp_row(row, schema));
    }
    rows
}

/// Drop a temp table.
fn drop_temp_table(conn: &Connection, name: &str) {
    let sql = format!("DROP TABLE IF EXISTS {};", quote_ident(name));
    debug_sql(&sql);
    let _ = conn.execute_batch(&sql);
}

/// Maximum number of SQL parameters for a VALUES clause.
/// SQLite's SQLITE_MAX_VARIABLE_NUMBER is 32766 in 3.32.0+.
const MAX_VALUES_PARAMS: usize = 10_000;

/// Number of SQL columns in a TempSchema.
fn schema_col_count(schema: &TempSchema) -> usize {
    match schema {
        TempSchema::Record(cols) => if cols.is_empty() { 1 } else { cols.len() },
        TempSchema::Scalar(_) => 1,
        TempSchema::Adt { all_fields, .. } => 1 + all_fields.len(),
        TempSchema::Unit => 1,
    }
}

/// Quoted column names for a TempSchema.
fn schema_col_names(schema: &TempSchema) -> Vec<String> {
    match schema {
        TempSchema::Record(cols) => {
            if cols.is_empty() {
                vec![quote_ident("_dummy")]
            } else {
                cols.iter().map(|(n, _)| quote_ident(n)).collect()
            }
        }
        TempSchema::Scalar(_) => vec![quote_ident("_val")],
        TempSchema::Adt { all_fields, .. } => {
            let mut names = vec![quote_ident("_tag")];
            for (n, _) in all_fields {
                names.push(quote_ident(n));
            }
            names
        }
        TempSchema::Unit => vec![quote_ident("_dummy")],
    }
}

/// Build a `VALUES (?1, ?2), (?3, ?4), ...` clause with flattened parameters.
/// `param_offset` is the number of params already bound (for numbering continuity).
fn build_values_clause(
    rows: &[*mut Value],
    schema: &TempSchema,
    param_offset: usize,
) -> (String, Vec<rusqlite::types::Value>) {
    let mut params = Vec::new();
    let mut row_clauses = Vec::with_capacity(rows.len());
    let mut idx = param_offset + 1;

    for row in rows {
        let row_params = temp_row_to_params(*row, schema);
        let placeholders: Vec<String> = row_params
            .iter()
            .map(|_| {
                let p = format!("?{}", idx);
                idx += 1;
                p
            })
            .collect();
        row_clauses.push(format!("({})", placeholders.join(", ")));
        params.extend(row_params);
    }

    (format!("VALUES {}", row_clauses.join(", ")), params)
}

/// Execute a parameterized SQL query and read rows using a TempSchema.
fn read_query_rows_params(
    conn: &Connection,
    sql: &str,
    params: &[rusqlite::types::Value],
    schema: &TempSchema,
) -> Vec<*mut Value> {
    debug_sql(sql);
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("knot runtime: query error: {}\n  SQL: {}", e, sql));
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
    let mut result_rows = stmt
        .query(param_refs.as_slice())
        .unwrap_or_else(|e| panic!("knot runtime: query exec error: {}\n  SQL: {}", e, sql));

    let mut rows: Vec<*mut Value> = Vec::new();
    while let Some(row) = result_rows
        .next()
        .unwrap_or_else(|e| panic!("knot runtime: query fetch error: {}", e))
    {
        rows.push(read_temp_row(row, schema));
    }
    rows
}

/// Materialize a relation into a temp table and return the table name.
fn materialize_relation(conn: &Connection, rows: &[*mut Value], schema: &TempSchema) -> String {
    let name = next_temp_name();
    create_temp_table(conn, &name, schema);
    if !rows.is_empty() {
        let ins_sql = temp_insert_sql(&name, schema);
        debug_sql(&ins_sql);
        let mut stmt = conn
            .prepare_cached(&ins_sql)
            .unwrap_or_else(|e| panic!("knot runtime: temp insert prepare error: {}", e));
        for row in rows {
            let params = temp_row_to_params(*row, schema);
            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
            stmt.execute(param_refs.as_slice())
                .unwrap_or_else(|e| panic!("knot runtime: temp table insert error: {}", e));
        }
    }
    name
}

// In-memory dedup fallback for relations that can't be stored in SQL.
//
// Reusable scratch for `in_memory_dedup`: the seen-set, the result
// vec, and a hash buffer.  `in_memory_dedup` is called on every
// union / relation-bind / groupBy when the SQL fallback doesn't
// fire, so allocating a fresh `HashSet` + `Vec<u8>` per call is
// wasteful in tight loops.  Reusing keeps the hashed-row storage
// and the capacity-hinted `result` around across calls.
thread_local! {
    static DEDUP_SCRATCH: RefCell<DedupScratch> = RefCell::new(DedupScratch {
        seen: HashSet::new(),
        result: Vec::new(),
        buf: Vec::new(),
    });
}

struct DedupScratch {
    seen: HashSet<Vec<u8>>,
    result: Vec<*mut Value>,
    buf: Vec<u8>,
}

fn in_memory_dedup(rows: Vec<*mut Value>) -> Vec<*mut Value> {
    DEDUP_SCRATCH.with(|cell| {
        let mut s = cell.borrow_mut();
        s.seen.clear();
        s.result.clear();
        s.buf.clear();
        // Reserve to minimise incremental growth in large-relation paths;
        // the Vec hangs on to its backing buffer across calls so future
        // calls don't pay the grow again.
        s.result.reserve(rows.len());
        for row in rows {
            s.buf.clear();
            value_to_hash_bytes(row, &mut s.buf);
            if !s.seen.contains(s.buf.as_slice()) {
                let key = std::mem::take(&mut s.buf);
                s.seen.insert(key);
                s.result.push(row);
            }
        }
        // Hand the result vec to the caller; swap in a fresh empty one
        // so the scratch's `result` slot keeps its capacity for next
        // call (the caller owns the big buffer until its Relation is
        // dropped and the relation-vec pool takes it).
        std::mem::take(&mut s.result)
    })
}

/// Perform a set operation (UNION/EXCEPT/INTERSECT) using SQLite.
/// Uses VALUES CTEs for small datasets, falls back to temp tables for large ones.
fn sql_set_op(
    conn: &Connection,
    a: &[*mut Value],
    b: &[*mut Value],
    op: &str,
) -> Option<Vec<*mut Value>> {
    if a.is_empty() && b.is_empty() {
        return Some(Vec::new());
    }
    // Infer schema from both sides combined so ADT unions see all constructors
    let combined: Vec<*mut Value> = a.iter().chain(b.iter()).copied().collect();
    let schema = infer_temp_schema(&combined)?;
    let n_cols = schema_col_count(&schema);

    if !a.is_empty() && !b.is_empty() && (a.len() + b.len()) * n_cols <= MAX_VALUES_PARAMS {
        let col_names = schema_col_names(&schema);
        let col_str = col_names.join(", ");
        let (values_a, params_a) = build_values_clause(a, &schema, 0);
        let (values_b, params_b) = build_values_clause(b, &schema, params_a.len());
        let mut all_params = params_a;
        all_params.extend(params_b);
        let sql = format!(
            "WITH _t1({c}) AS ({v1}), _t2({c}) AS ({v2}) \
             SELECT * FROM _t1 {op} SELECT * FROM _t2",
            c = col_str, v1 = values_a, v2 = values_b, op = op
        );
        return Some(read_query_rows_params(conn, &sql, &all_params, &schema));
    }

    let t1 = materialize_relation(conn, a, &schema);
    let t2 = materialize_relation(conn, b, &schema);

    let sql = format!(
        "SELECT * FROM {} {} SELECT * FROM {}",
        quote_ident(&t1),
        op,
        quote_ident(&t2)
    );
    let result = read_query_rows(conn, &sql, &schema);

    drop_temp_table(conn, &t1);
    drop_temp_table(conn, &t2);

    Some(result)
}

/// Dedup a list of values using SQL SELECT DISTINCT.
/// Uses a VALUES CTE for small datasets, falls back to a temp table for large ones.
fn sql_dedup(conn: &Connection, rows: &[*mut Value]) -> Option<Vec<*mut Value>> {
    if rows.is_empty() {
        return Some(Vec::new());
    }
    let schema = infer_temp_schema(rows)?;

    if rows.len() * schema_col_count(&schema) <= MAX_VALUES_PARAMS {
        let col_names = schema_col_names(&schema);
        let (values_sql, params) = build_values_clause(rows, &schema, 0);
        let sql = format!(
            "WITH _t({}) AS ({}) SELECT DISTINCT * FROM _t",
            col_names.join(", "),
            values_sql
        );
        return Some(read_query_rows_params(conn, &sql, &params, &schema));
    }

    let tmp = materialize_relation(conn, rows, &schema);
    let sql = format!("SELECT DISTINCT * FROM {}", quote_ident(&tmp));
    let result = read_query_rows(conn, &sql, &schema);
    drop_temp_table(conn, &tmp);
    Some(result)
}

/// Check if two relations are equal using SQL EXCEPT (symmetric difference).
/// Uses VALUES CTEs for small datasets, falls back to temp tables for large ones.
fn sql_relations_equal(conn: &Connection, a: &[*mut Value], b: &[*mut Value]) -> Option<bool> {
    if a.is_empty() && b.is_empty() {
        return Some(true);
    }
    if a.is_empty() || b.is_empty() {
        return Some(false);
    }
    // Don't short-circuit on a.len() != b.len() — in-memory vectors may contain
    // duplicates, so different lengths don't imply different logical sets.
    // Let the SQL EXCEPT (symmetric difference) handle deduplication correctly.
    let schema = infer_temp_schema(a)?;
    let n_cols = schema_col_count(&schema);

    if (a.len() + b.len()) * n_cols <= MAX_VALUES_PARAMS {
        let col_names = schema_col_names(&schema);
        let col_str = col_names.join(", ");
        let (values_a, params_a) = build_values_clause(a, &schema, 0);
        let (values_b, params_b) = build_values_clause(b, &schema, params_a.len());
        let mut all_params = params_a;
        all_params.extend(params_b);
        let sql = format!(
            "WITH _t1({c}) AS ({v1}), _t2({c}) AS ({v2}) \
             SELECT 1 FROM (\
               (SELECT * FROM _t1 EXCEPT SELECT * FROM _t2) \
               UNION ALL \
               (SELECT * FROM _t2 EXCEPT SELECT * FROM _t1)\
             ) LIMIT 1",
            c = col_str, v1 = values_a, v2 = values_b
        );
        debug_sql(&sql);
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            all_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
        return match conn
            .prepare(&sql)
            .and_then(|mut s| s.query_row(param_refs.as_slice(), |_| Ok(true)))
        {
            Ok(_) => Some(false),
            Err(rusqlite::Error::QueryReturnedNoRows) => Some(true),
            Err(_) => None,
        };
    }

    let t1 = materialize_relation(conn, a, &schema);
    let t2 = materialize_relation(conn, b, &schema);

    // Check symmetric difference: (a EXCEPT b) UNION ALL (b EXCEPT a) should be empty
    let sql = format!(
        "SELECT 1 FROM ((SELECT * FROM {} EXCEPT SELECT * FROM {}) UNION ALL (SELECT * FROM {} EXCEPT SELECT * FROM {})) LIMIT 1",
        quote_ident(&t1), quote_ident(&t2), quote_ident(&t2), quote_ident(&t1)
    );
    debug_sql(&sql);
    let result = conn
        .prepare_cached(&sql)
        .and_then(|mut s| s.query_row([], |_| Ok(true)));

    drop_temp_table(conn, &t1);
    drop_temp_table(conn, &t2);

    match result {
        Ok(_) => Some(false),
        Err(rusqlite::Error::QueryReturnedNoRows) => Some(true),
        Err(_) => None,
    }
}

// ── Relation operations ───────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_empty() -> *mut Value {
    alloc(Value::Relation(take_relation_vec(0)))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_with_capacity(cap: usize) -> *mut Value {
    alloc(Value::Relation(take_relation_vec(cap)))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_singleton(v: *mut Value) -> *mut Value {
    let mut buf = take_relation_vec(1);
    buf.push(v);
    alloc(Value::Relation(buf))
}

/// Unwrap a scalar source relation: extract the `_value` field from the first row.
/// Returns a default (Int 0) if the relation is empty.
#[unsafe(no_mangle)]
pub extern "C" fn knot_scalar_source_unwrap(rel: *mut Value) -> *mut Value {
    match unsafe { as_ref(rel) } {
        Value::Relation(rows) => {
            if rows.is_empty() {
                alloc_int(0)
            } else {
                knot_record_field(rows[0], "_value".as_ptr(), 6)
            }
        }
        _ => rel,
    }
}

/// Wrap a scalar value as a singleton relation with a `_value` field: [{_value: val}]
#[unsafe(no_mangle)]
pub extern "C" fn knot_scalar_source_wrap(val: *mut Value) -> *mut Value {
    let record = alloc(Value::Record(vec![
        RecordField { name: "_value".into(), value: val },
    ]));
    alloc(Value::Relation(vec![record]))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_push(rel: *mut Value, row: *mut Value) {
    let r = unsafe { &mut *rel };
    match r {
        Value::Relation(rows) => rows.push(row),
        _ => panic!("knot runtime: expected Relation in push, got {}", type_name(rel)),
    }
}

/// If the value is already a relation, return it as-is.
/// Otherwise, wrap it in a singleton relation.
/// Null (nullable none) wraps as a singleton containing null.
#[unsafe(no_mangle)]
pub extern "C" fn knot_ensure_relation(v: *mut Value) -> *mut Value {
    if v.is_null() {
        return alloc(Value::Relation(vec![v]));
    }
    match unsafe { as_ref(v) } {
        Value::Relation(_) => v,
        _ => alloc(Value::Relation(vec![v])),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_len(rel: *mut Value) -> usize {
    match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows.len(),
        _ => panic!("knot runtime: expected Relation in len, got {}", type_name(rel)),
    }
}

/// Take the first `n` elements from a relation.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_take(
    n_val: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let n = match unsafe { as_ref(n_val) } {
        Value::Int(i) => (*i).max(0) as usize,
        _ => 0,
    };
    match unsafe { as_ref(rel) } {
        Value::Relation(rows) => {
            let take_n = n.min(rows.len());
            alloc(Value::Relation(rows[..take_n].to_vec()))
        }
        _ => rel,
    }
}

/// Drop the first `n` elements from a relation.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_drop(
    n_val: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let n = match unsafe { as_ref(n_val) } {
        Value::Int(i) => (*i).max(0) as usize,
        _ => 0,
    };
    match unsafe { as_ref(rel) } {
        Value::Relation(rows) => {
            let drop_n = n.min(rows.len());
            alloc(Value::Relation(rows[drop_n..].to_vec()))
        }
        _ => rel,
    }
}

/// Sort a relation by a key function, returning a new relation.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_sort_by(
    db: *mut c_void,
    key_fn: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => return rel,
    };
    if rows.len() <= 1 {
        return rel;
    }
    let mut indexed: Vec<(*mut Value, *mut Value)> = rows
        .iter()
        .map(|&row| {
            let key = knot_value_call(db, key_fn, row);
            (row, key)
        })
        .collect();
    indexed.sort_by(|(_, a), (_, b)| {
        let ord = knot_value_compare_ord(*a, *b);
        if ord < 0 { std::cmp::Ordering::Less }
        else if ord > 0 { std::cmp::Ordering::Greater }
        else { std::cmp::Ordering::Equal }
    });
    let sorted: Vec<*mut Value> = indexed.into_iter().map(|(row, _)| row).collect();
    alloc(Value::Relation(sorted))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_get(rel: *mut Value, index: usize) -> *mut Value {
    match unsafe { as_ref(rel) } {
        Value::Relation(rows) => {
            if index < rows.len() {
                rows[index]
            } else {
                alloc(Value::Unit)
            }
        }
        _ => panic!("knot runtime: expected Relation in get, got {}", type_name(rel)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_union(
    db: *mut c_void,
    a: *mut Value,
    b: *mut Value,
) -> *mut Value {
    let empty = Vec::new();
    let rows_a = match unsafe { as_ref(a) } {
        Value::Relation(rows) => rows,
        Value::Unit => &empty,
        _ => panic!("knot runtime: expected Relation in union, got {}", type_name(a)),
    };
    let rows_b = match unsafe { as_ref(b) } {
        Value::Relation(rows) => rows,
        Value::Unit => &empty,
        _ => panic!("knot runtime: expected Relation in union, got {}", type_name(b)),
    };

    if rows_a.is_empty() && rows_b.is_empty() {
        return alloc(Value::Relation(Vec::new()));
    }
    // When one side is empty, still dedup the non-empty side for set semantics
    if rows_a.is_empty() {
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        let mut buf = Vec::new();
        for &row in rows_b.iter() {
            buf.clear();
            value_to_hash_bytes(row, &mut buf);
            if seen.insert(buf.clone()) {
                result.push(row);
            }
        }
        return alloc(Value::Relation(result));
    }
    if rows_b.is_empty() {
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        let mut buf = Vec::new();
        for &row in rows_a.iter() {
            buf.clear();
            value_to_hash_bytes(row, &mut buf);
            if seen.insert(buf.clone()) {
                result.push(row);
            }
        }
        return alloc(Value::Relation(result));
    }

    let db_ref = unsafe { &*(db as *mut KnotDb) };
    if let Some(result) = sql_set_op(&db_ref.conn, rows_a, rows_b, "UNION") {
        return alloc(Value::Relation(result));
    }

    // Fallback: in-memory hash-based dedup
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    let mut buf = Vec::new();
    for &row in rows_a.iter().chain(rows_b.iter()) {
        buf.clear();
        value_to_hash_bytes(row, &mut buf);
        if !seen.contains(buf.as_slice()) {
            seen.insert(buf.clone());
            result.push(row);
        }
    }
    alloc(Value::Relation(result))
}

/// Monadic bind for relations: iterate `rel`, call `func` on each element,
/// union all resulting relations into one.
/// Signature: (db, func, rel) -> rel
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_bind(
    db: *mut c_void,
    func: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        Value::Unit => return alloc(Value::Relation(Vec::new())),
        _ => panic!(
            "knot runtime: expected Relation in bind, got {}",
            type_name(rel)
        ),
    };

    if rows.is_empty() {
        return alloc(Value::Relation(Vec::new()));
    }

    // Collect all sub-relation rows
    let mut all_rows: Vec<*mut Value> = Vec::new();
    for &row in rows {
        let sub = knot_value_call(db, func, row);
        match unsafe { as_ref(sub) } {
            Value::Relation(sub_rows) => {
                all_rows.extend_from_slice(sub_rows);
            }
            _ => panic!(
                "knot runtime: bind function must return a Relation, got {}",
                type_name(sub)
            ),
        }
    }

    if all_rows.is_empty() {
        return alloc(Value::Relation(Vec::new()));
    }

    // Dedup via SQLite
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    if let Some(result) = sql_dedup(&db_ref.conn, &all_rows) {
        return alloc(Value::Relation(result));
    }

    // Fallback: in-memory dedup
    alloc(Value::Relation(in_memory_dedup(all_rows)))
}

/// Group a relation by key columns using SQLite ORDER BY for efficient grouping.
/// Inserts key columns + row indices into a temp table, sorts via ORDER BY,
/// then groups consecutive rows with matching keys in O(n).
/// Signature: (db, rel, schema_ptr, schema_len, key_cols_ptr, key_cols_len) -> [[row]]
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_group_by(
    db: *mut c_void,
    rel: *mut Value,
    schema_ptr: *const u8,
    schema_len: usize,
    key_cols_ptr: *const u8,
    key_cols_len: usize,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: expected Relation in group_by, got {}",
            type_name(rel)
        ),
    };

    if rows.is_empty() {
        return alloc(Value::Relation(Vec::new()));
    }

    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let schema_str = unsafe { str_from_raw(schema_ptr, schema_len) };
    let key_cols_str = unsafe { str_from_raw(key_cols_ptr, key_cols_len) };
    let schema = parse_record_schema(schema_str);
    let key_col_names: Vec<&str> = if key_cols_str.is_empty() {
        Vec::new()
    } else {
        key_cols_str.split(',').collect()
    };

    // Find key column specs in the schema via HashMap lookup
    let col_map: HashMap<&str, &ColumnSpec> = schema.columns.iter().map(|c| (c.name.as_str(), c)).collect();
    let key_specs: Vec<&ColumnSpec> = key_col_names
        .iter()
        .map(|kc| {
            *col_map.get(kc).unwrap_or_else(|| {
                panic!(
                    "knot runtime: key column '{}' not found in schema",
                    kc
                )
            })
        })
        .collect();

    // Build column name list: _idx + key columns
    let mut col_names = vec!["\"_idx\"".to_string()];
    for ks in &key_specs {
        col_names.push(quote_ident(&ks.name));
    }
    let col_str = col_names.join(", ");
    let order_cols: Vec<String> = key_specs
        .iter()
        .map(|ks| quote_ident(&ks.name))
        .collect();

    // Extract key params from each row (shared by both paths)
    let extract_key_params = |row_ptr: &*mut Value, key_specs: &[&ColumnSpec]| -> Vec<rusqlite::types::Value> {
        let fields = match unsafe { as_ref(*row_ptr) } {
            Value::Record(fields) => fields,
            _ => panic!("knot runtime: groupby rows must be Records"),
        };
        key_specs.iter().map(|ks| {
            let value = fields.iter().find(|f| &*f.name == ks.name.as_str())
                .unwrap_or_else(|| panic!("knot runtime: missing field '{}' in record", ks.name));
            value_to_sqlite(value.value, ks.ty)
        }).collect()
    };

    // Only key columns are params (_idx is a literal); check if VALUES is feasible
    let n_key_params = rows.len() * key_specs.len();
    let (select_sql, sql_params, temp_to_drop) = if n_key_params <= MAX_VALUES_PARAMS && !key_specs.is_empty() {
        // VALUES CTE path: _idx is a literal integer, key columns are params
        let mut params: Vec<rusqlite::types::Value> = Vec::with_capacity(n_key_params);
        let mut row_clauses = Vec::with_capacity(rows.len());
        let mut pidx = 1usize;

        for (i, row_ptr) in rows.iter().enumerate() {
            let key_params = extract_key_params(row_ptr, &key_specs);
            let mut placeholders = vec![format!("{}", i)]; // _idx literal
            for _ in &key_params {
                placeholders.push(format!("?{}", pidx));
                pidx += 1;
            }
            row_clauses.push(format!("({})", placeholders.join(", ")));
            params.extend(key_params);
        }

        let values_sql = format!("VALUES {}", row_clauses.join(", "));
        let sql = if order_cols.is_empty() {
            format!("WITH _t({}) AS ({}) SELECT {} FROM _t", col_str, values_sql, col_str)
        } else {
            format!(
                "WITH _t({}) AS ({}) SELECT {} FROM _t ORDER BY {}",
                col_str, values_sql, col_str, order_cols.join(", ")
            )
        };
        (sql, params, None)
    } else {
        // Temp table fallback for large datasets or no key columns
        let temp_name = next_temp_name();
        let temp = quote_ident(&temp_name);

        let _ = db_ref.conn.execute_batch(&format!("DROP TABLE IF EXISTS {};", temp));

        let mut col_defs = vec!["_idx INTEGER".to_string()];
        for ks in &key_specs {
            col_defs.push(format!("{} {}", quote_ident(&ks.name), sql_type(ks.ty)));
        }
        let create_sql = format!("CREATE TEMP TABLE {} ({});", temp, col_defs.join(", "));
        debug_sql(&create_sql);
        db_ref.conn.execute_batch(&create_sql)
            .expect("knot runtime: failed to create groupby temp table");

        let placeholders: Vec<String> = (1..=col_names.len()).map(|i| format!("?{}", i)).collect();
        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({});",
            temp, col_str, placeholders.join(", ")
        );
        debug_sql(&insert_sql);

        {
            let mut insert_stmt = db_ref.conn.prepare_cached(&insert_sql)
                .expect("knot runtime: failed to prepare groupby insert");
            for (idx, row_ptr) in rows.iter().enumerate() {
                let mut params: Vec<rusqlite::types::Value> =
                    vec![rusqlite::types::Value::Integer(idx as i64)];
                params.extend(extract_key_params(row_ptr, &key_specs));
                let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                    params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
                insert_stmt.execute(param_refs.as_slice())
                    .expect("knot runtime: groupby insert error");
            }
        }

        let sql = if order_cols.is_empty() {
            format!("SELECT {} FROM {}", col_str, temp)
        } else {
            format!("SELECT {} FROM {} ORDER BY {}", col_str, temp, order_cols.join(", "))
        };
        (sql, Vec::new(), Some(temp_name))
    };

    debug_sql(&select_sql);

    // Execute query and group consecutive rows by key values
    let groups = {
        let mut stmt = db_ref.conn.prepare(&select_sql)
            .expect("knot runtime: failed to prepare groupby select");
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            sql_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
        let mut result_rows = stmt.query(param_refs.as_slice())
            .expect("knot runtime: groupby select error");

        let mut groups: Vec<Vec<*mut Value>> = Vec::new();
        let mut current_group: Vec<*mut Value> = Vec::new();
        let mut prev_keys: Option<Vec<rusqlite::types::Value>> = None;

        while let Some(row) = result_rows
            .next()
            .unwrap_or_else(|e| panic!("knot runtime: groupby fetch error: {}", e))
        {
            let idx: i64 = row.get(0).unwrap();
            let keys: Vec<rusqlite::types::Value> = (1..=key_specs.len())
                .map(|i| row.get(i).unwrap())
                .collect();

            if let Some(ref prev) = prev_keys {
                if keys != *prev {
                    groups.push(std::mem::take(&mut current_group));
                }
            }

            current_group.push(rows[idx as usize]);
            prev_keys = Some(keys);
        }

        if !current_group.is_empty() {
            groups.push(current_group);
        }

        groups
    };

    // Clean up temp table if used
    if let Some(ref temp_name) = temp_to_drop {
        let _ = db_ref.conn.execute_batch(
            &format!("DROP TABLE IF EXISTS {};", quote_ident(temp_name))
        );
    }

    // Convert to a relation of relations
    let result: Vec<*mut Value> = groups
        .into_iter()
        .map(|rows| alloc(Value::Relation(rows)))
        .collect();

    alloc(Value::Relation(result))
}

// ── Value equality ────────────────────────────────────────────────

/// Recursively serialize a Value to bytes for hash-based set comparison.
fn value_to_hash_bytes(v: *mut Value, buf: &mut Vec<u8>) {
    if v.is_null() {
        buf.push(0xFF);
        return;
    }
    match unsafe { as_ref(v) } {
        Value::Int(n) => {
            buf.push(0);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::Float(f) => {
            buf.push(1);
            // Use raw bits for hashing to match total_cmp equality semantics
            // (total_cmp distinguishes -0.0 from +0.0). Canonicalize NaN so
            // all NaN bit patterns hash the same (total_cmp treats them equal).
            let bits = if f.is_nan() { f64::NAN.to_bits() } else { f.to_bits() };
            buf.extend_from_slice(&bits.to_le_bytes());
        }
        Value::Text(s) => {
            buf.push(2);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Bool(b) => {
            buf.push(3);
            buf.push(*b as u8);
        }
        Value::Bytes(b) => {
            buf.push(4);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Unit => {
            buf.push(5);
        }
        Value::Record(fields) => {
            buf.push(6);
            buf.extend_from_slice(&(fields.len() as u32).to_le_bytes());
            for field in fields {
                buf.extend_from_slice(&(field.name.len() as u32).to_le_bytes());
                buf.extend_from_slice(field.name.as_bytes());
                value_to_hash_bytes(field.value, buf);
            }
        }
        Value::Constructor(tag, payload) => {
            buf.push(7);
            buf.extend_from_slice(&(tag.len() as u32).to_le_bytes());
            buf.extend_from_slice(tag.as_bytes());
            value_to_hash_bytes(*payload, buf);
        }
        Value::Relation(rows) => {
            buf.push(8);
            buf.extend_from_slice(&(rows.len() as u32).to_le_bytes());
            // Sort serialized rows for order-independent comparison
            let mut row_bytes: Vec<Vec<u8>> = rows
                .iter()
                .map(|r| {
                    let mut rb = Vec::new();
                    value_to_hash_bytes(*r, &mut rb);
                    rb
                })
                .collect();
            row_bytes.sort_unstable();
            for rb in &row_bytes {
                buf.extend_from_slice(&(rb.len() as u32).to_le_bytes());
                buf.extend_from_slice(rb);
            }
        }
        Value::Function(f) => {
            buf.push(9);
            buf.extend_from_slice(&(f.source.len() as u32).to_le_bytes());
            buf.extend_from_slice(f.source.as_bytes());
            value_to_hash_bytes(f.env, buf);
        }
        Value::IO(_, _) => {
            buf.push(11);
        }
        Value::Pair(a, b) => {
            // Pair is an internal-only variant for IO thunk envs; it should
            // never reach user-visible hash/compare paths.  Handle it for
            // exhaustiveness but don't expect it.
            buf.push(12);
            value_to_hash_bytes(*a, buf);
            value_to_hash_bytes(*b, buf);
        }
    }
}



fn values_equal(a: *mut Value, b: *mut Value) -> bool {
    if a == b {
        return true;
    }
    // Nullable encoding: null represents the "none" variant
    if a.is_null() || b.is_null() {
        return false; // a == b already handled both-null
    }
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Int(x), Value::Int(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => x.total_cmp(y) == std::cmp::Ordering::Equal,
        (Value::Int(x), Value::Float(y)) => (*x as f64).total_cmp(y) == std::cmp::Ordering::Equal,
        (Value::Float(x), Value::Int(y)) => x.total_cmp(&(*y as f64)) == std::cmp::Ordering::Equal,
        (Value::Text(x), Value::Text(y)) => x == y,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Bytes(x), Value::Bytes(y)) => x == y,
        (Value::Unit, Value::Unit) => true,
        (Value::Record(fa), Value::Record(fb)) => {
            if fa.len() != fb.len() {
                return false;
            }
            // Fields are sorted by name — linear comparison
            fa.iter().zip(fb.iter()).all(|(a, b)| {
                a.name == b.name && values_equal(a.value, b.value)
            })
        }
        (Value::Constructor(ta, pa), Value::Constructor(tb, pb)) => {
            ta == tb && values_equal(*pa, *pb)
        }
        (Value::Relation(ra), Value::Relation(rb)) => {
            // Set semantics: compare unique elements (consistent with SQL paths)
            let set_a: HashSet<Vec<u8>> = ra.iter().map(|r| {
                let mut buf = Vec::new();
                value_to_hash_bytes(*r, &mut buf);
                buf
            }).collect();
            let set_b: HashSet<Vec<u8>> = rb.iter().map(|r| {
                let mut buf = Vec::new();
                value_to_hash_bytes(*r, &mut buf);
                buf
            }).collect();
            set_a == set_b
        }
        (Value::Function(a), Value::Function(b)) => {
            a.fn_ptr == b.fn_ptr && a.source == b.source && values_equal(a.env, b.env)
        }
        (Value::IO(fn_a, env_a), Value::IO(fn_b, env_b)) => {
            fn_a == fn_b && values_equal(*env_a, *env_b)
        }
        _ => false,
    }
}

// ── Binary operations ─────────────────────────────────────────────

enum NumView {
    Int(i64),
    Float(f64),
}

#[inline]
fn to_num_view(v: &Value) -> Option<NumView> {
    match v {
        Value::Int(n) => Some(NumView::Int(*n)),
        Value::Float(f) => Some(NumView::Float(*f)),
        _ => None,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_add(a: *mut Value, b: *mut Value) -> *mut Value {
    let av = unsafe { as_ref(a) };
    let bv = unsafe { as_ref(b) };
    match (to_num_view(av), to_num_view(bv)) {
        (Some(NumView::Int(x)), Some(NumView::Int(y))) => match x.checked_add(y) {
            Some(r) => alloc_int(r),
            None => panic!("knot runtime: integer overflow in {} + {}", x, y),
        },
        (Some(NumView::Float(x)), Some(NumView::Float(y))) => alloc_float(x + y),
        (Some(NumView::Int(x)), Some(NumView::Float(y))) => alloc_float(x as f64 + y),
        (Some(NumView::Float(x)), Some(NumView::Int(y))) => alloc_float(x + y as f64),
        _ => panic!("knot runtime: cannot add {} + {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_sub(a: *mut Value, b: *mut Value) -> *mut Value {
    let av = unsafe { as_ref(a) };
    let bv = unsafe { as_ref(b) };
    match (to_num_view(av), to_num_view(bv)) {
        (Some(NumView::Int(x)), Some(NumView::Int(y))) => match x.checked_sub(y) {
            Some(r) => alloc_int(r),
            None => panic!("knot runtime: integer overflow in {} - {}", x, y),
        },
        (Some(NumView::Float(x)), Some(NumView::Float(y))) => alloc_float(x - y),
        (Some(NumView::Int(x)), Some(NumView::Float(y))) => alloc_float(x as f64 - y),
        (Some(NumView::Float(x)), Some(NumView::Int(y))) => alloc_float(x - y as f64),
        _ => panic!("knot runtime: cannot subtract {} - {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_mul(a: *mut Value, b: *mut Value) -> *mut Value {
    let av = unsafe { as_ref(a) };
    let bv = unsafe { as_ref(b) };
    match (to_num_view(av), to_num_view(bv)) {
        (Some(NumView::Int(x)), Some(NumView::Int(y))) => match x.checked_mul(y) {
            Some(r) => alloc_int(r),
            None => panic!("knot runtime: integer overflow in {} * {}", x, y),
        },
        (Some(NumView::Float(x)), Some(NumView::Float(y))) => alloc_float(x * y),
        (Some(NumView::Int(x)), Some(NumView::Float(y))) => alloc_float(x as f64 * y),
        (Some(NumView::Float(x)), Some(NumView::Int(y))) => alloc_float(x * y as f64),
        _ => panic!("knot runtime: cannot multiply {} * {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_div(a: *mut Value, b: *mut Value) -> *mut Value {
    let av = unsafe { as_ref(a) };
    let bv = unsafe { as_ref(b) };
    match (to_num_view(av), to_num_view(bv)) {
        (Some(NumView::Int(x)), Some(NumView::Int(y))) => {
            if y == 0 {
                panic!("knot runtime: division by zero");
            }
            match x.checked_div(y) {
                Some(r) => alloc_int(r),
                None => panic!("knot runtime: integer overflow in {} / {}", x, y),
            }
        }
        (Some(NumView::Float(x)), Some(NumView::Float(y))) => {
            if y == 0.0 {
                panic!("knot runtime: division by zero");
            }
            alloc_float(x / y)
        }
        (Some(NumView::Int(x)), Some(NumView::Float(y))) => {
            if y == 0.0 {
                panic!("knot runtime: division by zero");
            }
            alloc_float(x as f64 / y)
        }
        (Some(NumView::Float(x)), Some(NumView::Int(y))) => {
            if y == 0 {
                panic!("knot runtime: division by zero");
            }
            alloc_float(x / y as f64)
        }
        _ => panic!("knot runtime: cannot divide {} / {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_eq(a: *mut Value, b: *mut Value) -> *mut Value {
    alloc_bool(values_equal(a, b))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_neq(a: *mut Value, b: *mut Value) -> *mut Value {
    alloc_bool(!values_equal(a, b))
}

// Unboxed variants returning i32 (0/1) — avoid Bool allocation when result feeds a branch
#[unsafe(no_mangle)]
pub extern "C" fn knot_value_eq_i32(a: *mut Value, b: *mut Value) -> i32 {
    values_equal(a, b) as i32
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_neq_i32(a: *mut Value, b: *mut Value) -> i32 {
    !values_equal(a, b) as i32
}

fn compare_lt(a: *mut Value, b: *mut Value) -> bool {
    if a.is_null() || b.is_null() {
        eprintln!("knot runtime: comparison with null value (a={}, b={})",
            if a.is_null() { "null".to_string() } else { brief_value(a) },
            if b.is_null() { "null".to_string() } else { brief_value(b) });
        return false;
    }
    let av = unsafe { as_ref(a) };
    let bv = unsafe { as_ref(b) };
    if let (Value::Text(x), Value::Text(y)) = (av, bv) {
        return x < y;
    }
    match (to_num_view(av), to_num_view(bv)) {
        (Some(NumView::Int(x)), Some(NumView::Int(y))) => x < y,
        (Some(NumView::Float(x)), Some(NumView::Float(y))) => x.total_cmp(&y) == std::cmp::Ordering::Less,
        (Some(NumView::Int(x)), Some(NumView::Float(y))) => (x as f64).total_cmp(&y) == std::cmp::Ordering::Less,
        (Some(NumView::Float(x)), Some(NumView::Int(y))) => x.total_cmp(&(y as f64)) == std::cmp::Ordering::Less,
        _ => panic!("knot runtime: cannot compare {} < {}", type_name(a), type_name(b)),
    }
}

fn compare_gt(a: *mut Value, b: *mut Value) -> bool {
    if a.is_null() || b.is_null() {
        eprintln!("knot runtime: comparison with null value (a={}, b={})",
            if a.is_null() { "null".to_string() } else { brief_value(a) },
            if b.is_null() { "null".to_string() } else { brief_value(b) });
        return false;
    }
    let av = unsafe { as_ref(a) };
    let bv = unsafe { as_ref(b) };
    if let (Value::Text(x), Value::Text(y)) = (av, bv) {
        return x > y;
    }
    match (to_num_view(av), to_num_view(bv)) {
        (Some(NumView::Int(x)), Some(NumView::Int(y))) => x > y,
        (Some(NumView::Float(x)), Some(NumView::Float(y))) => x.total_cmp(&y) == std::cmp::Ordering::Greater,
        (Some(NumView::Int(x)), Some(NumView::Float(y))) => (x as f64).total_cmp(&y) == std::cmp::Ordering::Greater,
        (Some(NumView::Float(x)), Some(NumView::Int(y))) => x.total_cmp(&(y as f64)) == std::cmp::Ordering::Greater,
        _ => panic!("knot runtime: cannot compare {} > {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_lt(a: *mut Value, b: *mut Value) -> *mut Value {
    if a.is_null() || b.is_null() { return alloc_bool(false); }
    alloc_bool(compare_lt(a, b))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_gt(a: *mut Value, b: *mut Value) -> *mut Value {
    if a.is_null() || b.is_null() { return alloc_bool(false); }
    alloc_bool(compare_gt(a, b))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_le(a: *mut Value, b: *mut Value) -> *mut Value {
    if a.is_null() || b.is_null() { return alloc_bool(false); }
    alloc_bool(!compare_gt(a, b))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_ge(a: *mut Value, b: *mut Value) -> *mut Value {
    if a.is_null() || b.is_null() { return alloc_bool(false); }
    alloc_bool(!compare_lt(a, b))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_lt_i32(a: *mut Value, b: *mut Value) -> i32 {
    if a.is_null() || b.is_null() { return 0; }
    compare_lt(a, b) as i32
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_gt_i32(a: *mut Value, b: *mut Value) -> i32 {
    if a.is_null() || b.is_null() { return 0; }
    compare_gt(a, b) as i32
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_le_i32(a: *mut Value, b: *mut Value) -> i32 {
    if a.is_null() || b.is_null() { return 0; }
    !compare_gt(a, b) as i32
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_ge_i32(a: *mut Value, b: *mut Value) -> i32 {
    if a.is_null() || b.is_null() { return 0; }
    !compare_lt(a, b) as i32
}

// Unboxed boolean operations returning i32 (0/1) — avoid Bool allocation in conditions
#[unsafe(no_mangle)]
pub extern "C" fn knot_value_and_i32(a: *mut Value, b: *mut Value) -> i32 {
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Bool(x), Value::Bool(y)) => (*x && *y) as i32,
        _ => panic!("knot runtime: && requires Bool operands, got {} && {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_or_i32(a: *mut Value, b: *mut Value) -> i32 {
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Bool(x), Value::Bool(y)) => (*x || *y) as i32,
        _ => panic!("knot runtime: || requires Bool operands, got {} || {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_and(a: *mut Value, b: *mut Value) -> *mut Value {
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Bool(x), Value::Bool(y)) => alloc_bool(*x && *y),
        _ => panic!("knot runtime: && requires Bool operands, got {} && {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_or(a: *mut Value, b: *mut Value) -> *mut Value {
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Bool(x), Value::Bool(y)) => alloc_bool(*x || *y),
        _ => panic!("knot runtime: || requires Bool operands, got {} || {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_concat(a: *mut Value, b: *mut Value) -> *mut Value {
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Text(x), Value::Text(y)) => {
            let mut s = String::with_capacity(x.len() + y.len());
            s.push_str(x);
            s.push_str(y);
            alloc(Value::Text(Arc::from(s)))
        }
        (Value::Relation(rows_a), Value::Relation(rows_b)) => {
            // ++ on relations is union (in-memory hash-based dedup)
            let total = rows_a.len() + rows_b.len();
            let mut seen = HashSet::with_capacity(total);
            let mut result = Vec::with_capacity(total);
            let mut buf = Vec::with_capacity(128);
            for &row in rows_a.iter().chain(rows_b.iter()) {
                buf.clear();
                value_to_hash_bytes(row, &mut buf);
                if !seen.contains(buf.as_slice()) {
                    seen.insert(std::mem::take(&mut buf));
                    result.push(row);
                }
            }
            alloc(Value::Relation(result))
        }
        _ => panic!("knot runtime: ++ requires Text or Relation operands, got {} ++ {}", type_name(a), type_name(b)),
    }
}

// ── Comparison (returns Ordering ADT) ─────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_compare(a: *mut Value, b: *mut Value) -> *mut Value {
    let ordering = compare_values(a, b);
    let tag = match ordering {
        std::cmp::Ordering::Less => "LT",
        std::cmp::Ordering::Equal => "EQ",
        std::cmp::Ordering::Greater => "GT",
    };
    alloc(Value::Constructor(
        intern_str(tag),
        alloc(Value::Unit),
    ))
}

/// Compare two values and return a raw i32: -1 (LT), 0 (EQ), 1 (GT).
/// Avoids allocating an Ordering constructor for use in comparison operators.
#[unsafe(no_mangle)]
pub extern "C" fn knot_value_compare_ord(a: *mut Value, b: *mut Value) -> i32 {
    match compare_values(a, b) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

fn compare_values(a: *mut Value, b: *mut Value) -> std::cmp::Ordering {
    let av = unsafe { as_ref(a) };
    let bv = unsafe { as_ref(b) };
    if let (Value::Text(x), Value::Text(y)) = (av, bv) {
        return x.cmp(y);
    }
    match (to_num_view(av), to_num_view(bv)) {
        (Some(NumView::Int(x)), Some(NumView::Int(y))) => x.cmp(&y),
        (Some(NumView::Float(x)), Some(NumView::Float(y))) => x.total_cmp(&y),
        (Some(NumView::Int(x)), Some(NumView::Float(y))) => (x as f64).total_cmp(&y),
        (Some(NumView::Float(x)), Some(NumView::Int(y))) => x.total_cmp(&(y as f64)),
        _ => panic!(
            "knot runtime: cannot compare {} with {}",
            type_name(a),
            type_name(b)
        ),
    }
}

/// Extract Ordering constructor tag as i32: 0=LT, 1=EQ, 2=GT.
/// Avoids string comparison when checking comparison results.
#[unsafe(no_mangle)]
pub extern "C" fn knot_ordering_tag_i32(v: *mut Value) -> i32 {
    match unsafe { as_ref(v) } {
        Value::Constructor(tag, _) => match &**tag {
            "LT" => 0,
            "EQ" => 1,
            "GT" => 2,
            _ => panic!("knot runtime: expected Ordering constructor, got {}", tag),
        },
        _ => panic!("knot runtime: expected Ordering Constructor, got {}", type_name(v)),
    }
}

// ── Unary operations ──────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_negate(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Int(n) => match n.checked_neg() {
            Some(r) => alloc_int(r),
            None => panic!("knot runtime: integer overflow in negation of {}", n),
        },
        Value::Float(n) => alloc_float(-n),
        _ => panic!("knot runtime: cannot negate {}", type_name(v)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_not(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Bool(b) => alloc_bool(!b),
        _ => panic!("knot runtime: 'not' requires Bool, got {}", type_name(v)),
    }
}

// ── Function calls ────────────────────────────────────────────────

/// Call a function value: fn_ptr(db, env, arg) -> result
#[unsafe(no_mangle)]
pub extern "C" fn knot_value_call(
    db: *mut c_void,
    func: *mut Value,
    arg: *mut Value,
) -> *mut Value {
    match unsafe { as_ref(func) } {
        Value::Function(f) => {
            let fun: extern "C" fn(*mut c_void, *mut Value, *mut Value) -> *mut Value =
                unsafe { std::mem::transmute(f.fn_ptr) };
            fun(db, f.env, arg)
        }
        _ => panic!("knot runtime: cannot call {}, expected Function", brief_value(func)),
    }
}

// ── Printing ──────────────────────────────────────────────────────

fn format_value(v: *mut Value) -> String {
    if v.is_null() {
        return "null".to_string();
    }
    match unsafe { as_ref(v) } {
        Value::Int(n) => n.to_string(),
        Value::Float(n) => {
            if n.is_nan() || n.is_infinite() {
                format!("{}", n)
            } else if n.fract() == 0.0 {
                format!("{:.1}", n)
            } else {
                n.to_string()
            }
        }
        Value::Text(s) => format!("\"{}\"", s),
        Value::Bytes(b) => {
            let mut hex = String::with_capacity(b.len() * 2 + 3);
            hex.push_str("b\"");
            for byte in b.iter() {
                use std::fmt::Write;
                let _ = write!(hex, "{:02x}", byte);
            }
            hex.push('"');
            hex
        }
        Value::Bool(b) => {
            if *b {
                "True {}".to_string()
            } else {
                "False {}".to_string()
            }
        }
        Value::Unit => "{}".to_string(),
        Value::Record(fields) => {
            let inner: Vec<String> = fields
                .iter()
                .map(|f| format!("{}: {}", f.name, format_value(f.value)))
                .collect();
            format!("{{{}}}", inner.join(", "))
        }
        Value::Relation(rows) => {
            let inner: Vec<String> = rows.iter().map(|r| format_value(*r)).collect();
            format!("[{}]", inner.join(", "))
        }
        Value::Constructor(tag, payload) => {
            let p = format_value(*payload);
            if p == "{}" {
                format!("{} {{}}", tag)
            } else {
                format!("{} {}", tag, p)
            }
        }
        Value::Function(f) => f.source.to_string(),
        Value::IO(_, _) => "<<IO>>".to_string(),
        Value::Pair(_, _) => "<<Pair>>".to_string(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_read_line() -> *mut Value {
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .expect("knot runtime: failed to read from stdin");
    // Strip trailing newline
    if line.ends_with('\n') {
        line.pop();
        if line.ends_with('\r') {
            line.pop();
        }
    }
    alloc(Value::Text(Arc::from(line)))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_print(v: *mut Value) -> *mut Value {
    print!("{}", format_value(v));
    alloc(Value::Unit)
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_println(v: *mut Value) -> *mut Value {
    println!("{}", format_value(v));
    alloc(Value::Unit)
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_log_info(v: *mut Value) -> *mut Value {
    log::log_info(&format_value(v));
    alloc(Value::Unit)
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_log_warn(v: *mut Value) -> *mut Value {
    log::log_warn(&format_value(v));
    alloc(Value::Unit)
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_log_error(v: *mut Value) -> *mut Value {
    log::log_error(&format_value(v));
    alloc(Value::Unit)
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_log_debug(v: *mut Value) -> *mut Value {
    log::log_debug(&format_value(v));
    alloc(Value::Unit)
}

/// Convert a value to its text representation (returned as a Value::Text).
/// Panic when a `where` guard fails inside an IO do-block.
#[unsafe(no_mangle)]
pub extern "C" fn knot_guard_failed() {
    panic!("knot runtime: where guard failed in IO do-block");
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_show(v: *mut Value) -> *mut Value {
    fn show_inner(v: *mut Value) -> String {
        if v.is_null() {
            return "null".to_string();
        }
        match unsafe { as_ref(v) } {
            Value::Int(n) => n.to_string(),
            Value::Float(n) => {
                if n.is_nan() || n.is_infinite() {
                    format!("{}", n)
                } else if n.fract() == 0.0 {
                    format!("{:.1}", n)
                } else {
                    n.to_string()
                }
            }
            Value::Text(s) => (**s).to_string(),
            Value::Bytes(b) => {
                let mut hex = String::with_capacity(b.len() * 2);
                for byte in b.iter() {
                    use std::fmt::Write;
                    let _ = write!(hex, "{:02x}", byte);
                }
                hex
            }
            Value::Bool(b) => {
                if *b { "True".to_string() } else { "False".to_string() }
            }
            Value::Unit => "{}".to_string(),
            Value::Record(fields) => {
                let inner: Vec<String> = fields
                    .iter()
                    .map(|f| format!("{}: {}", f.name, show_inner(f.value)))
                    .collect();
                format!("{{{}}}", inner.join(", "))
            }
            Value::Relation(rows) => {
                let inner: Vec<String> = rows.iter().map(|r| show_inner(*r)).collect();
                format!("[{}]", inner.join(", "))
            }
            Value::Constructor(tag, payload) => {
                let p = show_inner(*payload);
                if p == "{}" {
                    format!("{} {{}}", tag)
                } else {
                    format!("{} {}", tag, p)
                }
            }
            Value::Function(f) => f.source.to_string(),
            Value::IO(_, _) => "<<IO>>".to_string(),
            Value::Pair(_, _) => "<<Pair>>".to_string(),
        }
    }
    alloc(Value::Text(Arc::from(show_inner(v))))
}

// ── IO monad ─────────────────────────────────────────────────────

/// Create an IO value wrapping a thunk function pointer and captured environment.
#[unsafe(no_mangle)]
pub extern "C" fn knot_io_wrap(fn_ptr: *const u8, env: *mut Value) -> *mut Value {
    alloc(Value::IO(fn_ptr, env))
}

/// Create an IO thunk from a function pointer and captured environment.
/// Used by codegen to defer IO do-block execution until knot_io_run.
#[unsafe(no_mangle)]
pub extern "C" fn knot_io_new(fn_ptr: *const u8, env: *mut Value) -> *mut Value {
    alloc(Value::IO(fn_ptr, env))
}

/// Wrap a pure value in an IO thunk (IO.pure / return).
#[unsafe(no_mangle)]
pub extern "C" fn knot_io_pure(val: *mut Value) -> *mut Value {
    // Create a thunk that just returns val.
    // We encode this as IO with null fn_ptr — knot_io_run checks for this.
    alloc(Value::IO(std::ptr::null(), val))
}

/// Execute an IO thunk. If the value is not IO, return it as-is.
#[unsafe(no_mangle)]
pub extern "C" fn knot_io_run(db: *mut c_void, val: *mut Value) -> *mut Value {
    let mut current = val;
    loop {
        if current.is_null() {
            return current;
        }
        match unsafe { as_ref(current) } {
            Value::IO(fn_ptr, env) => {
                let fn_ptr = *fn_ptr;
                let env = *env;
                if fn_ptr.is_null() {
                    return env;
                }
                let thunk: extern "C" fn(*mut c_void, *mut Value) -> *mut Value =
                    unsafe { std::mem::transmute(fn_ptr) };
                // Trampoline: if the thunk returns another IO value,
                // loop instead of returning.  This prevents stack overflow
                // and arena growth in tail-recursive IO loops
                // (backgroundPrune, pollHeartbeat).
                current = thunk(db, env);
            }
            _ => return current,
        }
    }
}

/// Unpack a `Value::Pair` env built by `knot_io_bind`/`_then`/`_map`.
/// Panics if the env isn't a Pair — this is an internal invariant.
#[inline]
fn pair_unpack(env: *mut Value) -> (*mut Value, *mut Value) {
    match unsafe { as_ref(env) } {
        Value::Pair(a, b) => (*a, *b),
        _ => panic!(
            "knot runtime: expected Pair env in IO thunk, got {}",
            type_name(env)
        ),
    }
}

/// Monadic bind for IO: knot_io_bind(io, f) -> IO
/// Creates a new IO thunk that, when run:
///   1. Runs `io` to get result `a`
///   2. Calls `f(a)` to get a new IO action
///   3. Runs that IO action
///
/// The env is a `Value::Pair(io, f)` — a two-pointer tuple — instead of a
/// Record with named fields.  This skips one `Vec<RecordField>` allocation
/// and two `String` allocations (the field names "_io", "_f") per bind.
/// Over a long-running program with many IO chains that's millions of
/// avoided allocations.
#[unsafe(no_mangle)]
pub extern "C" fn knot_io_bind(io: *mut Value, f: *mut Value) -> *mut Value {
    let env = alloc(Value::Pair(io, f));

    extern "C" fn bind_thunk(db: *mut c_void, env: *mut Value) -> *mut Value {
        let (io, f) = pair_unpack(env);
        let a = knot_io_run(db, io);
        let io2 = knot_value_call(db, f, a);
        knot_io_run(db, io2)
    }

    alloc(Value::IO(bind_thunk as *const u8, env))
}

/// Sequence two IO actions, discarding the first result: knot_io_then(io1, io2) -> IO
#[unsafe(no_mangle)]
pub extern "C" fn knot_io_then(io1: *mut Value, io2: *mut Value) -> *mut Value {
    let env = alloc(Value::Pair(io1, io2));

    extern "C" fn then_thunk(db: *mut c_void, env: *mut Value) -> *mut Value {
        let (io1, io2) = pair_unpack(env);
        knot_io_run(db, io1);
        knot_io_run(db, io2)
    }

    alloc(Value::IO(then_thunk as *const u8, env))
}

/// map(f, io) — apply f to the result of an IO action
#[unsafe(no_mangle)]
pub extern "C" fn knot_io_map(f: *mut Value, io: *mut Value) -> *mut Value {
    let env = alloc(Value::Pair(io, f));

    extern "C" fn map_thunk(db: *mut c_void, env: *mut Value) -> *mut Value {
        let (io, f) = pair_unpack(env);
        let a = knot_io_run(db, io);
        knot_value_call(db, f, a)
    }

    alloc(Value::IO(map_thunk as *const u8, env))
}

// ── Spawn / threading ────────────────────────────────────────────

/// Deep-clone a Value tree so it can be sent to another thread.
/// Uses Box::new (not the thread-local arena) so values survive arena resets.
///
/// `Text` and `Bytes` use `Arc<str>` / `Arc<[u8]>`, so their clones are
/// atomic increments — large strings/blobs in fork envs are shared
/// across threads without copying.  Records and Relations still require
/// fresh Vec allocation (their backing is mutable `Vec`, not an Arc).
/// Iterative deep-clone.  Uses an explicit work stack so a deeply
/// nested structure (a long linked list in an ADT, a record whose
/// field is another record whose field is another ...) can't blow the
/// native call stack.
///
/// Two-phase: first allocate a shell for each reachable source pointer
/// (recording src→dst in a per-call map), then patch each shell's
/// children using the map.  Shared subtrees are cloned exactly once.
fn deep_clone_value(val: *mut Value) -> *mut Value {
    if val.is_null() { return val; }
    if is_tagged(val) { return val; }

    // src → dst map.  Shared subtrees (DAGs) are cloned exactly once.
    let mut map: HashMap<*mut Value, *mut Value> = HashMap::new();
    // Phase 1 stack: values to shell-clone.  We iterate depth-first so
    // children get allocated before we try to patch them.
    let mut to_alloc: Vec<*mut Value> = vec![val];
    // Phase 2 stack: (dst, children) — dst is a freshly-allocated shell,
    // children are the src pointers whose dsts need patching into dst's
    // slots.  The patching logic inspects dst's variant to know which
    // slot each child fills.
    let mut to_patch: Vec<*mut Value> = Vec::new();

    while let Some(src) = to_alloc.pop() {
        if src.is_null() || is_tagged(src) || map.contains_key(&src) {
            continue;
        }
        // Allocate a shell cloned value.  Leaf variants are complete;
        // compound variants have placeholder-null children to be filled
        // in phase 2.
        let shell = match unsafe { &*src } {
            Value::Int(n) => Value::Int(*n),
            Value::Float(f) => Value::Float(*f),
            Value::Text(s) => Value::Text(s.clone()),
            Value::Bool(b) => Value::Bool(*b),
            Value::Bytes(b) => Value::Bytes(b.clone()),
            Value::Unit => Value::Unit,
            Value::Record(fields) => {
                let mut new_fields = Vec::with_capacity(fields.len());
                for f in fields {
                    new_fields.push(RecordField {
                        name: f.name.clone(),
                        value: std::ptr::null_mut(),
                    });
                    to_alloc.push(f.value);
                }
                Value::Record(new_fields)
            }
            Value::Relation(rows) => {
                let mut new_rows = vec![std::ptr::null_mut(); rows.len()];
                for (i, &r) in rows.iter().enumerate() {
                    new_rows[i] = std::ptr::null_mut();
                    to_alloc.push(r);
                }
                Value::Relation(new_rows)
            }
            Value::Constructor(tag, inner) => {
                to_alloc.push(*inner);
                Value::Constructor(tag.clone(), std::ptr::null_mut())
            }
            Value::Function(f) => {
                to_alloc.push(f.env);
                Value::Function(Box::new(FunctionInner {
                    fn_ptr: f.fn_ptr,
                    env: std::ptr::null_mut(),
                    source: f.source.clone(),
                }))
            }
            Value::IO(fn_ptr, env) => {
                to_alloc.push(*env);
                Value::IO(*fn_ptr, std::ptr::null_mut())
            }
            Value::Pair(a, b) => {
                to_alloc.push(*a);
                to_alloc.push(*b);
                Value::Pair(std::ptr::null_mut(), std::ptr::null_mut())
            }
        };
        let dst = Box::into_raw(Box::new(shell));
        map.insert(src, dst);
        to_patch.push(src);
    }

    // Phase 2: patch children.  Walk each src whose dst shell was
    // allocated; look up each child's dst and write it into the
    // appropriate slot.  Null / tagged children resolve to themselves.
    for src in to_patch {
        let dst = *map.get(&src).unwrap();
        // SAFETY: we just allocated `dst` in phase 1; `src` is the
        // original — we're reading src's children and writing dst's.
        // `dst` is disjoint from `src`.
        match (unsafe { &*src }, unsafe { &mut *dst }) {
            (Value::Record(src_fields), Value::Record(dst_fields)) => {
                for (i, f) in src_fields.iter().enumerate() {
                    dst_fields[i].value = lookup_or_identity(&map, f.value);
                }
            }
            (Value::Relation(src_rows), Value::Relation(dst_rows)) => {
                for (i, &r) in src_rows.iter().enumerate() {
                    dst_rows[i] = lookup_or_identity(&map, r);
                }
            }
            (Value::Constructor(_, src_inner), Value::Constructor(_, dst_inner)) => {
                *dst_inner = lookup_or_identity(&map, *src_inner);
            }
            (Value::Function(src_f), Value::Function(dst_f)) => {
                dst_f.env = lookup_or_identity(&map, src_f.env);
            }
            (Value::IO(_, src_env), Value::IO(_, dst_env)) => {
                *dst_env = lookup_or_identity(&map, *src_env);
            }
            (Value::Pair(src_a, src_b), Value::Pair(dst_a, dst_b)) => {
                *dst_a = lookup_or_identity(&map, *src_a);
                *dst_b = lookup_or_identity(&map, *src_b);
            }
            _ => {}  // leaf: nothing to patch
        }
    }

    *map.get(&val).unwrap()
}

/// Resolve a child pointer through the src→dst map, or return it
/// unchanged if not in the map (null, tagged, or unchanged reference).
#[inline]
fn lookup_or_identity(map: &HashMap<*mut Value, *mut Value>, p: *mut Value) -> *mut Value {
    if p.is_null() || is_tagged(p) {
        return p;
    }
    map.get(&p).copied().unwrap_or(p)
}

/// Free a value tree allocated by `deep_clone_value`.
///
/// SAFETY: Every node in the tree must have been allocated by `Box::into_raw`.
/// Do NOT call this on arena-allocated values.
///
/// The cloned tree is a DAG (not a tree): `deep_clone_value` dedupes
/// shared subtrees via a src→dst `HashMap`, so a single `Box` may be
/// reachable via multiple paths.  A naive recursive walk would free
/// the same `Box` more than once (double-free), hence the iterative
/// two-phase algorithm below:
///
///   1. Walk the DAG with an explicit stack + visited set, collecting
///      every reachable node exactly once.
///   2. Free each collected node via `Box::from_raw` + drop.
///
/// Iteration (instead of recursion) also prevents stack overflow on
/// deeply nested structures (long linked lists in ADTs, etc.).
#[allow(dead_code)]
unsafe fn deep_drop_value(val: *mut Value) {
    if val.is_null() || is_tagged(val) {
        return;
    }
    let mut visited: HashSet<*mut Value> = HashSet::new();
    let mut stack: Vec<*mut Value> = Vec::new();
    stack.push(val);
    while let Some(v) = stack.pop() {
        if v.is_null() || is_tagged(v) || !visited.insert(v) {
            continue;
        }
        unsafe {
            match &*v {
                Value::Record(fields) => {
                    for f in fields {
                        stack.push(f.value);
                    }
                }
                Value::Relation(rows) => {
                    for r in rows {
                        stack.push(*r);
                    }
                }
                Value::Constructor(_, inner) => stack.push(*inner),
                Value::Function(f) => stack.push(f.env),
                Value::IO(_, env) => stack.push(*env),
                Value::Pair(a, b) => {
                    stack.push(*a);
                    stack.push(*b);
                }
                _ => {}
            }
        }
    }
    for v in visited {
        // SAFETY: each `v` was allocated by `Box::into_raw` in
        // `deep_clone_value`; `visited` ensures we reconstruct the
        // Box exactly once per unique pointer.
        unsafe { drop(Box::from_raw(v)); }
    }
}

/// Fork an IO action onto a new OS thread.
/// Takes an IO value, returns an IO thunk that spawns the thread.
#[unsafe(no_mangle)]
pub extern "C" fn knot_fork_io(io_val: *mut Value) -> *mut Value {
    // Capture the IO value in the thunk's environment
    let env = io_val;

    extern "C" fn spawn_thunk(_db: *mut c_void, env: *mut Value) -> *mut Value {
        // Deep-clone the IO value on the parent thread before sending.
        // Convert to usize to satisfy Send (deep_clone produces an independent tree).
        let cloned_io = deep_clone_value(env) as *mut u8 as usize;

        // Increment the live-fork counter *before* spawning so `knot_threads_join`
        // can't race past zero.  The spawned thread decrements via a drop guard.
        ACTIVE_FORKS.fetch_add(1, Ordering::SeqCst);

        // Detach: we drop the handle, so the thread runs to completion
        // independently.  Detached threads are reclaimed by the OS on exit.
        let _ = std::thread::spawn(move || {
            // Guards run in reverse declaration order.  `ForkCounter` is
            // declared first so it drops last: DB/IO cleanup happens before
            // the counter decrement, which prevents a race where a joiner
            // proceeds past `wait_while` while the thread is still cleaning up.
            struct ForkCounter;
            impl Drop for ForkCounter {
                fn drop(&mut self) {
                    ACTIVE_FORKS.fetch_sub(1, Ordering::SeqCst);
                    // Notify under the mutex so waiters never miss the wakeup.
                    let _g = ACTIVE_FORKS_MUTEX.lock().unwrap();
                    ACTIVE_FORKS_CVAR.notify_all();
                }
            }
            let _counter = ForkCounter;

            let io = cloned_io as *mut u8 as *mut Value;
            // Open a new DB connection for this thread
            let db_path = DB_PATH.lock().unwrap().clone();
            let db = knot_db_open(db_path.as_ptr(), db_path.len());

            // Use a drop guard to ensure cleanup even if knot_io_run panics
            struct CleanupGuard {
                db: *mut c_void,
                io: *mut Value,
            }
            impl Drop for CleanupGuard {
                fn drop(&mut self) {
                    knot_db_close(self.db);
                    unsafe { deep_drop_value(self.io); }
                }
            }
            let _guard = CleanupGuard { db, io };

            // Run the IO action
            knot_io_run(db, io);
        });

        alloc(Value::Unit)
    }

    alloc(Value::IO(spawn_thunk as *const u8, env))
}

/// Wait until all `fork`ed threads have completed.  Called from generated
/// main before `knot_db_close`.
///
/// Uses a condition variable keyed on `ACTIVE_FORKS`.  When the counter
/// reaches zero the wait returns.  Unlike the previous `JoinHandle` vector,
/// this keeps constant memory overhead regardless of how many threads have
/// been forked.
#[unsafe(no_mangle)]
pub extern "C" fn knot_threads_join() {
    let lock = ACTIVE_FORKS_MUTEX.lock().unwrap();
    let _guard = ACTIVE_FORKS_CVAR
        .wait_while(lock, |_| ACTIVE_FORKS.load(Ordering::SeqCst) > 0)
        .unwrap();
}

// ── STM retry functions ──────────────────────────────────────────

/// Called by `retry` in Knot. Sets thread-local flag and returns a dummy value.
#[unsafe(no_mangle)]
pub extern "C" fn knot_stm_retry() -> *mut Value {
    STM_RETRY.with(|r| r.set(true));
    alloc(Value::Unit)
}

/// Check if retry was requested, and clear the flag. Returns 1 if retry, 0 otherwise.
#[unsafe(no_mangle)]
pub extern "C" fn knot_stm_check_and_clear() -> i32 {
    STM_RETRY.with(|r| {
        let val = r.get();
        r.set(false);
        if val { 1 } else { 0 }
    })
}

/// Mark the current atomic body as having skipped early (failed pattern bind
/// or false `where` guard). The surrounding `atomic` will rollback instead of
/// committing.
#[unsafe(no_mangle)]
pub extern "C" fn knot_stm_skip() -> *mut Value {
    STM_SKIP.with(|s| s.set(true));
    alloc(Value::Unit)
}

/// Check if the atomic body skipped, and clear the flag. Returns 1 on skip.
#[unsafe(no_mangle)]
pub extern "C" fn knot_stm_check_skip_and_clear() -> i32 {
    STM_SKIP.with(|s| {
        let v = s.get();
        s.set(false);
        if v { 1 } else { 0 }
    })
}

/// Clear per-table read/write tracking to prepare for a new atomic body iteration.
/// Return value is unused but kept for ABI compatibility with codegen.
#[unsafe(no_mangle)]
pub extern "C" fn knot_stm_snapshot() -> i64 {
    STM_READ_VERSIONS.with(|rv| rv.borrow_mut().clear());
    STM_WRITTEN_TABLES.with(|wt| wt.borrow_mut().clear());
    0
}

/// Wait until a table in the read set has been modified since we read it.
/// Registers a per-thread wake slot so only writes to watched tables cause a wakeup.
/// Avoids cloning the read-version map on fast paths (empty / already changed).
/// The `_snapshot` parameter is unused but kept for ABI compatibility.
///
/// Releases the write lock (if held) before blocking so that other threads
/// can perform writes during the wait. Re-acquires after waking.
#[unsafe(no_mangle)]
pub extern "C" fn knot_stm_wait(_snapshot: i64) {
    // Release the write lock before any potential blocking so nested atomic
    // retries don't prevent other threads from writing.
    let saved_lock_depth = WRITE_LOCK_DEPTH.with(|d| {
        let depth = d.get();
        if depth > 0 {
            d.set(0);
            WRITE_LOCKED.store(false, Ordering::Release);
        }
        depth
    });

    let is_empty = STM_READ_VERSIONS.with(|rv| rv.borrow().is_empty());
    if is_empty {
        // No read set means there's nothing to wait *on* — the body retried
        // without observing any relation, so no notification will ever fire.
        // Yield briefly so other threads can run, then return; the previous
        // 1-second sleep stalled retry loops for an entire second per
        // iteration in this corner case.
        std::thread::sleep(Duration::from_millis(50));
        // Re-acquire write lock if we held it
        if saved_lock_depth > 0 {
            while WRITE_LOCKED
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                std::thread::yield_now();
            }
            WRITE_LOCK_DEPTH.with(|d| d.set(saved_lock_depth));
        }
        return;
    }

    // Fast path: check if already changed without cloning the map
    let already_changed = STM_READ_VERSIONS.with(|rv| {
        let rv = rv.borrow();
        let versions = TABLE_VERSIONS.read().unwrap();
        rv.iter().any(|(table, ver)| {
            versions
                .get(table)
                .map(|v| v.load(Ordering::Acquire))
                .unwrap_or(0)
                > *ver
        })
    });
    if already_changed {
        // Yield before returning so other threads (e.g. pollHeartbeat)
        // get a chance to acquire the write lock.  Without this, the
        // atomic retry loop re-acquires the lock immediately and can
        // spin-starve writers that would satisfy the retry condition,
        // causing unbounded memory growth from repeated SQL reads.
        std::thread::yield_now();
        if saved_lock_depth > 0 {
            while WRITE_LOCKED
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                std::thread::yield_now();
            }
            WRITE_LOCK_DEPTH.with(|d| d.set(saved_lock_depth));
        }
        return;
    }

    // Need to register — collect into a Vec (cheaper than HashMap clone)
    let read_versions: Vec<(String, u64)> = STM_READ_VERSIONS.with(|rv| {
        rv.borrow().iter().map(|(k, v)| (k.clone(), *v)).collect()
    });

    // Register a wake slot with each watched table
    let slot = Arc::new(WakeSlot::new());
    {
        let mut watchers = TABLE_WATCHERS.lock().unwrap();
        for (table, _) in &read_versions {
            watchers
                .entry(table.clone())
                .or_default()
                .push(Arc::downgrade(&slot));
        }
    }

    // Re-check after registration to prevent lost wakeups
    let changed_after_register = {
        let versions = TABLE_VERSIONS.read().unwrap();
        read_versions.iter().any(|(table, ver)| {
            versions
                .get(table)
                .map(|v| v.load(Ordering::Acquire))
                .unwrap_or(0)
                > *ver
        })
    };

    if !changed_after_register {
        slot.wait(Duration::from_secs(30));
        // slot drops → Weak refs become invalid, cleaned up lazily in notify
    } else {
        // Changed between registration and re-check — yield to prevent
        // spin-starvation (same rationale as the already_changed path).
        std::thread::yield_now();
    }

    // Re-acquire write lock if we held it before waiting
    if saved_lock_depth > 0 {
        while WRITE_LOCKED
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::thread::yield_now();
        }
        WRITE_LOCK_DEPTH.with(|d| d.set(saved_lock_depth));
    }
}

// ── IO wrappers for effectful functions ──────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_println_io(v: *mut Value) -> *mut Value {
    let env = v;
    extern "C" fn thunk(db: *mut c_void, env: *mut Value) -> *mut Value {
        let _ = db;
        knot_println(env)
    }
    alloc(Value::IO(thunk as *const u8, env))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_print_io(v: *mut Value) -> *mut Value {
    let env = v;
    extern "C" fn thunk(db: *mut c_void, env: *mut Value) -> *mut Value {
        let _ = db;
        knot_print(env)
    }
    alloc(Value::IO(thunk as *const u8, env))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_log_info_io(v: *mut Value) -> *mut Value {
    extern "C" fn thunk(db: *mut c_void, env: *mut Value) -> *mut Value {
        let _ = db;
        knot_log_info(env)
    }
    alloc(Value::IO(thunk as *const u8, v))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_log_warn_io(v: *mut Value) -> *mut Value {
    extern "C" fn thunk(db: *mut c_void, env: *mut Value) -> *mut Value {
        let _ = db;
        knot_log_warn(env)
    }
    alloc(Value::IO(thunk as *const u8, v))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_log_error_io(v: *mut Value) -> *mut Value {
    extern "C" fn thunk(db: *mut c_void, env: *mut Value) -> *mut Value {
        let _ = db;
        knot_log_error(env)
    }
    alloc(Value::IO(thunk as *const u8, v))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_log_debug_io(v: *mut Value) -> *mut Value {
    extern "C" fn thunk(db: *mut c_void, env: *mut Value) -> *mut Value {
        let _ = db;
        knot_log_debug(env)
    }
    alloc(Value::IO(thunk as *const u8, v))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_read_line_io() -> *mut Value {
    extern "C" fn thunk(db: *mut c_void, _env: *mut Value) -> *mut Value {
        let _ = db;
        knot_read_line()
    }
    alloc(Value::IO(thunk as *const u8, std::ptr::null_mut()))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_read_file_io(path: *mut Value) -> *mut Value {
    let env = path;
    extern "C" fn thunk(db: *mut c_void, env: *mut Value) -> *mut Value {
        let _ = db;
        knot_fs_read_file(env)
    }
    alloc(Value::IO(thunk as *const u8, env))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_write_file_io(path: *mut Value, contents: *mut Value) -> *mut Value {
    let env = alloc(Value::Record(vec![
        RecordField { name: "_c".into(), value: contents },
        RecordField { name: "_p".into(), value: path },
    ]));
    extern "C" fn thunk(db: *mut c_void, env: *mut Value) -> *mut Value {
        let _ = db;
        let p = knot_record_field(env, "_p\0".as_ptr(), 2);
        let c = knot_record_field(env, "_c\0".as_ptr(), 2);
        knot_fs_write_file(p, c)
    }
    alloc(Value::IO(thunk as *const u8, env))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_append_file_io(path: *mut Value, contents: *mut Value) -> *mut Value {
    let env = alloc(Value::Record(vec![
        RecordField { name: "_c".into(), value: contents },
        RecordField { name: "_p".into(), value: path },
    ]));
    extern "C" fn thunk(db: *mut c_void, env: *mut Value) -> *mut Value {
        let _ = db;
        let p = knot_record_field(env, "_p\0".as_ptr(), 2);
        let c = knot_record_field(env, "_c\0".as_ptr(), 2);
        knot_fs_append_file(p, c)
    }
    alloc(Value::IO(thunk as *const u8, env))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_file_exists_io(path: *mut Value) -> *mut Value {
    let env = path;
    extern "C" fn thunk(db: *mut c_void, env: *mut Value) -> *mut Value {
        let _ = db;
        knot_fs_file_exists(env)
    }
    alloc(Value::IO(thunk as *const u8, env))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_remove_file_io(path: *mut Value) -> *mut Value {
    let env = path;
    extern "C" fn thunk(db: *mut c_void, env: *mut Value) -> *mut Value {
        let _ = db;
        knot_fs_remove_file(env)
    }
    alloc(Value::IO(thunk as *const u8, env))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_list_dir_io(path: *mut Value) -> *mut Value {
    let env = path;
    extern "C" fn thunk(db: *mut c_void, env: *mut Value) -> *mut Value {
        let _ = db;
        knot_fs_list_dir(env)
    }
    alloc(Value::IO(thunk as *const u8, env))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_now_io() -> *mut Value {
    extern "C" fn thunk(db: *mut c_void, _env: *mut Value) -> *mut Value {
        let _ = db;
        knot_now()
    }
    alloc(Value::IO(thunk as *const u8, std::ptr::null_mut()))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_sleep_io(ms_val: *mut Value) -> *mut Value {
    let env = ms_val;
    extern "C" fn thunk(db: *mut c_void, env: *mut Value) -> *mut Value {
        let _ = db;
        knot_sleep(env)
    }
    alloc(Value::IO(thunk as *const u8, env))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_random_int_io(bound: *mut Value) -> *mut Value {
    let env = bound;
    extern "C" fn thunk(db: *mut c_void, env: *mut Value) -> *mut Value {
        let _ = db;
        knot_random_int(env)
    }
    alloc(Value::IO(thunk as *const u8, env))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_random_float_io() -> *mut Value {
    extern "C" fn thunk(db: *mut c_void, _env: *mut Value) -> *mut Value {
        let _ = db;
        knot_random_float()
    }
    alloc(Value::IO(thunk as *const u8, std::ptr::null_mut()))
}

// ── Standard library: relation operations ─────────────────────────


/// filter(pred, rel) — keep rows where pred returns true
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_filter(
    db: *mut c_void,
    pred: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        Value::Unit => return alloc(Value::Relation(Vec::new())),
        _ => panic!(
            "knot runtime: filter expected Relation, got {}",
            type_name(rel)
        ),
    };
    let mut result: Vec<*mut Value> = Vec::new();
    for &row in rows {
        let v = knot_value_call(db, pred, row);
        match unsafe { as_ref(v) } {
            Value::Bool(true) => result.push(row),
            Value::Bool(false) => {}
            _ => panic!("knot runtime: filter predicate must return Bool"),
        }
    }
    alloc(Value::Relation(result))
}

/// match(ctor, rel) — filter relation to rows matching a constructor tag, extract payloads
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_match(
    ctor: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let tag = match unsafe { as_ref(ctor) } {
        Value::Constructor(t, _) => &**t,
        _ => panic!(
            "knot runtime: match expected Constructor, got {}",
            type_name(ctor)
        ),
    };
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: match expected Relation, got {}",
            type_name(rel)
        ),
    };
    let mut result: Vec<*mut Value> = Vec::new();
    for &row in rows {
        match unsafe { as_ref(row) } {
            Value::Constructor(t, payload) if &**t == tag => {
                result.push(*payload);
            }
            _ => {}
        }
    }
    alloc(Value::Relation(result))
}

/// map(f, rel) — apply f to each row, collect results (deduplicating)
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_map(
    db: *mut c_void,
    func: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        Value::Unit => return alloc(Value::Relation(Vec::new())),
        _ => panic!(
            "knot runtime: map expected Relation, got {}",
            type_name(rel)
        ),
    };

    if rows.is_empty() {
        return alloc(Value::Relation(Vec::new()));
    }

    // Apply function to all rows
    let mapped: Vec<*mut Value> = rows.iter().map(|&r| knot_value_call(db, func, r)).collect();

    // Dedup via SQLite
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    if let Some(result) = sql_dedup(&db_ref.conn, &mapped) {
        return alloc(Value::Relation(result));
    }

    // Fallback: in-memory dedup
    alloc(Value::Relation(in_memory_dedup(mapped)))
}

/// ap(fs, xs) — applicative apply: apply each function in fs to each value in xs
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_ap(
    db: *mut c_void,
    fs: *mut Value,
    xs: *mut Value,
) -> *mut Value {
    let funcs = match unsafe { as_ref(fs) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: ap expected Relation of functions, got {}",
            type_name(fs)
        ),
    };
    let vals = match unsafe { as_ref(xs) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: ap expected Relation of values, got {}",
            type_name(xs)
        ),
    };

    if funcs.is_empty() || vals.is_empty() {
        return alloc(Value::Relation(Vec::new()));
    }

    // Apply all function-value pairs
    let mut all: Vec<*mut Value> = Vec::with_capacity(funcs.len() * vals.len());
    for &f in funcs {
        for &x in vals {
            all.push(knot_value_call(db, f, x));
        }
    }

    // Dedup via SQLite
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    if let Some(result) = sql_dedup(&db_ref.conn, &all) {
        return alloc(Value::Relation(result));
    }

    // Fallback: in-memory dedup
    alloc(Value::Relation(in_memory_dedup(all)))
}

/// fold(f, init, rel) — left fold over a relation
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_fold(
    db: *mut c_void,
    func: *mut Value,
    init: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        Value::Unit => return init,
        _ => panic!(
            "knot runtime: fold expected Relation, got {}",
            type_name(rel)
        ),
    };
    let mut acc = init;
    for &row in rows {
        // func is curried: func(acc) returns a function, then that function(row) returns new acc
        let partial = knot_value_call(db, func, acc);
        acc = knot_value_call(db, partial, row);
    }
    acc
}

/// Streaming fold over a source relation table.
/// Reads rows one-at-a-time from SQLite and applies the curried fold
/// function, never materialising the relation in memory.
/// Caller must guarantee the schema is a flat record schema (no nested
/// relations, no ADT) — those are excluded by the compiler so they fall
/// back to `knot_relation_fold`.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_fold(
    db: *mut c_void,
    func: *mut Value,
    init: *mut Value,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };

    if db_ref.atomic_depth.get() > 0 {
        stm_track_read(name);
    }

    let table_name = format!("_knot_{}", name);
    let table = quote_ident(&table_name);
    let rec = parse_record_schema(schema);

    let cols: Vec<String> = rec.columns.iter().map(|c| quote_ident(&c.name)).collect();
    let sql = format!(
        "SELECT {} FROM {}",
        if cols.is_empty() { "1".to_string() } else { cols.join(", ") },
        table
    );
    debug_sql(&sql);

    let mut stmt = db_ref
        .conn
        .prepare_cached(&sql)
        .unwrap_or_else(|e| panic!("knot runtime: source_fold error: {}\n  SQL: {}", e, sql));
    let mut result_rows = stmt
        .query([])
        .unwrap_or_else(|e| panic!("knot runtime: source_fold exec error: {}\n  SQL: {}", e, sql));

    let mut acc = init;
    while let Some(row) = result_rows
        .next()
        .unwrap_or_else(|e| panic!("knot runtime: source_fold row fetch error: {}", e))
    {
        let record = knot_record_empty(rec.columns.len());
        for (i, col) in rec.columns.iter().enumerate() {
            let val = read_sql_column(row, i, col.ty);
            let cname = col.name.as_bytes();
            knot_record_set_field(record, cname.as_ptr(), cname.len(), val);
        }
        let partial = knot_value_call(db, func, acc);
        acc = knot_value_call(db, partial, record);
    }
    acc
}

/// Streaming fold over an arbitrary record-producing SELECT.
/// Used by the compiler when fold is applied to `filter f *src` or to a
/// `do { ... }` block that maps to a flat SQL plan.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_query_fold(
    db: *mut c_void,
    func: *mut Value,
    init: *mut Value,
    sql_ptr: *const u8,
    sql_len: usize,
    result_schema_ptr: *const u8,
    result_schema_len: usize,
    params: *mut Value,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let sql = unsafe { str_from_raw(sql_ptr, sql_len) };
    let result_schema = unsafe { str_from_raw(result_schema_ptr, result_schema_len) };

    let param_values = match unsafe { as_ref(params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: source_query_fold params must be a Relation, got {}",
            type_name(params)
        ),
    };
    let sql_params: Vec<rusqlite::types::Value> =
        param_values.iter().map(|v| value_to_sql_param(*v)).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = sql_params
        .iter()
        .map(|p| p as &dyn rusqlite::types::ToSql)
        .collect();

    debug_sql_params(sql, &sql_params);

    let rec = parse_record_schema(result_schema);

    let mut stmt = db_ref
        .conn
        .prepare_cached(sql)
        .unwrap_or_else(|e| panic!("knot runtime: source_query_fold error: {}\n  SQL: {}", e, sql));
    let mut result_rows = stmt
        .query(param_refs.as_slice())
        .unwrap_or_else(|e| panic!("knot runtime: source_query_fold exec error: {}\n  SQL: {}", e, sql));

    let mut acc = init;
    while let Some(row) = result_rows
        .next()
        .unwrap_or_else(|e| panic!("knot runtime: source_query_fold row fetch error: {}", e))
    {
        let record = knot_record_empty(rec.columns.len());
        for (i, col) in rec.columns.iter().enumerate() {
            let val = read_sql_column(row, i, col.ty);
            let cname = col.name.as_bytes();
            knot_record_set_field(record, cname.as_ptr(), cname.len(), val);
        }
        let partial = knot_value_call(db, func, acc);
        acc = knot_value_call(db, partial, record);
    }
    acc
}

/// traverse(f, rel) — apply an applicative function to each element of a relation
/// and sequence the results. Determines the applicative type (IO, Maybe, Result, [])
/// by inspecting the first result.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_traverse(
    db: *mut c_void,
    func: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows.clone(),
        Value::Unit => Vec::new(),
        _ => panic!(
            "knot runtime: traverse expected Relation, got {}",
            type_name(rel)
        ),
    };

    if rows.is_empty() {
        // Cannot determine applicative from empty input; default to Relation applicative: [[]]
        return alloc(Value::Relation(vec![alloc(Value::Relation(vec![]))]));
    }

    // Apply func to each element
    let mut mapped: Vec<*mut Value> = Vec::with_capacity(rows.len());
    for &row in &rows {
        mapped.push(knot_value_call(db, func, row));
    }

    // Determine applicative type from first result and sequence accordingly
    match unsafe { as_ref(mapped[0]) } {
        Value::IO(..) => traverse_sequence_io(db, mapped),
        Value::Relation(..) => traverse_sequence_relation(mapped),
        Value::Constructor(tag, ..) => match &**tag {
            "Just" | "Nothing" => traverse_sequence_maybe(mapped),
            "Ok" | "Err" => traverse_sequence_result(mapped),
            _ => panic!(
                "knot runtime: traverse unsupported applicative (constructor: {})",
                tag
            ),
        },
        _ => panic!(
            "knot runtime: traverse unsupported applicative ({})",
            type_name(mapped[0])
        ),
    }
}

/// Sequence [IO a] into IO [a] — creates a single IO thunk that runs each action in order.
fn traverse_sequence_io(db: *mut c_void, ios: Vec<*mut Value>) -> *mut Value {
    let _ = db;
    let actions_rel = alloc(Value::Relation(ios));

    extern "C" fn run_sequence(db: *mut c_void, env: *mut Value) -> *mut Value {
        let actions = match unsafe { as_ref(env) } {
            Value::Relation(rows) => rows,
            _ => unreachable!(),
        };
        let mut results = Vec::with_capacity(actions.len());
        for &action in actions {
            results.push(knot_io_run(db, action));
        }
        alloc(Value::Relation(results))
    }

    alloc(Value::IO(run_sequence as *const u8, actions_rel))
}

/// Sequence [Maybe a] into Maybe [a] — Nothing if any element is Nothing.
fn traverse_sequence_maybe(maybes: Vec<*mut Value>) -> *mut Value {
    let mut values = Vec::with_capacity(maybes.len());
    for &m in &maybes {
        match unsafe { as_ref(m) } {
            Value::Constructor(tag, _) if &**tag == "Nothing" => {
                return alloc(Value::Constructor("Nothing".into(), alloc(Value::Unit)));
            }
            Value::Constructor(tag, inner) if &**tag == "Just" => {
                values.push(extract_value_field(*inner));
            }
            _ => panic!("knot runtime: traverse expected Maybe, got {}", type_name(m)),
        }
    }
    wrap_ok_or_just("Just", values)
}

/// Sequence [Result e a] into Result e [a] — first Err short-circuits.
fn traverse_sequence_result(results: Vec<*mut Value>) -> *mut Value {
    let mut values = Vec::with_capacity(results.len());
    for &r in &results {
        match unsafe { as_ref(r) } {
            Value::Constructor(tag, _) if &**tag == "Err" => return r,
            Value::Constructor(tag, inner) if &**tag == "Ok" => {
                values.push(extract_value_field(*inner));
            }
            _ => panic!("knot runtime: traverse expected Result, got {}", type_name(r)),
        }
    }
    wrap_ok_or_just("Ok", values)
}

/// Sequence [[a]] into [[a]] — cartesian product of all sub-relations.
fn traverse_sequence_relation(rels: Vec<*mut Value>) -> *mut Value {
    let mut current: Vec<Vec<*mut Value>> = vec![vec![]];
    for &rel in &rels {
        let rows = match unsafe { as_ref(rel) } {
            Value::Relation(rows) => rows,
            _ => panic!("knot runtime: traverse expected Relation, got {}", type_name(rel)),
        };
        let mut next = Vec::new();
        for prefix in &current {
            for &row in rows {
                let mut extended = prefix.clone();
                extended.push(row);
                next.push(extended);
            }
        }
        current = next;
    }
    alloc(Value::Relation(
        current
            .into_iter()
            .map(|row| alloc(Value::Relation(row)))
            .collect(),
    ))
}

/// Extract the `value` field from a record (used for Just/Ok payloads).
fn extract_value_field(payload: *mut Value) -> *mut Value {
    match unsafe { as_ref(payload) } {
        Value::Record(fields) => {
            for f in fields {
                if &*f.name == "value" {
                    return f.value;
                }
            }
            panic!("knot runtime: constructor payload missing 'value' field");
        }
        _ => panic!("knot runtime: constructor payload not a record"),
    }
}

/// Wrap a list of values in Constructor { value: [values] } (for Just/Ok).
fn wrap_ok_or_just(tag: &str, values: Vec<*mut Value>) -> *mut Value {
    let rel = alloc(Value::Relation(values));
    let rec = alloc(Value::Record(vec![RecordField {
        name: "value".into(),
        value: rel,
    }]));
    alloc(Value::Constructor(tag.into(), rec))
}

/// single(rel) — extract the single element from a one-element relation.
/// Returns `Just {value: x}` for a singleton, `Nothing {}` otherwise.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_single(rel: *mut Value) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: single expected Relation, got {}",
            type_name(rel)
        ),
    };
    if rows.len() == 1 {
        let record = alloc(Value::Record(vec![RecordField { name: "value".into(), value: rows[0] }]));
        alloc(Value::Constructor("Just".into(), record))
    } else {
        alloc(Value::Constructor("Nothing".into(), alloc(Value::Unit)))
    }
}

// ── Standard library: derived relation operations ────────────────

/// diff(a, b) — rows in a but not in b
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_diff(
    db: *mut c_void,
    a: *mut Value,
    b: *mut Value,
) -> *mut Value {
    let rows_a = match unsafe { as_ref(a) } {
        Value::Relation(rows) => rows,
        _ => panic!("knot runtime: diff expected Relation, got {}", type_name(a)),
    };
    let rows_b = match unsafe { as_ref(b) } {
        Value::Relation(rows) => rows,
        _ => panic!("knot runtime: diff expected Relation, got {}", type_name(b)),
    };

    if rows_a.is_empty() { return a; }
    if rows_b.is_empty() {
        // Dedup a for set semantics (SQL EXCEPT would dedup)
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        let mut buf = Vec::new();
        for &row in rows_a.iter() {
            buf.clear();
            value_to_hash_bytes(row, &mut buf);
            if seen.insert(buf.clone()) {
                result.push(row);
            }
        }
        return alloc(Value::Relation(result));
    }

    let db_ref = unsafe { &*(db as *mut KnotDb) };
    if let Some(result) = sql_set_op(&db_ref.conn, rows_a, rows_b, "EXCEPT") {
        return alloc(Value::Relation(result));
    }

    // Fallback: in-memory — hash-based O(n), with dedup for set semantics
    let set_b: HashSet<Vec<u8>> = rows_b.iter().map(|r| {
        let mut buf = Vec::new();
        value_to_hash_bytes(*r, &mut buf);
        buf
    }).collect();
    let mut seen = HashSet::new();
    let mut buf = Vec::new();
    let result: Vec<*mut Value> = rows_a
        .iter()
        .copied()
        .filter(|r| {
            buf.clear();
            value_to_hash_bytes(*r, &mut buf);
            !set_b.contains(buf.as_slice()) && seen.insert(buf.clone())
        })
        .collect();
    alloc(Value::Relation(result))
}

/// inter(a, b) — rows in both a and b
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_inter(
    db: *mut c_void,
    a: *mut Value,
    b: *mut Value,
) -> *mut Value {
    let rows_a = match unsafe { as_ref(a) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: inter expected Relation, got {}",
            type_name(a)
        ),
    };
    let rows_b = match unsafe { as_ref(b) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: inter expected Relation, got {}",
            type_name(b)
        ),
    };

    if rows_a.is_empty() || rows_b.is_empty() {
        return alloc(Value::Relation(Vec::new()));
    }

    let db_ref = unsafe { &*(db as *mut KnotDb) };
    if let Some(result) = sql_set_op(&db_ref.conn, rows_a, rows_b, "INTERSECT") {
        return alloc(Value::Relation(result));
    }

    // Fallback: in-memory — hash-based O(n), with dedup for set semantics
    let set_b: HashSet<Vec<u8>> = rows_b.iter().map(|r| {
        let mut buf = Vec::new();
        value_to_hash_bytes(*r, &mut buf);
        buf
    }).collect();
    let mut seen = HashSet::new();
    let mut buf = Vec::new();
    let result: Vec<*mut Value> = rows_a
        .iter()
        .copied()
        .filter(|r| {
            buf.clear();
            value_to_hash_bytes(*r, &mut buf);
            set_b.contains(buf.as_slice()) && seen.insert(buf.clone())
        })
        .collect();
    alloc(Value::Relation(result))
}

/// sum(f, rel) — sum of f(x) for each x in rel
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_sum(
    db: *mut c_void,
    f: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: sum expected Relation, got {}",
            type_name(rel)
        ),
    };
    let mut acc = alloc_int(0);
    for &row in rows {
        let val = knot_value_call(db, f, row);
        acc = knot_value_add(acc, val);
    }
    acc
}

/// avg(f, rel) — average of f(x) for each x in rel (returns Float)
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_avg(
    db: *mut c_void,
    f: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: avg expected Relation, got {}",
            type_name(rel)
        ),
    };
    if rows.is_empty() {
        return alloc_float(0.0);
    }
    // Kahan compensated summation: keeps the running sum accurate even when
    // the magnitudes of `total` and individual addends differ by many orders
    // of magnitude (e.g., averaging a million large unit-int values into a
    // single Float). Without compensation, low-order bits of small values
    // are dropped once `total` grows large, so `avg` would skew toward the
    // first few inputs.
    let mut total = 0.0f64;
    let mut comp = 0.0f64; // running compensation for lost low bits
    let count = rows.len();
    for &row in rows {
        let val = knot_value_call(db, f, row);
        let x = match unsafe { as_ref(val) } {
            Value::Int(n) => *n as f64,
            Value::Float(n) => *n,
            _ => panic!(
                "knot runtime: avg projection must return Int or Float, got {}",
                type_name(val)
            ),
        };
        let y = x - comp;
        let t = total + y;
        comp = (t - total) - y;
        total = t;
    }
    alloc_float(total / count as f64)
}

/// countWhere(pred, rel) — count rows where pred(row) is True.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_count_where(
    db: *mut c_void,
    pred: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: countWhere expected Relation, got {}",
            type_name(rel)
        ),
    };
    let mut n: i64 = 0;
    for &row in rows {
        let result = knot_value_call(db, pred, row);
        match unsafe { as_ref(result) } {
            Value::Bool(true) => n += 1,
            Value::Bool(false) => {}
            _ => panic!(
                "knot runtime: countWhere predicate must return Bool, got {}",
                type_name(result)
            ),
        }
    }
    alloc_int(n)
}

/// min(f, rel) — minimum of f(x) for each x in rel.
/// Panics on empty relation.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_min(
    db: *mut c_void,
    f: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: min expected Relation, got {}",
            type_name(rel)
        ),
    };
    if rows.is_empty() {
        panic!("knot runtime: min on empty relation");
    }
    let mut best = knot_value_call(db, f, rows[0]);
    for &row in &rows[1..] {
        let val = knot_value_call(db, f, row);
        if compare_values(val, best) == std::cmp::Ordering::Less {
            best = val;
        }
    }
    best
}

/// max(f, rel) — maximum of f(x) for each x in rel.
/// Panics on empty relation.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_max(
    db: *mut c_void,
    f: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: max expected Relation, got {}",
            type_name(rel)
        ),
    };
    if rows.is_empty() {
        panic!("knot runtime: max on empty relation");
    }
    let mut best = knot_value_call(db, f, rows[0]);
    for &row in &rows[1..] {
        let val = knot_value_call(db, f, row);
        if compare_values(val, best) == std::cmp::Ordering::Greater {
            best = val;
        }
    }
    best
}

// ── Standard library: text operations ─────────────────────────────

/// toUpper(text) — convert text to uppercase
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_to_upper(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => alloc(Value::Text(Arc::from(s.to_uppercase()))),
        _ => panic!("knot runtime: toUpper expected Text, got {}", type_name(v)),
    }
}

/// toLower(text) — convert text to lowercase
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_to_lower(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => alloc(Value::Text(Arc::from(s.to_lowercase()))),
        _ => panic!("knot runtime: toLower expected Text, got {}", type_name(v)),
    }
}

/// take(n, text) — first n characters
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_take(n: *mut Value, text: *mut Value) -> *mut Value {
    let n_idx = int_as_usize(unsafe { as_ref(n) })
        .unwrap_or_else(|| panic!("knot runtime: take expected Int as first arg, got {}", type_name(n)));
    match unsafe { as_ref(text) } {
        Value::Text(s) => {
            let result: String = s.chars().take(n_idx).collect();
            alloc(Value::Text(Arc::from(result)))
        }
        _ => panic!("knot runtime: take expected Text as second arg, got {}", type_name(text)),
    }
}

/// drop(n, text) — skip first n characters
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_drop(n: *mut Value, text: *mut Value) -> *mut Value {
    let n_idx = int_as_usize(unsafe { as_ref(n) })
        .unwrap_or_else(|| panic!("knot runtime: drop expected Int as first arg, got {}", type_name(n)));
    match unsafe { as_ref(text) } {
        Value::Text(s) => {
            let result: String = s.chars().skip(n_idx).collect();
            alloc(Value::Text(Arc::from(result)))
        }
        _ => panic!("knot runtime: drop expected Text as second arg, got {}", type_name(text)),
    }
}

/// length(text) — character count of a text value
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_length(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => knot_value_int(s.chars().count() as i64),
        _ => panic!("knot runtime: length expected Text, got {}", type_name(v)),
    }
}

/// trim(text) — strip leading and trailing whitespace
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_trim(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => alloc(Value::Text(Arc::from(s.trim()))),
        _ => panic!("knot runtime: trim expected Text, got {}", type_name(v)),
    }
}

/// contains(needle, haystack) — check if text contains a substring
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_contains(needle: *mut Value, haystack: *mut Value) -> *mut Value {
    let needle: &str = match unsafe { as_ref(needle) } {
        Value::Text(s) => &**s,
        _ => panic!("knot runtime: contains expected Text as first arg"),
    };
    match unsafe { as_ref(haystack) } {
        Value::Text(s) => alloc_bool(s.contains(needle)),
        _ => panic!("knot runtime: contains expected Text as second arg"),
    }
}

/// elem(needle, haystack) — check if a list contains a value (by structural equality)
#[unsafe(no_mangle)]
pub extern "C" fn knot_list_elem(needle: *mut Value, haystack: *mut Value) -> *mut Value {
    match unsafe { as_ref(haystack) } {
        Value::Relation(rows) => {
            for row in rows.iter() {
                if values_equal(needle, *row) {
                    return alloc_bool(true);
                }
            }
            alloc_bool(false)
        }
        _ => panic!(
            "knot runtime: elem expected list as second arg, got {}",
            type_name(haystack)
        ),
    }
}

/// reverse(text) — reverse a text value
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_reverse(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => {
            let result: String = s.chars().rev().collect();
            alloc(Value::Text(Arc::from(result)))
        }
        _ => panic!("knot runtime: reverse expected Text, got {}", type_name(v)),
    }
}

/// chars(text) — convert text to a relation of single characters
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_chars(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => {
            let mut seen = HashSet::new();
            let mut rows = Vec::new();
            for c in s.chars() {
                let cs = c.to_string();
                if seen.insert(cs.clone()) {
                    rows.push(alloc(Value::Text(Arc::from(cs))));
                }
            }
            alloc(Value::Relation(rows))
        }
        _ => panic!("knot runtime: chars expected Text, got {}", type_name(v)),
    }
}

// ── Standard library: bytes operations ─────────────────────────

/// bytesLength(bytes) — byte count
#[unsafe(no_mangle)]
pub extern "C" fn knot_bytes_length(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Bytes(b) => knot_value_int(b.len() as i64),
        _ => panic!("knot runtime: bytesLength expected Bytes, got {}", type_name(v)),
    }
}

/// bytesConcat(a, b) — concatenate two byte strings
#[unsafe(no_mangle)]
pub extern "C" fn knot_bytes_concat(a: *mut Value, b: *mut Value) -> *mut Value {
    let a_bytes = match unsafe { as_ref(a) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: bytesConcat expected Bytes as first arg, got {}", type_name(a)),
    };
    let b_bytes = match unsafe { as_ref(b) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: bytesConcat expected Bytes as second arg, got {}", type_name(b)),
    };
    let mut result = Vec::with_capacity(a_bytes.len() + b_bytes.len());
    result.extend_from_slice(a_bytes);
    result.extend_from_slice(b_bytes);
    alloc(Value::Bytes(Arc::from(result)))
}

/// bytesSlice(start, len, bytes) — extract a sub-range of bytes
#[unsafe(no_mangle)]
pub extern "C" fn knot_bytes_slice(
    _db: *mut c_void,
    start: *mut Value,
    len: *mut Value,
    bytes: *mut Value,
) -> *mut Value {
    let start = int_as_usize(unsafe { as_ref(start) })
        .unwrap_or_else(|| panic!("knot runtime: bytesSlice expected Int as first arg"));
    let len = int_as_usize(unsafe { as_ref(len) })
        .unwrap_or_else(|| panic!("knot runtime: bytesSlice expected Int as second arg"));
    match unsafe { as_ref(bytes) } {
        Value::Bytes(b) => {
            let end = start.saturating_add(len).min(b.len());
            let s = start.min(b.len());
            alloc(Value::Bytes(Arc::from(&b[s..end])))
        }
        _ => panic!("knot runtime: bytesSlice expected Bytes as third arg, got {}", type_name(bytes)),
    }
}

/// textToBytes(text) — encode text as UTF-8 bytes
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_to_bytes(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => alloc(Value::Bytes(Arc::from(s.as_bytes()))),
        _ => panic!("knot runtime: textToBytes expected Text, got {}", type_name(v)),
    }
}

/// Build `Just {value: payload}` for the built-in `Maybe a` ADT.
fn make_just(payload: *mut Value) -> *mut Value {
    let tag = intern_str("Just");
    let inner = alloc(Value::Record(vec![RecordField {
        name: intern_str("value"),
        value: payload,
    }]));
    alloc(Value::Constructor(tag, inner))
}

/// Build `Nothing {}` for the built-in `Maybe a` ADT.
fn make_nothing() -> *mut Value {
    let tag = intern_str("Nothing");
    let inner = alloc(Value::Record(Vec::new()));
    alloc(Value::Constructor(tag, inner))
}

/// bytesToText(bytes) — decode UTF-8 bytes to text. Returns `Maybe Text`:
/// `Just {value: text}` on success, `Nothing {}` if the bytes aren't valid
/// UTF-8. Wrong-type input (caller bug — should be unreachable from
/// well-typed Knot code) still panics.
#[unsafe(no_mangle)]
pub extern "C" fn knot_bytes_to_text(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Bytes(b) => match std::str::from_utf8(b) {
            Ok(s) => make_just(alloc(Value::Text(Arc::from(s)))),
            Err(_) => make_nothing(),
        },
        _ => panic!("knot runtime: bytesToText expected Bytes, got {}", type_name(v)),
    }
}

/// bytesToHex(bytes) — encode bytes as hex string
#[unsafe(no_mangle)]
pub extern "C" fn knot_bytes_to_hex(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Bytes(b) => {
            let mut hex = String::with_capacity(b.len() * 2);
            for byte in b.iter() {
                use std::fmt::Write;
                let _ = write!(hex, "{:02x}", byte);
            }
            alloc(Value::Text(Arc::from(hex)))
        }
        _ => panic!("knot runtime: bytesToHex expected Bytes, got {}", type_name(v)),
    }
}

/// bytesFromHex(text) — decode hex string to bytes. Returns `Maybe Bytes`:
/// `Just {value: bytes}` on success, `Nothing {}` if the input is non-ASCII,
/// odd-length, or contains non-hex characters. Wrong-type input still panics.
#[unsafe(no_mangle)]
pub extern "C" fn knot_bytes_from_hex(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => {
            let s = s.trim();
            if !s.is_ascii() || s.len() % 2 != 0 {
                return make_nothing();
            }
            let mut bytes: Vec<u8> = Vec::with_capacity(s.len() / 2);
            for i in (0..s.len()).step_by(2) {
                match u8::from_str_radix(&s[i..i + 2], 16) {
                    Ok(b) => bytes.push(b),
                    Err(_) => return make_nothing(),
                }
            }
            make_just(alloc(Value::Bytes(Arc::from(bytes))))
        }
        _ => panic!("knot runtime: bytesFromHex expected Text, got {}", type_name(v)),
    }
}

/// hash(value) — BLAKE3 digest of any value. Returns 32 bytes.
/// Bytes/Text hash their raw contents; structured values (records, ADTs,
/// relations) hash their canonical byte serialization (the same one used
/// for set dedup), so two values that are `==` produce the same digest.
#[unsafe(no_mangle)]
pub extern "C" fn knot_hash(v: *mut Value) -> *mut Value {
    let digest = match unsafe { as_ref(v) } {
        Value::Bytes(b) => blake3::hash(b.as_ref()),
        Value::Text(s) => blake3::hash(s.as_bytes()),
        _ => {
            let mut buf = Vec::new();
            value_to_hash_bytes(v, &mut buf);
            blake3::hash(&buf)
        }
    };
    alloc(Value::Bytes(Arc::from(digest.as_bytes().to_vec())))
}

/// bytesGet(index, bytes) — get byte at index as Int (0-255)
#[unsafe(no_mangle)]
pub extern "C" fn knot_bytes_get(index: *mut Value, bytes: *mut Value) -> *mut Value {
    let i = int_as_usize(unsafe { as_ref(index) })
        .unwrap_or_else(|| panic!("knot runtime: bytesGet expected Int as first arg"));
    match unsafe { as_ref(bytes) } {
        Value::Bytes(b) => {
            if i >= b.len() {
                panic!("knot runtime: bytesGet index {} out of bounds (length {})", i, b.len());
            }
            knot_value_int(b[i] as i64)
        }
        _ => panic!("knot runtime: bytesGet expected Bytes as second arg, got {}", type_name(bytes)),
    }
}

// ── Standard library: JSON operations ─────────────────────────

/// Register the compiled toJson trait dispatcher so the runtime can use
/// custom ToJSON impls for JSON encoding (e.g. HTTP responses).
#[unsafe(no_mangle)]
pub extern "C" fn knot_register_to_json(fn_ptr: *const u8) {
    TO_JSON_FN.store(fn_ptr as usize, Ordering::Release);
}

/// Encode a value to JSON, using the registered ToJSON dispatcher if available.
fn json_encode_value(db: *mut c_void, v: *mut Value) -> String {
    let fn_ptr = TO_JSON_FN.load(Ordering::Acquire);
    if fn_ptr != 0 {
        call_to_json_dispatcher(db, v, fn_ptr as *const u8)
    } else {
        value_to_json(v)
    }
}

/// toJson(value) — convert any Knot value to its JSON text representation
#[unsafe(no_mangle)]
pub extern "C" fn knot_json_encode(v: *mut Value) -> *mut Value {
    alloc(Value::Text(Arc::from(value_to_json(v))))
}

/// toJson fallback with dispatcher — encodes compound types by calling back
/// through the trait dispatcher for nested values, so custom ToJSON impls
/// are respected for elements inside records, relations, and constructors.
#[unsafe(no_mangle)]
pub extern "C" fn knot_json_encode_with(
    db: *mut c_void,
    v: *mut Value,
    to_json_fn: *const u8,
) -> *mut Value {
    alloc(Value::Text(Arc::from(value_to_json_with(db, v, to_json_fn))))
}

/// parseJson(text) — parse a JSON string into a Knot value
///
/// Mapping:
///   JSON object  → Record
///   JSON array   → Relation
///   JSON string  → Text
///   JSON number  → Int (if no decimal point) or Float
///   JSON boolean → Bool
///   JSON null    → Unit
#[unsafe(no_mangle)]
pub extern "C" fn knot_json_decode(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => {
            match serde_json::from_str::<serde_json::Value>(s) {
                Ok(json) => json_to_value(&json),
                Err(e) => panic!("knot runtime: parseJson failed: {}", e),
            }
        }
        _ => panic!("knot runtime: parseJson expected Text, got {}", type_name(v)),
    }
}

/// Convert a serde_json::Value into a Knot *mut Value.
fn json_to_value(json: &serde_json::Value) -> *mut Value {
    match json {
        serde_json::Value::Null => alloc(Value::Unit),
        serde_json::Value::Bool(b) => alloc_bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                alloc_int(i)
            } else if let Some(f) = n.as_f64() {
                alloc_float(f)
            } else {
                alloc_int(0)
            }
        }
        serde_json::Value::String(s) => alloc(Value::Text(Arc::from(s.as_str()))),
        serde_json::Value::Array(arr) => {
            let items: Vec<*mut Value> = arr.iter().map(json_to_value).collect();
            alloc(Value::Relation(items))
        }
        serde_json::Value::Object(obj) => {
            if obj.is_empty() {
                return alloc(Value::Record(Vec::new()));
            }
            // Reconstruct Bytes from {"__knot_bytes": "base64..."} format
            if obj.len() == 1 {
                if let Some(serde_json::Value::String(b64)) = obj.get("__knot_bytes") {
                    return alloc(Value::Bytes(Arc::from(base64_decode(b64))));
                }
            }
            // Reconstruct Constructor from the `__knot_ctor` marker shape
            // emitted by value_to_serde_json. Using a `__knot_`-prefixed key
            // (like `__knot_bytes`/`__knot_bigint`) avoids colliding with
            // legitimate user records that happen to have `tag`/`value` fields.
            if obj.len() == 1 {
                if let Some(serde_json::Value::Object(inner)) = obj.get("__knot_ctor") {
                    if let (Some(serde_json::Value::String(tag)), Some(val)) =
                        (inner.get("tag"), inner.get("value"))
                    {
                        return alloc(Value::Constructor(intern_str(tag), json_to_value(val)));
                    }
                }
            }
            let mut fields: Vec<RecordField> = obj
                .iter()
                .map(|(k, v)| RecordField { name: intern_str(k), value: json_to_value(v) })
                .collect();
            fields.sort_by(|a, b| a.name.cmp(&b.name));
            alloc(Value::Record(fields))
        }
    }
}

// ── Standard library: utility operations ──────────────────────

/// id(x) — identity function, returns its argument unchanged
#[unsafe(no_mangle)]
pub extern "C" fn knot_value_id(v: *mut Value) -> *mut Value {
    v
}

/// not(bool) — boolean negation (function form of !)
#[unsafe(no_mangle)]
pub extern "C" fn knot_value_not_fn(v: *mut Value) -> *mut Value {
    knot_value_not(v)
}

// ── Standard library: file system operations ──────────────────

/// readFile(path) — read entire file contents as Text
#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_read_file(path: *mut Value) -> *mut Value {
    match unsafe { as_ref(path) } {
        Value::Text(p) => match std::fs::read_to_string(&**p) {
            Ok(contents) => alloc(Value::Text(Arc::from(contents))),
            Err(e) => panic!("knot runtime: readFile failed for {:?}: {}", p, e),
        },
        _ => panic!(
            "knot runtime: readFile expected Text, got {}",
            type_name(path)
        ),
    }
}

/// writeFile(path, contents) — write Text to a file (creates or overwrites)
#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_write_file(path: *mut Value, contents: *mut Value) -> *mut Value {
    let p: &str = match unsafe { as_ref(path) } {
        Value::Text(s) => &**s,
        _ => panic!(
            "knot runtime: writeFile expected Text as first arg, got {}",
            type_name(path)
        ),
    };
    let c: &str = match unsafe { as_ref(contents) } {
        Value::Text(s) => &**s,
        _ => panic!(
            "knot runtime: writeFile expected Text as second arg, got {}",
            type_name(contents)
        ),
    };
    match std::fs::write(p, c) {
        Ok(()) => alloc(Value::Unit),
        Err(e) => panic!("knot runtime: writeFile failed for {:?}: {}", p, e),
    }
}

/// appendFile(path, contents) — append Text to a file
#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_append_file(path: *mut Value, contents: *mut Value) -> *mut Value {
    use std::io::Write;
    let p: &str = match unsafe { as_ref(path) } {
        Value::Text(s) => &**s,
        _ => panic!(
            "knot runtime: appendFile expected Text as first arg, got {}",
            type_name(path)
        ),
    };
    let c: &str = match unsafe { as_ref(contents) } {
        Value::Text(s) => &**s,
        _ => panic!(
            "knot runtime: appendFile expected Text as second arg, got {}",
            type_name(contents)
        ),
    };
    match std::fs::OpenOptions::new().create(true).append(true).open(p) {
        Ok(mut f) => {
            f.write_all(c.as_bytes())
                .unwrap_or_else(|e| panic!("knot runtime: appendFile write failed for {:?}: {}", p, e));
            alloc(Value::Unit)
        }
        Err(e) => panic!("knot runtime: appendFile failed for {:?}: {}", p, e),
    }
}

/// fileExists(path) — check whether a file exists
#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_file_exists(path: *mut Value) -> *mut Value {
    match unsafe { as_ref(path) } {
        Value::Text(p) => alloc_bool(std::path::Path::new(&**p).exists()),
        _ => panic!(
            "knot runtime: fileExists expected Text, got {}",
            type_name(path)
        ),
    }
}

/// removeFile(path) — delete a file
#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_remove_file(path: *mut Value) -> *mut Value {
    match unsafe { as_ref(path) } {
        Value::Text(p) => match std::fs::remove_file(&**p) {
            Ok(()) => alloc(Value::Unit),
            Err(e) => panic!("knot runtime: removeFile failed for {:?}: {}", p, e),
        },
        _ => panic!(
            "knot runtime: removeFile expected Text, got {}",
            type_name(path)
        ),
    }
}

/// listDir(path) — list directory entries as a relation of Text
#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_list_dir(path: *mut Value) -> *mut Value {
    match unsafe { as_ref(path) } {
        Value::Text(p) => {
            let entries: Vec<*mut Value> = match std::fs::read_dir(&**p) {
                Ok(rd) => rd
                    .filter_map(|entry| entry.ok())
                    .map(|entry| alloc(Value::Text(Arc::from(entry.file_name().to_string_lossy().as_ref()))))
                    .collect(),
                Err(e) => panic!("knot runtime: listDir failed for {:?}: {}", p, e),
            };
            alloc(Value::Relation(entries))
        }
        _ => panic!(
            "knot runtime: listDir expected Text, got {}",
            type_name(path)
        ),
    }
}

// ── Database operations ───────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_db_open(path_ptr: *const u8, path_len: usize) -> *mut c_void {
    let path = unsafe { str_from_raw(path_ptr, path_len) };
    // Store path globally so spawned threads can open their own connections
    *DB_PATH.lock().unwrap() = path.to_string();
    let conn = Connection::open(path).expect("knot runtime: failed to open database");
    conn.create_collation("KNOT_INT", |a: &str, b: &str| {
        match (a.parse::<i64>(), b.parse::<i64>()) {
            (Ok(pa), Ok(pb)) => pa.cmp(&pb),
            (Ok(_), Err(_)) => std::cmp::Ordering::Less,
            (Err(_), Ok(_)) => std::cmp::Ordering::Greater,
            (Err(_), Err(_)) => a.cmp(b),
        }
    })
    .expect("knot runtime: failed to create KNOT_INT collation");
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=30000; PRAGMA foreign_keys=ON;")
        .expect("knot runtime: failed to set pragmas");
    let db = Box::new(KnotDb {
        conn,
        atomic_depth: std::cell::Cell::new(0),
        indexed: RefCell::new(HashSet::new()),
    });
    Box::into_raw(db) as *mut c_void
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_db_close(db: *mut c_void) {
    if !db.is_null() {
        let _ = unsafe { Box::from_raw(db as *mut KnotDb) };
    }
}

/// Execute a SQL statement (e.g., CREATE TABLE).
#[unsafe(no_mangle)]
pub extern "C" fn knot_db_exec(db: *mut c_void, sql_ptr: *const u8, sql_len: usize) {
    let db = unsafe { &*(db as *mut KnotDb) };
    let sql = unsafe { str_from_raw(sql_ptr, sql_len) };
    debug_sql(sql);
    db.conn
        .execute_batch(sql)
        .unwrap_or_else(|e| panic!("knot runtime: SQL error: {}\n  SQL: {}", e, sql));
}

// ── Schema tracking ──────────────────────────────────────────────

/// Create the schema metadata table that tracks each source's column layout.
#[unsafe(no_mangle)]
pub extern "C" fn knot_schema_init(db: *mut c_void) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let sql =
        "CREATE TABLE IF NOT EXISTS _knot_schema (name TEXT PRIMARY KEY, schema TEXT NOT NULL);";
    debug_sql(sql);
    db_ref
        .conn
        .execute_batch(sql)
        .expect("knot runtime: failed to create schema tracking table");
}

/// Apply a migration to a source relation.
///
/// Checks the stored schema in `_knot_schema`:
/// - If stored == new_schema: already migrated, skip.
/// - If stored == old_schema: read old rows, transform each via `migrate_fn`,
///   drop & recreate the table, insert transformed rows, update stored schema.
/// - If no stored schema: new table, skip.
/// - Otherwise: error (unexpected schema).
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_migrate(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    old_schema_ptr: *const u8,
    old_schema_len: usize,
    new_schema_ptr: *const u8,
    new_schema_len: usize,
    migrate_fn: *mut Value,
) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let old_schema = unsafe { str_from_raw(old_schema_ptr, old_schema_len) };
    let new_schema = unsafe { str_from_raw(new_schema_ptr, new_schema_len) };

    // Check stored schema
    let stored: Option<String> = db_ref
        .conn
        .query_row(
            "SELECT schema FROM _knot_schema WHERE name = ?1;",
            rusqlite::params![name],
            |row| row.get(0),
        )
        .ok();

    match &stored {
        Some(s) if s == new_schema => return,
        Some(s) if s == old_schema => {}
        Some(s) => panic!(
            "knot runtime: source '{}' has schema '{}', expected '{}' (pre-migration) or '{}' (post-migration).\n\
             Check your migrate block.",
            name, s, old_schema, new_schema
        ),
        None => return,
    }

    eprintln!("Migrating source '{}'...", name);

    // 1. Read all rows using old schema
    let old_data = knot_source_read(db, name_ptr, name_len, old_schema_ptr, old_schema_len);
    let old_rows = match unsafe { as_ref(old_data) } {
        Value::Relation(rows) => rows.clone(),
        _ => panic!("knot runtime: expected relation during migration"),
    };

    // 2. Transform each row through the migration function
    let mut new_rows: Vec<*mut Value> = Vec::with_capacity(old_rows.len());
    for row in &old_rows {
        let new_row = knot_value_call(db, migrate_fn, *row);
        new_rows.push(new_row);
    }

    // 3. Drop old table + index and recreate with new schema (in a transaction)
    let table_name = format!("_knot_{}", name);
    let table = quote_ident(&table_name);

    db_ref
        .conn
        .execute_batch("SAVEPOINT knot_migrate;")
        .expect("knot runtime: failed to begin migration transaction");

    // Drop old child tables (for nested relation fields) before dropping parent.
    // Recurse to handle grandchild+ tables (deepest first).
    fn drop_nested_tables(conn: &rusqlite::Connection, parent_table: &str, nested: &[NestedField]) {
        for nf in nested {
            let child = format!("{}__{}", parent_table, nf.name);
            // Drop grandchildren first (depth-first)
            drop_nested_tables(conn, &child, &nf.nested);
            let drop_child = format!("DROP TABLE IF EXISTS {};", quote_ident(&child));
            debug_sql(&drop_child);
            let _ = conn.execute_batch(&drop_child);
        }
    }
    if !is_adt_schema(old_schema) {
        let old_rec = parse_record_schema(old_schema);
        drop_nested_tables(&db_ref.conn, &table_name, &old_rec.nested);
    }

    let drop_sql = format!("DROP TABLE IF EXISTS {};", table);
    debug_sql(&drop_sql);
    db_ref
        .conn
        .execute_batch(&drop_sql)
        .expect("knot runtime: failed to drop table during migration");

    if is_adt_schema(new_schema) {
        // ADT schema: recreate using the same logic as knot_source_init
        let adt = parse_adt_schema(new_schema);
        let mut col_defs = vec![format!("{} TEXT NOT NULL", quote_ident("_tag"))];
        let mut col_names = vec![quote_ident("_tag")];
        for f in &adt.all_fields {
            col_defs.push(format!("{} {}", quote_ident(&f.name), sql_type(f.ty)));
            col_names.push(quote_ident(&f.name));
        }

        let create_sql = format!("CREATE TABLE {} ({});", table, col_defs.join(", "));
        debug_sql(&create_sql);
        db_ref
            .conn
            .execute_batch(&create_sql)
            .expect("knot runtime: failed to create table during migration");

        // Unique index with COALESCE for NULLs (same as knot_source_init)
        let coalesced: Vec<String> = std::iter::once(quote_ident("_tag"))
            .chain(adt.all_fields.iter().map(|f| {
                null_safe_coalesce(&quote_ident(&f.name), f.ty)
            }))
            .collect();
        let idx_sql = format!(
            "CREATE UNIQUE INDEX {} ON {} ({});",
            quote_ident(&format!("_knot_{}_unique", name)),
            table,
            coalesced.join(", ")
        );
        debug_sql(&idx_sql);
        if let Err(e) = db_ref.conn.execute_batch(&idx_sql) {
            eprintln!("knot runtime: warning: failed to create unique index during migration for {}: {}", name, e);
        }

        // Insert transformed rows (ADT: constructor values)
        if !new_rows.is_empty() {
            let placeholders: Vec<String> = col_names
                .iter()
                .enumerate()
                .map(|(i, _)| format!("?{}", i + 1))
                .collect();
            let insert_sql = format!(
                "INSERT OR IGNORE INTO {} ({}) VALUES ({});",
                table,
                col_names.join(", "),
                placeholders.join(", ")
            );
            debug_sql(&insert_sql);

            let mut stmt = db_ref
                .conn
                .prepare_cached(&insert_sql)
                .expect("knot runtime: failed to prepare insert during migration");

            for row_ptr in &new_rows {
                let row_ref = unsafe { as_ref(*row_ptr) };
                if let Value::Constructor(tag, payload) = row_ref {
                    let mut params: Vec<rusqlite::types::Value> = Vec::new();
                    params.push(rusqlite::types::Value::Text(tag.to_string()));
                    let payload_fields = match unsafe { as_ref(*payload) } {
                        Value::Record(f) => f,
                        Value::Unit => &Vec::new() as &Vec<RecordField>,
                        _ => panic!("knot runtime: ADT migration result has non-record payload"),
                    };
                    for f in &adt.all_fields {
                        let val = payload_fields
                            .iter()
                            .find(|pf| &*pf.name == f.name.as_str())
                            .map(|pf| value_to_sql_param(pf.value))
                            .unwrap_or(rusqlite::types::Value::Null);
                        params.push(val);
                    }
                    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                        params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
                    stmt.execute(param_refs.as_slice())
                        .expect("knot runtime: failed to insert row during migration");
                }
            }
        }
    } else {
        // Record schema — use init_record_table + write_record_rows so that
        // nested relation fields (child tables) and _id AUTOINCREMENT are
        // handled correctly, and value_to_sqlite is used for type-aware
        // serialization.
        let new_rec = parse_record_schema(new_schema);
        init_record_table(&db_ref.conn, &table_name, &new_rec);
        write_record_rows(&db_ref.conn, &table_name, &new_rec, &new_rows);
    }

    // 5. Update stored schema
    db_ref
        .conn
        .execute(
            "INSERT OR REPLACE INTO _knot_schema (name, schema) VALUES (?1, ?2);",
            rusqlite::params![name, new_schema],
        )
        .expect("knot runtime: failed to update schema after migration");

    db_ref
        .conn
        .execute_batch("RELEASE SAVEPOINT knot_migrate;")
        .expect("knot runtime: failed to commit migration");

    eprintln!("Migrated source '{}': {} rows", name, old_rows.len());
}

// ── Source operations ─────────────────────────────────────────────

/// Schema descriptor format: "col1:type1,col2:type2,..."
/// Types: "int", "float", "text", "bool", "tag"
/// Nested relations: "col:[inner_schema]"
/// ADT schema format: "#Ctor1:f1=t1;f2=t2|Ctor2|Ctor3:f3=t3"
struct ColumnSpec {
    name: String,
    ty: ColType,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ColType {
    Int,
    Float,
    Text,
    Bool,
    Bytes,
    /// Stored as TEXT, reconstructed as Constructor on read
    Tag,
    /// Nested relation stored as JSON text in SQLite. In practice this variant
    /// never appears in `RecordSchema::columns`/`NestedField::columns` because
    /// `parse_record_schema` routes `[...]` types into the `nested` channel
    /// instead — but it remains for ADT field schemas (`parse_adt_schema`).
    Json,
}

/// A nested relation field stored in a child table.
struct NestedField {
    name: String,
    /// Scalar columns in the child table
    columns: Vec<ColumnSpec>,
    /// Further nested relations within this child
    nested: Vec<NestedField>,
}

/// Parsed record schema with both scalar columns and nested relation fields.
struct RecordSchema {
    /// Scalar (non-relation) columns stored directly in this table
    columns: Vec<ColumnSpec>,
    /// Nested relation fields stored in child tables
    nested: Vec<NestedField>,
}

/// ADT constructor schema: constructor name and its fields
struct CtorSpec {
    name: String,
    fields: Vec<ColumnSpec>,
}

/// Parsed ADT schema for direct ADT relations
struct AdtSpec {
    constructors: Vec<CtorSpec>,
    /// Union of all fields across all constructors (for wide table columns)
    all_fields: Vec<ColumnSpec>,
}

/// Determine if a schema descriptor is an ADT schema (starts with '#')
fn is_adt_schema(spec: &str) -> bool {
    spec.starts_with('#')
}

/// Parse an ADT schema descriptor: "#Ctor1:f1=t1;f2=t2|Ctor2|Ctor3:f3=t3"
fn parse_adt_schema(spec: &str) -> AdtSpec {
    let body = &spec[1..]; // strip '#'
    let mut constructors = Vec::new();
    let mut all_field_names: HashSet<String> = HashSet::new();
    let mut all_fields: Vec<ColumnSpec> = Vec::new();

    for ctor_part in split_respecting_brackets(body, '|') {
        let mut parts = ctor_part.splitn(2, ':');
        let name = parts.next().unwrap().to_string();
        let fields: Vec<ColumnSpec> = if let Some(field_spec) = parts.next() {
            split_respecting_brackets(field_spec, ';')
                .iter()
                .map(|f| {
                    let mut fp = f.splitn(2, '=');
                    let fname = fp.next().unwrap().to_string();
                    let fty = match fp.next().unwrap_or("text") {
                        "int" => ColType::Int,
                        "float" => ColType::Float,
                        "text" => ColType::Text,
                        "bool" => ColType::Bool,
                        "bytes" => ColType::Bytes,
                        "tag" => ColType::Tag,
                        s if s.starts_with('[') => ColType::Json,
                        other => panic!("knot runtime: unknown ADT field type '{}'", other),
                    };
                    ColumnSpec {
                        name: fname,
                        ty: fty,
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        // Add unique fields to the all_fields list
        for f in &fields {
            if all_field_names.insert(f.name.clone()) {
                all_fields.push(ColumnSpec {
                    name: f.name.clone(),
                    ty: f.ty,
                });
            }
        }

        constructors.push(CtorSpec { name, fields });
    }

    AdtSpec {
        constructors,
        all_fields,
    }
}

/// Split a string by `sep` while respecting `[...]` bracket nesting.
fn split_respecting_brackets(s: &str, sep: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '[' => depth += 1,
            ']' => depth = depth.saturating_sub(1),
            c if c == sep && depth == 0 => {
                parts.push(&s[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

fn parse_col_type(s: &str) -> ColType {
    match s {
        "int" => ColType::Int,
        "float" => ColType::Float,
        "text" => ColType::Text,
        "bool" => ColType::Bool,
        "bytes" => ColType::Bytes,
        "tag" => ColType::Tag,
        "json" => ColType::Json,
        other => panic!("knot runtime: unknown column type '{}'", other),
    }
}

fn parse_record_schema(spec: &str) -> RecordSchema {
    if spec.is_empty() {
        return RecordSchema { columns: Vec::new(), nested: Vec::new() };
    }
    let mut columns = Vec::new();
    let mut nested = Vec::new();
    for part in split_respecting_brackets(spec, ',') {
        // Find the first ':' (field name separator)
        let colon = part.find(':').unwrap_or_else(|| {
            panic!(
                "knot runtime: malformed schema field '{}' (full schema: '{}')",
                part, spec
            )
        });
        let name = part[..colon].to_string();
        let type_str = &part[colon + 1..];
        if type_str.starts_with('[') && type_str.ends_with(']') {
            // Nested relation: parse child schema recursively
            let inner = &type_str[1..type_str.len() - 1];
            let child = parse_record_schema(inner);
            nested.push(NestedField {
                name,
                columns: child.columns,
                nested: child.nested,
            });
        } else {
            columns.push(ColumnSpec { name, ty: parse_col_type(type_str) });
        }
    }
    RecordSchema { columns, nested }
}

/// Backward-compatible: parse a flat schema (no nested fields) into Vec<ColumnSpec>.
fn parse_schema(spec: &str) -> Vec<ColumnSpec> {
    parse_record_schema(spec).columns
}

/// Build a COALESCE expression that maps NULL to a sentinel value for use in
/// UNIQUE indexes (SQLite treats NULLs as distinct, so we need a non-NULL stand-in).
///
/// The sentinel MUST have a different SQLite storage class than real column values
/// so it can never collide with actual data.  Storage class order:
///   NULL < INTEGER < REAL < TEXT < BLOB
/// Values of different storage classes are never considered equal by SQLite.
fn null_safe_coalesce(col: &str, ty: ColType) -> String {
    match ty {
        // Int stored as TEXT, Bool stored as INTEGER — INTEGER sentinel can't match either
        ColType::Int | ColType::Bool => format!("COALESCE({}, -9223372036854775808)", col),
        // Float stored as REAL — TEXT sentinel can't match REAL
        ColType::Float => format!("COALESCE({}, '')", col),
        // Bytes stored as BLOB — TEXT sentinel can't match BLOB
        ColType::Bytes => format!("COALESCE({}, '')", col),
        // Text/Tag/Json stored as TEXT — BLOB sentinel can't match TEXT
        _ => format!("COALESCE({}, X'00')", col),
    }
}

fn sql_type(ty: ColType) -> &'static str {
    match ty {
        ColType::Int => "TEXT COLLATE KNOT_INT",
        ColType::Float => "REAL",
        ColType::Text => "TEXT",
        ColType::Bool => "INTEGER",
        ColType::Bytes => "BLOB",
        ColType::Tag => "TEXT",
        ColType::Json => "TEXT",
    }
}

/// Read a column value from a SQLite row, returning null pointer for SQL NULL.
///
/// All `unwrap`s on `row.get_ref(i)` and `row.get(i)` are guarded by callers
/// that pass a column index in range; if a schema migration drift makes one
/// out-of-range, the panic message names the column so the cause is
/// identifiable in `panic = "abort"` builds.
fn read_sql_column(row: &rusqlite::Row, i: usize, ty: ColType) -> *mut Value {
    let mismatch = || format!(
        "knot runtime: schema mismatch reading column {} (type {:?}) — possible migration drift",
        i, ty
    );
    if matches!(row.get_ref(i).unwrap_or_else(|_| panic!("{}", mismatch())), ValueRef::Null) {
        return std::ptr::null_mut();
    }
    match ty {
        ColType::Int => {
            match row.get_ref(i).unwrap_or_else(|_| panic!("{}", mismatch())) {
                ValueRef::Integer(n) => alloc_int(n),
                ValueRef::Text(s) => {
                    let s = std::str::from_utf8(s).expect("knot runtime: invalid UTF-8 in int column");
                    let n: i64 = s.parse().expect("knot runtime: invalid integer in column");
                    alloc_int(n)
                }
                other => panic!("knot runtime: unexpected SQLite type for Int column {}: {:?}", i, other),
            }
        }
        ColType::Float => knot_value_float(row.get::<_, f64>(i).unwrap_or_else(|_| panic!("{}", mismatch()))),
        ColType::Text => {
            let s: String = row.get(i).unwrap_or_else(|_| panic!("{}", mismatch()));
            alloc(Value::Text(Arc::from(s)))
        }
        ColType::Bool => knot_value_bool(row.get::<_, i32>(i).unwrap_or_else(|_| panic!("{}", mismatch()))),
        ColType::Bytes => {
            let b: Vec<u8> = row.get(i).unwrap_or_else(|_| panic!("{}", mismatch()));
            alloc(Value::Bytes(Arc::from(b)))
        }
        ColType::Tag => {
            // Read TEXT but reconstruct as a Constructor with Unit payload
            let tag: String = row.get(i).unwrap_or_else(|_| panic!("{}", mismatch()));
            alloc(Value::Constructor(intern_str(&tag), alloc(Value::Unit)))
        }
        ColType::Json => {
            // Read TEXT and parse as JSON back into a Knot value (typically a relation)
            let s: String = row.get(i).unwrap_or_else(|_| panic!("{}", mismatch()));
            match serde_json::from_str::<serde_json::Value>(&s) {
                Ok(json) => json_to_value(&json),
                Err(_) => alloc(Value::Text(Arc::from(s))),
            }
        }
    }
}

/// Create a record table and any child tables for nested relation fields.
/// Tables with nested children get `_id INTEGER PRIMARY KEY AUTOINCREMENT`.
fn init_record_table(conn: &rusqlite::Connection, table_name: &str, schema: &RecordSchema) {
    let table = quote_ident(table_name);
    let has_children = !schema.nested.is_empty();

    let mut col_defs: Vec<String> = Vec::new();
    let mut unique_cols: Vec<String> = Vec::new();

    if has_children {
        col_defs.push("_id INTEGER PRIMARY KEY AUTOINCREMENT".to_string());
    }

    for c in &schema.columns {
        col_defs.push(format!("{} {}", quote_ident(&c.name), sql_type(c.ty)));
        unique_cols.push(quote_ident(&c.name));
    }

    if col_defs.is_empty() {
        col_defs.push("_dummy INTEGER DEFAULT 0".to_string());
    }

    let sql = format!("CREATE TABLE IF NOT EXISTS {} ({});", table, col_defs.join(", "));
    debug_sql(&sql);
    conn.execute_batch(&sql).unwrap_or_else(|e| {
        panic!("knot runtime: failed to create table '{}': {}", table_name, e)
    });

    if !unique_cols.is_empty() {
        let idx_sql = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({});",
            quote_ident(&format!("{}_unique", table_name)),
            table,
            unique_cols.join(", ")
        );
        debug_sql(&idx_sql);
        let _ = conn.execute_batch(&idx_sql);
    }

    // Recursively create child tables
    for nf in &schema.nested {
        init_child_table(conn, table_name, nf);
    }
}

/// Create a child table for a nested relation field, recursing for deeper nesting.
fn init_child_table(conn: &rusqlite::Connection, parent_table: &str, nf: &NestedField) {
    let child_table_name = format!("{}__{}", parent_table, nf.name);
    let child_table = quote_ident(&child_table_name);
    let has_children = !nf.nested.is_empty();

    let mut col_defs = vec!["_parent_id INTEGER NOT NULL".to_string()];
    let mut unique_cols = vec![quote_ident("_parent_id")];

    if has_children {
        col_defs.push("_id INTEGER PRIMARY KEY AUTOINCREMENT".to_string());
    }

    for c in &nf.columns {
        col_defs.push(format!("{} {}", quote_ident(&c.name), sql_type(c.ty)));
        unique_cols.push(quote_ident(&c.name));
    }

    let sql = format!("CREATE TABLE IF NOT EXISTS {} ({});", child_table, col_defs.join(", "));
    debug_sql(&sql);
    conn.execute_batch(&sql).unwrap_or_else(|e| {
        panic!("knot runtime: failed to create child table '{}': {}", child_table_name, e)
    });

    // Unique index: (_parent_id, scalar_cols) for set semantics within each parent row
    if unique_cols.len() > 1 {
        let idx_sql = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({});",
            quote_ident(&format!("{}_unique", child_table_name)),
            child_table,
            unique_cols.join(", ")
        );
        debug_sql(&idx_sql);
        let _ = conn.execute_batch(&idx_sql);
    }

    // Recurse for deeper nesting
    for grandchild in &nf.nested {
        init_child_table(conn, &child_table_name, grandchild);
    }
}

/// Try to auto-apply a safe schema change (e.g. adding ADT constructors).
/// Returns true if the change was applied, false if it's a breaking change.
fn auto_apply_schema_change(
    conn: &Connection,
    name: &str,
    stored: &str,
    compiled: &str,
) -> bool {
    let table = format!("_knot_{}", name);
    let stored_is_adt = is_adt_schema(stored);
    let compiled_is_adt = is_adt_schema(compiled);

    if stored_is_adt != compiled_is_adt {
        return false;
    }

    if stored_is_adt {
        auto_apply_adt_change(conn, &table, name, stored, compiled)
    } else {
        auto_apply_record_change(conn, &table, name, stored, compiled)
    }
}

fn auto_apply_adt_change(
    conn: &Connection,
    table: &str,
    name: &str,
    stored: &str,
    compiled: &str,
) -> bool {
    let old_adt = parse_adt_schema(stored);
    let new_adt = parse_adt_schema(compiled);

    // Every old constructor must exist in new with identical fields
    for old_ctor in &old_adt.constructors {
        match new_adt.constructors.iter().find(|c| c.name == old_ctor.name) {
            Some(new_ctor) => {
                if old_ctor.fields.len() != new_ctor.fields.len() {
                    return false;
                }
                for (of, nf) in old_ctor.fields.iter().zip(&new_ctor.fields) {
                    if of.name != nf.name || std::mem::discriminant(&of.ty) != std::mem::discriminant(&nf.ty) {
                        return false;
                    }
                }
            }
            None => return false,
        }
    }

    // Add new columns for new constructor fields
    let old_field_names: HashSet<&str> = old_adt.all_fields.iter().map(|f| f.name.as_str()).collect();
    for f in &new_adt.all_fields {
        if !old_field_names.contains(f.name.as_str()) {
            let sql = format!(
                "ALTER TABLE {} ADD COLUMN {} {};",
                quote_ident(table),
                quote_ident(&f.name),
                sql_type(f.ty)
            );
            debug_sql(&sql);
            if conn.execute_batch(&sql).is_err() {
                return false;
            }
        }
    }

    // Drop and recreate unique index with full column set
    let drop_idx = format!(
        "DROP INDEX IF EXISTS {};",
        quote_ident(&format!("{}_unique", table))
    );
    debug_sql(&drop_idx);
    let _ = conn.execute_batch(&drop_idx);

    let coalesced: Vec<String> = std::iter::once(quote_ident("_tag"))
        .chain(new_adt.all_fields.iter().map(|f| {
            null_safe_coalesce(&quote_ident(&f.name), f.ty)
        }))
        .collect();
    let idx_sql = format!(
        "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({});",
        quote_ident(&format!("{}_unique", table)),
        quote_ident(table),
        coalesced.join(", ")
    );
    debug_sql(&idx_sql);
    let _ = conn.execute_batch(&idx_sql);

    // Update stored schema
    let _ = conn.execute(
        "INSERT OR REPLACE INTO _knot_schema (name, schema) VALUES (?1, ?2);",
        rusqlite::params![name, compiled],
    );

    true
}

/// Recursively migrate a child table when its inner schema changes.
/// Handles added columns (ALTER TABLE ADD COLUMN), removed columns (breaking),
/// type changes (breaking), and nested-within-nested fields.
fn auto_apply_child_change(
    conn: &Connection,
    parent_table: &str,
    old_nf: &NestedField,
    new_nf: &NestedField,
) -> bool {
    let child_table = format!("{}__{}", parent_table, new_nf.name);

    // Check that all old columns still exist with same type
    for old_col in &old_nf.columns {
        match new_nf.columns.iter().find(|c| c.name == old_col.name) {
            Some(new_col) => {
                if old_col.ty != new_col.ty {
                    return false;
                }
            }
            None => return false,
        }
    }

    // Any removed nested sub-fields → breaking
    for old_sub in &old_nf.nested {
        if !new_nf.nested.iter().any(|n| n.name == old_sub.name) {
            return false;
        }
    }

    // Add new columns to the child table
    let old_col_names: HashSet<&str> = old_nf.columns.iter().map(|c| c.name.as_str()).collect();
    for c in &new_nf.columns {
        if !old_col_names.contains(c.name.as_str()) {
            let sql = format!(
                "ALTER TABLE {} ADD COLUMN {} {};",
                quote_ident(&child_table),
                quote_ident(&c.name),
                sql_type(c.ty)
            );
            debug_sql(&sql);
            if conn.execute_batch(&sql).is_err() {
                return false;
            }
        }
    }

    // Drop and recreate unique index with full column set
    let drop_idx = format!(
        "DROP INDEX IF EXISTS {};",
        quote_ident(&format!("{}_unique", child_table))
    );
    debug_sql(&drop_idx);
    let _ = conn.execute_batch(&drop_idx);

    let mut unique_cols = vec![quote_ident("_parent_id")];
    for c in &new_nf.columns {
        unique_cols.push(quote_ident(&c.name));
    }
    if unique_cols.len() > 1 {
        let idx_sql = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({});",
            quote_ident(&format!("{}_unique", child_table)),
            quote_ident(&child_table),
            unique_cols.join(", ")
        );
        debug_sql(&idx_sql);
        let _ = conn.execute_batch(&idx_sql);
    }

    // Recurse into nested-within-nested fields
    for new_sub in &new_nf.nested {
        if let Some(old_sub) = old_nf.nested.iter().find(|n| n.name == new_sub.name) {
            if !auto_apply_child_change(conn, &child_table, old_sub, new_sub) {
                return false;
            }
        }
    }

    // Initialize any brand-new nested sub-tables
    let old_sub_names: HashSet<&str> = old_nf.nested.iter().map(|n| n.name.as_str()).collect();
    for sub in &new_nf.nested {
        if !old_sub_names.contains(sub.name.as_str()) {
            init_child_table(conn, &child_table, sub);
        }
    }

    true
}

fn auto_apply_record_change(
    conn: &Connection,
    table: &str,
    name: &str,
    stored: &str,
    compiled: &str,
) -> bool {
    let old_rec = parse_record_schema(stored);
    let new_rec = parse_record_schema(compiled);

    // Every old column must exist in new with same type
    for old_col in &old_rec.columns {
        match new_rec.columns.iter().find(|c| c.name == old_col.name) {
            Some(new_col) => {
                if old_col.ty != new_col.ty {
                    return false;
                }
            }
            None => return false,
        }
    }

    // Adding nested fields to a table that had none → breaking.
    // The parent table needs `_id INTEGER PRIMARY KEY AUTOINCREMENT` for
    // child table FK references, and SQLite cannot add a PRIMARY KEY via
    // ALTER TABLE.
    if old_rec.nested.is_empty() && !new_rec.nested.is_empty() {
        return false;
    }

    // Any removed nested fields → breaking
    for old_nf in &old_rec.nested {
        if !new_rec.nested.iter().any(|n| n.name == old_nf.name) {
            return false;
        }
    }

    // Add new columns
    let old_col_names: HashSet<&str> = old_rec.columns.iter().map(|c| c.name.as_str()).collect();
    for c in &new_rec.columns {
        if !old_col_names.contains(c.name.as_str()) {
            let sql = format!(
                "ALTER TABLE {} ADD COLUMN {} {};",
                quote_ident(table),
                quote_ident(&c.name),
                sql_type(c.ty)
            );
            debug_sql(&sql);
            if conn.execute_batch(&sql).is_err() {
                return false;
            }
        }
    }

    // Drop and recreate unique index with full column set
    let drop_idx = format!(
        "DROP INDEX IF EXISTS {};",
        quote_ident(&format!("{}_unique", table))
    );
    debug_sql(&drop_idx);
    let _ = conn.execute_batch(&drop_idx);

    let unique_cols: Vec<String> = new_rec.columns.iter().map(|c| quote_ident(&c.name)).collect();
    if !unique_cols.is_empty() {
        let idx_sql = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({});",
            quote_ident(&format!("{}_unique", table)),
            quote_ident(table),
            unique_cols.join(", ")
        );
        debug_sql(&idx_sql);
        let _ = conn.execute_batch(&idx_sql);
    }

    // Migrate existing child tables whose inner schema changed
    let old_nested_names: HashSet<&str> = old_rec.nested.iter().map(|n| n.name.as_str()).collect();
    for new_nf in &new_rec.nested {
        if let Some(old_nf) = old_rec.nested.iter().find(|n| n.name == new_nf.name) {
            if !auto_apply_child_change(conn, table, old_nf, new_nf) {
                return false;
            }
        }
    }

    // Initialize any new child tables for nested relations
    for nf in &new_rec.nested {
        if !old_nested_names.contains(nf.name.as_str()) {
            init_child_table(conn, table, nf);
        }
    }

    // Update stored schema
    let _ = conn.execute(
        "INSERT OR REPLACE INTO _knot_schema (name, schema) VALUES (?1, ?2);",
        rusqlite::params![name, compiled],
    );

    true
}

/// Initialize a source table. Creates it if it doesn't exist.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_init(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };

    let table = quote_ident(&format!("_knot_{}", name));

    if is_adt_schema(schema) {
        // ADT schema: create wide table with _tag + all constructor fields
        let adt = parse_adt_schema(schema);
        let mut col_defs = vec![format!("{} TEXT NOT NULL", quote_ident("_tag"))];
        let mut col_names = vec![quote_ident("_tag")];
        for f in &adt.all_fields {
            col_defs.push(format!("{} {}", quote_ident(&f.name), sql_type(f.ty)));
            col_names.push(quote_ident(&f.name));
        }

        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({});",
            table,
            col_defs.join(", ")
        );
        debug_sql(&sql);
        db_ref.conn.execute_batch(&sql).unwrap_or_else(|e| {
            panic!("knot runtime: failed to create table '{}': {}", name, e)
        });

        // Unique index using COALESCE to treat NULLs as equal
        let coalesced: Vec<String> = std::iter::once(quote_ident("_tag"))
            .chain(adt.all_fields.iter().map(|f| {
                null_safe_coalesce(&quote_ident(&f.name), f.ty)
            }))
            .collect();
        let idx_sql = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({});",
            quote_ident(&format!("_knot_{}_unique", name)),
            table,
            coalesced.join(", ")
        );
        debug_sql(&idx_sql);
        let _ = db_ref.conn.execute_batch(&idx_sql);

        // Auto-index _tag for efficient pattern matching (WHERE _tag = ?)
        db_ref.ensure_index(&format!("_knot_{}", name), "_tag");
    } else {
        // Regular record schema (may include nested relations)
        let rec = parse_record_schema(schema);
        init_record_table(&db_ref.conn, &format!("_knot_{}", name), &rec);
    }

    // Check stored schema against compiled schema
    let stored: Option<String> = db_ref
        .conn
        .query_row(
            "SELECT schema FROM _knot_schema WHERE name = ?1;",
            rusqlite::params![name],
            |row| row.get(0),
        )
        .ok();

    if let Some(ref stored_schema) = stored {
        if stored_schema != schema {
            if !auto_apply_schema_change(&db_ref.conn, name, stored_schema, schema) {
                panic!(
                    "knot runtime: schema mismatch for source '*{}'.\n\
                     Stored:   {}\n\
                     Compiled: {}\n\
                     Add a `migrate *{} from {{...}} to {{...}} using (\\old -> ...)` block to your source.",
                    name, stored_schema, schema, name
                );
            }
        }
    }

    // Record current schema
    db_ref
        .conn
        .execute(
            "INSERT OR REPLACE INTO _knot_schema (name, schema) VALUES (?1, ?2);",
            rusqlite::params![name, schema],
        )
        .expect("knot runtime: failed to record schema");
}

/// Read all rows from a source relation.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_read(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };

    if db_ref.atomic_depth.get() > 0 {
        stm_track_read(name);
    }

    let table = quote_ident(&format!("_knot_{}", name));

    if is_adt_schema(schema) {
        let adt = parse_adt_schema(schema);
        // Build field name → index map for O(1) lookups
        let field_idx: HashMap<&str, usize> = adt.all_fields.iter().enumerate()
            .map(|(i, f)| (f.name.as_str(), i)).collect();
        // SELECT _tag + all fields from the wide table
        let mut select_cols = vec![quote_ident("_tag")];
        for f in &adt.all_fields {
            select_cols.push(quote_ident(&f.name));
        }
        let sql = format!("SELECT {} FROM {}", select_cols.join(", "), table);
        debug_sql(&sql);

        let mut stmt = db_ref
            .conn
            .prepare_cached(&sql)
            .unwrap_or_else(|e| panic!("knot runtime: query error: {}", e));
        let mut rows: Vec<*mut Value> = Vec::new();
        let mut result_rows = stmt
            .query([])
            .unwrap_or_else(|e| panic!("knot runtime: query exec error: {}", e));

        while let Some(row) = result_rows
            .next()
            .unwrap_or_else(|e| panic!("knot runtime: row fetch error: {}", e))
        {
            let tag: String = row.get(0).unwrap();
            // Find the constructor spec for this tag
            let ctor = adt.constructors.iter().find(|c| c.name == tag);
            let payload = if let Some(ctor) = ctor {
                if ctor.fields.is_empty() {
                    alloc(Value::Unit)
                } else {
                    // Build a record from the constructor's specific fields
                    let record = knot_record_empty(ctor.fields.len());
                    for field in &ctor.fields {
                        let col_idx = *field_idx.get(field.name.as_str()).unwrap_or_else(|| {
                            panic!(
                                "knot runtime: schema mismatch in `{}` — constructor `{}` field `{}` not present in stored ADT layout",
                                name, tag, field.name
                            )
                        });
                        let val = read_sql_column(row, col_idx + 1, field.ty); // +1 for _tag
                        let fname = field.name.as_bytes();
                        knot_record_set_field(record, fname.as_ptr(), fname.len(), val);
                    }
                    record
                }
            } else {
                // Unknown constructor: include all non-NULL fields
                let record = knot_record_empty(adt.all_fields.len());
                let mut has_fields = false;
                for (i, field) in adt.all_fields.iter().enumerate() {
                    if !matches!(row.get_ref(i + 1).unwrap(), ValueRef::Null) {
                        let val = read_sql_column(row, i + 1, field.ty);
                        let fname = field.name.as_bytes();
                        knot_record_set_field(record, fname.as_ptr(), fname.len(), val);
                        has_fields = true;
                    }
                }
                if has_fields {
                    record
                } else {
                    alloc(Value::Unit)
                }
            };
            rows.push(alloc(Value::Constructor(intern_str(&tag), payload)));
        }
        alloc(Value::Relation(rows))
    } else {
        let rec = parse_record_schema(schema);
        read_record_table(&db_ref.conn, &format!("_knot_{}", name), &rec)
    }
}

/// Execute an arbitrary SQL query that returns COUNT(*), with bind parameters.
/// Returns a boxed Int value.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_query_count(
    db: *mut c_void,
    sql_ptr: *const u8,
    sql_len: usize,
    params: *mut Value,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let sql = unsafe { str_from_raw(sql_ptr, sql_len) };

    let param_values = match unsafe { as_ref(params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: query_count params must be a Relation, got {}",
            type_name(params)
        ),
    };
    let sql_params: Vec<rusqlite::types::Value> =
        param_values.iter().map(|v| value_to_sql_param(*v)).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        sql_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    debug_sql_params(sql, &sql_params);

    let count: i64 = db_ref
        .conn
        .query_row(sql, param_refs.as_slice(), |row| row.get(0))
        .unwrap_or_else(|e| panic!("knot runtime: query_count error: {}\n  SQL: {}", e, sql));
    alloc_int(count)
}

/// Execute a SQL aggregate query returning a float (e.g. AVG).
/// Returns a boxed Float value. Returns 0.0 when the result is NULL
/// (e.g. AVG on an empty table).
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_query_float(
    db: *mut c_void,
    sql_ptr: *const u8,
    sql_len: usize,
    params: *mut Value,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let sql = unsafe { str_from_raw(sql_ptr, sql_len) };

    let param_values = match unsafe { as_ref(params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: query_float params must be a Relation, got {}",
            type_name(params)
        ),
    };
    let sql_params: Vec<rusqlite::types::Value> =
        param_values.iter().map(|v| value_to_sql_param(*v)).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        sql_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    debug_sql_params(sql, &sql_params);

    let result: Option<f64> = db_ref
        .conn
        .query_row(sql, param_refs.as_slice(), |row| row.get(0))
        .unwrap_or_else(|e| panic!("knot runtime: query_float error: {}\n  SQL: {}", e, sql));
    alloc_float(result.unwrap_or(0.0))
}

/// Execute a SQL SUM() query, preserving the numeric type.
/// Returns Int when SQLite produces an integer result (SUM on integer columns),
/// Float when it produces a real result (SUM on float columns).
/// Returns Int 0 when the result is NULL (SUM on an empty table).
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_query_sum(
    db: *mut c_void,
    sql_ptr: *const u8,
    sql_len: usize,
    params: *mut Value,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let sql = unsafe { str_from_raw(sql_ptr, sql_len) };

    let param_values = match unsafe { as_ref(params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: query_sum params must be a Relation, got {}",
            type_name(params)
        ),
    };
    let sql_params: Vec<rusqlite::types::Value> =
        param_values.iter().map(|v| value_to_sql_param(*v)).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        sql_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    debug_sql_params(sql, &sql_params);

    db_ref
        .conn
        .query_row(sql, param_refs.as_slice(), |row| {
            match row.get_ref(0).unwrap() {
                ValueRef::Null => Ok(alloc_int(0)),
                ValueRef::Integer(n) => Ok(alloc_int(n)),
                ValueRef::Real(f) => Ok(alloc_float(f)),
                ValueRef::Text(s) => {
                    let s = std::str::from_utf8(s).expect("knot runtime: invalid UTF-8 in sum result");
                    if let Ok(n) = s.parse::<i64>() {
                        Ok(alloc_int(n))
                    } else if let Ok(f) = s.parse::<f64>() {
                        Ok(alloc_float(f))
                    } else {
                        Ok(alloc_int(0))
                    }
                }
                _ => Ok(alloc_int(0)),
            }
        })
        .unwrap_or_else(|e| panic!("knot runtime: query_sum error: {}\n  SQL: {}", e, sql))
}

/// Execute a SQL aggregate query (e.g. MIN, MAX) and return the result
/// as an Int, Float, or Text Value matching the SQLite column type.
/// Panics if the result is NULL (e.g. min/max on an empty table).
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_query_value(
    db: *mut c_void,
    sql_ptr: *const u8,
    sql_len: usize,
    params: *mut Value,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let sql = unsafe { str_from_raw(sql_ptr, sql_len) };

    let param_values = match unsafe { as_ref(params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: query_value params must be a Relation, got {}",
            type_name(params)
        ),
    };
    let sql_params: Vec<rusqlite::types::Value> =
        param_values.iter().map(|v| value_to_sql_param(*v)).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        sql_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    debug_sql_params(sql, &sql_params);

    db_ref
        .conn
        .query_row(sql, param_refs.as_slice(), |row| {
            match row.get_ref(0).unwrap() {
                ValueRef::Null => panic!("knot runtime: aggregate on empty relation\n  SQL: {}", sql),
                ValueRef::Integer(n) => Ok(alloc_int(n)),
                ValueRef::Real(f) => Ok(alloc_float(f)),
                ValueRef::Text(s) => {
                    let s = std::str::from_utf8(s)
                        .expect("knot runtime: invalid UTF-8 in aggregate result");
                    Ok(alloc(Value::Text(Arc::from(s))))
                }
                ValueRef::Blob(_) => panic!(
                    "knot runtime: aggregate result is BLOB, expected Int/Float/Text\n  SQL: {}",
                    sql
                ),
            }
        })
        .unwrap_or_else(|e| panic!("knot runtime: query_value error: {}\n  SQL: {}", e, sql))
}

/// Count rows in a source relation via SQL COUNT(*).
/// Returns a boxed Int value.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_count(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let table = quote_ident(&format!("_knot_{}", name));
    let sql = format!("SELECT COUNT(*) FROM {}", table);
    debug_sql(&sql);
    let count: i64 = db_ref
        .conn
        .query_row(&sql, [], |row| row.get(0))
        .unwrap_or_else(|e| panic!("knot runtime: count error: {}", e));
    alloc_int(count)
}

/// Read rows from a source relation with a WHERE clause.
/// Params is a Relation of bind parameter values (?1, ?2, ...).
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_read_where(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    where_ptr: *const u8,
    where_len: usize,
    params: *mut Value,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };
    let where_clause = unsafe { str_from_raw(where_ptr, where_len) };

    if db_ref.atomic_depth.get() > 0 {
        stm_track_read(name);
    }

    let table_name = format!("_knot_{}", name);
    let table = quote_ident(&table_name);

    // Auto-index columns used in the WHERE clause
    db_ref.ensure_indexes_for_where(&table_name, where_clause);

    let param_values = match unsafe { as_ref(params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: read_where params must be a Relation, got {}",
            type_name(params)
        ),
    };
    let sql_params: Vec<rusqlite::types::Value> =
        param_values.iter().map(|v| value_to_sql_param(*v)).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        sql_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    if is_adt_schema(schema) {
        let adt = parse_adt_schema(schema);
        let field_idx: HashMap<&str, usize> = adt.all_fields.iter().enumerate()
            .map(|(i, f)| (f.name.as_str(), i)).collect();
        let mut select_cols = vec![quote_ident("_tag")];
        for f in &adt.all_fields {
            select_cols.push(quote_ident(&f.name));
        }
        let sql = format!(
            "SELECT {} FROM {} WHERE {}",
            select_cols.join(", "),
            table,
            where_clause
        );
        debug_sql_params(&sql, &sql_params);

        let mut stmt = db_ref
            .conn
            .prepare_cached(&sql)
            .unwrap_or_else(|e| panic!("knot runtime: read_where query error: {}", e));
        let mut rows: Vec<*mut Value> = Vec::new();
        let mut result_rows = stmt
            .query(param_refs.as_slice())
            .unwrap_or_else(|e| panic!("knot runtime: read_where query exec error: {}", e));

        while let Some(row) = result_rows
            .next()
            .unwrap_or_else(|e| panic!("knot runtime: read_where row fetch error: {}", e))
        {
            let tag: String = row.get(0).unwrap();
            let ctor = adt.constructors.iter().find(|c| c.name == tag);
            let payload = if let Some(ctor) = ctor {
                if ctor.fields.is_empty() {
                    alloc(Value::Unit)
                } else {
                    let record = knot_record_empty(ctor.fields.len());
                    for field in &ctor.fields {
                        let col_idx = *field_idx.get(field.name.as_str()).unwrap_or_else(|| {
                            panic!(
                                "knot runtime: schema mismatch in `{}` — constructor `{}` field `{}` not present in stored ADT layout",
                                name, tag, field.name
                            )
                        });
                        let val = read_sql_column(row, col_idx + 1, field.ty);
                        let fname = field.name.as_bytes();
                        knot_record_set_field(record, fname.as_ptr(), fname.len(), val);
                    }
                    record
                }
            } else {
                let record = knot_record_empty(adt.all_fields.len());
                let mut has_fields = false;
                for (i, field) in adt.all_fields.iter().enumerate() {
                    if !matches!(row.get_ref(i + 1).unwrap(), ValueRef::Null) {
                        let val = read_sql_column(row, i + 1, field.ty);
                        let fname = field.name.as_bytes();
                        knot_record_set_field(record, fname.as_ptr(), fname.len(), val);
                        has_fields = true;
                    }
                }
                if has_fields { record } else { alloc(Value::Unit) }
            };
            rows.push(alloc(Value::Constructor(intern_str(&tag), payload)));
        }
        alloc(Value::Relation(rows))
    } else {
        let rec = parse_record_schema(schema);
        let table_q = quote_ident(&table_name);
        let has_children = !rec.nested.is_empty();

        let mut select_cols: Vec<String> = Vec::new();
        if has_children {
            select_cols.push(quote_ident("_id"));
        }
        for c in &rec.columns {
            select_cols.push(quote_ident(&c.name));
        }

        let sql = format!(
            "SELECT {} FROM {} WHERE {}",
            if select_cols.is_empty() { "1".to_string() } else { select_cols.join(", ") },
            table_q,
            where_clause
        );
        debug_sql_params(&sql, &sql_params);

        let mut stmt = db_ref
            .conn
            .prepare_cached(&sql)
            .unwrap_or_else(|e| panic!("knot runtime: read_where query error: {}", e));
        let mut rows: Vec<*mut Value> = Vec::new();
        let mut result_rows = stmt
            .query(param_refs.as_slice())
            .unwrap_or_else(|e| panic!("knot runtime: read_where query exec error: {}", e));

        while let Some(row) = result_rows
            .next()
            .unwrap_or_else(|e| panic!("knot runtime: read_where row fetch error: {}", e))
        {
            let total_fields = rec.columns.len() + rec.nested.len();
            let record = knot_record_empty(total_fields);
            let col_offset = if has_children { 1 } else { 0 };

            for (i, col) in rec.columns.iter().enumerate() {
                let val = read_sql_column(row, i + col_offset, col.ty);
                let cname = col.name.as_bytes();
                knot_record_set_field(record, cname.as_ptr(), cname.len(), val);
            }

            if has_children {
                let parent_id: i64 = row.get(0).unwrap();
                for nf in &rec.nested {
                    let child_table_name = format!("{}__{}", table_name, nf.name);
                    let val = read_child_table(&db_ref.conn, &child_table_name, nf, parent_id);
                    let fname = nf.name.as_bytes();
                    knot_record_set_field(record, fname.as_ptr(), fname.len(), val);
                }
            }

            rows.push(record);
        }
        alloc(Value::Relation(rows))
    }
}

/// Execute an arbitrary SQL SELECT and return results as a relation of records.
/// Used by the compiler for full SQL query compilation of do-blocks.
///
/// `sql` is the complete SELECT statement (with `?` placeholders).
/// `result_schema` is a record schema descriptor for constructing result records
/// (e.g., `"name:text,dept:text,budget:int"`).
/// `params` is a Relation of parameter values to bind to `?` placeholders.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_query(
    db: *mut c_void,
    sql_ptr: *const u8,
    sql_len: usize,
    result_schema_ptr: *const u8,
    result_schema_len: usize,
    params: *mut Value,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let sql = unsafe { str_from_raw(sql_ptr, sql_len) };
    let result_schema = unsafe { str_from_raw(result_schema_ptr, result_schema_len) };

    let param_values = match unsafe { as_ref(params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: source_query params must be a Relation, got {}",
            type_name(params)
        ),
    };
    let sql_params: Vec<rusqlite::types::Value> =
        param_values.iter().map(|v| value_to_sql_param(*v)).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        sql_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    debug_sql_params(sql, &sql_params);

    let rec = parse_record_schema(result_schema);

    let mut stmt = db_ref
        .conn
        .prepare_cached(sql)
        .unwrap_or_else(|e| panic!("knot runtime: source_query error: {}\n  SQL: {}", e, sql));
    let mut rows: Vec<*mut Value> = Vec::new();
    let mut result_rows = stmt
        .query(param_refs.as_slice())
        .unwrap_or_else(|e| panic!("knot runtime: source_query exec error: {}\n  SQL: {}", e, sql));

    while let Some(row) = result_rows
        .next()
        .unwrap_or_else(|e| panic!("knot runtime: source_query row fetch error: {}", e))
    {
        let record = knot_record_empty(rec.columns.len());
        for (i, col) in rec.columns.iter().enumerate() {
            let val = read_sql_column(row, i, col.ty);
            let cname = col.name.as_bytes();
            knot_record_set_field(record, cname.as_ptr(), cname.len(), val);
        }
        rows.push(record);
    }
    alloc(Value::Relation(rows))
}

/// Read rows from a source ADT relation matching a specific constructor tag.
/// Executes `SELECT <ctor_fields> FROM table WHERE _tag = ?` at the SQL level.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_match(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    tag_ptr: *const u8,
    tag_len: usize,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };
    let tag = unsafe { str_from_raw(tag_ptr, tag_len) };

    let table = quote_ident(&format!("_knot_{}", name));
    let adt = parse_adt_schema(schema);

    let ctor = adt
        .constructors
        .iter()
        .find(|c| c.name == tag)
        .unwrap_or_else(|| panic!("knot runtime: match: unknown constructor '{}'", tag));

    if ctor.fields.is_empty() {
        // Nullary constructor: count matching rows, return that many Unit values
        let sql = format!(
            "SELECT COUNT(*) FROM {} WHERE {} = ?1",
            table,
            quote_ident("_tag")
        );
        debug_sql(&sql);
        let count: i64 = db_ref
            .conn
            .query_row(&sql, rusqlite::params![tag], |row| row.get(0))
            .unwrap();
        let mut rows = Vec::with_capacity(count as usize);
        for _ in 0..count {
            rows.push(alloc(Value::Unit));
        }
        alloc(Value::Relation(rows))
    } else {
        let select_cols: Vec<String> =
            ctor.fields.iter().map(|f| quote_ident(&f.name)).collect();
        let sql = format!(
            "SELECT {} FROM {} WHERE {} = ?1",
            select_cols.join(", "),
            table,
            quote_ident("_tag")
        );
        debug_sql(&sql);

        let mut stmt = db_ref
            .conn
            .prepare_cached(&sql)
            .unwrap_or_else(|e| panic!("knot runtime: match query error: {}", e));
        let mut rows: Vec<*mut Value> = Vec::new();
        let mut result_rows = stmt
            .query(rusqlite::params![tag])
            .unwrap_or_else(|e| panic!("knot runtime: match query exec error: {}", e));

        while let Some(row) = result_rows
            .next()
            .unwrap_or_else(|e| panic!("knot runtime: match row fetch error: {}", e))
        {
            let record = knot_record_empty(ctor.fields.len());
            for (i, field) in ctor.fields.iter().enumerate() {
                let val = read_sql_column(row, i, field.ty);
                let fname = field.name.as_bytes();
                knot_record_set_field(record, fname.as_ptr(), fname.len(), val);
            }
            rows.push(record);
        }
        alloc(Value::Relation(rows))
    }
}

/// Read all rows from a record table, including nested relation fields from child tables.
fn read_record_table(
    conn: &rusqlite::Connection,
    table_name: &str,
    schema: &RecordSchema,
) -> *mut Value {
    let table = quote_ident(table_name);
    let has_children = !schema.nested.is_empty();

    // Build SELECT: _id (if has children) + scalar columns
    let mut select_cols: Vec<String> = Vec::new();
    if has_children {
        select_cols.push(quote_ident("_id"));
    }
    for c in &schema.columns {
        select_cols.push(quote_ident(&c.name));
    }

    let sql = format!(
        "SELECT {} FROM {}",
        if select_cols.is_empty() { "1".to_string() } else { select_cols.join(", ") },
        table
    );
    debug_sql(&sql);

    let mut stmt = conn
        .prepare_cached(&sql)
        .unwrap_or_else(|e| panic!("knot runtime: query error: {}", e));
    let mut rows: Vec<*mut Value> = Vec::new();
    let mut result_rows = stmt
        .query([])
        .unwrap_or_else(|e| panic!("knot runtime: query exec error: {}", e));

    while let Some(row) = result_rows
        .next()
        .unwrap_or_else(|e| panic!("knot runtime: row fetch error: {}", e))
    {
        let total_fields = schema.columns.len() + schema.nested.len();
        let record = knot_record_empty(total_fields);
        let col_offset = if has_children { 1 } else { 0 }; // skip _id column

        // Read scalar columns
        for (i, col) in schema.columns.iter().enumerate() {
            let val = read_sql_column(row, i + col_offset, col.ty);
            let name = col.name.as_bytes();
            knot_record_set_field(record, name.as_ptr(), name.len(), val);
        }

        // Read nested relation fields from child tables
        if has_children {
            let parent_id: i64 = row.get(0).unwrap();
            for nf in &schema.nested {
                let child_table_name = format!("{}__{}", table_name, nf.name);
                let val = read_child_table(conn, &child_table_name, nf, parent_id);
                let name = nf.name.as_bytes();
                knot_record_set_field(record, name.as_ptr(), name.len(), val);
            }
        }

        rows.push(record);
    }

    alloc(Value::Relation(rows))
}

/// Read child rows for a nested relation field, filtered by parent_id.
fn read_child_table(
    conn: &rusqlite::Connection,
    table_name: &str,
    nf: &NestedField,
    parent_id: i64,
) -> *mut Value {
    let table = quote_ident(table_name);
    let has_children = !nf.nested.is_empty();

    let mut select_cols: Vec<String> = Vec::new();
    if has_children {
        select_cols.push(quote_ident("_id"));
    }
    for c in &nf.columns {
        select_cols.push(quote_ident(&c.name));
    }

    let sql = format!(
        "SELECT {} FROM {} WHERE _parent_id = ?1",
        if select_cols.is_empty() { "1".to_string() } else { select_cols.join(", ") },
        table
    );
    debug_sql(&sql);

    let mut stmt = conn
        .prepare_cached(&sql)
        .unwrap_or_else(|e| panic!("knot runtime: child query error: {}", e));
    let mut rows: Vec<*mut Value> = Vec::new();
    let mut result_rows = stmt
        .query(rusqlite::params![parent_id])
        .unwrap_or_else(|e| panic!("knot runtime: child query exec error: {}", e));

    while let Some(row) = result_rows
        .next()
        .unwrap_or_else(|e| panic!("knot runtime: child row fetch error: {}", e))
    {
        let total_fields = nf.columns.len() + nf.nested.len();
        let record = knot_record_empty(total_fields);
        let col_offset = if has_children { 1 } else { 0 };

        for (i, col) in nf.columns.iter().enumerate() {
            let val = read_sql_column(row, i + col_offset, col.ty);
            let name = col.name.as_bytes();
            knot_record_set_field(record, name.as_ptr(), name.len(), val);
        }

        if has_children {
            let child_id: i64 = row.get(0).unwrap();
            for grandchild in &nf.nested {
                let gc_table = format!("{}__{}", table_name, grandchild.name);
                let val = read_child_table(conn, &gc_table, grandchild, child_id);
                let name = grandchild.name.as_bytes();
                knot_record_set_field(record, name.as_ptr(), name.len(), val);
            }
        }

        rows.push(record);
    }

    alloc(Value::Relation(rows))
}

/// Serialize a Constructor value into SQL params for an ADT wide table.
/// Returns params for [_tag, field1, field2, ...] columns.
fn adt_row_to_params(
    row_ptr: *mut Value,
    adt: &AdtSpec,
) -> Vec<rusqlite::types::Value> {
    let row = unsafe { as_ref(row_ptr) };
    match row {
        Value::Constructor(tag, payload) => {
            let mut params = Vec::with_capacity(1 + adt.all_fields.len());
            // First column: _tag
            params.push(rusqlite::types::Value::Text(tag.to_string()));

            // Find which fields belong to this constructor
            let ctor = adt.constructors.iter().find(|c| c.name.as_str() == &**tag);
            let ctor_field_names: HashSet<&str> = ctor
                .map(|c| c.fields.iter().map(|f| f.name.as_str()).collect())
                .unwrap_or_default();

            // For each field in the wide table
            for field in &adt.all_fields {
                if ctor_field_names.contains(field.name.as_str()) {
                    // This field belongs to this constructor — extract from payload
                    let payload_ref = unsafe { as_ref(*payload) };
                    match payload_ref {
                        Value::Record(fields) => {
                            let val = fields
                                .iter()
                                .find(|f| &*f.name == field.name.as_str())
                                .map(|f| value_to_sqlite(f.value, field.ty))
                                .unwrap_or(rusqlite::types::Value::Null);
                            params.push(val);
                        }
                        _ => params.push(rusqlite::types::Value::Null),
                    }
                } else {
                    // Field doesn't belong to this constructor — NULL
                    params.push(rusqlite::types::Value::Null);
                }
            }
            params
        }
        _ => panic!(
            "knot runtime: ADT source rows must be Constructors, got {}",
            type_name(row_ptr)
        ),
    }
}

/// Delete all rows from a record table and its child tables (children first).
fn delete_record_table(conn: &rusqlite::Connection, table_name: &str, schema: &RecordSchema) {
    // Delete children first
    for nf in &schema.nested {
        delete_child_table(conn, table_name, nf);
    }
    let sql = format!("DELETE FROM {};", quote_ident(table_name));
    debug_sql(&sql);
    conn.execute_batch(&sql).expect("knot runtime: failed to delete rows");
}

fn delete_child_table(conn: &rusqlite::Connection, parent_table: &str, nf: &NestedField) {
    let child_table = format!("{}__{}", parent_table, nf.name);
    // Recurse to delete grandchildren first
    for grandchild in &nf.nested {
        delete_child_table(conn, &child_table, grandchild);
    }
    let sql = format!("DELETE FROM {};", quote_ident(&child_table));
    debug_sql(&sql);
    conn.execute_batch(&sql).expect("knot runtime: failed to delete child rows");
}

/// Delete child rows for a specific parent _id, recursing for deeper nesting.
fn delete_child_rows_for_parent(conn: &rusqlite::Connection, child_table: &str, parent_id: i64, nf: &NestedField) {
    // If this child has its own children, collect its _ids first and recurse
    if !nf.nested.is_empty() {
        let select_sql = format!("SELECT _id FROM {} WHERE _parent_id = ?1;", quote_ident(child_table));
        if let Ok(mut stmt) = conn.prepare(&select_sql) {
            let ids: Vec<i64> = stmt
                .query_map([parent_id], |row| row.get::<_, i64>(0))
                .into_iter()
                .flatten()
                .filter_map(|r| r.ok())
                .collect();
            for grandchild in &nf.nested {
                let gc_table = format!("{}__{}", child_table, grandchild.name);
                for &child_id in &ids {
                    delete_child_rows_for_parent(conn, &gc_table, child_id, grandchild);
                }
            }
        }
    }
    let sql = format!("DELETE FROM {} WHERE _parent_id = ?1;", quote_ident(child_table));
    debug_sql(&sql);
    conn.execute(&sql, [parent_id]).expect("knot runtime: failed to delete child rows for parent");
}

/// Insert rows into a record table and its child tables.
fn write_record_rows(
    conn: &rusqlite::Connection,
    table_name: &str,
    schema: &RecordSchema,
    rows: &[*mut Value],
) {
    if rows.is_empty() {
        return;
    }

    let table = quote_ident(table_name);
    let has_children = !schema.nested.is_empty();

    // Build INSERT for scalar columns only
    let col_names: Vec<String> = schema.columns.iter().map(|c| quote_ident(&c.name)).collect();
    if col_names.is_empty() && !has_children {
        // Unit-type relation: insert rows via the _dummy column
        let sql = format!("INSERT INTO {} (\"_dummy\") VALUES (0);", table);
        let mut stmt = conn.prepare_cached(&sql)
            .expect("knot runtime: prepare unit insert failed");
        for _ in rows.iter() {
            stmt.execute([]).expect("knot runtime: failed to insert unit row");
        }
        return;
    }

    let placeholders: Vec<String> = (1..=col_names.len()).map(|i| format!("?{}", i)).collect();

    // For tables with children, we need the _id back.
    // Use INSERT OR IGNORE to handle duplicate parent rows gracefully,
    // then look up the existing _id if the insert was ignored.
    let insert_sql = if has_children && !col_names.is_empty() {
        format!(
            "INSERT OR IGNORE INTO {} ({}) VALUES ({});",
            table, col_names.join(", "), placeholders.join(", ")
        )
    } else if has_children {
        // No scalar columns, just get an _id
        format!("INSERT INTO {} DEFAULT VALUES;", table)
    } else {
        format!(
            "INSERT OR IGNORE INTO {} ({}) VALUES ({});",
            table, col_names.join(", "), placeholders.join(", ")
        )
    };
    debug_sql(&insert_sql);

    // Prepare a SELECT to look up existing _id when INSERT OR IGNORE skips a duplicate
    let select_id_sql = if has_children && !col_names.is_empty() {
        let where_conds: Vec<String> = col_names
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{} IS ?{}", c, i + 1))
            .collect();
        Some(format!(
            "SELECT _id FROM {} WHERE {} LIMIT 1;",
            table,
            where_conds.join(" AND ")
        ))
    } else {
        None
    };

    let mut stmt = conn.prepare_cached(&insert_sql).expect("knot runtime: failed to prepare insert");

    for row_ptr in rows {
        let row = unsafe { as_ref(*row_ptr) };
        let fields = match row {
            Value::Record(fields) => fields,
            _ => panic!("knot runtime: relation rows must be Records, got {}", type_name(*row_ptr)),
        };

        // Build field lookup map for O(1) access
        let field_map: HashMap<&str, *mut Value> = fields.iter().map(|f| (&*f.name, f.value)).collect();

        // Build scalar params
        let params: Vec<rusqlite::types::Value> = schema.columns
            .iter()
            .map(|col| {
                let value = field_map.get(col.name.as_str())
                    .unwrap_or_else(|| panic!("knot runtime: missing field '{}' in record", col.name));
                value_to_sqlite(*value, col.ty)
            })
            .collect();

        if !params.is_empty() {
            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
            stmt.execute(param_refs.as_slice()).unwrap_or_else(|e| {
                panic!("knot runtime: insert error: {}", e)
            });
        } else {
            stmt.execute([]).unwrap_or_else(|e| {
                panic!("knot runtime: insert error: {}", e)
            });
        }

        // Write nested relation fields to child tables
        if has_children {
            let parent_id = if conn.changes() == 0 {
                // INSERT OR IGNORE skipped this row (duplicate) — look up existing _id
                if let Some(ref sql) = select_id_sql {
                    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                        params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
                    conn.query_row(sql, param_refs.as_slice(), |row| row.get::<_, i64>(0))
                        .expect("knot runtime: failed to look up existing parent _id")
                } else {
                    // No scalar columns — DEFAULT VALUES always inserts, so this shouldn't happen
                    conn.last_insert_rowid()
                }
            } else {
                conn.last_insert_rowid()
            };
            for nf in &schema.nested {
                let child_table = format!("{}__{}", table_name, nf.name);
                let child_val = field_map.get(nf.name.as_str())
                    .copied()
                    .unwrap_or(std::ptr::null_mut());
                if !child_val.is_null() {
                    if let Value::Relation(child_rows) = unsafe { as_ref(child_val) } {
                        write_child_rows(conn, &child_table, nf, parent_id, child_rows);
                    }
                }
            }
        }
    }
}

/// Insert rows into a child table for a nested relation field.
fn write_child_rows(
    conn: &rusqlite::Connection,
    table_name: &str,
    nf: &NestedField,
    parent_id: i64,
    rows: &[*mut Value],
) {
    if rows.is_empty() {
        return;
    }

    let table = quote_ident(table_name);
    let has_children = !nf.nested.is_empty();

    let mut col_names = vec![quote_ident("_parent_id")];
    for c in &nf.columns {
        col_names.push(quote_ident(&c.name));
    }
    let placeholders: Vec<String> = (1..=col_names.len()).map(|i| format!("?{}", i)).collect();

    let insert_sql = format!(
        "INSERT OR IGNORE INTO {} ({}) VALUES ({});",
        table, col_names.join(", "), placeholders.join(", ")
    );
    debug_sql(&insert_sql);

    // Prepare a SELECT to look up existing _id when INSERT OR IGNORE skips a duplicate
    let select_id_sql = if has_children && !nf.columns.is_empty() {
        let where_conds: Vec<String> = col_names
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{} IS ?{}", c, i + 1))
            .collect();
        Some(format!(
            "SELECT _id FROM {} WHERE {} LIMIT 1;",
            table,
            where_conds.join(" AND ")
        ))
    } else {
        None
    };

    let mut stmt = conn.prepare_cached(&insert_sql).expect("knot runtime: failed to prepare child insert");

    for row_ptr in rows {
        let row = unsafe { as_ref(*row_ptr) };
        let fields = match row {
            Value::Record(fields) => fields,
            _ => panic!("knot runtime: child rows must be Records"),
        };

        let field_map: HashMap<&str, *mut Value> = fields.iter().map(|f| (&*f.name, f.value)).collect();

        let mut params: Vec<rusqlite::types::Value> = vec![
            rusqlite::types::Value::Integer(parent_id),
        ];
        for col in &nf.columns {
            let value = field_map.get(col.name.as_str())
                .unwrap_or_else(|| panic!("knot runtime: missing field '{}' in child record", col.name));
            params.push(value_to_sqlite(*value, col.ty));
        }

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
        stmt.execute(param_refs.as_slice()).unwrap_or_else(|e| {
            panic!("knot runtime: child insert error: {}", e)
        });

        // Recurse for deeper nesting
        if has_children {
            let child_id = if conn.changes() == 0 {
                // INSERT OR IGNORE skipped this row (duplicate) — look up existing _id
                if let Some(ref sql) = select_id_sql {
                    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                        params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
                    conn.query_row(sql, param_refs.as_slice(), |row| row.get::<_, i64>(0))
                        .expect("knot runtime: failed to look up existing child _id")
                } else {
                    conn.last_insert_rowid()
                }
            } else {
                conn.last_insert_rowid()
            };
            for grandchild in &nf.nested {
                let gc_table = format!("{}__{}", table_name, grandchild.name);
                let gc_val = field_map.get(grandchild.name.as_str())
                    .copied()
                    .unwrap_or(std::ptr::null_mut());
                if !gc_val.is_null() {
                    if let Value::Relation(gc_rows) = unsafe { as_ref(gc_val) } {
                        write_child_rows(conn, &gc_table, grandchild, child_id, gc_rows);
                    }
                }
            }
        }
    }
}

/// Write a relation to a source (replaces all rows).
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_write(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    relation: *mut Value,
) {
    let _wl = write_lock_guard();
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };

    let rows = match unsafe { as_ref(relation) } {
        Value::Relation(rows) => rows,
        _ => panic!("knot runtime: source_write expects a Relation, got {}", type_name(relation)),
    };

    // Delete all existing rows and insert new ones in a transaction
    db_ref
        .conn
        .execute_batch("SAVEPOINT knot_replace;")
        .expect("knot runtime: failed to begin transaction");

    let table_name = format!("_knot_{}", name);

    if is_adt_schema(schema) {
        let table = quote_ident(&table_name);
        let delete_sql = format!("DELETE FROM {};", table);
        debug_sql(&delete_sql);
        db_ref.conn.execute_batch(&delete_sql)
            .expect("knot runtime: failed to delete rows");

        let adt = parse_adt_schema(schema);
        if !rows.is_empty() {
            let mut col_names = vec![quote_ident("_tag")];
            for f in &adt.all_fields {
                col_names.push(quote_ident(&f.name));
            }
            let placeholders: Vec<String> = (1..=col_names.len())
                .map(|i| format!("?{}", i))
                .collect();
            let insert_sql = format!(
                "INSERT OR IGNORE INTO {} ({}) VALUES ({});",
                table,
                col_names.join(", "),
                placeholders.join(", ")
            );
            debug_sql(&insert_sql);

            let mut stmt = db_ref
                .conn
                .prepare_cached(&insert_sql)
                .expect("knot runtime: failed to prepare insert");

            for row_ptr in rows {
                let params = adt_row_to_params(*row_ptr, &adt);
                let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                    params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
                stmt.execute(param_refs.as_slice()).unwrap_or_else(|e| {
                    panic!("knot runtime: insert error: {}", e)
                });
            }
        }
    } else {
        let rec = parse_record_schema(schema);
        // Delete child tables first (deepest first), then parent
        delete_record_table(&db_ref.conn, &table_name, &rec);
        // Insert all rows
        write_record_rows(&db_ref.conn, &table_name, &rec, rows);
    }

    db_ref
        .conn
        .execute_batch("RELEASE SAVEPOINT knot_replace;")
        .expect("knot runtime: failed to commit transaction");
    if db_ref.atomic_depth.get() > 0 {
        stm_track_write(name);
    } else {
        notify_relation_changed(name);
    }
}

/// Append rows to a source relation (INSERT only, no DELETE).
/// Used when the compiler detects `set *rel = union *rel <new_rows>`.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_append(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    relation: *mut Value,
) {
    let _wl = write_lock_guard();
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };

    let rows = match unsafe { as_ref(relation) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: source_append expects a Relation, got {}",
            type_name(relation)
        ),
    };

    if rows.is_empty() {
        return;
    }

    let table = quote_ident(&format!("_knot_{}", name));

    db_ref
        .conn
        .execute_batch("SAVEPOINT knot_set;")
        .expect("knot runtime: failed to begin transaction");

    if is_adt_schema(schema) {
        let adt = parse_adt_schema(schema);
        let mut col_names = vec![quote_ident("_tag")];
        for f in &adt.all_fields {
            col_names.push(quote_ident(&f.name));
        }
        let placeholders: Vec<String> = (1..=col_names.len())
            .map(|i| format!("?{}", i))
            .collect();
        let insert_sql = format!(
            "INSERT OR IGNORE INTO {} ({}) VALUES ({});",
            table,
            col_names.join(", "),
            placeholders.join(", ")
        );
        debug_sql(&insert_sql);

        let mut stmt = db_ref
            .conn
            .prepare_cached(&insert_sql)
            .expect("knot runtime: failed to prepare insert");

        for row_ptr in rows {
            let params = adt_row_to_params(*row_ptr, &adt);
            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
            stmt.execute(param_refs.as_slice()).unwrap_or_else(|e| {
                panic!("knot runtime: insert error: {}", e)
            });
        }
    } else {
        let rec = parse_record_schema(schema);
        write_record_rows(&db_ref.conn, &format!("_knot_{}", name), &rec, rows);
    }

    db_ref
        .conn
        .execute_batch("RELEASE SAVEPOINT knot_set;")
        .expect("knot runtime: failed to commit transaction");
    if db_ref.atomic_depth.get() > 0 {
        stm_track_write(name);
    } else {
        notify_relation_changed(name);
    }
}

/// Diff-based write: compute minimal INSERT/DELETE against the existing table.
/// Used when the value expression reads from the same source relation.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_diff_write(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    relation: *mut Value,
) {
    let _wl = write_lock_guard();
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };

    let rows = match unsafe { as_ref(relation) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: source_diff_write expects a Relation, got {}",
            type_name(relation)
        ),
    };

    let table = quote_ident(&format!("_knot_{}", name));
    let temp = quote_ident(&format!("_knot_{}_new", name));

    db_ref
        .conn
        .execute_batch("SAVEPOINT knot_diff_write;")
        .expect("knot runtime: failed to begin transaction");

    if is_adt_schema(schema) {
        let adt = parse_adt_schema(schema);
        let mut col_names = vec![quote_ident("_tag")];
        for f in &adt.all_fields {
            col_names.push(quote_ident(&f.name));
        }
        let col_str = col_names.join(", ");
        let n_cols = col_names.len();

        // Build match conditions: _tag equality + IS for NULL-safe field comparison
        let build_adt_match = |src: &str| -> Vec<String> {
            std::iter::once(
                format!("{s}.{c} = {t}.{c}", s = src, t = table, c = quote_ident("_tag"))
            ).chain(adt.all_fields.iter().map(|f| {
                let c = quote_ident(&f.name);
                format!("{}.{} IS {}.{}", src, c, table, c)
            })).collect()
        };

        if !rows.is_empty() && rows.len() * n_cols <= MAX_VALUES_PARAMS {
            // VALUES CTE path
            let mut all_params = Vec::with_capacity(rows.len() * n_cols);
            let mut row_clauses = Vec::with_capacity(rows.len());
            let mut pidx = 1usize;
            for row_ptr in rows {
                let rp = adt_row_to_params(*row_ptr, &adt);
                let ph: Vec<String> = rp.iter()
                    .map(|_| { let p = format!("?{}", pidx); pidx += 1; p })
                    .collect();
                row_clauses.push(format!("({})", ph.join(", ")));
                all_params.extend(rp);
            }
            let values_sql = format!("VALUES {}", row_clauses.join(", "));
            let match_conds = build_adt_match("_new");

            let delete_sql = format!(
                "WITH _new({c}) AS ({v}) \
                 DELETE FROM {t} WHERE NOT EXISTS (SELECT 1 FROM _new WHERE {m});",
                c = col_str, v = values_sql, t = table, m = match_conds.join(" AND ")
            );
            debug_sql(&delete_sql);
            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                all_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
            db_ref.conn.prepare(&delete_sql)
                .and_then(|mut s| s.execute(param_refs.as_slice()))
                .expect("knot runtime: failed to delete removed rows");

            let insert_sql = format!(
                "WITH _new({c}) AS ({v}) \
                 INSERT OR IGNORE INTO {t} ({c}) SELECT * FROM _new;",
                c = col_str, v = values_sql, t = table
            );
            debug_sql(&insert_sql);
            db_ref.conn.prepare(&insert_sql)
                .and_then(|mut s| s.execute(param_refs.as_slice()))
                .expect("knot runtime: failed to insert new rows");
        } else {
            // Temp table fallback (handles empty rows and large datasets)
            let match_conds = build_adt_match(&temp);

            let mut col_defs = vec![format!("{} TEXT NOT NULL", quote_ident("_tag"))];
            for f in &adt.all_fields {
                col_defs.push(format!("{} {}", quote_ident(&f.name), sql_type(f.ty)));
            }
            let create_temp = format!("CREATE TEMP TABLE {} ({});", temp, col_defs.join(", "));
            debug_sql(&create_temp);
            db_ref.conn.execute_batch(&create_temp)
                .expect("knot runtime: failed to create temp table");

            if !rows.is_empty() {
                let placeholders: Vec<String> = (1..=n_cols).map(|i| format!("?{}", i)).collect();
                let insert_sql = format!(
                    "INSERT INTO {} ({}) VALUES ({});", temp, col_str, placeholders.join(", ")
                );
                debug_sql(&insert_sql);
                let mut stmt = db_ref.conn.prepare_cached(&insert_sql)
                    .expect("knot runtime: failed to prepare temp insert");
                for row_ptr in rows {
                    let params = adt_row_to_params(*row_ptr, &adt);
                    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                        params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
                    stmt.execute(param_refs.as_slice())
                        .unwrap_or_else(|e| panic!("knot runtime: temp insert error: {}", e));
                }
            }

            let delete_sql = format!(
                "DELETE FROM {} WHERE NOT EXISTS (SELECT 1 FROM {} WHERE {});",
                table, temp, match_conds.join(" AND ")
            );
            debug_sql(&delete_sql);
            db_ref.conn.execute_batch(&delete_sql)
                .expect("knot runtime: failed to delete removed rows");

            let insert_new_sql = format!(
                "INSERT OR IGNORE INTO {} ({}) SELECT {} FROM {};",
                table, col_str, col_str, temp
            );
            debug_sql(&insert_new_sql);
            db_ref.conn.execute_batch(&insert_new_sql)
                .expect("knot runtime: failed to insert new rows");

            let drop_sql = format!("DROP TABLE IF EXISTS {};", temp);
            debug_sql(&drop_sql);
            db_ref.conn.execute_batch(&drop_sql)
                .expect("knot runtime: failed to drop temp table");
        }
    } else {
        let rec_schema = parse_record_schema(schema);

        // If there are nested relation fields, fall back to full clear + rewrite
        // since the diff logic only handles scalar columns.
        if !rec_schema.nested.is_empty() {
            let table_name = format!("_knot_{}", name);
            delete_record_table(&db_ref.conn, &table_name, &rec_schema);
            write_record_rows(&db_ref.conn, &table_name, &rec_schema, rows);

            db_ref.conn.execute_batch("RELEASE SAVEPOINT knot_diff_write;")
                .expect("knot runtime: failed to commit transaction");
            if db_ref.atomic_depth.get() > 0 {
                stm_track_write(name);
            } else {
                notify_relation_changed(name);
            }
            return;
        }

        // Zero-column records have nothing to diff — fall back to clear + rewrite
        if rec_schema.columns.is_empty() {
            let table_name = format!("_knot_{}", name);
            delete_record_table(&db_ref.conn, &table_name, &rec_schema);
            write_record_rows(&db_ref.conn, &table_name, &rec_schema, rows);

            db_ref.conn.execute_batch("RELEASE SAVEPOINT knot_diff_write;")
                .expect("knot runtime: failed to commit transaction");
            if db_ref.atomic_depth.get() > 0 {
                stm_track_write(name);
            } else {
                notify_relation_changed(name);
            }
            return;
        }

        let cols = rec_schema.columns;
        let col_names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();
        let col_str = col_names.join(", ");
        let n_cols = cols.len();

        // Build NULL-safe match conditions
        let build_rec_match = |src: &str| -> Vec<String> {
            cols.iter().map(|c| {
                let cq = quote_ident(&c.name);
                format!(
                    "({s}.{c} = {t}.{c} OR ({s}.{c} IS NULL AND {t}.{c} IS NULL))",
                    s = src, t = table, c = cq
                )
            }).collect()
        };

        // Extract record row to SQL params
        let rec_row_to_params = |row_ptr: *mut Value| -> Vec<rusqlite::types::Value> {
            match unsafe { as_ref(row_ptr) } {
                Value::Record(fields) => cols.iter().map(|col| {
                    let field = fields.iter().find(|f| &*f.name == col.name.as_str())
                        .unwrap_or_else(|| panic!("knot runtime: missing field '{}' in record", col.name));
                    value_to_sqlite(field.value, col.ty)
                }).collect(),
                _ => panic!("knot runtime: relation rows must be Records, got {}", type_name(row_ptr)),
            }
        };

        if !rows.is_empty() && rows.len() * n_cols <= MAX_VALUES_PARAMS {
            // VALUES CTE path
            let mut all_params = Vec::with_capacity(rows.len() * n_cols);
            let mut row_clauses = Vec::with_capacity(rows.len());
            let mut pidx = 1usize;
            for row_ptr in rows {
                let rp = rec_row_to_params(*row_ptr);
                let ph: Vec<String> = rp.iter()
                    .map(|_| { let p = format!("?{}", pidx); pidx += 1; p })
                    .collect();
                row_clauses.push(format!("({})", ph.join(", ")));
                all_params.extend(rp);
            }
            let values_sql = format!("VALUES {}", row_clauses.join(", "));
            let match_conds = build_rec_match("_new");

            let delete_sql = format!(
                "WITH _new({c}) AS ({v}) \
                 DELETE FROM {t} WHERE NOT EXISTS (SELECT 1 FROM _new WHERE {m});",
                c = col_str, v = values_sql, t = table, m = match_conds.join(" AND ")
            );
            debug_sql(&delete_sql);
            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                all_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
            db_ref.conn.prepare(&delete_sql)
                .and_then(|mut s| s.execute(param_refs.as_slice()))
                .expect("knot runtime: failed to delete removed rows");

            // INSERT rows not in main. Use NOT EXISTS to avoid duplicates
            // when writing through a projected view.
            let insert_sql = format!(
                "WITH _new({c}) AS ({v}) \
                 INSERT OR IGNORE INTO {t} ({c}) SELECT * FROM _new \
                 WHERE NOT EXISTS (SELECT 1 FROM {t} WHERE {m});",
                c = col_str, v = values_sql, t = table, m = match_conds.join(" AND ")
            );
            debug_sql(&insert_sql);
            db_ref.conn.prepare(&insert_sql)
                .and_then(|mut s| s.execute(param_refs.as_slice()))
                .expect("knot runtime: failed to insert new rows");
        } else {
            // Temp table fallback (handles empty rows and large datasets)
            let match_conds = build_rec_match(&temp);

            let col_defs: Vec<String> = cols.iter()
                .map(|c| format!("{} {}", quote_ident(&c.name), sql_type(c.ty)))
                .collect();
            let create_temp = format!("CREATE TEMP TABLE {} ({});", temp, col_defs.join(", "));
            debug_sql(&create_temp);
            db_ref.conn.execute_batch(&create_temp)
                .expect("knot runtime: failed to create temp table");

            if !rows.is_empty() {
                let placeholders: Vec<String> = (1..=n_cols).map(|i| format!("?{}", i)).collect();
                let insert_sql = format!(
                    "INSERT INTO {} ({}) VALUES ({});", temp, col_str, placeholders.join(", ")
                );
                debug_sql(&insert_sql);
                let mut stmt = db_ref.conn.prepare_cached(&insert_sql)
                    .expect("knot runtime: failed to prepare temp insert");
                for row_ptr in rows {
                    let params = rec_row_to_params(*row_ptr);
                    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                        params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
                    stmt.execute(param_refs.as_slice())
                        .unwrap_or_else(|e| panic!("knot runtime: temp insert error: {}", e));
                }
            }

            let delete_sql = format!(
                "DELETE FROM {} WHERE NOT EXISTS (SELECT 1 FROM {} WHERE {});",
                table, temp, match_conds.join(" AND ")
            );
            debug_sql(&delete_sql);
            db_ref.conn.execute_batch(&delete_sql)
                .expect("knot runtime: failed to delete removed rows");

            let insert_new_sql = format!(
                "INSERT OR IGNORE INTO {} ({}) SELECT {} FROM {} \
                 WHERE NOT EXISTS (SELECT 1 FROM {} WHERE {});",
                table, col_str, col_str, temp, table, match_conds.join(" AND ")
            );
            debug_sql(&insert_new_sql);
            db_ref.conn.execute_batch(&insert_new_sql)
                .expect("knot runtime: failed to insert new rows");

            let drop_sql = format!("DROP TABLE IF EXISTS {};", temp);
            debug_sql(&drop_sql);
            db_ref.conn.execute_batch(&drop_sql)
                .expect("knot runtime: failed to drop temp table");
        }
    }

    db_ref.conn.execute_batch("RELEASE SAVEPOINT knot_diff_write;")
        .expect("knot runtime: failed to commit transaction");
    if db_ref.atomic_depth.get() > 0 {
        stm_track_write(name);
    } else {
        notify_relation_changed(name);
    }
}

/// DELETE rows that don't match a WHERE condition.
/// Used for `set *rel = do { t <- *rel; where cond; yield t }`.
/// The where_clause is the *keep* condition; rows NOT matching are deleted.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_delete_where(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    where_ptr: *const u8,
    where_len: usize,
    params: *mut Value,
) {
    let _wl = write_lock_guard();
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let where_clause = unsafe { str_from_raw(where_ptr, where_len) };

    // Auto-index columns used in the WHERE clause
    let table = format!("_knot_{}", name);
    db_ref.ensure_indexes_for_where(&table, where_clause);

    let param_values = match unsafe { as_ref(params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: delete_where params must be a Relation, got {}",
            type_name(params)
        ),
    };

    let sql_params: Vec<rusqlite::types::Value> =
        param_values.iter().map(|v| value_to_sql_param(*v)).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        sql_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    // Cascade delete to child tables (nested relation fields) before
    // deleting parent rows.  Discover ALL descendant tables by querying
    // SQLite metadata for tables matching the `{parent}__{...}` pattern
    // (including grandchildren like `{parent}__{field}__{subfield}`).
    // Delete deepest tables first so FK ordering is respected.
    let qt = quote_ident(&table);
    let mut descendant_tables: Vec<String> = {
        let prefix = format!("{}__", table);
        let mut stmt = db_ref.conn.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?1"
        ).unwrap();
        stmt.query_map([format!("{}%", prefix)], |row| row.get::<_, String>(0))
            .into_iter()
            .flatten()
            .filter_map(|r| r.ok())
            .filter(|n| n.starts_with(&prefix))
            .collect()
    };
    // Sort by depth (number of `__` segments) ascending so direct children are deleted first.
    // Grandchild+ deletion uses `NOT IN (SELECT _id FROM parent_table)` to find orphans,
    // which requires the intermediate parent rows to already be gone.
    descendant_tables.sort_by(|a, b| {
        let depth_a = a.matches("__").count();
        let depth_b = b.matches("__").count();
        depth_a.cmp(&depth_b)
    });
    if !descendant_tables.is_empty() {
        // Collect _ids of parent rows that will be deleted
        let id_sql = format!(
            "SELECT _id FROM {} WHERE NOT ({});",
            qt, where_clause
        );
        debug_sql(&id_sql);
        if let Ok(mut stmt) = db_ref.conn.prepare(&id_sql) {
            let ids: Vec<i64> = stmt
                .query_map(param_refs.as_slice(), |row| row.get::<_, i64>(0))
                .into_iter()
                .flatten()
                .filter_map(|r| r.ok())
                .collect();
            if !ids.is_empty() {
                // For direct children, delete by _parent_id matching the deleted parent rows.
                // For grandchildren+, we need to find their parent IDs transitively.
                let direct_prefix = format!("{}__", table);
                for ct in &descendant_tables {
                    let suffix = &ct[direct_prefix.len()..];
                    if !suffix.contains("__") {
                        // Direct child: delete by parent _id
                        let del = format!(
                            "DELETE FROM {} WHERE _parent_id IN ({})",
                            quote_ident(ct),
                            ids.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(",")
                        );
                        debug_sql(&del);
                        let _ = db_ref.conn.execute_batch(&del);
                    } else {
                        // Grandchild+: find its immediate parent table and delete rows
                        // whose _parent_id no longer exists in the parent table.
                        let parent_table = &ct[..ct.rfind("__").unwrap()];
                        let del = format!(
                            "DELETE FROM {} WHERE _parent_id NOT IN (SELECT _id FROM {})",
                            quote_ident(ct),
                            quote_ident(parent_table)
                        );
                        debug_sql(&del);
                        let _ = db_ref.conn.execute_batch(&del);
                    }
                }
            }
        }
    }

    let sql = format!(
        "DELETE FROM {} WHERE NOT ({});",
        qt,
        where_clause
    );
    debug_sql_params(&sql, &sql_params);
    // Rebuild param_refs (moved above)
    let param_refs2: Vec<&dyn rusqlite::types::ToSql> =
        sql_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
    db_ref
        .conn
        .execute(&sql, param_refs2.as_slice())
        .unwrap_or_else(|e| panic!("knot runtime: delete_where error: {}\n  SQL: {}", e, sql));
    if db_ref.atomic_depth.get() > 0 {
        stm_track_write(name);
    } else {
        notify_relation_changed(name);
    }
}

/// UPDATE rows matching a WHERE condition with new field values.
/// Used for `set *rel = do { t <- *rel; yield (if cond then {t | ...} else t) }`.
/// Params relation contains SET values first, then WHERE values.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_update_where(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    set_clause_ptr: *const u8,
    set_clause_len: usize,
    where_ptr: *const u8,
    where_len: usize,
    params: *mut Value,
) {
    let _wl = write_lock_guard();
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let set_clause = unsafe { str_from_raw(set_clause_ptr, set_clause_len) };
    let where_clause = unsafe { str_from_raw(where_ptr, where_len) };

    // Auto-index columns used in the WHERE clause
    let table = format!("_knot_{}", name);
    db_ref.ensure_indexes_for_where(&table, where_clause);

    let param_values = match unsafe { as_ref(params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: update_where params must be a Relation, got {}",
            type_name(params)
        ),
    };

    let sql = format!(
        "UPDATE OR REPLACE {} SET {} WHERE {};",
        quote_ident(&table),
        set_clause,
        where_clause
    );

    let sql_params: Vec<rusqlite::types::Value> =
        param_values.iter().map(|v| value_to_sql_param(*v)).collect();
    debug_sql_params(&sql, &sql_params);
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        sql_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    db_ref
        .conn
        .execute(&sql, param_refs.as_slice())
        .unwrap_or_else(|e| panic!("knot runtime: update_where error: {}\n  SQL: {}", e, sql));
    if db_ref.atomic_depth.get() > 0 {
        stm_track_write(name);
    } else {
        notify_relation_changed(name);
    }
}

fn value_to_sql_param(v: *mut Value) -> rusqlite::types::Value {
    if v.is_null() {
        return rusqlite::types::Value::Null;
    }
    match unsafe { as_ref(v) } {
        Value::Int(n) => rusqlite::types::Value::Text(n.to_string()),
        Value::Float(n) => rusqlite::types::Value::Real(*n),
        Value::Text(s) => rusqlite::types::Value::Text((**s).to_string()),
        Value::Bool(b) => rusqlite::types::Value::Integer(*b as i64),
        Value::Bytes(b) => rusqlite::types::Value::Blob((**b).to_vec()),
        Value::Constructor(tag, _) => rusqlite::types::Value::Text(tag.to_string()),
        Value::Relation(_) | Value::Record(_) => {
            rusqlite::types::Value::Text(value_to_json(v))
        }
        _ => panic!(
            "knot runtime: cannot use {} as SQL parameter",
            brief_value(v)
        ),
    }
}

fn value_to_sqlite(v: *mut Value, ty: ColType) -> rusqlite::types::Value {
    if v.is_null() {
        return rusqlite::types::Value::Null;
    }
    match (unsafe { as_ref(v) }, ty) {
        (Value::Int(n), _) => rusqlite::types::Value::Text(n.to_string()),
        (Value::Float(n), _) => rusqlite::types::Value::Real(*n),
        (Value::Text(s), _) => rusqlite::types::Value::Text((**s).to_string()),
        (Value::Bool(b), _) => rusqlite::types::Value::Integer(*b as i64),
        (Value::Bytes(b), _) => rusqlite::types::Value::Blob((**b).to_vec()),
        (Value::Constructor(tag, _), ColType::Tag) => {
            rusqlite::types::Value::Text(tag.to_string())
        }
        (Value::Constructor(_, _), ColType::Json) => {
            rusqlite::types::Value::Text(value_to_json(v))
        }
        (Value::Constructor(tag, _), _) => rusqlite::types::Value::Text(tag.to_string()),
        (Value::Relation(_), ColType::Json) => {
            rusqlite::types::Value::Text(value_to_json(v))
        }
        (Value::Record(_), ColType::Json) => {
            rusqlite::types::Value::Text(value_to_json(v))
        }
        _ => panic!("knot runtime: cannot convert {} to SQL", brief_value(v)),
    }
}

// ── Temporal queries (history tracking) ───────────────────────────

/// Return current time as milliseconds since Unix epoch.
#[unsafe(no_mangle)]
pub extern "C" fn knot_now() -> *mut Value {
    let ms: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .expect("knot runtime: system clock milliseconds overflowed i64");
    knot_value_int(ms)
}

/// Sleep for the given number of milliseconds.
#[unsafe(no_mangle)]
pub extern "C" fn knot_sleep(ms_val: *mut Value) -> *mut Value {
    let ms: u64 = match unsafe { as_ref(ms_val) } {
        Value::Int(i) => u64::try_from(*i).expect("knot runtime: sleep duration must be non-negative"),
        _ => panic!("knot runtime: sleep expects Int argument"),
    };
    std::thread::sleep(std::time::Duration::from_millis(ms));
    alloc(Value::Unit)
}

// ── Random number generation ─────────────────────────────────────

/// Return a random integer in [0, bound).
#[unsafe(no_mangle)]
pub extern "C" fn knot_random_int(bound: *mut Value) -> *mut Value {
    let n: u64 = match unsafe { as_ref(bound) } {
        Value::Int(i) => u64::try_from(*i).expect("knot runtime: randomInt bound must be positive"),
        _ => panic!(
            "knot runtime: randomInt expected Int, got {}",
            type_name(bound)
        ),
    };
    assert!(n > 0, "knot runtime: randomInt bound must be > 0");
    // Rejection sampling to avoid modulo bias
    let threshold = u64::MAX - (u64::MAX % n);
    let result = loop {
        let mut buf = [0u8; 8];
        getrandom::fill(&mut buf).expect("knot runtime: failed to get random bytes");
        let raw = u64::from_le_bytes(buf);
        if raw < threshold {
            break raw % n;
        }
    };
    alloc_int(result as i64)
}

/// Return a random Float in [0.0, 1.0).
#[unsafe(no_mangle)]
pub extern "C" fn knot_random_float() -> *mut Value {
    let mut buf = [0u8; 8];
    getrandom::fill(&mut buf).expect("knot runtime: failed to get random bytes");
    let raw = u64::from_le_bytes(buf);
    // f64 has 53 mantissa bits; mask `raw` to 53 bits and divide by 2^53 so
    // the result is exactly representable and stays inside [0.0, 1.0). The
    // earlier `(raw as f64) / ((u64::MAX as f64) + 1.0)` formulation rounds
    // the divisor down to 2^64 (precision loss), so for `raw == u64::MAX`
    // the result was exactly 1.0, violating the half-open contract.
    let bits = raw >> 11;
    let result = (bits as f64) * f64::from_bits(0x3CA0_0000_0000_0000); // 2^-53
    alloc_float(result)
}

/// Initialize a history table for a source with `with history`.
/// Creates `_knot_{name}_history` with the same columns plus `_knot_valid_from`
/// and `_knot_valid_to` timestamp columns.
#[unsafe(no_mangle)]
pub extern "C" fn knot_history_init(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };

    let history_table = quote_ident(&format!("_knot_{}_history", name));

    let mut col_defs: Vec<String> = if is_adt_schema(schema) {
        let adt = parse_adt_schema(schema);
        let mut defs = vec![format!("{} TEXT NOT NULL", quote_ident("_tag"))];
        for f in &adt.all_fields {
            defs.push(format!("{} {}", quote_ident(&f.name), sql_type(f.ty)));
        }
        defs
    } else {
        let cols = parse_schema(schema);
        cols.iter()
            .map(|c| format!("{} {}", quote_ident(&c.name), sql_type(c.ty)))
            .collect()
    };
    col_defs.push("\"_knot_valid_from\" INTEGER NOT NULL".to_string());
    col_defs.push("\"_knot_valid_to\" INTEGER".to_string());

    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {} ({});",
        history_table,
        col_defs.join(", ")
    );
    debug_sql(&sql);
    db_ref.conn.execute_batch(&sql).unwrap_or_else(|e| {
        panic!(
            "knot runtime: failed to create history table for '{}': {}",
            name, e
        )
    });

    // Index on valid_from/valid_to for efficient temporal queries
    let idx_sql = format!(
        "CREATE INDEX IF NOT EXISTS {} ON {} (\"_knot_valid_from\", \"_knot_valid_to\");",
        quote_ident(&format!("_knot_{}_history_time", name)),
        history_table
    );
    debug_sql(&idx_sql);
    let _ = db_ref.conn.execute_batch(&idx_sql);
}

/// Snapshot the current state of a source into its history table.
/// Called before each write to a history-enabled source.
/// Closes out any open history rows (valid_to IS NULL) and inserts
/// the current state with valid_from = now and valid_to = NULL.
#[unsafe(no_mangle)]
pub extern "C" fn knot_history_snapshot(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };

    let col_names: Vec<String> = if is_adt_schema(schema) {
        let adt = parse_adt_schema(schema);
        let mut names = vec![quote_ident("_tag")];
        names.extend(adt.all_fields.iter().map(|f| quote_ident(&f.name)));
        names
    } else {
        let cols = parse_schema(schema);
        cols.iter().map(|c| quote_ident(&c.name)).collect()
    };

    let now_ms: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .expect("knot runtime: system clock milliseconds overflowed i64");

    let table = quote_ident(&format!("_knot_{}", name));
    let history_table = quote_ident(&format!("_knot_{}_history", name));

    // Close out currently-open history rows
    let close_sql = format!(
        "UPDATE {} SET \"_knot_valid_to\" = ?1 WHERE \"_knot_valid_to\" IS NULL;",
        history_table
    );
    debug_sql(&close_sql);
    db_ref
        .conn
        .execute(&close_sql, rusqlite::params![now_ms])
        .unwrap_or_else(|e| {
            panic!(
                "knot runtime: failed to close history rows for '{}': {}",
                name, e
            )
        });

    // Insert current state as new open rows
    if !col_names.is_empty() {
        let insert_sql = format!(
            "INSERT INTO {} ({}, \"_knot_valid_from\", \"_knot_valid_to\") SELECT {}, ?1, NULL FROM {};",
            history_table,
            col_names.join(", "),
            col_names.join(", "),
            table
        );
        debug_sql(&insert_sql);
        db_ref
            .conn
            .execute(&insert_sql, rusqlite::params![now_ms])
            .unwrap_or_else(|e| {
                panic!(
                    "knot runtime: failed to snapshot history for '{}': {}",
                    name, e
                )
            });
    }
}

/// Read a source relation at a specific point in time.
/// Returns the rows that were valid at the given timestamp (milliseconds since epoch).
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_read_at(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    timestamp: *mut Value,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };

    if db_ref.atomic_depth.get() > 0 {
        stm_track_read(name);
    }

    let ts = int_as_i64(unsafe { as_ref(timestamp) })
        .unwrap_or_else(|| panic!(
            "knot runtime: temporal query timestamp must be Int, got {}",
            type_name(timestamp)
        ));

    let history_table = quote_ident(&format!("_knot_{}_history", name));

    if is_adt_schema(schema) {
        let adt = parse_adt_schema(schema);
        let field_idx: HashMap<&str, usize> = adt.all_fields.iter().enumerate()
            .map(|(i, f)| (f.name.as_str(), i)).collect();
        let mut select_cols = vec![quote_ident("_tag")];
        for f in &adt.all_fields {
            select_cols.push(quote_ident(&f.name));
        }
        let sql = format!(
            "SELECT {} FROM {} WHERE \"_knot_valid_from\" <= ?1 AND (\"_knot_valid_to\" IS NULL OR \"_knot_valid_to\" > ?1)",
            select_cols.join(", "),
            history_table
        );
        debug_sql(&sql);
        let mut stmt = db_ref
            .conn
            .prepare_cached(&sql)
            .unwrap_or_else(|e| panic!("knot runtime: temporal query error: {}", e));
        let mut rows: Vec<*mut Value> = Vec::new();
        let mut result_rows = stmt
            .query(rusqlite::params![ts])
            .unwrap_or_else(|e| panic!("knot runtime: temporal query exec error: {}", e));

        while let Some(row) = result_rows
            .next()
            .unwrap_or_else(|e| panic!("knot runtime: temporal row fetch error: {}", e))
        {
            let tag: String = row.get(0).unwrap();
            let ctor = adt.constructors.iter().find(|c| c.name == tag);
            let payload = if let Some(ctor) = ctor {
                if ctor.fields.is_empty() {
                    alloc(Value::Unit)
                } else {
                    let record = knot_record_empty(ctor.fields.len());
                    for field in &ctor.fields {
                        let col_idx = *field_idx.get(field.name.as_str()).unwrap_or_else(|| {
                            panic!(
                                "knot runtime: schema mismatch in `{}` history — constructor `{}` field `{}` not present in stored ADT layout",
                                name, tag, field.name
                            )
                        });
                        let val = read_sql_column(row, col_idx + 1, field.ty);
                        let fname = field.name.as_bytes();
                        knot_record_set_field(record, fname.as_ptr(), fname.len(), val);
                    }
                    record
                }
            } else {
                let record = knot_record_empty(adt.all_fields.len());
                let mut has_fields = false;
                for (i, field) in adt.all_fields.iter().enumerate() {
                    if !matches!(row.get_ref(i + 1).unwrap(), ValueRef::Null) {
                        let val = read_sql_column(row, i + 1, field.ty);
                        let fname = field.name.as_bytes();
                        knot_record_set_field(record, fname.as_ptr(), fname.len(), val);
                        has_fields = true;
                    }
                }
                if has_fields { record } else { alloc(Value::Unit) }
            };
            rows.push(alloc(Value::Constructor(intern_str(&tag), payload)));
        }
        alloc(Value::Relation(rows))
    } else {
        let cols = parse_schema(schema);
        let col_names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();

        let sql = format!(
            "SELECT {} FROM {} WHERE \"_knot_valid_from\" <= ?1 AND (\"_knot_valid_to\" IS NULL OR \"_knot_valid_to\" > ?1)",
            if col_names.is_empty() {
                "1".to_string()
            } else {
                col_names.join(", ")
            },
            history_table
        );
        debug_sql(&sql);
        let mut stmt = db_ref
            .conn
            .prepare_cached(&sql)
            .unwrap_or_else(|e| panic!("knot runtime: temporal query error: {}", e));

        let mut rows: Vec<*mut Value> = Vec::new();
        let mut result_rows = stmt
            .query(rusqlite::params![ts])
            .unwrap_or_else(|e| panic!("knot runtime: temporal query exec error: {}", e));

        while let Some(row) = result_rows
            .next()
            .unwrap_or_else(|e| panic!("knot runtime: temporal row fetch error: {}", e))
        {
            let record = knot_record_empty(cols.len());
            for (i, col) in cols.iter().enumerate() {
                let val = read_sql_column(row, i, col.ty);
                let field_name = col.name.as_bytes();
                knot_record_set_field(record, field_name.as_ptr(), field_name.len(), val);
            }
            rows.push(record);
        }
        alloc(Value::Relation(rows))
    }
}

// ── Subset constraints ────────────────────────────────────────────

// ── Result monad operations ──────────────────────────────────────

/// Result.bind: (a -> Result e b) -> Result e a -> Result e b
/// If Ok, apply function to value. If Err, propagate.
#[unsafe(no_mangle)]
pub extern "C" fn knot_result_bind(
    db: *mut c_void,
    func: *mut Value,
    result: *mut Value,
) -> *mut Value {
    match unsafe { as_ref(result) } {
        Value::Constructor(tag, payload) if &**tag == "Ok" => {
            // Extract value from Ok {value: v}
            let v = knot_record_field(*payload, "value".as_ptr(), "value".len());
            knot_value_call(db, func, v)
        }
        Value::Constructor(tag, _) if &**tag == "Err" => {
            // Propagate error
            result
        }
        _ => result,
    }
}

/// Result.yield (pure/return): a -> Result e a
/// Wraps value in Ok {value: a}
#[unsafe(no_mangle)]
pub extern "C" fn knot_result_yield(value: *mut Value) -> *mut Value {
    let rec = alloc(Value::Record(vec![
        RecordField { name: "value".into(), value },
    ]));
    alloc(Value::Constructor("Ok".into(), rec))
}

/// Result.empty: Result e a (always Err)
/// Returns Err {error: {typeName: "", violations: []}}
#[unsafe(no_mangle)]
pub extern "C" fn knot_result_empty() -> *mut Value {
    let violations = alloc(Value::Relation(Vec::new()));
    let error_rec = alloc(Value::Record(vec![
        RecordField { name: "typeName".into(), value: alloc(Value::Text(Arc::from(""))) },
        RecordField { name: "violations".into(), value: violations },
    ]));
    let err_rec = alloc(Value::Record(vec![
        RecordField { name: "error".into(), value: error_rec },
    ]));
    alloc(Value::Constructor("Err".into(), err_rec))
}

// ── Refinement validation ─────────────────────────────────────────

/// Validate that all rows in a relation satisfy a predicate.
/// Panics with a descriptive error if any row fails.
/// `field_ptr`/`field_len` = field name (empty string if whole-element check).
#[unsafe(no_mangle)]
pub extern "C" fn knot_refinement_validate_relation(
    db: *mut c_void,
    relation: *mut Value,
    predicate: *mut Value,
    type_name_ptr: *const u8,
    type_name_len: usize,
    field_ptr: *const u8,
    field_len: usize,
) {
    let type_name = unsafe { str_from_raw(type_name_ptr, type_name_len) };
    let field_name = unsafe { str_from_raw(field_ptr, field_len) };
    let rows = match unsafe { as_ref(relation) } {
        Value::Relation(rows) => rows,
        _ => {
            // Single value — check directly
            let result = knot_value_call(db, predicate, relation);
            match unsafe { as_ref(result) } {
                Value::Bool(true) => return,
                _ => panic!(
                    "refinement violation: value does not satisfy '{}' predicate",
                    type_name
                ),
            }
        }
    };
    for (i, row) in rows.iter().enumerate() {
        let check_val = if field_name.is_empty() {
            *row
        } else {
            // Extract the field from the record
            let field_ptr_inner = field_name.as_ptr();
            let field_len_inner = field_name.len();
            knot_record_field(*row, field_ptr_inner, field_len_inner)
        };
        let result = knot_value_call(db, predicate, check_val);
        match unsafe { as_ref(result) } {
            Value::Bool(true) => {}
            _ => {
                if field_name.is_empty() {
                    panic!(
                        "refinement violation: row {} does not satisfy '{}' predicate",
                        i, type_name
                    );
                } else {
                    panic!(
                        "refinement violation: row {} field '{}' does not satisfy '{}' predicate",
                        i, field_name, type_name
                    );
                }
            }
        }
    }
}

/// Validate that a single value (from JSON decode) satisfies a predicate.
/// Returns 1 if valid, 0 if not.
#[unsafe(no_mangle)]
pub extern "C" fn knot_refinement_check_value(
    db: *mut c_void,
    value: *mut Value,
    predicate: *mut Value,
) -> i32 {
    let result = knot_value_call(db, predicate, value);
    match unsafe { as_ref(result) } {
        Value::Bool(true) => 1,
        _ => 0,
    }
}

/// Register a refinement predicate for a route body field.
/// Called during program initialization after route table is set up.
#[unsafe(no_mangle)]
pub extern "C" fn knot_route_set_field_refinement(
    table: *mut c_void,
    ctor_ptr: *const u8,
    ctor_len: usize,
    field_ptr: *const u8,
    field_len: usize,
    predicate: *mut Value,
    type_name_ptr: *const u8,
    type_name_len: usize,
) {
    let ctor = unsafe { str_from_raw(ctor_ptr, ctor_len) };
    let field = unsafe { str_from_raw(field_ptr, field_len) };
    let type_name = unsafe { str_from_raw(type_name_ptr, type_name_len) };
    let table = unsafe { &mut *(table as *mut RouteTable) };
    table.field_refinements.push(FieldRefinement {
        constructor: ctor.to_string(),
        field_name: field.to_string(),
        predicate,
        type_name: type_name.to_string(),
    });
}

/// Register a subset constraint. Called at program startup.
/// Empty field strings mean "no field" (whole relation).
#[unsafe(no_mangle)]
pub extern "C" fn knot_constraint_register(
    db: *mut c_void,
    sub_rel_ptr: *const u8,
    sub_rel_len: usize,
    sub_field_ptr: *const u8,
    sub_field_len: usize,
    sup_rel_ptr: *const u8,
    sup_rel_len: usize,
    sup_field_ptr: *const u8,
    sup_field_len: usize,
) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let sub_rel = unsafe { str_from_raw(sub_rel_ptr, sub_rel_len) }.to_string();
    let sub_field_str = unsafe { str_from_raw(sub_field_ptr, sub_field_len) };
    let sub_field = if sub_field_str.is_empty() {
        None
    } else {
        Some(sub_field_str.to_string())
    };
    let sup_rel = unsafe { str_from_raw(sup_rel_ptr, sup_rel_len) }.to_string();
    let sup_field_str = unsafe { str_from_raw(sup_field_ptr, sup_field_len) };
    let sup_field = if sup_field_str.is_empty() {
        None
    } else {
        Some(sup_field_str.to_string())
    };

    // Enforce constraint via SQL indexes and triggers
    match (&sub_field, &sup_field) {
        // Uniqueness: *rel <= *rel.field — index + trigger
        (None, Some(sf)) if sub_rel == sup_rel => {
            let table = quote_ident(&format!("_knot_{}", sub_rel));
            let col = quote_ident(sf);

            // Index for efficient lookups
            let idx_sql = format!(
                "CREATE INDEX IF NOT EXISTS {} ON {} ({});",
                quote_ident(&format!("_knot_{}_idx_{}", sub_rel, sf)),
                table,
                col,
            );
            debug_sql(&idx_sql);
            let _ = db_ref.conn.execute_batch(&idx_sql);

            // Trigger: reject INSERT if value already exists
            let msg = format!(
                "uniqueness constraint violated: *{} <= *{}.{}",
                sub_rel, sup_rel, sf
            ).replace('\'', "''");
            let trigger_sql = format!(
                "CREATE TRIGGER IF NOT EXISTS {trg} \
                 BEFORE INSERT ON {table} \
                 FOR EACH ROW \
                 WHEN EXISTS (SELECT 1 FROM {table} WHERE {col} = NEW.{col}) \
                 BEGIN SELECT RAISE(ABORT, '{msg}'); END;",
                trg = quote_ident(&format!("_knot_uniq_{}_{}_ins", sub_rel, sf)),
                table = table,
                col = col,
                msg = msg,
            );
            debug_sql(&trigger_sql);
            db_ref.conn.execute_batch(&trigger_sql)
                .expect("knot runtime: failed to create uniqueness trigger");

            // Trigger: reject UPDATE if new value already exists
            let upd_trigger_sql = format!(
                "CREATE TRIGGER IF NOT EXISTS {trg} \
                 BEFORE UPDATE OF {col} ON {table} \
                 FOR EACH ROW \
                 WHEN NEW.{col} != OLD.{col} AND EXISTS (SELECT 1 FROM {table} WHERE {col} = NEW.{col}) \
                 BEGIN SELECT RAISE(ABORT, '{msg}'); END;",
                trg = quote_ident(&format!("_knot_uniq_{}_{}_upd", sub_rel, sf)),
                table = table,
                col = col,
                msg = msg,
            );
            debug_sql(&upd_trigger_sql);
            db_ref.conn.execute_batch(&upd_trigger_sql)
                .expect("knot runtime: failed to create uniqueness update trigger");
        }
        // Referential integrity: *sub.sf <= *sup.spf — indexes + triggers
        (Some(sf), Some(spf)) => {
            // Indexes for efficient lookups
            let sub_idx = format!(
                "CREATE INDEX IF NOT EXISTS {} ON {} ({});",
                quote_ident(&format!("_knot_{}_idx_{}", sub_rel, sf)),
                quote_ident(&format!("_knot_{}", sub_rel)),
                quote_ident(sf),
            );
            debug_sql(&sub_idx);
            let _ = db_ref.conn.execute_batch(&sub_idx);

            let sup_idx = format!(
                "CREATE INDEX IF NOT EXISTS {} ON {} ({});",
                quote_ident(&format!("_knot_{}_idx_{}", sup_rel, spf)),
                quote_ident(&format!("_knot_{}", sup_rel)),
                quote_ident(spf),
            );
            debug_sql(&sup_idx);
            let _ = db_ref.conn.execute_batch(&sup_idx);

            let sub_table = quote_ident(&format!("_knot_{}", sub_rel));
            let sup_table = quote_ident(&format!("_knot_{}", sup_rel));
            let sub_col = quote_ident(sf);
            let sup_col = quote_ident(spf);
            let msg = format!(
                "subset constraint violated: *{}.{} <= *{}.{}",
                sub_rel, sf, sup_rel, spf
            ).replace('\'', "''");

            // Trigger: reject INSERT into sub if value doesn't exist in sup
            let insert_trigger = format!(
                "CREATE TRIGGER IF NOT EXISTS {trg} \
                 BEFORE INSERT ON {sub_table} \
                 FOR EACH ROW \
                 WHEN NOT EXISTS (SELECT 1 FROM {sup_table} WHERE {sup_col} = NEW.{sub_col}) \
                 BEGIN SELECT RAISE(ABORT, '{msg}'); END;",
                trg = quote_ident(&format!("_knot_fk_{}_{}_ins", sub_rel, sf)),
                sub_table = sub_table,
                sup_table = sup_table,
                sub_col = sub_col,
                sup_col = sup_col,
                msg = msg,
            );
            debug_sql(&insert_trigger);
            db_ref.conn.execute_batch(&insert_trigger)
                .expect("knot runtime: failed to create insert trigger");

            // Trigger: reject UPDATE on sub if new value doesn't exist in sup
            let update_trigger = format!(
                "CREATE TRIGGER IF NOT EXISTS {trg} \
                 BEFORE UPDATE OF {sub_col} ON {sub_table} \
                 FOR EACH ROW \
                 WHEN NEW.{sub_col} != OLD.{sub_col} AND NOT EXISTS (SELECT 1 FROM {sup_table} WHERE {sup_col} = NEW.{sub_col}) \
                 BEGIN SELECT RAISE(ABORT, '{msg}'); END;",
                trg = quote_ident(&format!("_knot_fk_{}_{}_upd", sub_rel, sf)),
                sub_table = sub_table,
                sup_table = sup_table,
                sub_col = sub_col,
                sup_col = sup_col,
                msg = msg,
            );
            debug_sql(&update_trigger);
            db_ref.conn.execute_batch(&update_trigger)
                .expect("knot runtime: failed to create update trigger");

            // Trigger: reject DELETE from sup if sub still references the value
            let delete_msg = format!(
                "subset constraint violated: cannot delete from *{}.{} while referenced by *{}.{}",
                sup_rel, spf, sub_rel, sf
            ).replace('\'', "''");
            let delete_trigger = format!(
                "CREATE TRIGGER IF NOT EXISTS {trg} \
                 BEFORE DELETE ON {sup_table} \
                 FOR EACH ROW \
                 WHEN EXISTS (SELECT 1 FROM {sub_table} WHERE {sub_col} = OLD.{sup_col}) \
                 BEGIN SELECT RAISE(ABORT, '{msg}'); END;",
                trg = quote_ident(&format!("_knot_fk_{}_{}_del", sup_rel, spf)),
                sup_table = sup_table,
                sub_table = sub_table,
                sub_col = sub_col,
                sup_col = sup_col,
                msg = delete_msg,
            );
            debug_sql(&delete_trigger);
            db_ref.conn.execute_batch(&delete_trigger)
                .expect("knot runtime: failed to create delete trigger");
        }
        _ => {}
    }
}

// ── Atomic (transactions) ─────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_atomic_begin(db: *mut c_void) {
    let _guard = write_lock_guard();
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let depth = db_ref.atomic_depth.get() + 1;
    db_ref
        .conn
        .execute_batch(&format!("SAVEPOINT knot_atomic_{depth};"))
        .expect("knot runtime: failed to begin atomic");
    // Only update depth after SAVEPOINT succeeds, so rollback/commit
    // never targets a non-existent savepoint on SQL failure.
    db_ref.atomic_depth.set(depth);
    // Lock stays held across begin/commit/rollback — forget the guard
    // so it doesn't release on drop. Commit/rollback will release.
    std::mem::forget(_guard);
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_atomic_commit(db: *mut c_void) {
    // RAII guard: the lock was acquired in knot_atomic_begin; this guard
    // ensures it is released even if code below panics during unwinding.
    let _guard = WriteLockGuard;
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let depth = db_ref.atomic_depth.get();
    assert!(depth > 0, "knot runtime: atomic commit without matching begin");
    // Execute SQL first, then decrement depth. If SQL panics, depth is
    // still > 0, so WriteLockGuard's drop can safely call write_lock_release
    // without hitting the depth > 0 assertion.
    db_ref
        .conn
        .execute_batch(&format!("RELEASE SAVEPOINT knot_atomic_{depth};"))
        .expect("knot runtime: failed to commit atomic");
    db_ref.atomic_depth.set(depth - 1);
    if depth == 1 {
        let written = STM_WRITTEN_TABLES.with(|wt| std::mem::take(&mut *wt.borrow_mut()));
        if !written.is_empty() {
            // Batch: read lock + atomic increment for existing tables
            let mut new_tables = Vec::new();
            {
                let versions = TABLE_VERSIONS.read().unwrap();
                for table in &written {
                    if let Some(v) = versions.get(table.as_str()) {
                        v.fetch_add(1, Ordering::Release);
                    } else {
                        new_tables.push(table.clone());
                    }
                }
            }
            // Write lock only for newly seen tables (rare at steady state)
            if !new_tables.is_empty() {
                let mut versions = TABLE_VERSIONS.write().unwrap();
                for table in new_tables {
                    versions
                        .entry(table)
                        .or_insert_with(|| Arc::new(AtomicU64::new(0)))
                        .fetch_add(1, Ordering::Release);
                }
            }
            // Batch: wake all relevant watchers under one lock
            let mut watchers = TABLE_WATCHERS.lock().unwrap();
            for table in &written {
                if let Some(slots) = watchers.get_mut(table.as_str()) {
                    slots.retain(|weak| match weak.upgrade() {
                        Some(slot) => {
                            slot.wake();
                            true
                        }
                        None => false,
                    });
                }
            }
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_atomic_rollback(db: *mut c_void) {
    // RAII guard: the lock was acquired in knot_atomic_begin; this guard
    // ensures it is released even if code below panics during unwinding.
    let _guard = WriteLockGuard;
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let depth = db_ref.atomic_depth.get();
    assert!(depth > 0, "knot runtime: atomic rollback without matching begin");
    // ROLLBACK TO undoes changes but keeps the savepoint alive.
    // RELEASE then removes it so the next begin creates a clean one.
    // Execute SQL first, then decrement depth (same rationale as commit).
    db_ref
        .conn
        .execute_batch(&format!(
            "ROLLBACK TO SAVEPOINT knot_atomic_{depth}; RELEASE SAVEPOINT knot_atomic_{depth};"
        ))
        .expect("knot runtime: failed to rollback atomic");
    db_ref.atomic_depth.set(depth - 1);
    if depth == 1 {
        STM_WRITTEN_TABLES.with(|wt| wt.borrow_mut().clear());
    }
}

// ── Record update ─────────────────────────────────────────────────

/// Create a new record by copying `base` and overriding fields.
/// This implements `{base | field1: val1, field2: val2}`.
#[unsafe(no_mangle)]
pub extern "C" fn knot_record_update(base: *mut Value) -> *mut Value {
    match unsafe { as_ref(base) } {
        Value::Record(fields) => {
            let new_fields: Vec<RecordField> = fields
                .iter()
                .map(|f| RecordField {
                    name: f.name.clone(),
                    value: f.value,
                })
                .collect();
            alloc(Value::Record(new_fields))
        }
        _ => panic!("knot runtime: record update requires a Record base, got {}", type_name(base)),
    }
}

/// Batch record update: copy base and merge sorted update fields in one pass.
/// `data` points to a flat array of triples: [key_ptr, key_len, value, ...]
/// Fields MUST be pre-sorted by name. O(n+m) merge vs O(m log n) repeated insert.
#[unsafe(no_mangle)]
pub extern "C" fn knot_record_update_batch(
    base: *mut Value,
    data: *const usize,
    count: usize,
) -> *mut Value {
    let base_fields = match unsafe { as_ref(base) } {
        Value::Record(fields) => fields,
        _ => panic!("knot runtime: record update requires a Record base, got {}", type_name(base)),
    };

    // Parse update fields from flat array
    let updates: Vec<(&str, *mut Value)> = (0..count)
        .map(|i| {
            let offset = i * 3;
            let key_ptr = unsafe { *data.add(offset) as *const u8 };
            let key_len = unsafe { *data.add(offset + 1) };
            let value = unsafe { *data.add(offset + 2) as *mut Value };
            let name = unsafe { str_from_raw(key_ptr, key_len) };
            (name, value)
        })
        .collect();

    // Merge sorted base fields with sorted update fields
    let mut result = Vec::with_capacity(base_fields.len() + count);
    let mut base_idx = 0;
    let mut upd_idx = 0;

    while base_idx < base_fields.len() && upd_idx < updates.len() {
        let base_name: &str = &*base_fields[base_idx].name;
        let upd_name = updates[upd_idx].0;
        match base_name.cmp(upd_name) {
            std::cmp::Ordering::Less => {
                result.push(RecordField {
                    name: base_fields[base_idx].name.clone(),
                    value: base_fields[base_idx].value,
                });
                base_idx += 1;
            }
            std::cmp::Ordering::Equal => {
                result.push(RecordField {
                    name: base_fields[base_idx].name.clone(),
                    value: updates[upd_idx].1,
                });
                base_idx += 1;
                upd_idx += 1;
            }
            std::cmp::Ordering::Greater => {
                result.push(RecordField {
                    name: intern_str(updates[upd_idx].0),
                    value: updates[upd_idx].1,
                });
                upd_idx += 1;
            }
        }
    }
    while base_idx < base_fields.len() {
        result.push(RecordField {
            name: base_fields[base_idx].name.clone(),
            value: base_fields[base_idx].value,
        });
        base_idx += 1;
    }
    while upd_idx < updates.len() {
        result.push(RecordField {
            name: intern_str(updates[upd_idx].0),
            value: updates[upd_idx].1,
        });
        upd_idx += 1;
    }

    alloc(Value::Record(result))
}

// ── View operations ──────────────────────────────────────────────

/// Read through a view: SELECT only view columns WHERE constant columns match.
/// `view_schema` contains only the columns visible in the view (source columns).
/// `filter_where` is the WHERE clause for constant column filtering.
/// `filter_params` is a flat relation of values for the WHERE placeholders.
#[unsafe(no_mangle)]
pub extern "C" fn knot_view_read(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    filter_ptr: *const u8,
    filter_len: usize,
    filter_params: *mut Value,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let view_schema = unsafe { str_from_raw(schema_ptr, schema_len) };
    let filter_where = unsafe { str_from_raw(filter_ptr, filter_len) };
    let cols = parse_schema(view_schema);

    let filter_values = match unsafe { as_ref(filter_params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: view_read filter_params must be Relation, got {}",
            type_name(filter_params)
        ),
    };

    let col_names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();
    let sql = if filter_where.is_empty() {
        format!(
            "SELECT {} FROM {}",
            if col_names.is_empty() {
                "1".to_string()
            } else {
                col_names.join(", ")
            },
            quote_ident(&format!("_knot_{}", name))
        )
    } else {
        format!(
            "SELECT {} FROM {} WHERE {}",
            if col_names.is_empty() {
                "1".to_string()
            } else {
                col_names.join(", ")
            },
            quote_ident(&format!("_knot_{}", name)),
            filter_where
        )
    };

    let sql_params: Vec<rusqlite::types::Value> = filter_values
        .iter()
        .map(|v| value_to_sql_param(*v))
        .collect();
    debug_sql_params(&sql, &sql_params);
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = sql_params
        .iter()
        .map(|p| p as &dyn rusqlite::types::ToSql)
        .collect();

    let mut stmt = db_ref
        .conn
        .prepare_cached(&sql)
        .unwrap_or_else(|e| panic!("knot runtime: view_read query error: {}", e));

    let mut rows: Vec<*mut Value> = Vec::new();
    let mut result_rows = stmt
        .query(param_refs.as_slice())
        .unwrap_or_else(|e| panic!("knot runtime: view_read exec error: {}", e));

    while let Some(row) = result_rows
        .next()
        .unwrap_or_else(|e| panic!("knot runtime: view_read fetch error: {}", e))
    {
        let record = knot_record_empty(cols.len());
        for (i, col) in cols.iter().enumerate() {
            let val = read_sql_column(row, i, col.ty);
            let name_bytes = col.name.as_bytes();
            knot_record_set_field(record, name_bytes.as_ptr(), name_bytes.len(), val);
        }
        rows.push(record);
    }

    alloc(Value::Relation(rows))
}

/// Read a view at a specific point in time, combining temporal and view filtering.
/// Queries the underlying source's history table with both temporal and constant column filters.
#[unsafe(no_mangle)]
pub extern "C" fn knot_view_read_at(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    filter_ptr: *const u8,
    filter_len: usize,
    filter_params: *mut Value,
    timestamp: *mut Value,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let view_schema = unsafe { str_from_raw(schema_ptr, schema_len) };
    let filter_where = unsafe { str_from_raw(filter_ptr, filter_len) };
    let cols = parse_schema(view_schema);

    let ts = int_as_i64(unsafe { as_ref(timestamp) })
        .unwrap_or_else(|| panic!(
            "knot runtime: temporal query timestamp must be Int, got {}",
            type_name(timestamp)
        ));

    let filter_values = match unsafe { as_ref(filter_params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: view_read_at filter_params must be Relation, got {}",
            type_name(filter_params)
        ),
    };

    let history_table = quote_ident(&format!("_knot_{}_history", name));
    let col_names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();

    // Temporal condition uses the first parameter slot
    let temporal_cond =
        "\"_knot_valid_from\" <= ?1 AND (\"_knot_valid_to\" IS NULL OR \"_knot_valid_to\" > ?1)";

    // View filter params are offset by 1 (timestamp takes ?1)
    let view_filter = if filter_where.is_empty() {
        String::new()
    } else {
        // Rewrite ?1, ?2, ... to ?2, ?3, ... to account for timestamp param.
        // Use char-by-char scan to match exact parameter tokens (e.g. ?1 not
        // the ?1 inside ?11).
        let mut rewritten = String::with_capacity(filter_where.len() + 8);
        let chars: Vec<char> = filter_where.chars().collect();
        let mut ci = 0;
        while ci < chars.len() {
            if chars[ci] == '?' && ci + 1 < chars.len() && chars[ci + 1].is_ascii_digit() {
                let start = ci + 1;
                let mut end = start;
                while end < chars.len() && chars[end].is_ascii_digit() {
                    end += 1;
                }
                let num: usize = chars[start..end].iter().collect::<String>().parse().unwrap();
                rewritten.push_str(&format!("?{}", num + 1));
                ci = end;
            } else {
                rewritten.push(chars[ci]);
                ci += 1;
            }
        }
        format!(" AND {}", rewritten)
    };

    let sql = format!(
        "SELECT {} FROM {} WHERE {}{}",
        if col_names.is_empty() {
            "1".to_string()
        } else {
            col_names.join(", ")
        },
        history_table,
        temporal_cond,
        view_filter,
    );

    // Build params: timestamp first, then view filter values
    let mut sql_params: Vec<rusqlite::types::Value> = Vec::new();
    sql_params.push(rusqlite::types::Value::Integer(ts));
    for v in filter_values.iter() {
        sql_params.push(value_to_sql_param(*v));
    }

    debug_sql_params(&sql, &sql_params);
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = sql_params
        .iter()
        .map(|p| p as &dyn rusqlite::types::ToSql)
        .collect();

    let mut stmt = db_ref
        .conn
        .prepare_cached(&sql)
        .unwrap_or_else(|e| panic!("knot runtime: view_read_at query error: {}", e));

    let mut rows: Vec<*mut Value> = Vec::new();
    let mut result_rows = stmt
        .query(param_refs.as_slice())
        .unwrap_or_else(|e| panic!("knot runtime: view_read_at exec error: {}", e));

    while let Some(row) = result_rows
        .next()
        .unwrap_or_else(|e| panic!("knot runtime: view_read_at fetch error: {}", e))
    {
        let record = knot_record_empty(cols.len());
        for (i, col) in cols.iter().enumerate() {
            let val = read_sql_column(row, i, col.ty);
            let name_bytes = col.name.as_bytes();
            knot_record_set_field(record, name_bytes.as_ptr(), name_bytes.len(), val);
        }
        rows.push(record);
    }

    alloc(Value::Relation(rows))
}

/// Add fields from `extra_fields` record to each row in `relation`.
/// Returns a new relation with augmented rows.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_add_fields(
    relation: *mut Value,
    extra_fields: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(relation) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: relation_add_fields expects Relation, got {}",
            type_name(relation)
        ),
    };
    let extra = match unsafe { as_ref(extra_fields) } {
        Value::Record(fields) => fields,
        _ => panic!(
            "knot runtime: relation_add_fields extra must be Record, got {}",
            type_name(extra_fields)
        ),
    };

    let new_rows: Vec<*mut Value> = rows
        .iter()
        .map(|row_ptr| {
            let updated = knot_record_update(*row_ptr);
            for field in extra {
                let name_bytes = field.name.as_bytes();
                knot_record_set_field(
                    updated,
                    name_bytes.as_ptr(),
                    name_bytes.len(),
                    field.value,
                );
            }
            updated
        })
        .collect();

    alloc(Value::Relation(new_rows))
}

/// Rename fields in every record of a relation.
/// `mapping` is a comma-separated string of `old_name>new_name` pairs.
/// Fields not mentioned in the mapping are kept unchanged.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_rename_columns(
    relation: *mut Value,
    mapping_ptr: *const u8,
    mapping_len: usize,
) -> *mut Value {
    let rows = match unsafe { as_ref(relation) } {
        Value::Relation(rows) => rows,
        _ => return relation,
    };
    let mapping_str = unsafe { str_from_raw(mapping_ptr, mapping_len) };
    if mapping_str.is_empty() {
        return relation;
    }
    let renames: Vec<(&str, &str)> = mapping_str
        .split(',')
        .filter_map(|pair| pair.split_once('>'))
        .collect();
    if renames.is_empty() {
        return relation;
    }

    let new_rows: Vec<*mut Value> = rows
        .iter()
        .map(|row_ptr| {
            let fields = match unsafe { as_ref(*row_ptr) } {
                Value::Record(fields) => fields,
                _ => return *row_ptr,
            };
            let new_rec = knot_record_empty(fields.len());
            for field in fields {
                let field_name_str: &str = &*field.name;
                let new_name: &str = renames
                    .iter()
                    .find(|(old, _)| **old == *field_name_str)
                    .map(|(_, new)| *new)
                    .unwrap_or(field_name_str);
                let name_bytes = new_name.as_bytes();
                knot_record_set_field(
                    new_rec,
                    name_bytes.as_ptr(),
                    name_bytes.len(),
                    field.value,
                );
            }
            new_rec
        })
        .collect();

    alloc(Value::Relation(new_rows))
}

/// Write through a view: delete rows matching filter, insert new rows.
/// `filter_params` is a flat relation of values for the WHERE clause placeholders.
/// `new_relation` has ALL columns (including constants that were added back).
#[unsafe(no_mangle)]
pub extern "C" fn knot_view_write(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    filter_ptr: *const u8,
    filter_len: usize,
    filter_params: *mut Value,
    new_relation: *mut Value,
) {
    let _wl = write_lock_guard();
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };
    let filter_where = unsafe { str_from_raw(filter_ptr, filter_len) };
    let rec_schema = parse_record_schema(schema);

    let filter_values = match unsafe { as_ref(filter_params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: view_write filter_params must be Relation, got {}",
            type_name(filter_params)
        ),
    };

    let rows = match unsafe { as_ref(new_relation) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: view_write new_relation must be Relation, got {}",
            type_name(new_relation)
        ),
    };

    let table_name = format!("_knot_{}", name);
    let table = quote_ident(&table_name);

    db_ref
        .conn
        .execute_batch("SAVEPOINT knot_view_write;")
        .expect("knot runtime: view_write begin failed");

    // 1. Delete rows matching the view's constant filter.
    //    For sources with nested relations, delete child rows first to avoid orphans.
    if !rec_schema.nested.is_empty() {
        // Collect _ids of parent rows about to be deleted
        let select_sql = if filter_where.is_empty() {
            format!("SELECT _id FROM {};", table)
        } else {
            format!("SELECT _id FROM {} WHERE {};", table, filter_where)
        };
        let sql_params: Vec<rusqlite::types::Value> = filter_values
            .iter()
            .map(|v| value_to_sql_param(*v))
            .collect();
        let param_refs: Vec<&dyn rusqlite::types::ToSql> = sql_params
            .iter()
            .map(|p| p as &dyn rusqlite::types::ToSql)
            .collect();
        let mut stmt = db_ref.conn.prepare(&select_sql).expect("knot runtime: view_write select _id failed");
        let ids: Vec<i64> = stmt
            .query_map(param_refs.as_slice(), |row| row.get::<_, i64>(0))
            .expect("knot runtime: view_write query _id failed")
            .filter_map(|r| r.ok())
            .collect();
        drop(stmt);

        // Delete child rows for each parent _id
        for nf in &rec_schema.nested {
            let child_table = format!("{}__{}", table_name, nf.name);
            for &parent_id in &ids {
                delete_child_rows_for_parent(&db_ref.conn, &child_table, parent_id, nf);
            }
        }
    }

    let delete_sql = if filter_where.is_empty() {
        format!("DELETE FROM {};", table)
    } else {
        format!("DELETE FROM {} WHERE {};", table, filter_where)
    };
    let sql_params: Vec<rusqlite::types::Value> = filter_values
        .iter()
        .map(|v| value_to_sql_param(*v))
        .collect();
    debug_sql_params(&delete_sql, &sql_params);
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = sql_params
        .iter()
        .map(|p| p as &dyn rusqlite::types::ToSql)
        .collect();
    db_ref
        .conn
        .execute(&delete_sql, param_refs.as_slice())
        .unwrap_or_else(|e| {
            panic!(
                "knot runtime: view_write delete error: {}\n  SQL: {}",
                e, delete_sql
            )
        });

    // 2. Insert new rows (including child tables for nested relations)
    if !rows.is_empty() {
        write_record_rows(&db_ref.conn, &table_name, &rec_schema, rows);
    }

    db_ref
        .conn
        .execute_batch("RELEASE SAVEPOINT knot_view_write;")
        .expect("knot runtime: view_write commit failed");
    if db_ref.atomic_depth.get() > 0 {
        stm_track_write(name);
    } else {
        notify_relation_changed(name);
    }
}

// ── Pipe (|>) support ─────────────────────────────────────────────

/// Apply a function value to an argument: `arg |> func`
#[unsafe(no_mangle)]
pub extern "C" fn knot_value_pipe(
    db: *mut c_void,
    arg: *mut Value,
    func: *mut Value,
) -> *mut Value {
    knot_value_call(db, func, arg)
}

// ── Constructor matching ──────────────────────────────────────────

/// Check if a value is a constructor with the given tag.
#[unsafe(no_mangle)]
pub extern "C" fn knot_constructor_matches(
    v: *mut Value,
    tag_ptr: *const u8,
    tag_len: usize,
) -> i32 {
    if v.is_null() {
        return 0; // Null (nullable none) never matches a constructor tag
    }
    let tag = unsafe { str_from_raw(tag_ptr, tag_len) };
    match unsafe { as_ref(v) } {
        Value::Constructor(t, _) => (&**t == tag) as i32,
        // Text values can appear as implicit nullary constructors (e.g. from JSON deserialization)
        Value::Text(s) => (&**s == tag) as i32,
        _ => 0,
    }
}

/// Return a pointer to the constructor tag string data.
/// Used to extract the tag once and compare multiple times.
#[unsafe(no_mangle)]
pub extern "C" fn knot_constructor_tag_ptr(v: *mut Value) -> *const u8 {
    match unsafe { as_ref(v) } {
        Value::Constructor(t, _) => t.as_ptr(),
        // Text values can appear as implicit nullary constructors (e.g. from JSON deserialization)
        Value::Text(s) => s.as_ptr(),
        _ => panic!("knot runtime: expected Constructor in tag_ptr, got {} = {}", type_name(v), brief_value(v)),
    }
}

/// Return the length of the constructor tag string.
#[unsafe(no_mangle)]
pub extern "C" fn knot_constructor_tag_len(v: *mut Value) -> usize {
    match unsafe { as_ref(v) } {
        Value::Constructor(t, _) => t.len(),
        // Text values can appear as implicit nullary constructors (e.g. from JSON deserialization)
        Value::Text(s) => s.len(),
        _ => panic!("knot runtime: expected Constructor in tag_len, got {}", type_name(v)),
    }
}

/// Pure string equality comparison (no Value deref needed).
/// Used for comparing extracted constructor tags against static strings.
#[unsafe(no_mangle)]
pub extern "C" fn knot_str_eq(
    a_ptr: *const u8,
    a_len: usize,
    b_ptr: *const u8,
    b_len: usize,
) -> i32 {
    if a_len != b_len {
        return 0;
    }
    let a = unsafe { slice::from_raw_parts(a_ptr, a_len) };
    let b = unsafe { slice::from_raw_parts(b_ptr, b_len) };
    (a == b) as i32
}

/// Get the payload of a constructor value.
/// For nullable-encoded types, the value IS the payload (or null for none).
#[unsafe(no_mangle)]
pub extern "C" fn knot_constructor_payload(v: *mut Value) -> *mut Value {
    if v.is_null() {
        return v; // Nullable none: return null
    }
    match unsafe { as_ref(v) } {
        Value::Constructor(_, payload) => *payload,
        // Text values can appear as implicit nullary constructors (e.g. from JSON deserialization)
        Value::Text(_) => alloc(Value::Unit),
        _ => panic!("knot runtime: expected Constructor, got {}", type_name(v)),
    }
}

// ── Recursive derived relations (fixpoint iteration) ──────────────

/// Iterates a body function to a fixed point for recursive derived relations.
/// `body` is a raw function pointer: `extern "C" fn(db, current) -> new_result`.
/// Starts with `initial` and calls body repeatedly until the result stabilizes.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_fixpoint(
    db: *mut c_void,
    body: *const u8,
    initial: *mut Value,
) -> *mut Value {
    let body_fn: extern "C" fn(*mut c_void, *mut Value) -> *mut Value =
        unsafe { std::mem::transmute(body) };
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let mut current = initial;
    for _ in 0..10_000 {
        let next = body_fn(db, current);

        // Try SQL-based equality check (O(n log n) via EXCEPT)
        let equal = match (unsafe { as_ref(current) }, unsafe { as_ref(next) }) {
            (Value::Relation(curr_rows), Value::Relation(next_rows)) => {
                sql_relations_equal(&db_ref.conn, curr_rows, next_rows)
                    .unwrap_or_else(|| values_equal(current, next))
            }
            _ => values_equal(current, next),
        };

        if equal {
            return next;
        }
        current = next;
    }
    panic!("knot runtime: recursive derived relation did not converge after 10000 iterations");
}

// ── HTTP server (routes) ──────────────────────────────────────────

#[derive(Clone)]
enum PathPart {
    Literal(String),
    Param(String, String), // (name, type)
}

#[derive(Clone)]
struct RouteTableEntry {
    method: String,
    path_parts: Vec<PathPart>,
    constructor: String,
    body_fields: Vec<(String, String)>,
    query_fields: Vec<(String, String)>,
    response_type: String,
    request_headers: Vec<(String, String)>,
    response_headers: Vec<(String, String)>,
}

struct FieldRefinement {
    constructor: String,
    field_name: String,
    predicate: *mut Value,
    type_name: String,
}

// Safety: predicate is a Knot function value that lives for the program's lifetime
unsafe impl Send for FieldRefinement {}
unsafe impl Sync for FieldRefinement {}

impl Clone for FieldRefinement {
    fn clone(&self) -> Self {
        Self {
            constructor: self.constructor.clone(),
            field_name: self.field_name.clone(),
            predicate: self.predicate,
            type_name: self.type_name.clone(),
        }
    }
}

#[derive(Clone)]
struct RouteTable {
    entries: Vec<RouteTableEntry>,
    field_refinements: Vec<FieldRefinement>,
}

fn parse_descriptor(desc: &str) -> Vec<(String, String)> {
    if desc.is_empty() {
        return Vec::new();
    }
    desc.split(',')
        .map(|part| {
            let mut split = part.splitn(2, ':');
            let name = split.next().unwrap_or("").to_string();
            let ty = split.next().unwrap_or("text").to_string();
            (name, ty)
        })
        .collect()
}

fn parse_path_pattern(path: &str) -> Vec<PathPart> {
    path.split('/')
        .filter(|s| !s.is_empty())
        .map(|seg| {
            if seg.starts_with('{') && seg.ends_with('}') {
                let inner = &seg[1..seg.len() - 1];
                let mut split = inner.splitn(2, ':');
                let name = split.next().unwrap_or("").to_string();
                let ty = split.next().unwrap_or("text").to_string();
                PathPart::Param(name, ty)
            } else {
                PathPart::Literal(seg.to_string())
            }
        })
        .collect()
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_route_table_new() -> *mut c_void {
    let table = Box::new(RouteTable {
        entries: Vec::new(),
        field_refinements: Vec::new(),
    });
    Box::into_raw(table) as *mut c_void
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_route_table_add(
    table: *mut c_void,
    method_ptr: *const u8,
    method_len: usize,
    path_ptr: *const u8,
    path_len: usize,
    ctor_ptr: *const u8,
    ctor_len: usize,
    body_desc_ptr: *const u8,
    body_desc_len: usize,
    query_desc_ptr: *const u8,
    query_desc_len: usize,
    resp_ptr: *const u8,
    resp_len: usize,
    req_hdrs_ptr: *const u8,
    req_hdrs_len: usize,
    resp_hdrs_ptr: *const u8,
    resp_hdrs_len: usize,
) {
    let table = unsafe { &mut *(table as *mut RouteTable) };
    let method = unsafe { str_from_raw(method_ptr, method_len) }.to_string();
    let path = unsafe { str_from_raw(path_ptr, path_len) };
    let ctor = unsafe { str_from_raw(ctor_ptr, ctor_len) }.to_string();
    let body_desc = unsafe { str_from_raw(body_desc_ptr, body_desc_len) };
    let query_desc = unsafe { str_from_raw(query_desc_ptr, query_desc_len) };
    let resp = unsafe { str_from_raw(resp_ptr, resp_len) }.to_string();
    let req_hdrs = unsafe { str_from_raw(req_hdrs_ptr, req_hdrs_len) };
    let resp_hdrs = unsafe { str_from_raw(resp_hdrs_ptr, resp_hdrs_len) };

    log_debug!("[ROUTE] {} {} -> {}", method, path, ctor);

    table.entries.push(RouteTableEntry {
        method,
        path_parts: parse_path_pattern(path),
        constructor: ctor,
        body_fields: parse_descriptor(body_desc),
        query_fields: parse_descriptor(query_desc),
        response_type: resp,
        request_headers: parse_descriptor(req_hdrs),
        response_headers: parse_descriptor(resp_hdrs),
    });
}

fn match_route<'a>(
    entries: &'a [RouteTableEntry],
    method: &str,
    path_segments: &[&str],
) -> Option<(&'a RouteTableEntry, Vec<(String, String)>)> {
    for entry in entries {
        if !entry.method.eq_ignore_ascii_case(method) {
            continue;
        }
        if entry.path_parts.len() != path_segments.len() {
            continue;
        }
        let mut params = Vec::new();
        let mut matched = true;
        for (part, seg) in entry.path_parts.iter().zip(path_segments.iter()) {
            match part {
                PathPart::Literal(lit) => {
                    if lit != seg {
                        matched = false;
                        break;
                    }
                }
                PathPart::Param(name, _ty) => {
                    params.push((name.clone(), url_decode(seg)));
                }
            }
        }
        if matched {
            return Some((entry, params));
        }
    }
    None
}

fn parse_query_string(qs: &str) -> HashMap<String, String> {
    if qs.is_empty() {
        return HashMap::new();
    }
    qs.split('&')
        .filter_map(|pair| {
            let mut split = pair.splitn(2, '=');
            let key = split.next()?;
            let val = split.next().unwrap_or("");
            Some((
                url_decode(key),
                url_decode(val),
            ))
        })
        .collect()
}

fn url_decode(s: &str) -> String {
    let mut bytes = Vec::with_capacity(s.len());
    let raw = s.as_bytes();
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'%' && i + 2 < raw.len() {
            if let (Some(h), Some(l)) = (hex_val(raw[i + 1]), hex_val(raw[i + 2])) {
                bytes.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        if raw[i] == b'+' {
            bytes.push(b' ');
        } else {
            bytes.push(raw[i]);
        }
        i += 1;
    }
    String::from_utf8(bytes).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}



fn string_to_value(s: &str, ty: &str) -> *mut Value {
    match ty {
        "int" => {
            let n: i64 = s.parse().unwrap_or(0);
            alloc_int(n)
        }
        "float" => {
            let n: f64 = s.parse().unwrap_or(0.0);
            alloc_float(n)
        }
        "bool" => {
            let b = s == "true" || s == "True";
            alloc_bool(b)
        }
        "tag" => {
            alloc(Value::Constructor(intern_str(s), alloc(Value::Unit)))
        }
        _ => alloc(Value::Text(Arc::from(s))),
    }
}

/// Strict variant of `string_to_value`: returns `None` when `s` does not
/// parse as `ty`. Used for HTTP path and query parameters so the server can
/// reply with a 400 instead of silently coercing `"abc"` to `0` (which would
/// hide caller bugs and let malformed traffic through unannotated).
fn try_string_to_value(s: &str, ty: &str) -> Option<*mut Value> {
    match ty {
        "int" => s.parse::<i64>().ok().map(alloc_int),
        "float" => s.parse::<f64>().ok().map(alloc_float),
        "bool" => match s {
            "true" | "True" => Some(alloc_bool(true)),
            "false" | "False" => Some(alloc_bool(false)),
            _ => None,
        },
        "tag" => Some(alloc(Value::Constructor(intern_str(s), alloc(Value::Unit)))),
        _ => Some(alloc(Value::Text(Arc::from(s)))),
    }
}

/// Coerce a JSON-parsed value to match the expected field type.
/// JSON strings become Constructors for "tag"-typed fields (all-nullary ADTs).
fn coerce_json_field(v: *mut Value, ty: &str) -> *mut Value {
    if ty == "tag" {
        if let Value::Text(s) = unsafe { as_ref(v) } {
            return alloc(Value::Constructor(intern_str(s), alloc(Value::Unit)));
        }
    }
    v
}

const BASE64_CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(BASE64_CHARS[((triple >> 18) & 0x3F) as usize] as char);
        out.push(BASE64_CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(BASE64_CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(BASE64_CHARS[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn base64_decode(s: &str) -> Vec<u8> {
    fn char_to_val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let bytes: Vec<u8> = s.bytes()
        .filter(|&b| b != b'=' && b != b'\n' && b != b'\r' && b != b' ' && b != b'\t')
        .filter_map(|b| char_to_val(b))
        .collect();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        if chunk.len() < 2 {
            // A single base64 character is malformed (encodes only 6 bits,
            // not enough for a full byte). Skip rather than silently losing data.
            break;
        }
        let b0 = chunk[0] as u32;
        let b1 = chunk[1] as u32;
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let b3 = if chunk.len() > 3 { chunk[3] as u32 } else { 0 };
        let triple = (b0 << 18) | (b1 << 12) | (b2 << 6) | b3;
        out.push(((triple >> 16) & 0xFF) as u8);
        if chunk.len() > 2 { out.push(((triple >> 8) & 0xFF) as u8); }
        if chunk.len() > 3 { out.push((triple & 0xFF) as u8); }
    }
    out
}

fn value_to_json(v: *mut Value) -> String {
    serde_json::to_string(&value_to_serde_json(v)).unwrap_or_else(|_| "null".to_string())
}

/// Convert a Knot *mut Value into a serde_json::Value.
fn value_to_serde_json(v: *mut Value) -> serde_json::Value {
    if v.is_null() {
        return serde_json::Value::Null;
    }
    match unsafe { as_ref(v) } {
        Value::Int(n) => serde_json::Value::Number((*n).into()),
        Value::Float(n) => {
            if n.is_finite() {
                serde_json::json!(*n)
            } else {
                panic!("knot runtime: toJson: cannot serialize non-finite float ({}) to JSON", n)
            }
        }
        Value::Text(s) => serde_json::Value::String((**s).to_string()),
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Bytes(b) => {
            let mut map = serde_json::Map::with_capacity(1);
            map.insert("__knot_bytes".into(), serde_json::Value::String(base64_encode(b)));
            serde_json::Value::Object(map)
        }
        Value::Unit => serde_json::Value::Null,
        Value::Record(fields) => {
            let mut map = serde_json::Map::with_capacity(fields.len());
            for f in fields {
                map.insert(f.name.to_string(), value_to_serde_json(f.value));
            }
            serde_json::Value::Object(map)
        }
        Value::Relation(rows) => {
            serde_json::Value::Array(rows.iter().map(|r| value_to_serde_json(*r)).collect())
        }
        Value::Constructor(tag, payload) => {
            // Wrap in `__knot_ctor` so parseJson can reconstruct without
            // colliding with user records that happen to use `tag`/`value` keys.
            let mut inner = serde_json::Map::with_capacity(2);
            inner.insert("tag".into(), serde_json::Value::String(tag.to_string()));
            inner.insert("value".into(), value_to_serde_json(*payload));
            let mut map = serde_json::Map::with_capacity(1);
            map.insert("__knot_ctor".into(), serde_json::Value::Object(inner));
            serde_json::Value::Object(map)
        }
        Value::Function(f) => serde_json::Value::String(format!("<function: {}>", &*f.source)),
        Value::IO(_, _) => serde_json::Value::String("<<IO>>".into()),
        Value::Pair(_, _) => serde_json::Value::String("<<Pair>>".into()),
    }
}

/// Call the compiled toJson dispatcher for a sub-value, returning the JSON string.
fn call_to_json_dispatcher(db: *mut c_void, v: *mut Value, to_json_fn: *const u8) -> String {
    let f: extern "C" fn(*mut c_void, *mut Value) -> *mut Value =
        unsafe { std::mem::transmute(to_json_fn) };
    let result = f(db, v);
    match unsafe { as_ref(result) } {
        Value::Text(s) => (**s).to_string(),
        _ => value_to_json(v),
    }
}

/// Like value_to_json but calls back through the trait dispatcher for nested values.
fn value_to_json_with(db: *mut c_void, v: *mut Value, to_json_fn: *const u8) -> String {
    if v.is_null() {
        return "null".to_string();
    }
    match unsafe { as_ref(v) } {
        // Compound types: recurse through the dispatcher for sub-values
        Value::Record(fields) => {
            let mut json = String::from("{");
            for (i, f) in fields.iter().enumerate() {
                if i > 0 { json.push(','); }
                // Use serde to properly escape the field name
                json.push_str(&serde_json::to_string(&*f.name).unwrap());
                json.push(':');
                json.push_str(&call_to_json_dispatcher(db, f.value, to_json_fn));
            }
            json.push('}');
            json
        }
        Value::Relation(rows) => {
            let mut json = String::from("[");
            for (i, r) in rows.iter().enumerate() {
                if i > 0 { json.push(','); }
                json.push_str(&call_to_json_dispatcher(db, *r, to_json_fn));
            }
            json.push(']');
            json
        }
        Value::Constructor(tag, payload) => {
            // Match value_to_serde_json's `__knot_ctor` wrapper so reconstruction
            // round-trips and user records with `tag`/`value` keys aren't shadowed.
            let mut json = String::from("{\"__knot_ctor\":{\"tag\":");
            json.push_str(&serde_json::to_string(&**tag).unwrap());
            json.push_str(",\"value\":");
            json.push_str(&call_to_json_dispatcher(db, *payload, to_json_fn));
            json.push_str("}}");
            json
        }
        // Leaf types: encode directly (no sub-values to dispatch on)
        _ => value_to_json(v),
    }
}

/// Convert camelCase field name to HTTP-Header-Case.
/// e.g. "authorization" → "Authorization", "contentType" → "Content-Type",
///      "xRequestId" → "X-Request-Id"
fn camel_to_header_case(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    let mut result = String::new();
    let len = chars.len();
    for (i, &c) in chars.iter().enumerate() {
        if i == 0 {
            result.extend(c.to_uppercase());
        } else if c.is_uppercase() {
            let prev_upper = chars[i - 1].is_uppercase();
            let next_lower = i + 1 < len && chars[i + 1].is_lowercase();
            // Insert hyphen before an uppercase letter when:
            //   - previous char was lowercase (new word: "contentType" → "Content-Type")
            //   - OR this is the last uppercase in a run followed by lowercase
            //     (acronym end: "xHTTPStatus" → "X-HTTP-Status")
            if !prev_upper || next_lower {
                result.push('-');
            }
            result.push(c);
        } else {
            result.push(c);
        }
    }
    result
}

/// Default ceiling on bytes read from a single HTTP request or response body.
/// Bounds memory exposure from unbounded body streams (DoS protection for
/// `listen`; protection against malicious upstreams for `fetch`).
const HTTP_MAX_BODY_BYTES_DEFAULT: u64 = 16 * 1024 * 1024;

/// Configurable HTTP body cap. Two ways to override the default:
///   1. Set `KNOT_HTTP_MAX_BODY_BYTES` in the environment before starting
///      the program (read once on first access).
///   2. Call `knot_set_http_max_body_bytes` from the host or from generated
///      code at any point — subsequent reads see the new limit.
/// Stored as `AtomicU64` so updates are visible to all threads (`listen`
/// runs request handlers on a worker pool; `fetch` may be called from
/// `fork`ed threads).
static HTTP_MAX_BODY_BYTES_CELL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn http_max_body_bytes() -> u64 {
    let current = HTTP_MAX_BODY_BYTES_CELL.load(std::sync::atomic::Ordering::Relaxed);
    if current != 0 {
        return current;
    }
    let resolved = std::env::var("KNOT_HTTP_MAX_BODY_BYTES")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(HTTP_MAX_BODY_BYTES_DEFAULT);
    // Best-effort cache; if another thread also resolved it, either store wins.
    HTTP_MAX_BODY_BYTES_CELL.store(resolved, std::sync::atomic::Ordering::Relaxed);
    resolved
}

/// Override the HTTP body cap at runtime. A value of `0` reverts to the
/// env-or-default resolution path. Exposed through the C ABI so generated
/// Knot code (or an embedder) can configure the runtime.
#[unsafe(no_mangle)]
pub extern "C" fn knot_set_http_max_body_bytes(bytes: u64) {
    HTTP_MAX_BODY_BYTES_CELL.store(bytes, std::sync::atomic::Ordering::Relaxed);
}

/// Parse a byte count with an optional `K`/`M`/`G` suffix (binary, so `1K` is
/// 1024, `2M` is 2 * 1024 * 1024). Empty input or unrecognised suffix
/// returns `None`. Used by `--http-max-body-bytes` so users can write `16M`
/// instead of 16777216.
fn parse_byte_size(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (num_str, mul): (&str, u64) = match s.as_bytes().last().copied() {
        Some(b'K') | Some(b'k') => (&s[..s.len() - 1], 1024),
        Some(b'M') | Some(b'm') => (&s[..s.len() - 1], 1024 * 1024),
        Some(b'G') | Some(b'g') => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        _ => (s, 1),
    };
    num_str.parse::<u64>().ok().and_then(|n| n.checked_mul(mul))
}

/// Scan process args for `--http-max-body-bytes=<size>` (or the two-arg form
/// `--http-max-body-bytes <size>`) and apply the value via
/// `knot_set_http_max_body_bytes`. Generated `main` calls this once at
/// startup so embedders don't need to do anything for the flag to work.
/// Invalid values exit with a clear error, matching `knot_override_lookup`.
#[unsafe(no_mangle)]
pub extern "C" fn knot_http_config_init() {
    const FLAG: &str = "--http-max-body-bytes";
    let args: Vec<String> = std::env::args().collect();
    let mut value: Option<String> = None;
    let mut i = 1;
    while i < args.len() {
        if let Some(rest) = args[i].strip_prefix(FLAG) {
            if let Some(v) = rest.strip_prefix('=') {
                value = Some(v.to_string());
                break;
            } else if rest.is_empty()
                && i + 1 < args.len()
                && !args[i + 1].starts_with("--")
            {
                value = Some(args[i + 1].clone());
                break;
            }
        }
        i += 1;
    }
    let Some(raw) = value else { return };
    match parse_byte_size(&raw) {
        Some(n) if n > 0 => knot_set_http_max_body_bytes(n),
        _ => {
            eprintln!(
                "Error: invalid value '{}' for {} (expected positive byte count, optionally with K/M/G suffix)",
                raw, FLAG
            );
            std::process::exit(1);
        }
    }
}

/// Parse a `Value` carrying a port number into a `u16`.
/// Used by both `knot_http_listen` and `knot_http_listen_on`.
fn parse_port_value(port_val: *mut Value, fn_name: &str) -> u16 {
    match unsafe { as_ref(port_val) } {
        Value::Int(n) => u16::try_from(*n).expect("knot runtime: port number out of range"),
        _ => panic!(
            "knot runtime: {} expects Int port, got {}",
            fn_name,
            type_name(port_val)
        ),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_http_listen(
    _db: *mut c_void,
    port_val: *mut Value,
    route_table: *mut c_void,
    handler: *mut Value,
) -> *mut Value {
    let port = parse_port_value(port_val, "listen");
    http_serve_loop(format!("0.0.0.0:{}", port), route_table, handler)
}

/// Like `knot_http_listen`, but binds to the supplied host. If `host` looks
/// like a bare IPv6 address (contains `:` and isn't already bracketed), wrap
/// it in `[...]` so `tiny_http` parses the `host:port` correctly.
#[unsafe(no_mangle)]
pub extern "C" fn knot_http_listen_on(
    _db: *mut c_void,
    host_val: *mut Value,
    port_val: *mut Value,
    route_table: *mut c_void,
    handler: *mut Value,
) -> *mut Value {
    let host: String = match unsafe { as_ref(host_val) } {
        Value::Text(s) => s.to_string(),
        _ => panic!(
            "knot runtime: listenOn expects Text host, got {}",
            type_name(host_val)
        ),
    };
    let port = parse_port_value(port_val, "listenOn");
    let addr = if host.contains(':') && !host.starts_with('[') {
        format!("[{}]:{}", host, port)
    } else {
        format!("{}:{}", host, port)
    };
    http_serve_loop(addr, route_table, handler)
}

/// Shared body for `knot_http_listen` / `knot_http_listen_on`: bind, log,
/// then run the request-accept loop forever.
fn http_serve_loop(
    addr: String,
    route_table: *mut c_void,
    handler: *mut Value,
) -> *mut Value {
    let table = Arc::new(*unsafe { Box::from_raw(route_table as *mut RouteTable) });
    let server = Arc::new(tiny_http::Server::http(&addr)
        .unwrap_or_else(|e| panic!("knot runtime: failed to start HTTP server on {}: {}", addr, e)));
    eprintln!("Knot HTTP server listening on http://{}", addr);

    loop {
        // Isolate each request's main-thread allocations in a dedicated
        // arena frame.  Previously we used `mark`/`reset_to` on the caller's
        // frame — that freed chunk-allocated temporaries but any values
        // that ended up in the frame's `pinned` set (e.g., intermediate
        // constructors from request parsing that got promoted) persisted
        // forever.  A fresh child frame discarded at request-cycle end
        // makes request handling zero-allocation-overhead across the
        // program's lifetime.  Handler threads still run on their own
        // thread-local arenas (spawned below).
        ARENA.with(|a| a.borrow_mut().push_frame());

        let mut request = match server.recv() {
            Ok(req) => req,
            Err(e) => {
                eprintln!("knot runtime: error receiving request: {}", e);
                ARENA.with(|a| a.borrow_mut().pop_frame());
                continue;
            }
        };

        let method = request.method().as_str().to_string();
        let url = request.url().to_string();

        if log::debug_enabled() {
            log_debug!("[HTTP] <-- {} {}", method, url);
            for header in request.headers() {
                log_debug!("[HTTP]     {}: {}", header.field, header.value);
            }
        }

        let (path, query_string) = match url.split_once('?') {
            Some((p, q)) => (p.to_string(), q.to_string()),
            None => (url.clone(), String::new()),
        };
        let path_segments: Vec<String> = path.split('/').filter(|s| !s.is_empty()).map(|s| s.to_string()).collect();

        let table = Arc::clone(&table);
        let path_seg_refs: Vec<&str> = path_segments.iter().map(|s| s.as_str()).collect();
        let matched = match_route(&table.entries, &method, &path_seg_refs);

        match matched {
            Some((entry, path_params)) => {
                // Read the body on the main thread before moving the request
                // GET and DELETE requests don't have bodies — skip reading even
                // if the route entry happens to declare body_fields.
                let has_body = !entry.body_fields.is_empty()
                    && entry.method != "GET" && entry.method != "HEAD";
                let body_bytes = if has_body {
                    use std::io::Read;
                    let mut buf = Vec::new();
                    // Cap body size to prevent OOM from oversized requests.
                    let max = http_max_body_bytes();
                    let mut limited = request.as_reader().take(max + 1);
                    let _ = limited.read_to_end(&mut buf);
                    if buf.len() as u64 > max {
                        eprintln!(
                            "knot runtime: request body exceeds {} byte limit; rejecting",
                            max
                        );
                        let response = tiny_http::Response::from_string("{\"error\":\"payload too large\"}")
                            .with_status_code(413)
                            .with_header("Content-Type: application/json".parse::<tiny_http::Header>().unwrap());
                        let _ = request.respond(response);
                        ARENA.with(|a| a.borrow_mut().pop_frame());
                        continue;
                    }
                    buf
                } else {
                    Vec::new()
                };

                // Collect request headers as owned strings
                let req_headers: Vec<(String, String)> = request.headers().iter()
                    .map(|h| (h.field.as_str().as_str().to_string(), h.value.as_str().to_string()))
                    .collect();

                // Clone route entry data we need
                let entry_method = entry.method.clone();
                let entry_body_fields = entry.body_fields.clone();
                let entry_query_fields = entry.query_fields.clone();
                let entry_path_parts = entry.path_parts.clone();
                let entry_request_headers = entry.request_headers.clone();
                let entry_response_headers = entry.response_headers.clone();
                let entry_constructor = entry.constructor.clone();
                let entry_refinements: Vec<FieldRefinement> = table.field_refinements
                    .iter()
                    .filter(|r| r.constructor == entry_constructor)
                    .cloned()
                    .collect();

                // Deep-clone handler for the worker thread
                let handler_cloned = deep_clone_value(handler) as usize;

                let handle = std::thread::spawn(move || {
                    let handler = handler_cloned as *mut Value;

                    // Open a DB connection for this thread
                    let db_path = DB_PATH.lock().unwrap().clone();
                    let db = knot_db_open(db_path.as_ptr(), db_path.len());

                    let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<*mut Value, (u16, String)> {
                    // Build record from path params, query params, and body
                    let mut fields: Vec<RecordField> = Vec::new();

                    // Path params
                    for (name, val) in &path_params {
                        let ty = entry_path_parts
                            .iter()
                            .find_map(|p| match p {
                                PathPart::Param(n, t) if *n == *name => Some(t.as_str()),
                                _ => None,
                            })
                            .unwrap_or("text");
                        let value = match try_string_to_value(val, ty) {
                            Some(v) => v,
                            None => return Err((400, format!(
                                "invalid path parameter '{}': '{}' is not a valid {}",
                                name, val, ty
                            ))),
                        };
                        fields.push(RecordField {
                            name: intern_str(name),
                            value,
                        });
                    }

                    // Query params
                    let qs = parse_query_string(&query_string);
                    for (qname, qty) in &entry_query_fields {
                        let is_maybe = qty.starts_with('?');
                        let inner_ty = if is_maybe { &qty[1..] } else { qty.as_str() };
                        let raw_val = qs.get(qname).map(|v| v.as_str());
                        let value = if is_maybe {
                            match raw_val {
                                Some(v) => {
                                    let inner = match try_string_to_value(v, inner_ty) {
                                        Some(iv) => iv,
                                        None => return Err((400, format!(
                                            "invalid query parameter '{}': '{}' is not a valid {}",
                                            qname, v, inner_ty
                                        ))),
                                    };
                                    alloc(Value::Constructor(
                                        "Just".into(),
                                        alloc(Value::Record(vec![
                                            RecordField { name: "value".into(), value: inner },
                                        ])),
                                    ))
                                }
                                None => alloc(Value::Constructor("Nothing".into(), alloc(Value::Unit))),
                            }
                        } else {
                            let raw = raw_val.unwrap_or("");
                            match try_string_to_value(raw, inner_ty) {
                                Some(v) => v,
                                None => return Err((400, format!(
                                    "invalid query parameter '{}': '{}' is not a valid {}",
                                    qname, raw, inner_ty
                                ))),
                            }
                        };
                        fields.push(RecordField {
                            name: intern_str(qname),
                            value,
                        });
                    }

                    // Body fields (JSON) — only parse for methods that carry a body
                    let has_body = !entry_body_fields.is_empty()
                        && entry_method != "GET" && entry_method != "HEAD";
                    if has_body {
                        let body_str = String::from_utf8_lossy(&body_bytes);
                        log_debug!("[HTTP]     body: {}", body_str);
                        let body_val = match serde_json::from_str::<serde_json::Value>(&body_str) {
                            Ok(json) => json_to_value(&json),
                            Err(e) => {
                                let msg = format!("invalid JSON body: {}", e);
                                log_debug!("[HTTP] --> 400 {}", msg);
                                return Err((400, msg));
                            }
                        };
                        match unsafe { as_ref(body_val) } {
                            Value::Record(body_fields) => {
                                for (bname, bty) in &entry_body_fields {
                                    let is_maybe = bty.starts_with('?');
                                    let inner_ty = if is_maybe { &bty[1..] } else { bty.as_str() };
                                    let raw_val = body_fields.iter()
                                        .find(|f| &*f.name == bname.as_str())
                                        .map(|f| f.value);
                                    let value = if is_maybe {
                                        match raw_val {
                                            Some(v) => {
                                                let coerced = coerce_json_field(v, inner_ty);
                                                alloc(Value::Constructor(
                                                    "Just".into(),
                                                    alloc(Value::Record(vec![
                                                        RecordField { name: "value".into(), value: coerced },
                                                    ])),
                                                ))
                                            }
                                            None => alloc(Value::Constructor("Nothing".into(), alloc(Value::Unit))),
                                        }
                                    } else {
                                        match raw_val {
                                            Some(v) => coerce_json_field(v, inner_ty),
                                            None => string_to_value("", inner_ty),
                                        }
                                    };
                                    fields.push(RecordField {
                                        name: intern_str(bname),
                                        value,
                                    });
                                }
                            }
                            _ => {
                                for (bname, bty) in &entry_body_fields {
                                    let is_maybe = bty.starts_with('?');
                                    let value = if is_maybe {
                                        alloc(Value::Constructor("Nothing".into(), alloc(Value::Unit)))
                                    } else {
                                        string_to_value("", bty)
                                    };
                                    fields.push(RecordField {
                                        name: intern_str(bname),
                                        value,
                                    });
                                }
                            }
                        }
                    }

                    // Request headers
                    for (hname, hty) in &entry_request_headers {
                        let http_name = camel_to_header_case(hname);
                        let is_maybe = hty.starts_with('?');
                        let inner_ty = if is_maybe { &hty[1..] } else { hty.as_str() };
                        let raw_val = req_headers.iter()
                            .find(|(k, _)| k.eq_ignore_ascii_case(&http_name))
                            .map(|(_, v)| v.clone());
                        let value = if is_maybe {
                            match raw_val {
                                Some(v) => {
                                    let inner = string_to_value(&v, inner_ty);
                                    alloc(Value::Constructor(
                                        "Just".into(),
                                        alloc(Value::Record(vec![
                                            RecordField { name: "value".into(), value: inner },
                                        ])),
                                    ))
                                }
                                None => alloc(Value::Constructor("Nothing".into(), alloc(Value::Unit))),
                            }
                        } else {
                            let v = raw_val.unwrap_or_default();
                            string_to_value(&v, inner_ty)
                        };
                        fields.push(RecordField {
                            name: intern_str(hname),
                            value,
                        });
                    }

                    // Validate refined body fields
                    for refinement in &entry_refinements {
                        if let Some(field) = fields.iter().find(|f| &*f.name == refinement.field_name.as_str()) {
                            let check = knot_refinement_check_value(db, field.value, refinement.predicate);
                            if check == 0 {
                                return Err((400, format!(
                                    "validation error: field '{}' does not satisfy '{}' constraint",
                                    refinement.field_name, refinement.type_name
                                )));
                            }
                        }
                    }

                    fields.sort_by(|a, b| a.name.cmp(&b.name));
                    let record = alloc(Value::Record(fields));
                    let ctor_val = alloc(Value::Constructor(intern_str(&entry_constructor), record));

                    // Call handler. The Server value is just a Knot function
                    // that takes the route ADT and returns the endpoint's
                    // declared response (or {body, headers} when response
                    // headers are declared). Run any IO thunks to completion.
                    let mut result = knot_value_call(db, handler, ctor_val);
                    while matches!(unsafe { as_ref(result) }, Value::IO(..)) {
                        result = knot_io_run(db, result);
                    }
                    Ok(result)
                    }));

                    match panic_result {
                        Ok(Ok(result)) => {
                    let has_resp_headers = !entry_response_headers.is_empty();
                    if has_resp_headers {
                        let body_val = knot_record_field(result, "body".as_ptr(), 4);
                        let hdrs_val = knot_record_field(result, "headers".as_ptr(), 7);
                        let json = json_encode_value(db, body_val);
                        let mut response = tiny_http::Response::from_string(&json)
                            .with_header(
                                "Content-Type: application/json"
                                    .parse::<tiny_http::Header>()
                                    .unwrap(),
                            );
                        if let Value::Record(hdr_fields) = unsafe { as_ref(hdrs_val) } {
                            for hf in hdr_fields {
                                let http_name = camel_to_header_case(&hf.name);
                                let hdr_value = fetch_value_to_text(hf.value);
                                if let Ok(header) = format!("{}: {}", http_name, hdr_value)
                                    .parse::<tiny_http::Header>()
                                {
                                    response = response.with_header(header);
                                }
                            }
                        }
                        log_debug!("[HTTP] --> 200 {}", json);
                        let _ = request.respond(response);
                    } else {
                        let json = json_encode_value(db, result);
                        log_debug!("[HTTP] --> 200 {}", json);
                        let response = tiny_http::Response::from_string(&json)
                            .with_header(
                                "Content-Type: application/json"
                                    .parse::<tiny_http::Header>()
                                    .unwrap(),
                            );
                        let _ = request.respond(response);
                    }
                        }
                        Ok(Err((status_code, error_msg))) => {
                            log_warn!("[HTTP] --> {} {}", status_code, error_msg);
                            let body = format!("{{\"error\":\"{}\"}}", json_escape(&error_msg));
                            let response = tiny_http::Response::from_string(&body)
                                .with_status_code(status_code)
                                .with_header(
                                    "Content-Type: application/json"
                                        .parse::<tiny_http::Header>()
                                        .unwrap(),
                                );
                            let _ = request.respond(response);
                        }
                        Err(panic_err) => {
                            // Release any write locks held by the panicked
                            // atomic block to prevent permanent deadlock.
                            write_lock_force_release();
                            let db_ref = unsafe { &*(db as *mut KnotDb) };
                            // Roll back any open savepoints before resetting depth.
                            let depth = db_ref.atomic_depth.get();
                            for d in (1..=depth).rev() {
                                let sp = format!("knot_atomic_{}", d);
                                let _ = db_ref.conn.execute_batch(
                                    &format!("ROLLBACK TO SAVEPOINT {}; RELEASE SAVEPOINT {};", sp, sp),
                                );
                            }
                            db_ref.atomic_depth.set(0);

                            let msg = if let Some(s) = panic_err.downcast_ref::<&str>() {
                                s.to_string()
                            } else if let Some(s) = panic_err.downcast_ref::<String>() {
                                s.clone()
                            } else {
                                "internal server error".to_string()
                            };

                            // Panics with "400:..." prefix indicate bad requests
                            let (status_code, error_msg) = if let Some(rest) = msg.strip_prefix("400:") {
                                (400, rest.to_string())
                            } else {
                                (500, msg.clone())
                            };

                            eprintln!("[HTTP] handler panicked: {}", msg);
                            let body = format!("{{\"error\":\"{}\"}}", json_escape(&error_msg));
                            let response = tiny_http::Response::from_string(&body)
                                .with_status_code(status_code)
                                .with_header(
                                    "Content-Type: application/json"
                                        .parse::<tiny_http::Header>()
                                        .unwrap(),
                                );
                            let _ = request.respond(response);
                        }
                    }

                    knot_db_close(db);
                    // Free the deep-cloned handler tree. This is safe because:
                    // 1. All handler execution is complete and the response is sent
                    // 2. The DB connection is closed
                    // 3. No other thread has access (this was deep-cloned for us)
                    // 4. Any arena values referencing into this tree are thread-local
                    //    and will be abandoned when this thread exits
                    unsafe { deep_drop_value(handler); }
                });
                // Don't push HTTP request handles into THREAD_HANDLES — the
                // server loop runs forever so they would accumulate without
                // bound.  Spawn a monitor thread to join and report panics.
                std::thread::spawn(move || {
                    if let Err(e) = handle.join() {
                        let msg = if let Some(s) = e.downcast_ref::<&str>() {
                            s.to_string()
                        } else if let Some(s) = e.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "unknown panic".to_string()
                        };
                        log_error!("[HTTP] handler thread panicked: {}", msg);
                    }
                });
            }
            None => {
                log_debug!("[HTTP] --> 404 not found");
                let response = tiny_http::Response::from_string("{\"error\":\"not found\"}")
                    .with_status_code(404)
                    .with_header(
                        "Content-Type: application/json"
                            .parse::<tiny_http::Header>()
                            .unwrap(),
                    );
                let _ = request.respond(response);
            }
        }

        // Free main-thread arena allocations (chunks + pinned) from this
        // request cycle.  Handler threads have their own thread-local arenas
        // which are dropped when the handler thread exits.
        ARENA.with(|a| a.borrow_mut().pop_frame());
    }
}

// ── HTTP client (fetch) ─────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_http_fetch_io(
    base_url: *mut Value,
    method_ptr: *const u8,
    method_len: usize,
    path_ptr: *const u8,
    path_len: usize,
    payload: *mut Value,
    body_ptr: *const u8,
    body_len: usize,
    query_ptr: *const u8,
    query_len: usize,
    resp_ptr: *const u8,
    resp_len: usize,
    headers: *mut Value,
    req_hdrs_ptr: *const u8,
    req_hdrs_len: usize,
    resp_hdrs_ptr: *const u8,
    resp_hdrs_len: usize,
) -> *mut Value {
    let method = alloc(Value::Text(Arc::from(
        unsafe { str_from_raw(method_ptr, method_len) },
    )));
    let path = alloc(Value::Text(Arc::from(
        unsafe { str_from_raw(path_ptr, path_len) },
    )));
    let body_desc = alloc(Value::Text(Arc::from(
        unsafe { str_from_raw(body_ptr, body_len) },
    )));
    let query_desc = alloc(Value::Text(Arc::from(
        unsafe { str_from_raw(query_ptr, query_len) },
    )));
    let resp_desc = alloc(Value::Text(Arc::from(
        unsafe { str_from_raw(resp_ptr, resp_len) },
    )));
    let req_hdrs_desc = alloc(Value::Text(Arc::from(
        unsafe { str_from_raw(req_hdrs_ptr, req_hdrs_len) },
    )));
    let resp_hdrs_desc = alloc(Value::Text(Arc::from(
        unsafe { str_from_raw(resp_hdrs_ptr, resp_hdrs_len) },
    )));

    // Env record — fields sorted alphabetically for index-based access
    // 0: base_url, 1: body_desc, 2: headers, 3: method, 4: path, 5: payload,
    // 6: query_desc, 7: req_hdrs_desc, 8: resp_desc, 9: resp_hdrs_desc
    let env = alloc(Value::Record(vec![
        RecordField { name: "base_url".into(), value: base_url },
        RecordField { name: "body_desc".into(), value: body_desc },
        RecordField { name: "headers".into(), value: headers },
        RecordField { name: "method".into(), value: method },
        RecordField { name: "path".into(), value: path },
        RecordField { name: "payload".into(), value: payload },
        RecordField { name: "query_desc".into(), value: query_desc },
        RecordField { name: "req_hdrs_desc".into(), value: req_hdrs_desc },
        RecordField { name: "resp_desc".into(), value: resp_desc },
        RecordField { name: "resp_hdrs_desc".into(), value: resp_hdrs_desc },
    ]));

    extern "C" fn fetch_thunk(_db: *mut c_void, env: *mut Value) -> *mut Value {
        let base_url = knot_record_field_by_index(env, 0);
        let body_desc = knot_record_field_by_index(env, 1);
        let headers = knot_record_field_by_index(env, 2);
        let method = knot_record_field_by_index(env, 3);
        let path = knot_record_field_by_index(env, 4);
        let payload = knot_record_field_by_index(env, 5);
        let query_desc = knot_record_field_by_index(env, 6);
        let req_hdrs_desc = knot_record_field_by_index(env, 7);
        let resp_desc = knot_record_field_by_index(env, 8);
        let resp_hdrs_desc = knot_record_field_by_index(env, 9);

        let base = match unsafe { as_ref(base_url) } {
            Value::Text(s) => s.clone(),
            _ => panic!("knot runtime: fetch expected Text base URL"),
        };
        let path_pattern: String = match unsafe { as_ref(path) } {
            Value::Text(s) => (**s).to_string(),
            _ => panic!("knot runtime: fetch expected Text path"),
        };
        let method_str: String = match unsafe { as_ref(method) } {
            Value::Text(s) => (**s).to_string(),
            _ => panic!("knot runtime: fetch expected Text method"),
        };

        // Build URL with path param substitution
        let url = fetch_build_url(&base, &path_pattern, payload);

        // Build body JSON from body field descriptor (skip for GET/HEAD)
        let body_json = match unsafe { as_ref(body_desc) } {
            Value::Text(s) if !s.is_empty()
                && method_str.as_str() != "GET" && method_str.as_str() != "HEAD" =>
            {
                Some(fetch_build_body(&**s, payload))
            }
            _ => None,
        };

        // Build query string from query field descriptor
        let query_string = match unsafe { as_ref(query_desc) } {
            Value::Text(s) if !s.is_empty() => Some(fetch_build_query(&**s, payload)),
            _ => None,
        };

        let full_url = match &query_string {
            Some(qs) if !qs.is_empty() => format!("{}?{}", url, qs),
            _ => url,
        };

        // Collect request headers into a vec (ureq 3 uses typed builders
        // that differ for methods with/without body, so we can't share a
        // single mutable request across the match arms)
        let mut req_headers: Vec<(String, String)> = Vec::new();
        let req_hdrs_str: String = match unsafe { as_ref(req_hdrs_desc) } {
            Value::Text(s) => (**s).to_string(),
            _ => String::new(),
        };
        let mut has_content_type = false;
        if !req_hdrs_str.is_empty() {
            for field_desc in req_hdrs_str.split(',') {
                if field_desc.is_empty() { continue; }
                let (name, ty) = field_desc.split_once(':').unwrap_or((field_desc, "text"));
                let is_maybe = ty.starts_with('?');
                let http_name = camel_to_header_case(name);
                if http_name.eq_ignore_ascii_case("Content-Type") {
                    has_content_type = true;
                }
                let field_val = knot_record_field(payload, name.as_ptr(), name.len());
                if is_maybe {
                    // Maybe type: skip Nothing, extract Just value
                    if !field_val.is_null() {
                        if let Value::Constructor(tag, inner) = unsafe { as_ref(field_val) } {
                            if &**tag == "Just" {
                                let v = knot_record_field(*inner, "value".as_ptr(), 5);
                                req_headers.push((http_name, fetch_value_to_text(v)));
                            }
                        }
                    }
                } else {
                    req_headers.push((http_name, fetch_value_to_text(field_val)));
                }
            }
        }

        // Set default Content-Type for JSON bodies, unless already set by
        // route-declared headers.  Ad-hoc fetchWith headers can still override.
        if body_json.is_some() && !has_content_type {
            req_headers.push(("Content-Type".to_string(), "application/json".to_string()));
        }

        // Ad-hoc headers from fetchWith options (override route-declared headers)
        if !headers.is_null() {
            if let Value::Relation(rows) = unsafe { as_ref(headers) } {
                for row in rows {
                    let n = fetch_record_text_field(*row, "name");
                    let v = fetch_record_text_field(*row, "value");
                    req_headers.push((n, v));
                }
            }
        }

        // Debug log outgoing fetch
        if log::debug_enabled() {
            log_debug!("[HTTP] --> {} {}", method_str, full_url);
            if let Some(ref json) = body_json {
                log_debug!("[HTTP]     body: {}", json);
            }
        }

        // Build agent that returns all HTTP responses as Ok
        // (ureq 3 default converts 4xx/5xx to Err without the body)
        let agent = ureq::Agent::new_with_config(
            ureq::config::Config::builder()
                .http_status_as_error(false)
                .build()
        );

        // Send request — split by method type since ureq 3 uses different
        // builder types for methods with/without body
        let result = match method_str.as_str() {
            "GET" | "HEAD" | "DELETE" => {
                let mut req = match method_str.as_str() {
                    "GET" => agent.get(&full_url),
                    "HEAD" => agent.head(&full_url),
                    _ => agent.delete(&full_url),
                };
                for (k, v) in &req_headers {
                    req = req.header(k.as_str(), v.as_str());
                }
                req.call()
            }
            _ => {
                let mut req = match method_str.as_str() {
                    "POST" => agent.post(&full_url),
                    "PUT" => agent.put(&full_url),
                    "PATCH" => agent.patch(&full_url),
                    _ => panic!("knot runtime: fetch unsupported method: {}", method_str),
                };
                for (k, v) in &req_headers {
                    req = req.header(k.as_str(), v.as_str());
                }
                match &body_json {
                    Some(json) => req.send(json.as_str()),
                    None => req.send_empty(),
                }
            }
        };

        // Check if we need to parse response headers
        let resp_hdrs_str: String = match unsafe { as_ref(resp_hdrs_desc) } {
            Value::Text(s) => (**s).to_string(),
            _ => String::new(),
        };
        let has_resp_hdrs = !resp_hdrs_str.is_empty();

        // Build Result ADT
        match result {
            Ok(mut response) => {
                let status = response.status().as_u16();
                log_debug!("[HTTP] <-- {} {}", status, full_url);

                // Parse declared response headers before touching the body so
                // the immutable header borrow is dropped before `body_mut`.
                let parsed_headers = if has_resp_hdrs && status < 400 {
                    let mut hdr_fields = Vec::new();
                    for field_desc in resp_hdrs_str.split(',') {
                        if field_desc.is_empty() { continue; }
                        let (name, ty) = field_desc.split_once(':').unwrap_or((field_desc, "text"));
                        let is_maybe = ty.starts_with('?');
                        let inner_ty = if is_maybe { &ty[1..] } else { ty };
                        let http_name = camel_to_header_case(name);
                        let raw_val = response.headers().get(&http_name)
                            .and_then(|v| v.to_str().ok())
                            .map(|s| s.to_string());
                        let value = if is_maybe {
                            match raw_val {
                                Some(v) => {
                                    let inner = string_to_value(&v, inner_ty);
                                    alloc(Value::Constructor(
                                        "Just".into(),
                                        alloc(Value::Record(vec![
                                            RecordField { name: "value".into(), value: inner },
                                        ])),
                                    ))
                                }
                                None => alloc(Value::Constructor("Nothing".into(), alloc(Value::Unit))),
                            }
                        } else {
                            let v = raw_val.unwrap_or_default();
                            string_to_value(&v, inner_ty)
                        };
                        hdr_fields.push(RecordField {
                            name: intern_str(name),
                            value,
                        });
                    }
                    hdr_fields.sort_by(|a, b| a.name.cmp(&b.name));
                    Some(alloc(Value::Record(hdr_fields)))
                } else {
                    None
                };

                let body_text = match fetch_read_capped_body(response.body_mut()) {
                    Ok(s) => s,
                    Err(e) => return fetch_build_err(status, &e),
                };

                if status >= 400 {
                    fetch_build_err(status, &body_text)
                } else {
                    let has_resp_schema = matches!(unsafe { as_ref(resp_desc) }, Value::Text(s) if !s.is_empty());
                    let parsed_body = if has_resp_schema {
                        match serde_json::from_str::<serde_json::Value>(&body_text) {
                            Ok(json) => json_to_value(&json),
                            Err(e) => return fetch_build_err(
                                status,
                                &format!("invalid JSON in response: {}", e),
                            ),
                        }
                    } else {
                        alloc(Value::Text(Arc::from(body_text)))
                    };

                    // Wrap with headers if response headers declared
                    let ok_value = match parsed_headers {
                        Some(hdrs) => alloc(Value::Record(vec![
                            RecordField { name: "body".into(), value: parsed_body },
                            RecordField { name: "headers".into(), value: hdrs },
                        ])),
                        None => parsed_body,
                    };

                    // Ok {value: ok_value}
                    alloc(Value::Constructor(
                        "Ok".into(),
                        alloc(Value::Record(vec![
                            RecordField { name: "value".into(), value: ok_value },
                        ])),
                    ))
                }
            }
            Err(e) => {
                log_debug!("[HTTP] <-- ERR {}", e);
                fetch_build_err(0, &format!("Network error: {}", e))
            }
        }
    }

    alloc(Value::IO(fetch_thunk as *const u8, env))
}

/// Percent-encode a string for use in URL path segments or query values.
fn percent_encode(s: &str) -> String {
    s.bytes()
        .flat_map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![b as char]
            }
            _ => format!("%{:02X}", b).chars().collect(),
        })
        .collect()
}

/// Unwrap a Maybe-typed value: returns Some(inner) for Just, None for Nothing.
fn unwrap_maybe(v: *mut Value) -> Option<*mut Value> {
    if v.is_null() {
        return None;
    }
    match unsafe { as_ref(v) } {
        Value::Constructor(tag, inner) if &**tag == "Just" => {
            Some(knot_record_field(*inner, "value".as_ptr(), 5))
        }
        Value::Constructor(tag, _) if &**tag == "Nothing" => None,
        _ => Some(v),
    }
}

/// Read an HTTP response body up to the runtime-configurable HTTP body cap.
/// Returns Err on either a read error or a body exceeding the cap — bounds
/// memory exposure from malicious or runaway upstreams (the matching ceiling
/// on inbound requests is enforced in `knot_http_listen`).
fn fetch_read_capped_body(body: &mut ureq::Body) -> Result<String, String> {
    use std::io::Read;
    let max = http_max_body_bytes();
    let mut buf = Vec::new();
    body.as_reader()
        .take(max + 1)
        .read_to_end(&mut buf)
        .map_err(|e| format!("response read error: {}", e))?;
    if buf.len() as u64 > max {
        return Err(format!("response body exceeds {} byte limit", max));
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// Build a full URL by substituting `{name:type}` path params from a record.
fn fetch_build_url(base: &str, path_pattern: &str, payload: *mut Value) -> String {
    let mut url = base.trim_end_matches('/').to_string();
    let mut remaining = path_pattern;
    while let Some(start) = remaining.find('{') {
        url.push_str(&remaining[..start]);
        // Search for the matching `}` *after* the `{` we just found — searching
        // from the front of `remaining` would let a stray earlier `}` produce
        // `end < start` and panic on the slice below.
        let Some(rel_end) = remaining[start + 1..].find('}') else {
            // Unmatched `{` — emit the rest verbatim and stop substituting.
            url.push_str(&remaining[start..]);
            return url;
        };
        let end = start + 1 + rel_end;
        let param = &remaining[start + 1..end];
        let (name, _ty) = param.split_once(':').unwrap_or((param, "text"));
        let field_val = knot_record_field(payload, name.as_ptr(), name.len());
        url.push_str(&percent_encode(&fetch_value_to_text(field_val)));
        remaining = &remaining[end + 1..];
    }
    url.push_str(remaining);
    url
}

/// Build a JSON body string from a field descriptor and record payload.
fn fetch_build_body(body_desc: &str, payload: *mut Value) -> String {
    let mut map = serde_json::Map::new();
    for field_desc in body_desc.split(',') {
        if field_desc.is_empty() {
            continue;
        }
        let (name, ty) = field_desc.split_once(':').unwrap_or((field_desc, "text"));
        let is_maybe = ty.starts_with('?');
        let field_val = knot_record_field(payload, name.as_ptr(), name.len());
        if is_maybe {
            match unwrap_maybe(field_val) {
                Some(inner) => { map.insert(name.to_string(), value_to_serde_json(inner)); }
                None => { map.insert(name.to_string(), serde_json::Value::Null); }
            }
        } else {
            map.insert(name.to_string(), value_to_serde_json(field_val));
        }
    }
    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
}

/// Build a query string from a field descriptor and record payload.
fn fetch_build_query(query_desc: &str, payload: *mut Value) -> String {
    let mut parts = Vec::new();
    for field_desc in query_desc.split(',') {
        if field_desc.is_empty() {
            continue;
        }
        let (name, ty) = field_desc.split_once(':').unwrap_or((field_desc, "text"));
        let is_maybe = ty.starts_with('?');
        let field_val = knot_record_field(payload, name.as_ptr(), name.len());
        let val = if is_maybe {
            match unwrap_maybe(field_val) {
                Some(inner) => inner,
                None => continue, // Skip Nothing query params
            }
        } else {
            field_val
        };
        let val_str = fetch_value_to_text(val);
        parts.push(format!("{}={}", name, percent_encode(&val_str)));
    }
    parts.join("&")
}

/// Convert a Knot value to its text representation for URL params.
fn fetch_value_to_text(v: *mut Value) -> String {
    if v.is_null() {
        return String::new();
    }
    match unsafe { as_ref(v) } {
        Value::Int(n) => n.to_string(),
        Value::Float(n) => n.to_string(),
        Value::Text(s) => (**s).to_string(),
        Value::Bool(b) => b.to_string(),
        _ => panic!(
            "knot runtime: cannot convert {} to text for URL parameter",
            type_name(v)
        ),
    }
}

/// Extract a Text field from a record by name.
fn fetch_record_text_field(record: *mut Value, field: &str) -> String {
    let val = knot_record_field(record, field.as_ptr(), field.len());
    if val.is_null() {
        return String::new();
    }
    match unsafe { as_ref(val) } {
        Value::Text(s) => (**s).to_string(),
        _ => String::new(),
    }
}

/// Build an Err {error: {message: Text, status: Int}} value.
fn fetch_build_err(status: u16, message: &str) -> *mut Value {
    let error_record = alloc(Value::Record(vec![
        RecordField {
            name: "message".into(),
            value: alloc(Value::Text(Arc::from(message))),
        },
        RecordField {
            name: "status".into(),
            value: alloc_int(status as i64),
        },
    ]));
    alloc(Value::Constructor(
        "Err".into(),
        alloc(Value::Record(vec![RecordField {
            name: "error".into(),
            value: error_record,
        }])),
    ))
}

// ── OpenAPI spec generation ──────────────────────────────────────

struct SendPtr(*mut c_void);
unsafe impl Send for SendPtr {}

static API_REGISTRY: Mutex<Vec<(String, SendPtr)>> = Mutex::new(Vec::new());

#[unsafe(no_mangle)]
pub extern "C" fn knot_api_register(
    name_ptr: *const u8,
    name_len: usize,
    table: *mut c_void,
) {
    let name = unsafe { str_from_raw(name_ptr, name_len) }.to_string();
    // Clone the table so the registry has its own independent copy,
    // allowing knot_http_listen to consume the original without use-after-free.
    let table_ref = unsafe { &*(table as *const RouteTable) };
    let cloned = Box::into_raw(Box::new(table_ref.clone())) as *mut c_void;
    API_REGISTRY.lock().unwrap().push((name, SendPtr(cloned)));
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_api_handle(argc: i32, argv: *const *const u8) -> i32 {
    if argc < 2 {
        return 0;
    }
    let args: Vec<String> = (0..argc as usize)
        .map(|i| unsafe {
            let ptr = *argv.add(i);
            let mut len = 0;
            while *ptr.add(len) != 0 {
                len += 1;
            }
            String::from_utf8_lossy(std::slice::from_raw_parts(ptr, len)).to_string()
        })
        .collect();

    if args.get(1).map(|s| s.as_str()) != Some("api") {
        return 0;
    }

    let registry = API_REGISTRY.lock().unwrap();

    if argc < 3 {
        eprintln!("Usage: <program> api <RouteName>");
        eprintln!();
        eprintln!("Available routes:");
        for (name, _) in registry.iter() {
            eprintln!("  {}", name);
        }
        std::process::exit(1);
    }

    let route_name = &args[2];

    for (name, SendPtr(table_ptr)) in registry.iter() {
        if name == route_name {
            let table = unsafe { &*(*table_ptr as *const RouteTable) };
            let spec = generate_openapi(name, table);
            println!("{}", spec);
            return 1;
        }
    }

    eprintln!("Unknown route: {}", route_name);
    eprintln!();
    eprintln!("Available routes:");
    for (name, _) in registry.iter() {
        eprintln!("  {}", name);
    }
    std::process::exit(1);
}

/// Handle `<program> db` subcommand: launch TUI database explorer.
/// Returns 1 if handled (caller should exit), 0 otherwise.
#[unsafe(no_mangle)]
pub extern "C" fn knot_db_handle(
    argc: i32,
    argv: *const *const u8,
    db_path_ptr: *const u8,
    db_path_len: usize,
) -> i32 {
    if argc < 2 {
        return 0;
    }
    let arg1 = unsafe {
        let ptr = *argv.add(1);
        let mut len = 0;
        while *ptr.add(len) != 0 {
            len += 1;
        }
        String::from_utf8_lossy(std::slice::from_raw_parts(ptr, len)).to_string()
    };

    if arg1 != "db" {
        return 0;
    }

    let db_path = unsafe { str_from_raw(db_path_ptr, db_path_len) };

    if let Err(e) = tui::run_db_explorer(db_path) {
        eprintln!("knot db: {}", e);
        std::process::exit(1);
    }
    1
}

fn generate_openapi(name: &str, table: &RouteTable) -> String {
    let mut out = String::new();
    out.push_str("{\n");
    out.push_str("  \"openapi\": \"3.0.3\",\n");
    out.push_str(&format!(
        "  \"info\": {{ \"title\": \"{}\", \"version\": \"1.0.0\" }},\n",
        json_escape(name)
    ));
    out.push_str("  \"paths\": {\n");

    // Group entries by path
    let mut path_map: HashMap<String, Vec<&RouteTableEntry>> = HashMap::new();
    for entry in &table.entries {
        let path_str = openapi_path(&entry.path_parts);
        path_map.entry(path_str).or_default().push(entry);
    }
    let paths: Vec<(String, Vec<&RouteTableEntry>)> = path_map.into_iter().collect();

    for (i, (path, entries)) in paths.iter().enumerate() {
        out.push_str(&format!("    \"{}\": {{\n", json_escape(path)));
        for (j, entry) in entries.iter().enumerate() {
            let method = entry.method.to_lowercase();
            out.push_str(&format!("      \"{}\": {{\n", method));
            out.push_str(&format!(
                "        \"operationId\": \"{}\",\n",
                json_escape(&entry.constructor)
            ));

            // Collect parameters (path + query)
            let mut params = Vec::new();
            for part in &entry.path_parts {
                if let PathPart::Param(pname, pty) = part {
                    params.push(format!(
                        "{{ \"name\": \"{}\", \"in\": \"path\", \"required\": true, \"schema\": {} }}",
                        json_escape(pname),
                        type_to_openapi_schema(pty)
                    ));
                }
            }
            for (qname, qty) in &entry.query_fields {
                params.push(format!(
                    "{{ \"name\": \"{}\", \"in\": \"query\", \"required\": false, \"schema\": {} }}",
                    json_escape(qname),
                    type_to_openapi_schema(qty)
                ));
            }

            let has_body = !entry.body_fields.is_empty();
            let has_response = !entry.response_type.is_empty();

            if !params.is_empty() {
                out.push_str("        \"parameters\": [\n");
                for (k, param) in params.iter().enumerate() {
                    out.push_str(&format!("          {}", param));
                    if k + 1 < params.len() {
                        out.push(',');
                    }
                    out.push('\n');
                }
                out.push_str("        ]");
                if has_body || has_response {
                    out.push(',');
                }
                out.push('\n');
            }

            // Request body
            if has_body {
                out.push_str("        \"requestBody\": {\n");
                out.push_str("          \"required\": true,\n");
                out.push_str("          \"content\": {\n");
                out.push_str("            \"application/json\": {\n");
                out.push_str("              \"schema\": {\n");
                out.push_str("                \"type\": \"object\",\n");
                out.push_str("                \"properties\": {\n");
                for (k, (fname, fty)) in entry.body_fields.iter().enumerate() {
                    out.push_str(&format!(
                        "                  \"{}\": {}",
                        json_escape(fname),
                        type_to_openapi_schema(fty)
                    ));
                    if k + 1 < entry.body_fields.len() {
                        out.push(',');
                    }
                    out.push('\n');
                }
                out.push_str("                },\n");
                out.push_str("                \"required\": [");
                for (k, (fname, _)) in entry.body_fields.iter().enumerate() {
                    out.push_str(&format!("\"{}\"", json_escape(fname)));
                    if k + 1 < entry.body_fields.len() {
                        out.push_str(", ");
                    }
                }
                out.push_str("]\n");
                out.push_str("              }\n");
                out.push_str("            }\n");
                out.push_str("          }\n");
                out.push_str("        }");
                if has_response {
                    out.push(',');
                }
                out.push('\n');
            }

            // Response
            out.push_str("        \"responses\": {\n");
            out.push_str("          \"200\": {\n");
            out.push_str("            \"description\": \"Successful response\"");
            if has_response {
                out.push_str(",\n");
                out.push_str("            \"content\": {\n");
                out.push_str("              \"application/json\": {\n");
                out.push_str(&format!(
                    "                \"schema\": {}\n",
                    response_type_to_schema(&entry.response_type)
                ));
                out.push_str("              }\n");
                out.push_str("            }\n");
            } else {
                out.push('\n');
            }
            out.push_str("          }\n");
            out.push_str("        }\n");

            out.push_str("      }");
            if j + 1 < entries.len() {
                out.push(',');
            }
            out.push('\n');
        }
        out.push_str("    }");
        if i + 1 < paths.len() {
            out.push(',');
        }
        out.push('\n');
    }

    out.push_str("  }\n");
    out.push_str("}\n");
    out
}

fn openapi_path(parts: &[PathPart]) -> String {
    if parts.is_empty() {
        return "/".to_string();
    }
    let mut s = String::new();
    for part in parts {
        s.push('/');
        match part {
            PathPart::Literal(lit) => s.push_str(lit),
            PathPart::Param(name, _) => {
                s.push('{');
                s.push_str(name);
                s.push('}');
            }
        }
    }
    s
}

fn type_to_openapi_schema(ty: &str) -> &'static str {
    match ty {
        "int" => "{ \"type\": \"integer\" }",
        "float" => "{ \"type\": \"number\" }",
        "bool" => "{ \"type\": \"boolean\" }",
        "text" => "{ \"type\": \"string\" }",
        _ => "{ \"type\": \"string\" }",
    }
}

/// Parse a response type descriptor and produce an OpenAPI schema JSON string.
///
/// Descriptor format:
/// - `int` / `float` / `text` / `bool` — primitives
/// - `[<inner>]` — array of inner type
/// - `{name:type,name:type}` — object
/// - Anything else — treated as string
fn response_type_to_schema(desc: &str) -> String {
    let desc = desc.trim();
    if desc.is_empty() {
        return "{}".to_string();
    }
    match desc {
        "int" => "{ \"type\": \"integer\" }".to_string(),
        "float" => "{ \"type\": \"number\" }".to_string(),
        "bool" => "{ \"type\": \"boolean\" }".to_string(),
        "text" => "{ \"type\": \"string\" }".to_string(),
        "unit" => "{ \"type\": \"object\" }".to_string(),
        _ if desc.starts_with('[') && desc.ends_with(']') => {
            let inner = &desc[1..desc.len() - 1];
            format!(
                "{{ \"type\": \"array\", \"items\": {} }}",
                response_type_to_schema(inner)
            )
        }
        _ if desc.starts_with('{') && desc.ends_with('}') => {
            let inner = &desc[1..desc.len() - 1];
            let fields = parse_response_fields(inner);
            let mut s = String::new();
            s.push_str("{ \"type\": \"object\", \"properties\": { ");
            for (i, (fname, fty)) in fields.iter().enumerate() {
                s.push_str(&format!(
                    "\"{}\": {}",
                    json_escape(fname),
                    response_type_to_schema(fty)
                ));
                if i + 1 < fields.len() {
                    s.push_str(", ");
                }
            }
            s.push_str(" } }");
            s
        }
        _ => "{ \"type\": \"string\" }".to_string(),
    }
}

/// Parse comma-separated `name:type` fields, respecting nested brackets/braces.
fn parse_response_fields(s: &str) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    let bytes = s.as_bytes();
    for i in 0..bytes.len() {
        match bytes[i] {
            b'[' | b'{' => depth += 1,
            b']' | b'}' => depth -= 1,
            b',' if depth == 0 => {
                let part = s[start..i].trim();
                if let Some((name, ty)) = part.split_once(':') {
                    fields.push((name.trim().to_string(), ty.trim().to_string()));
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    let part = s[start..].trim();
    if let Some((name, ty)) = part.split_once(':') {
        fields.push((name.trim().to_string(), ty.trim().to_string()));
    }
    fields
}

fn json_escape(s: &str) -> String {
    // serde_json::to_string produces a quoted string with proper escaping;
    // strip the surrounding quotes for use in manually-built JSON.
    let quoted = serde_json::to_string(s).unwrap_or_else(|_| format!("\"{}\"", s));
    quoted[1..quoted.len() - 1].to_string()
}

// ── Hash index for equi-join optimization ──────────────────────────

struct HashIndex {
    map: HashMap<Vec<u8>, Vec<*mut Value>>,
}

/// Serialize a Value to compact bytes for use as a hash key.
fn serialize_value_for_hash(v: *mut Value) -> Vec<u8> {
    let mut buf = Vec::new();
    serialize_value_for_hash_into(v, &mut buf);
    buf
}

fn serialize_value_for_hash_into(v: *mut Value, buf: &mut Vec<u8>) {
    if v.is_null() {
        buf.push(0xFF);
        return;
    }
    // Tag bytes must match value_to_hash_bytes for cross-path consistency.
    match unsafe { as_ref(v) } {
        Value::Int(n) => {
            buf.push(0);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::Float(f) => {
            buf.push(1);
            // Use raw bits for hashing to match total_cmp equality semantics
            // (total_cmp distinguishes -0.0 from +0.0). Canonicalize NaN so
            // all NaN bit patterns hash the same (total_cmp treats them equal).
            let bits = if f.is_nan() { f64::NAN.to_bits() } else { f.to_bits() };
            buf.extend_from_slice(&bits.to_le_bytes());
        }
        Value::Text(s) => {
            buf.push(2);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Bool(b) => {
            buf.push(3);
            buf.push(*b as u8);
        }
        Value::Bytes(b) => {
            buf.push(4);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Unit => {
            buf.push(5);
        }
        Value::Record(fields) => {
            buf.push(6);
            buf.extend_from_slice(&(fields.len() as u32).to_le_bytes());
            for field in fields {
                buf.extend_from_slice(&(field.name.len() as u32).to_le_bytes());
                buf.extend_from_slice(field.name.as_bytes());
                serialize_value_for_hash_into(field.value, buf);
            }
        }
        Value::Constructor(tag, payload) => {
            buf.push(7);
            let tag_bytes = tag.as_bytes();
            buf.extend_from_slice(&(tag_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(tag_bytes);
            serialize_value_for_hash_into(*payload, buf);
        }
        Value::Relation(rows) => {
            buf.push(8);
            buf.extend_from_slice(&(rows.len() as u32).to_le_bytes());
            let mut row_bytes: Vec<Vec<u8>> = rows
                .iter()
                .map(|r| {
                    let mut rb = Vec::new();
                    serialize_value_for_hash_into(*r, &mut rb);
                    rb
                })
                .collect();
            row_bytes.sort_unstable();
            for rb in &row_bytes {
                buf.extend_from_slice(&(rb.len() as u32).to_le_bytes());
                buf.extend_from_slice(rb);
            }
        }
        Value::Function(f) => {
            buf.push(9);
            buf.extend_from_slice(&(f.source.len() as u32).to_le_bytes());
            buf.extend_from_slice(f.source.as_bytes());
            serialize_value_for_hash_into(f.env, buf);
        }
        Value::IO(_, _) => {
            buf.push(11);
        }
        Value::Pair(a, b) => {
            buf.push(12);
            serialize_value_for_hash_into(*a, buf);
            serialize_value_for_hash_into(*b, buf);
        }
    }
}

/// Build a hash index over a relation on a given field.
/// Returns an opaque pointer to a heap-allocated HashIndex.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_build_index(
    rel: *mut Value,
    field_ptr: *const u8,
    field_len: usize,
) -> *mut c_void {
    let field_name = unsafe { str_from_raw(field_ptr, field_len) };
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!("knot runtime: build_index expected Relation, got {}", type_name(rel)),
    };

    let mut map: HashMap<Vec<u8>, Vec<*mut Value>> = HashMap::new();
    for &row in rows {
        let key_val = knot_record_field(row, field_ptr, field_len);
        let key = serialize_value_for_hash(key_val);
        map.entry(key).or_default().push(row);
    }

    log_debug!(
        "[OPT] hash index on .{}: {} keys from {} rows",
        field_name,
        map.len(),
        rows.len()
    );

    Box::into_raw(Box::new(HashIndex { map })) as *mut c_void
}

/// Look up matching rows in a hash index by key value.
/// Returns a Relation of matching rows (empty if no match).
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_index_lookup(
    index: *mut c_void,
    key: *mut Value,
) -> *mut Value {
    let idx = unsafe { &*(index as *mut HashIndex) };
    let hash_key = serialize_value_for_hash(key);
    match idx.map.get(&hash_key) {
        Some(rows) => alloc(Value::Relation(rows.clone())),
        None => alloc(Value::Relation(Vec::new())),
    }
}

/// Free a hash index.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_index_free(index: *mut c_void) {
    if index.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(index as *mut HashIndex));
    }
}

// ── Elliptic curve cryptography ──────────────────────────────────

/// Generate an X25519 key pair for encryption.
/// Returns Record {privateKey: Bytes, publicKey: Bytes}.
#[unsafe(no_mangle)]
pub extern "C" fn knot_crypto_generate_key_pair() -> *mut Value {
    let mut secret_bytes = [0u8; 32];
    getrandom::fill(&mut secret_bytes).expect("knot runtime: failed to generate random bytes");
    let secret = x25519_dalek::StaticSecret::from(secret_bytes);
    let public = x25519_dalek::PublicKey::from(&secret);

    let record = knot_record_empty(2);
    let k = b"privateKey";
    knot_record_set_field(record, k.as_ptr(), k.len(), alloc(Value::Bytes(Arc::from(secret_bytes.to_vec()))));
    let k = b"publicKey";
    knot_record_set_field(record, k.as_ptr(), k.len(), alloc(Value::Bytes(Arc::from(public.as_bytes().to_vec()))));
    record
}

/// Generate an Ed25519 key pair for signing.
/// Returns Record {privateKey: Bytes, publicKey: Bytes}.
#[unsafe(no_mangle)]
pub extern "C" fn knot_crypto_generate_signing_key_pair() -> *mut Value {
    let mut secret_bytes = [0u8; 32];
    getrandom::fill(&mut secret_bytes).expect("knot runtime: failed to generate random bytes");
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&secret_bytes);
    let verifying_key = signing_key.verifying_key();

    let record = knot_record_empty(2);
    let k = b"privateKey";
    knot_record_set_field(record, k.as_ptr(), k.len(), alloc(Value::Bytes(Arc::from(signing_key.to_bytes().to_vec()))));
    let k = b"publicKey";
    knot_record_set_field(record, k.as_ptr(), k.len(), alloc(Value::Bytes(Arc::from(verifying_key.to_bytes().to_vec()))));
    record
}

/// Sealed-box encryption: X25519 ECDH + ChaCha20-Poly1305.
/// Takes (publicKey: Bytes, plaintext: Bytes), returns ciphertext Bytes.
/// Format: [ephemeral_pub: 32][nonce: 12][encrypted + tag: len+16]
#[unsafe(no_mangle)]
pub extern "C" fn knot_crypto_encrypt(public_key: *mut Value, plaintext: *mut Value) -> *mut Value {
    use chacha20poly1305::{ChaCha20Poly1305, KeyInit};
    use chacha20poly1305::aead::Aead;

    let pub_bytes = match unsafe { as_ref(public_key) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: encrypt expected Bytes for publicKey, got {}", type_name(public_key)),
    };
    let plain = match unsafe { as_ref(plaintext) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: encrypt expected Bytes for plaintext, got {}", type_name(plaintext)),
    };

    let recipient_pub: [u8; 32] = (**pub_bytes).try_into()
        .expect("knot runtime: encrypt publicKey must be 32 bytes");
    let recipient_public = x25519_dalek::PublicKey::from(recipient_pub);

    // Generate ephemeral key pair
    let mut eph_secret_bytes = [0u8; 32];
    getrandom::fill(&mut eph_secret_bytes).expect("knot runtime: failed to generate random bytes");
    let eph_secret = x25519_dalek::StaticSecret::from(eph_secret_bytes);
    let eph_public = x25519_dalek::PublicKey::from(&eph_secret);

    // ECDH shared secret
    let shared = eph_secret.diffie_hellman(&recipient_public);
    let key = chacha20poly1305::Key::from_slice(shared.as_bytes());
    let cipher = ChaCha20Poly1305::new(key);

    // Random nonce
    let mut nonce_bytes = [0u8; 12];
    getrandom::fill(&mut nonce_bytes).expect("knot runtime: failed to generate nonce");
    let nonce = chacha20poly1305::Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher.encrypt(nonce, &**plain)
        .expect("knot runtime: encryption failed");

    // Pack: ephemeral_public (32) + nonce (12) + ciphertext
    let mut result = Vec::with_capacity(32 + 12 + ciphertext.len());
    result.extend_from_slice(eph_public.as_bytes());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);
    alloc(Value::Bytes(Arc::from(result)))
}

/// Sealed-box decryption: reverse of encrypt.
/// Takes (privateKey: Bytes, ciphertext: Bytes), returns plaintext Bytes.
#[unsafe(no_mangle)]
pub extern "C" fn knot_crypto_decrypt(private_key: *mut Value, ciphertext: *mut Value) -> *mut Value {
    use chacha20poly1305::{ChaCha20Poly1305, KeyInit};
    use chacha20poly1305::aead::Aead;

    let priv_bytes = match unsafe { as_ref(private_key) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: decrypt expected Bytes for privateKey, got {}", type_name(private_key)),
    };
    let ct = match unsafe { as_ref(ciphertext) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: decrypt expected Bytes for ciphertext, got {}", type_name(ciphertext)),
    };

    if ct.len() < 32 + 12 + 16 {
        panic!("knot runtime: decrypt ciphertext too short (need at least 60 bytes, got {})", ct.len());
    }

    let secret_bytes: [u8; 32] = (**priv_bytes).try_into()
        .expect("knot runtime: decrypt privateKey must be 32 bytes");
    let secret = x25519_dalek::StaticSecret::from(secret_bytes);

    // Unpack
    let eph_pub_bytes: [u8; 32] = ct[..32].try_into().unwrap();
    let nonce_bytes: [u8; 12] = ct[32..44].try_into().unwrap();
    let encrypted = &ct[44..];

    let eph_public = x25519_dalek::PublicKey::from(eph_pub_bytes);
    let shared = secret.diffie_hellman(&eph_public);
    let key = chacha20poly1305::Key::from_slice(shared.as_bytes());
    let cipher = ChaCha20Poly1305::new(key);
    let nonce = chacha20poly1305::Nonce::from_slice(&nonce_bytes);

    let plaintext = cipher.decrypt(nonce, encrypted)
        .expect("knot runtime: decryption failed (invalid key or corrupted ciphertext)");
    alloc(Value::Bytes(Arc::from(plaintext)))
}

/// Ed25519 signing. Takes (privateKey: Bytes, message: Bytes), returns signature Bytes.
#[unsafe(no_mangle)]
pub extern "C" fn knot_crypto_sign(private_key: *mut Value, message: *mut Value) -> *mut Value {
    use ed25519_dalek::Signer;

    let priv_bytes = match unsafe { as_ref(private_key) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: sign expected Bytes for privateKey, got {}", type_name(private_key)),
    };
    let msg = match unsafe { as_ref(message) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: sign expected Bytes for message, got {}", type_name(message)),
    };

    let secret_bytes: [u8; 32] = (**priv_bytes).try_into()
        .expect("knot runtime: sign privateKey must be 32 bytes");
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&secret_bytes);
    let signature = signing_key.sign(msg);
    alloc(Value::Bytes(Arc::from(signature.to_bytes().to_vec())))
}

/// Ed25519 verification. Takes (db, publicKey: Bytes, message: Bytes, signature: Bytes), returns Bool.
#[unsafe(no_mangle)]
pub extern "C" fn knot_crypto_verify(
    _db: *mut c_void,
    public_key: *mut Value,
    message: *mut Value,
    signature: *mut Value,
) -> *mut Value {
    use ed25519_dalek::Verifier;

    let pub_bytes = match unsafe { as_ref(public_key) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: verify expected Bytes for publicKey, got {}", type_name(public_key)),
    };
    let msg = match unsafe { as_ref(message) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: verify expected Bytes for message, got {}", type_name(message)),
    };
    let sig_bytes = match unsafe { as_ref(signature) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: verify expected Bytes for signature, got {}", type_name(signature)),
    };

    let pub_arr: [u8; 32] = (**pub_bytes).try_into()
        .expect("knot runtime: verify publicKey must be 32 bytes");
    let sig_arr: [u8; 64] = (**sig_bytes).try_into()
        .expect("knot runtime: verify signature must be 64 bytes");

    let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&pub_arr)
        .expect("knot runtime: verify invalid public key");
    let signature = ed25519_dalek::Signature::from_bytes(&sig_arr);

    let valid = verifying_key.verify(msg, &signature).is_ok();
    alloc_bool(valid)
}

// IO wrappers for effectful crypto builtins

#[unsafe(no_mangle)]
pub extern "C" fn knot_crypto_generate_key_pair_io() -> *mut Value {
    extern "C" fn thunk(_db: *mut c_void, _env: *mut Value) -> *mut Value {
        knot_crypto_generate_key_pair()
    }
    alloc(Value::IO(thunk as *const u8, std::ptr::null_mut()))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_crypto_generate_signing_key_pair_io() -> *mut Value {
    extern "C" fn thunk(_db: *mut c_void, _env: *mut Value) -> *mut Value {
        knot_crypto_generate_signing_key_pair()
    }
    alloc(Value::IO(thunk as *const u8, std::ptr::null_mut()))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_crypto_encrypt_io(public_key: *mut Value, plaintext: *mut Value) -> *mut Value {
    let env = knot_record_empty(2);
    let k = b"a";
    knot_record_set_field(env, k.as_ptr(), k.len(), public_key);
    let k = b"b";
    knot_record_set_field(env, k.as_ptr(), k.len(), plaintext);
    extern "C" fn thunk(_db: *mut c_void, env: *mut Value) -> *mut Value {
        let a = b"a";
        let public_key = knot_record_field(env, a.as_ptr(), a.len());
        let b = b"b";
        let plaintext = knot_record_field(env, b.as_ptr(), b.len());
        knot_crypto_encrypt(public_key, plaintext)
    }
    alloc(Value::IO(thunk as *const u8, env))
}

#[cfg(test)]
mod _size_tests {
    use super::*;
    /// Sanity-check the `Value` size assumption baked into `CHUNK_CAP`.
    /// If this starts failing, re-evaluate `CHUNK_CAP`'s 20 KB/chunk target.
    #[test]
    fn value_size_fits_l1() {
        let slot = std::mem::size_of::<std::mem::MaybeUninit<Value>>();
        let chunk_bytes = CHUNK_CAP * slot;
        assert!(chunk_bytes <= 32 * 1024,
            "chunk size {} B exceeds 32 KB Intel L1D budget (sizeof(Value) = {})",
            chunk_bytes, std::mem::size_of::<Value>());
    }

    /// Report sizes to stdout when run with --nocapture.  Useful for
    /// tuning CHUNK_CAP or evaluating layout changes.
    #[test]
    fn report_sizes() {
        eprintln!("sizeof(Value) = {}", std::mem::size_of::<Value>());
        eprintln!("sizeof(RecordField) = {}", std::mem::size_of::<RecordField>());
        eprintln!("sizeof(FunctionInner) = {}", std::mem::size_of::<FunctionInner>());
        eprintln!("sizeof(Arc<str>) = {}", std::mem::size_of::<Arc<str>>());
    }
}
