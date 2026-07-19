# Shared mux broker — design doc (not adopted)

**Status:** investigated and prototyped, not merged to `main`. The working
implementation is preserved verbatim on branch `archived/mux-broker-poc`
(commit `a21628cd693a0d4223fef17c8c5a4c1702c8a8b2`). This doc exists so that
work isn't wasted if the feature is reconsidered later: it captures the
problem, the real findings from testing against production TVheadend, the
architecture, every pitfall hit and how it was fixed, and the final
measurements — enough to resurrect this in an hour instead of a day.

## Why this was investigated

`abertpy proxy` is spawned by TVheadend once per pPID/channel (a `pipe://`
IPTV mux). Several pPIDs commonly live on the same physical Hispasat
transponder. Watching several concurrently meant N independent
`/stream/service/<uuid>` subscriptions to TVheadend and N separate Python
processes, each doing its own HTTP fetch + TS parsing. Reported symptom:
4-5 concurrent pPID watches on the same transponder noticeably degraded
host performance.

The original idea (from the user): use something like ZeroMQ so a second
client "subscribes" to an already-fetched stream instead of independently
re-fetching.

## Investigation findings (true regardless of whether this gets implemented)

These were established by directly querying and load-testing the real
TVheadend host (`http://192.168.3.148:9981`, TVheadend 4.3-2498, adapter
"TurboSight TBS 6909x (Octa DVB-S/S2/S2X)", 4 tuners). Re-verify against
whatever host is current before trusting old specifics.

1. **A "private PID" (pPID) is a full hidden video elementary stream, not a
   small ECM/key trickle.** Measured bitrates across real pPIDs on
   transponder 11222H ranged from ~235 kbps to ~12.5 Mbps. This rules out
   the naive "just always fetch the whole raw mux" approach: some
   transponders on this host had up to 12 enabled pPID overrides, so
   fetching everything unconditionally could mean 50-100+ Mbps flowing into
   a Python process to serve one ~10 Mbps channel.

2. **TVheadend already shares the physical DVB-S tuner correctly** across
   multiple different-pid subscriptions to the same transponder. Firing 5
   concurrent requests for 5 different pPIDs on one transponder showed all
   5 landing on the *same* physical adapter (`subs=5` on one adapter entry
   in `/api/status/inputs`, not 5 adapters retuning). No wasted duplicate
   satellite reception at the hardware level.

3. **TVheadend's `/stream/mux/<uuid>?pids=<csv>` endpoint can serve the
   union of several pPIDs through one subscription.** This is documented
   TVheadend behavior (see "Endpoints" below), not something added for this
   feature. Verified directly: requesting the exact same union of 5 pids
   through one call showed zero discontinuities and per-pid bitrates
   matching what each pid measured individually. This is the mechanism the
   whole design rests on.

4. **A red herring worth remembering:** an early test made it look like
   TVheadend was "starving" some of 5 concurrent subscriptions (3 of 5
   capped at exactly 256KB while 2 got much more). Re-requesting the
   identical union of pids through *one* unified subscription reproduced
   the exact same low bitrates for the exact same pids — i.e. those 3 pids
   are just genuinely low-bitrate at that moment, not being throttled. Real
   contention was never observed on this hardware at this concurrency
   (adapter `te`/`unc`/`cc` error counters stayed at 0 in every test, old
   architecture and new).

### Endpoints (from TVheadend docs, cross-checked against the real server)

- `/stream/service/<uuid>` — single service, already used by the existing
  (pre-this-feature) code.
- `/stream/mux/<muxid>?pids=<csv>` — raw mux, filtered server-side to the
  given pids. This is the endpoint the whole design depends on.
- A `User-Agent` of `curl/`, `wget/`, `MPlayer`, `TVHeadend`, `Lavf`, or
  containing `shoutcastsource` gets the stream directly with no ticket
  needed (this is why the existing code already sends
  `User-Agent: curl/aiohttp` and never deals with `/play/ticket/...`).

## Architecture

Per real Hispasat transponder (the `parent_dvb_mux_uuid`, already resolved
inside the existing `recreate_mux_if_needed` — it needed to start returning
this in addition to the service uuid), exactly one `abertpy proxy` process
becomes the "leader":

- **Leadership** is decided with `fcntl.flock(fd, LOCK_EX | LOCK_NB)` on a
  lock file at a path deterministic from the transponder uuid — *not* via
  ZeroMQ. This is the key simplifying idea: flock is released by the OS the
  instant a process dies for *any* reason (crash, SIGKILL, normal exit), so
  a follower's next non-blocking flock attempt just succeeds. No heartbeat
  protocol, no explicit liveness checks needed for correctness.
- The leader opens **one** `/stream/mux/<transponder_uuid>?pids=<union>`
  subscription and republishes the raw bytes over a local ZeroMQ PUB socket
  (`ipc://` transport, path also deterministic from the transponder uuid).
- Every other process wanting a different pPID from the *same* transponder
  connects as a ZeroMQ SUB instead of opening its own TVheadend
  subscription, and filters locally by its own pid using the exact same
  `process_data()` logic the code already had.
- **Registry of "who wants what":** each attached process (leader or
  follower) writes a marker file named after its own OS pid, containing
  the TS pid it wants, under a directory keyed by the transponder uuid. The
  leader periodically recomputes the union from this directory (pruning
  markers whose owning pid is dead via `os.kill(pid, 0)`) and reconnects
  upstream with the wider pid list when the union changes.
- **Failover:** a follower whose feed goes quiet, or that receives an
  explicit "stepping down" sentinel message, attempts the flock; if it
  succeeds, it becomes the new leader in place.
- **Orderly shutdown:** TVheadend stops a pipe input by killing the
  process, normally with SIGTERM (not by just closing a pipe). A SIGTERM
  handler was added that **raises a custom exception** rather than calling
  `generator.close()` directly:

  ```python
  class _ShuttingDown(Exception): pass
  def _raise_shutting_down(signum, frame): raise _ShuttingDown
  signal.signal(signal.SIGTERM, _raise_shutting_down)
  ```

  This matters: the generator may be suspended mid-syscall (blocked in a
  socket read) when the signal arrives, and calling `.close()` on it from
  outside at that point is not safe (`ValueError: generator already
  executing` — the frame is still on the call stack). Raising an exception
  instead lets it unwind through the normal exception path, including
  whatever `finally` blocks are currently open — the same mechanism Python's
  own `KeyboardInterrupt` uses. This is what lets the leader's `finally`
  send the stepping-down sentinel before it actually exits.

## Pitfalls hit during implementation (the expensive lessons — don't re-learn these)

1. **Publishing one ZeroMQ message per 188-byte TS frame is a severe
   regression, not an optimization.** First real measurement (5 real pPIDs,
   20s, against live TVheadend) showed the new architecture using **~4x
   more CPU and ~20x more syscalls/context-switches** than the old one.
   ZeroMQ's per-message overhead, paid thousands of times/sec at these
   bitrates (a 12 Mbps pid is ~8000 packets/sec), dominated everything.
   **Fix:** batch many frames into one message before publishing.

2. **Batching by size alone reintroduces a latency bug.** Naively reading
   via `response.iter_content(chunk_size=256*1024)` and publishing whichever
   whole batch arrives works great for high-bitrate pids, but for a
   low-bitrate one (~235 kbps measured) it can take many seconds to fill a
   256KB batch — and since the "did the wanted pid set change" recheck only
   ran once a batch was yielded, a newly-joined sibling could wait 8-10+
   seconds before the leader noticed and widened its fetch (instead of the
   intended ~5s). **Fix:** decouple "how often do we get control back to
   check" from "how big a batch we publish" — read the underlying HTTP
   stream in small increments (~4KB) so the recheck runs on a bounded
   cadence regardless of bitrate, and flush the accumulated buffer to
   ZeroMQ on `_READ_CHUNK_BYTES` (256KB) **or** `_MAX_BATCH_LATENCY_S`
   (1.0s) elapsed, whichever comes first. A read-level timeout
   (`timeout=(10, _PID_RECHECK_INTERVAL_S)` on the `requests.get` call) is
   still needed as a backstop for a *fully* idle connection (zero bytes at
   all), which the small-read trick alone doesn't cover.

3. **`uv run <cmd>` forks a child rather than exec'ing into it.** `$!`
   after `uv run abertpy proxy ...` captures the `uv` *wrapper's* pid, not
   the real worker (`pstree` showed `uv -> abertpy -> {threads}`). A whole
   benchmark run was invalidated by measuring the wrong (idle) process
   before this was caught. **Fix:** invoke the venv's binaries directly
   (`.venv/bin/abertpy`, `.venv/bin/python`) for any process-level resource
   measurement.

4. **ZeroMQ contexts are not fork-safe.** Test harnesses spawning worker
   processes must use `multiprocessing.get_context("spawn")`, never the
   default fork context.

5. **Leader-election tests need real separate OS processes, not threads.**
   The registry keys markers by `os.getpid()`, which assumes one broker
   attachment per OS process — true in production but violated by a
   thread-based test harness (two threads in one pytest process collide on
   the same marker file). Also: a `multiprocessing.Process` left un-joined
   when a mid-test assertion fails will hang the *entire test session* at
   interpreter shutdown (Python's multiprocessing atexit hook blocks
   joining non-daemonic children) — always wrap spawned processes in
   `try/finally` that kills+joins them unconditionally, not just on the
   happy path.

## Measurements (final, after both fixes above)

Method: 5 real pPIDs (2025, 2026, 2027, 2028, 2035) on transponder 11222H
(idle at test time — nothing else was tuned there, avoiding disruption to
the one real viewer active elsewhere), sampled via `/proc/[pid]/stat` +
`/proc/[pid]/io` + `/proc/[pid]/status` every 0.5s for 20s. Compared the
literal pre-existing code path (5 independent `/stream/service/<uuid>`
fetches, run via a standalone script calling the real unmodified
`recreate_mux_if_needed` + a direct `requests.get`) against the new broker
(1 shared `/stream/mux/<uuid>?pids=...` subscription, real `abertpy proxy`
processes).

| Metric | Old (5 separate subscriptions) | New (1 shared, both fixes applied) |
|---|---|---|
| TVheadend subscriptions | 5 | **1** |
| Client CPU (sum of 5 processes, 20s) | 5.17 CPU-s | **3.49 CPU-s (−33%)** |
| Read syscalls (sum) | 5,209 | 6,665 (1.3x) |
| Write syscalls (sum) | 719 | 1,773 (2.5x) |
| Context switches (sum) | 5,128 | **3,947 (−23%)** |
| DVB-S adapter errors (te/unc/cc) | 0 / 0 / 0 | 0 / 0 / 0 |

Also measured, orthogonal to the above: leader failover latency after the
crash-detection timeout was tuned down (see "Follower timeout" below) —
orderly shutdown (SIGTERM path, the stepping-down sentinel) handed data
back to siblings in ~0.1s measured; a true crash (SIGKILL, no chance to
signal) is bounded by `_FOLLOWER_TIMEOUT_S` (tuned to 2.0s, down from an
initial overly-conservative 15.0s — see the comment in `broker.py` on
branch `archived/mux-broker-poc` for why the tighter value is provably
safe: the flock, not the timeout, is what arbitrates who's allowed to lead,
so checking early just means an extra harmless failed lock attempt, not a
false takeover).

**Caveat:** TVheadend's own server-side CPU/memory was never directly
measured — no shell access to the TVheadend host was available during this
investigation, only its HTTP API. Subscription count (5→1) was used as a
proxy for server-side load. If this is revisited, getting `top`/`pidstat`
on the TVheadend host itself during an equivalent test would settle whether
the *real* win is bigger than the client-side numbers above suggest (a
plausible hypothesis, since TVheadend runs one full demux/remux pipeline
per subscription).

## Real identifiers used during testing (for reference only — expect these to be stale)

- Host: `http://192.168.3.148:9981/`
- Idle test transponder: `11222H`, mux uuid `de5eccb4b5c0509b48a02abac4edf1a3`
- pPID override service uuids used (on that transponder):
  `2025→5360768be0824b0804d4f074044d7e85`,
  `2026→a0e71cacd26654790a21ce6a67e8609d`,
  `2027→2db64a0d37fdfda89b384ac98deab7a2`,
  `2028→e15abf529ac2e4f2d36a4bcbd61cc44b`,
  `2035→eff4f1b613840934f1f92415599dae8b`,
  `2036→eb431b97045e2f33f82fc9f76fa9aac8`,
  `2050→d5f4c1c3d93f807cf2701d8234d51b7c`

These override "services" get recreated by `recreate_mux_if_needed`
whenever TVheadend invalidates them, so **don't assume these uuids still
resolve** — rediscover current ones with:

```bash
curl -sS -X POST "$BASE/api/mpegts/service/grid" -d hidemode=none -d limit=99999 \
  -d 'filter=[{"type":"string","field":"svcname","value":"abertpy"}]' \
  | python3 -c "import json,sys; [print(s['uuid'], s.get('sid'), s.get('svcname'), s.get('multiplex_uuid')) for s in json.load(sys.stdin)['entries']]"
```

and group by `multiplex_uuid` to find a transponder with several enabled
pPIDs to test concurrency against. Check `/api/status/inputs` /
`/api/status/subscriptions` first to find a transponder nothing is
currently tuned to, to avoid disrupting a real viewer.

## How to resurrect this quickly

1. `git log --all --oneline | grep mux-broker` (or check
   `archived/mux-broker-poc` still exists) to find the preserved commit.
2. `git cherry-pick a21628cd693a0d4223fef17c8c5a4c1702c8a8b2` onto current
   `main` (or `git diff main archived/mux-broker-poc | git apply` if the
   branch is gone but the commit is reachable). Resolve any drift in
   `proxy.py`/`recreate_mux_if_needed` if `main` has moved since.
3. `uv sync` to reinstall `pyzmq`/`pytest`.
4. Re-run `uv run pytest tests/test_broker.py -v` — should pass in ~5s, 4
   tests (dedup+fan-out, dynamic widening, crash failover, graceful
   handover timing).
5. Re-validate against the real host: find a live idle transponder with 2+
   enabled pPIDs (query above), run two real `abertpy proxy` invocations
   for different pPIDs on it concurrently, confirm
   `/api/status/subscriptions` shows exactly one "Raw PID Subscription"
   entry whose `pids` is the union.
6. Re-run the CPU/IO comparison in "Measurements" above with whatever pPIDs
   are live at the time, using `.venv/bin/abertpy` directly (not `uv run`
   — see pitfall #3) and sampling `/proc/[pid]/{stat,io,status}`.

## Recommendation

Given the measured result — 33% less client CPU, 5-to-1 fewer TVheadend
subscriptions, but only modest (1.3-2.5x) syscall overhead remaining, at
the cost of a new ~330-line IPC/concurrency module and two new runtime
dependencies (`pyzmq`, plus `pytest` for testing it) — this is a reasonable
thing to defer unless one of these becomes true:

- TVheadend's own server-side CPU/thread cost per subscription turns out
  (once actually measured on the host) to be the dominant real bottleneck,
  not client-side Python overhead. The subscription-count reduction (5→1,
  scaling to N→1) directly targets that.
- Typical concurrency on a given transponder grows well past ~5 pPIDs
  (several of this host's transponders had up to 12 enabled) — the
  subscription-count win scales linearly with N, so it matters more the
  more channels share a transponder.
- The 15s (pre-fix) or even 2s (post-fix) failover freeze on a hard crash
  becomes unacceptable for some downstream consumer of the pPID data —
  worth knowing this is a real, if now small, tradeoff of the design.
