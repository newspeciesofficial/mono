# IVM Explained Simply (xyne-spaces edition)

> If you can read SQL, you can understand this. We use only xyne-spaces
> constructs you already see in the UI: the conversation sidebar, the message
> thread, the clock-icon "sending" state, the typing indicator.

---

## The problem in one paragraph

Open xyne-spaces. The left sidebar shows your **20 most recent conversations**.
Click one — the right pane shows the **last 50 messages** of that conversation.
Both views are SQL queries:

```sql
-- sidebar
SELECT * FROM conversations
WHERE participant_id = me
ORDER BY last_message_at DESC
LIMIT 20;

-- thread
SELECT * FROM messages
WHERE conversation_id = X
ORDER BY sent_at DESC
LIMIT 50;
```

Someone sends you a new message. The browser needs to update **both** views.
The dumb way: re-run both queries, ship all 70 rows back. The smart way:
figure out **just the diff** — "this conversation moves to the top of the
sidebar; this one new message appears at the bottom of the thread; the oldest
message scrolls out because we're at limit 50" — and ship only that diff.

**IVM is the smart way.** It's what makes xyne-spaces feel instant.

---

## The mental model: a chain of "filters" between DB and screen

A query in xyne-spaces is not one big SELECT statement run on every change.
It's a **chain of small workers**, where each worker handles one part of the
SQL clause. Picture it as the path a single message takes from the database
to your screen:

```
   PostgreSQL row     →   "is it for this conversation?"   →   "attach sender info"
   (database table)       (WHERE conversation_id = X)          (JOIN users)

                     →   "keep only top 50 by sent_at"   →   message thread
                         (ORDER BY … LIMIT 50)               (what user sees)
```

Each worker does **one job and only one job** — exactly one SQL clause.
Together they produce the answer to your full query.

When something changes in the database, you don't redo the whole chain. You
hand the change to the first worker, who passes it down. By the time it gets
to the screen, it's been transformed into one of:

- **"add this row"** — a new message bubble appears,
- **"remove this row"** — a message disappears (someone deleted it),
- **"replace this row"** — a message edit (text changed),
- **"throw it out"** — the change didn't pass a filter (e.g. message belongs
  to a different conversation; sidebar doesn't care).

---

## What each worker does (mapped to SQL)

### Worker 1 — The Filter (`WHERE`)
Looks at every change. If it doesn't match the `WHERE` clause, drops it.

In xyne-spaces: when someone sends a message in a conversation **you're not
currently viewing**, the Filter for the open thread says "this message's
`conversation_id` ≠ X, drop it" — that's why your open thread doesn't suddenly
fill with messages from elsewhere. Stateless. Has no memory.

### Worker 2 — The Joiner (`JOIN`)
Attaches related data. In xyne-spaces, every message needs the sender's name
and avatar — that's a join from `messages` to `users`.

```sql
SELECT m.*, u.name, u.avatar_url
FROM messages m JOIN users u ON m.sender_id = u.id;
```

When a new message arrives, the Joiner **looks up the sender** and attaches
their name + avatar to the message before forwarding it.

When a *user* changes their display name, the Joiner has to find **every
message currently visible from that user** and update them all. One DB
change → many UI updates. This is why joins are the trickiest workers.

### Worker 3 — The Limiter (`LIMIT`)
This is the most interesting worker. Holds a strict rule for the sidebar:
**never more than 20 conversations on screen**. They keep a sticky note
saying *"the oldest visible conversation was last active at 9:47 AM"*. That
sticky note is the **bound**.

When a conversation gets a new message (so it would jump to the top):

- **If the sidebar has fewer than 20 conversations:** add it. Update the
  sticky note if needed.
- **If the sidebar is full and this conversation is older than 9:47 AM:**
  ignore it. Doesn't qualify for the visible top-20.
- **If the sidebar is full and this conversation is newer than 9:47 AM:**
  *first* push the conversation at 9:47 out of the visible list, *then* slot
  the new one in. Update the sticky note.

That last case — **kick one out, slot one in** — produces two outputs from the
Limiter: a Remove followed by an Add. **Both the TS and Rust implementations
do this exactly the same way** (it's how `LIMIT N` is required to work).

### Worker 4 — The Sorter (`ORDER BY`)
Already baked into how messages come out of SQLite (always pre-sorted by the
ordering the query asked for). No separate worker.

---

## How a row change finds its chains (the easy part you already get)

When a row in `messages` changes, we don't broadcast it to every chain on the
server. We do exactly what you described:

1. The change carries its **table name** (`'messages'`).
2. We look up which chains have a worker reading from that table.
3. We push the change into only those chains.

So if you have 100 chains running for 100 connected users but only 5 of them
care about `messages`, the change goes into 5 chains, not 100. The other 95
never see it. **This is just a hash-map lookup, not interesting.**

The interesting part is what happens **inside** each of those 5 chains —
specifically how JOIN and LIMIT update incrementally without re-running the
whole query. That's the next two sections.

---

## How JOIN actually works incrementally

The JOIN worker is unique: **it has two upstream inputs, not one.** That's
because a join joins two tables. For

```sql
SELECT m.*, u.name FROM messages m JOIN users u ON m.sender_id = u.id;
```

the JOIN worker has:

- a **parent** input reading from `messages`,
- a **child** input reading from `users`.

A change can arrive on either input. The JOIN handles them differently.

### Case A: change on the parent side (a new message arrives)

State: thread X currently shows 50 message bubbles. Priya sends "hey".

1. JOIN's parent input pushes: `{add, row: {id: 'm99', sender_id: 'priya', text: 'hey'}}`.
2. JOIN says: "to forward this, I need to attach the sender". So it does a
   one-time lookup against its child input: *"give me the user row where
   `id = 'priya'`"*. Returns `{name: 'Priya', avatar: '…'}`.
3. JOIN forwards `{add, row: {id, text, sender: {name, avatar}}}` upstream.

This is the simple case. **One push in, one push out**, plus one tiny fetch
on the child side to enrich.

### Case B: change on the child side (Priya renames herself)

This is the case that breaks people's intuition. The change is to a `users`
row, not a `messages` row. Yet message bubbles on screen need to update.

State: thread X has 50 messages. 12 of them are from Priya. Priya updates
her display name to "Priya S.".

1. JOIN's **child** input pushes: `{edit, row: {id: 'priya', name: 'Priya S.'}, oldRow: {…name: 'Priya'}}`.
2. JOIN now has a problem: *which parent rows (messages) are affected?*
   It doesn't know — the parent input is independent. So it asks: *"give me
   every parent row where `sender_id = 'priya'`"* (this is the **fetch
   downward** we discussed). Returns the 12 messages from Priya in this thread.
3. For **each** of those 12 messages, JOIN emits **one** "child changed"
   event upstream. Limiter forwards them. Screen re-renders 12 bubbles.

**One push in → 12 pushes out.** This is the fan-out property of JOIN. It's
why one user renaming themselves can result in many UI updates from a single
DB row change.

### What "child" means in IVM (vs SQL)

In SQL, JOIN is symmetric — `messages JOIN users` flattens both sides into
one big row. In Zero IVM, JOIN is **hierarchical**: the parent row carries a
`relationships` field that lazily produces child rows. So one parent + N
children comes out as one parent node with a child-stream attached, not N
flat rows. This matches how UI components are nested (one message component,
one sender chip inside it) and avoids shipping the same user data 12 times
when 12 messages share a sender.

The "child changed" event in Case B is exactly the protocol for *"the parent
itself didn't change, but one of the things hanging off its `relationships`
did"*.

---

## How LIMIT actually works incrementally

LIMIT is the second hard one. It looks trivial in SQL (`LIMIT 50`) but
incrementally maintaining it requires actual state.

### What state the Limiter keeps

Two things, persisted across pushes:

- **`size`** — how many rows are currently visible (0..50).
- **`bound`** — the **worst-ranked row currently visible**. For our message
  thread sorted `sentAt DESC`, the bound is the *oldest* visible message.
  For the sidebar, the bound is the conversation that was last active longest
  ago among the visible 20.

The bound is the Limiter's whole world: any new row's job is to be compared
against the bound.

### Case 1: Limiter receives an Add

The new row's `sentAt` decides what happens. Three branches:

| Situation | Limiter's action |
|---|---|
| `size < 50` (still filling up) | Accept. Emit `Add(new)`. If this row is now the worst-ranked of the visible set, update `bound = new`. |
| `size = 50` and new row is **older** than bound | Reject silently. The new row doesn't qualify for the visible top-50. **No output.** |
| `size = 50` and new row is **newer** than bound | Two-step: emit `Remove(bound)` then emit `Add(new)`. Then **fetch** the new bound from the source: "give me the row that's currently the 50th-newest" — that becomes the new bound. |

The third row is the one that produces visible flicker (in our Rust port at
least) — it's two emissions instead of one.

### Case 2: Limiter receives a Remove

Someone deleted a message that was visible. Now `size` would drop to 49.
But the user expects to always see 50. So:

1. Emit `Remove(deletedRow)`.
2. **Fetch** the source: "give me the next row past my current bound" — i.e.
   the one that was previously message #51 (just outside the visible window).
3. Emit `Add(replacement)`. Update `bound = replacement` (it's now the new
   worst-ranked).

**One push in → two pushes out, plus one fetch.** The fetch is what makes
Limiter complicated: it has to *go ask for replacements* to maintain the
"always exactly 50 visible" invariant.

### Case 3: Limiter receives an Edit

If the edit doesn't change the sort key (the message text changed but
`sentAt` didn't), forward as-is. Easy.

If the edit changes the sort key (someone changed `sentAt` — rare in
practice), the Limiter conceptually treats it as Remove(old) + Add(new) and
runs both Cases above.

### Why this is interesting

The Limiter is the **only worker that issues fetches in response to pushes**.
Filter and Joiner mostly just transform the change in front of them. Limiter
*reaches back* to the source whenever it has to evict and replace. This is
why our deadlock chase ended up centered on Limiter ↔ JOIN: the Limiter's
"fetch a replacement" call eventually goes through JOIN's parent input,
JOIN had locks held, → hang.

---

## The two directions: forward (push) and backward (fetch)

The chain of workers carries data in **two opposite directions**. Most people
miss this. Walk through both with a real xyne-spaces event.

### Picture the chain physically

Place the workers vertically. **Bottom** = the SQLite database. **Top** =
your screen.

```
                    ┌──────────────────────────┐
       ▲            │      your screen         │            │
       │            └──────────────────────────┘            │
   forward                Limiter (LIMIT 50)              backward
   (push:                 Joiner  (JOIN users)            (fetch:
   "here is a             Filter  (WHERE conv_id=X)       "give me
   change")               SQLite source                   matching rows")
                    ┌──────────────────────────┐            │
                    │      SQLite (replica)    │            ▼
                    └──────────────────────────┘
```

### Forward (push): a friend sends you a message

You are looking at conversation X. Your friend Priya sends "hey".

1. **PostgreSQL row gets inserted** into `messages`. The replicator mirrors it
   into your sidecar's local SQLite (~1ms).
2. **Source emits a push:** `{type: 'add', row: {id: 'm99', conversation_id:
   'X', sender_id: 'priya', text: 'hey'}}`. Hands it up to the Filter.
3. **Filter** checks `conversation_id === 'X'` → ✅. Forwards the same change
   up to the Joiner.
4. **Joiner** needs Priya's name + avatar to attach. It does a tiny lookup
   (more on this in a second), produces an enriched row `{id, text, sender:
   {name: 'Priya', avatar: '…'}}`. Forwards up to the Limiter.
5. **Limiter** checks: am I full? Yes (50 messages). Is this newer than my
   sticky-note bound? Yes. Emits **two** outputs: `Remove(oldest)` and
   `Add(new)`. Both go to the screen.
6. **Screen** receives a poke over WebSocket containing both events. React
   re-renders.

This whole thing — steps 2 through 6 — is **forward**. A change walks **up
the chain** (from DB toward screen), one worker at a time, getting filtered,
enriched, and limited along the way. We call it "push" because each worker
*pushes* the change to the next.

### Backward (fetch): Priya renames herself "Priya S."

This time the change is a `users` row, not a `messages` row. None of your
visible message bubbles directly mention the user table — but every bubble
sent by Priya needs its name updated.

1. **Source emits a push:** `{type: 'edit', row: {id: 'priya', name: 'Priya
   S.'}, oldRow: {…name: 'Priya'}}` going up the **users** chain (separate
   from the messages chain).
2. **Joiner receives this on its *child* side** (the users side of the JOIN).
   It now needs to know: *which message bubbles currently on screen are from
   Priya?* It can't push an answer until it knows.
3. **Joiner reaches DOWN to ask** the worker above (the messages Filter):
   *"give me every message where `sender_id = 'priya'`."* This is a **fetch**
   — going **backward** down the chain.
4. **Filter** runs the same `conversation_id === 'X'` check on every matching
   row (so it only returns messages in the open thread). Hands rows back to
   the Joiner one at a time.
5. **For each returned message**, the Joiner emits a `ChildChange` push going
   forward up to the Limiter and screen, saying *"this bubble's sender info
   changed"*. The bubble re-renders with "Priya S."

So **fetch = a worker asks the chain for context it doesn't have**. The
answer flows back **toward the source** — the opposite direction from push.

### The two directions in one sentence

> **Push** = a change flows from the database UP to the screen.
> **Fetch** = a worker asks the database DOWN for rows it needs to handle a push.

---

## Streaming and lazy: why this isn't a giant SQL query each time

The "chain of workers" picture would be useless if each worker waited for the
one above to finish completely before starting. That would mean: read all
50,000 of your conversations from SQLite, sort them, filter them, then take
the top 20. **The whole point is that this never happens.**

Instead, the chain works **one row at a time**, and any worker can say "stop,
I have enough" — at which point the workers above stop too, and SQLite stops
reading.

### Concrete walkthrough: opening xyne-spaces with 50,000 conversations

Sidebar query: `SELECT * FROM conversations WHERE participant_id = me ORDER BY last_message_at DESC LIMIT 20`.

The **dumb** way (not streaming):
1. SQLite reads all 50,000 rows of `conversations`.
2. Filter eliminates the ones not yours — say 10,000 remain.
3. Sorter sorts those 10,000.
4. Limiter takes the top 20.

That's 50,000 reads. Slow.

The **streaming + lazy** way (what actually happens):
1. Limiter says: "give me one row."
2. Sorter (which is part of how SQLite returns rows in index order) asks the
   Filter: "give me one row."
3. Filter asks SQLite: "give me one row."
4. SQLite returns row #1. Filter checks: is it mine? Yes. Hands to Limiter.
   *("Mine? No"? Filter asks SQLite for the next row, doesn't bother the
   Limiter.)*
5. Limiter accepts row 1: "I have 1 of 20. Give me another."
6. Repeat until Limiter has 20. Then **Limiter says "stop"**.
7. The "stop" propagates backward — Filter stops asking, SQLite closes its
   cursor.

SQLite reads ~25 rows total (20 yours + a handful of others' it had to skip).
**Two thousand times less work.** That's the streaming + lazy pattern.

### Why this matters for our debugging

The TS implementation does this perfectly because TS generators (`function*`)
are lazy by definition — `yield`-ing one row at a time, stopping when the
caller stops asking.

The Rust port currently **breaks this property in some places**: it
`.collect()`s the upstream's rows into a Vec before processing. That means
when we should have read 25 rows from SQLite, we read 50,000. For small
tables it's invisible. For your real workload it could be a 1000x slowdown.
This is on the open-issues list separately from the flicker bug.

### Why this matters for forward/backward

Both push **and** fetch are streaming/lazy:

- **Push** flows one change at a time. The source picks one row that changed,
  walks it through the chain, then picks the next.
- **Fetch** also flows one row at a time. When the Joiner asks "give me every
  message from Priya", the Filter doesn't return all 1000 of them in one
  batch — it returns row 1, then row 2, etc., and the Joiner emits one
  `ChildChange` per row. The Joiner could even stop early if it only needed
  the first match.

---

## Why generators, `yield`, and `await` exist (in xyne-spaces terms)

The "one row at a time, stop when you have enough" pattern from the previous
section sounds simple, but a normal function can't do it. A normal function
runs to completion and returns one thing. So TypeScript reaches for three
language features. Each solves a specific xyne-spaces problem.

### 1. `function*` + `yield` = "hand over one row, then pause"

Imagine the Filter worker. The Limiter says "give me one row". Filter has to
read from SQLite, check the predicate, hand the row up — and **then stop and
wait** for Limiter to either ask for another or say "done".

A normal function can't pause halfway. It has to either:

- Read all rows, return an array → **kills laziness** (Filter scans 50,000
  conversations for nothing).
- Take a callback `onRow(row)` → **callback hell** as workers call other
  workers; impossible to use `for-of`/`break`.

A **generator** (`function*`) solves this exactly:

```ts
*push(change) {
  if (this.predicate(change.row)) {
    yield this.next.push(change);  // hands the row up, pauses here
  }
  // resumes here when caller asks for the next change
}
```

`yield` = "hand over this value, freeze my position, wait for the caller to
come back". This is the **only reason** the chain works as "one row at a
time". In xyne-spaces: when you scroll through old messages, each scroll
triggers one fetch step at a time; without generators, the chain would read
the whole archive on every scroll.

### 2. `await` = "network/disk takes real time, don't block on it"

The chain itself is synchronous (CPU-bound). But the **boundaries** of the
chain talk to slow things:

- **PostgreSQL** for reading change-log rows (~1–5ms per round trip).
- **WebSocket sends** to the browser (~unbounded — depends on the user's
  network).
- **CVR persistence** to the PG cvr database between advances.

Each of those is an `await`. Without `await`, the server would block on every
network call, dropping connections.

You see this in xyne-spaces when your laptop wakes up after sleep: the
WebSocket reconnects, the server `await`s the catch-up read from PostgreSQL,
hydration runs (with `yield 'yield'` interspersed so other users' tabs keep
working), and the messages stream in. Three async behaviours, one user
experience.

### Putting it together — one xyne-spaces event

A friend sends a message in a conversation you have open. Here's what each
keyword does at each step:

| Step | Mechanism | Why |
|---|---|---|
| Replicator reads change from PG | `await` | network round-trip |
| Source emits push to Filter | generator `yield` | one change at a time |
| Filter checks predicate, forwards | generator `yield` | one change at a time |
| Joiner does sender lookup, forwards | generator `yield` | one change at a time |
| Limiter accepts/evicts, forwards 2 events | generator `yield` × 2 | one change at a time |
| Pipeline driver `await`s the next async boundary | `await` | hand control to event loop |
| View-syncer sends poke to browser | `await` (ws.send) | network |

**Generators give us laziness. `await` gives us non-blocking I/O.** Both are
required; neither substitutes for the other.

### Why this matters for the Rust port

Rust has **no native `function*` / `yield`**. The equivalent —
`std::ops::Coroutine` — has been unstable since RFC 2033 in 2017 and is
still nightly-only as of April 2026, with **no stabilization timeline**. The
Rust team's stated priority was async/await first; generators are
"someday." So we don't get the language feature.

> **If Rust had stable native generators, the TS↔Rust port would be a near
> mechanical, line-for-line translation.** Most of the structural divergence
> and complexity in our Rust IVM port comes from working around the absence
> of this **one** language feature:
>
> - **Eager `.collect()` into `Vec` between operators** (the streaming-loss
>   issue) — exists because we can't `yield` rows lazily across operator
>   boundaries while keeping the borrow checker happy.
> - **`Arc<Mutex<…>>` wrapping every operator field** — exists because
>   without generators, we can't pause an operator mid-call; instead we
>   re-enter via separate method calls that need shared `&self` access.
> - **`new_wired()` + `XBackEdge(Arc<X>)` adapter structs** — exist because
>   TS wires `parent.setOutput({push: c => this.#pushParent(c)})` inline in
>   the constructor using a closure that captures `this`. Rust closures
>   capturing `Arc<Self>` would create a cycle, so we add adapter structs.
> - **The whole class of deadlocks we just fixed** (lock held across
>   `sink.push` re-entering `Join::fetch`) — wouldn't exist if upstream
>   reads were generator yields instead of locked method calls.
>
> Said another way: TS generators give the operator graph a kind of *implicit
> cooperative coroutine machinery* that lets every worker pause/resume
> naturally, share no mutable state, and avoid call-stack re-entrancy. Rust
> stable doesn't have any of that, so we rebuild it manually with locks,
> Vecs, and adapter structs — and every workaround invites its own bug class.
>
> **Decision: we will adopt [genawaiter](https://docs.rs/genawaiter/) (sync
> flavor) for the operator port.** It emulates `function*` / `yield` on
> stable Rust by piggybacking on async/await, and `genawaiter::sync` is
> `Send + Sync` — which is what we need (operators run inside
> `spawn_blocking` and are touched from worker threads).
>
> Trade-offs we accept:
> - Per-yield overhead of ~10–100 ns. Negligible for normal push paths
>   (hundreds of items per advance); only matters for tight inner loops over
>   millions of rows. Profile if it shows up.
> - Pulls in async-await machinery for what is morally synchronous iteration.
>   Compile-time and binary-size cost, but no runtime async runtime needed.
>
> What it buys us:
> - **Operators become near-mechanical translations of the TS code.** A TS
>   `*push(change) { yield* this.#output.push(change, this); }` maps to a
>   Rust `gen!({ … co.yield(…).await … })` with the same body shape.
> - **Eager `.collect()` to `Vec` between operators goes away.** Real
>   streaming, true to the TS implementation.
> - **`Arc<Mutex<…>>` on most fields goes away.** Generator state lives
>   inside the closure; no shared mutable state to protect.
> - **The deadlock class disappears.** No locks → can't hold a lock across a
>   downstream call.
> - **Back-edge adapter structs (`new_wired`, `XBackEdge`) go away.** TS-style
>   constructor wiring becomes possible because the closure can capture a
>   weak ref / `Rc` without the ownership puzzles `dyn Output` creates today.
>
> [fauxgen](https://github.com/Phantomical/fauxgen) is a viable alternative
> with a slightly nicer API; we pick genawaiter only because it has more
> production usage. Either works.

We are **not** doing cooperative pausing on the Rust side. The plan is in
two steps:

1. **Now: serial, run-to-completion.** Each chain runs synchronously inside
   `spawn_blocking`. No `'yield'` sentinel, no setImmediate, no mid-chain
   pauses. The chain blocks one OS thread until it finishes; Tokio has many
   threads, so other connections keep being served. This is the simplest
   thing that works and is what we should get correct first.

2. **Later: break a chain into smaller IVMs.** Once serial IVM is solid,
   split the workload — multiple smaller pipelines that can run on different
   threads in parallel. Parallelism happens by *splitting the work*, not by
   pausing inside a single chain. That's future work; don't conflate it with
   today's serial path.

---

## Two lifecycle phases: opening the app vs. live updates

When you first **open xyne-spaces** (or click into a new conversation):

- **Hydration.** The whole chain runs from scratch. The database hands over
  every relevant row, each worker processes them, and 50 messages end up on
  the screen. This is "run the SQL query once" — takes tens of milliseconds.
  This is what the loading spinner covers.

After that, **every change is incremental**:

- **Advance.** A row changes anywhere in the database. The change walks down
  the chain. At most a handful of UI elements update. **Sub-millisecond.**
  No spinner — the new bubble just appears.

The whole point of IVM is making **every change after the first** essentially free.

---

## Mapping the bugs we've been hunting to the chain

### The flicker on a new conversation arriving (Rust-only — open issue)
A new conversation gets a message. The Limiter emits a Remove + Add pair
(see Worker 3 above). On the **TypeScript** server, the user sees a clean
swap with no flicker. On the **Rust** server (same query, same data, same
client), the same swap visibly flickers.

Both implementations do the same Remove+Add — so the flicker is **not**
inherent to `LIMIT`. The cause must be a difference in how the Rust port
delivers those two events to the client. We have **not** root-caused it yet.
Open hypotheses to investigate (in order of suspicion):

1. **Eager `.collect()` between operators in Rust** breaks an atomicity
   guarantee TS gets from generator-based streaming — the Remove and Add may
   be reaching the WebSocket in separate pokes instead of one.
2. **A second push happens** (duplicate emit) only on the Rust path.
3. **Order is reversed** (Add before Remove) on the Rust path, briefly
   exceeding the limit and then re-rendering.

Verifying which one requires comparing the `[TRACE row_change]` log on the
Rust server against the equivalent push log on the TS server for the same
event. **Don't trust the explanation until those logs are diffed.**

### The clock icon (message stuck in "sending")
You send a message. The bubble appears in your thread with a clock icon ("not
yet confirmed"). The clock should disappear within ~50ms once the server
confirms.

When the clock **never disappears**, it means the worker chain on the server
delivered the message to PostgreSQL fine, but the *confirmation push back to
your client* didn't make it down the chain on your sidecar. Either:

- A worker dropped the change (filter mismatch — usually a JOIN that couldn't
  find the related row).
- A worker hung (the deadlock we just fixed — Worker 2 was waiting for Worker
  1 to answer a fetch, while Worker 3 was waiting for Worker 2 to answer a
  separate fetch, and neither could move).

### A message you sent appears, then disappears, then reappears
The Limiter saw a remove (older message scrolled out due to the new one), then
the add. If the removed and re-added rows are similar, it can look like the
new message itself flickered. Not a bug — same as above, a rendering-layer
batching question.

---

## How to explain this to a teammate

Three sentences:

1. *"In xyne-spaces, every query you see — the sidebar, the thread, the
   typing indicator — is built as a small chain of workers, one per SQL clause
   (WHERE, JOIN, LIMIT)."*

2. *"When data changes, instead of re-running the whole query, we walk just
   the change through the chain. Each worker updates only what it has to.
   That's why the UI feels instant."*

3. *"Workers move data in two directions — a change pushed up to the screen,
   or a fetch reaching back down for context. Both are lazy: each worker
   processes one row at a time and stops as soon as it has enough."*

That's IVM. The rest is engineering.

---

## When you're ready for the real terms

| xyne-spaces description | Real name |
|---|---|
| The PostgreSQL → SQLite mirror | **Source** (`TableSource`) |
| The "is this for this conversation?" worker | **Filter** operator |
| The "attach sender info" worker | **Join** operator |
| The "top 20 in the sidebar" worker | **Take** operator (with `LIMIT N`) |
| The chain of workers for one query | **Pipeline** / operator graph |
| A change walking down the chain | **push** (a `Change` flowing downstream) |
| A worker asking the one above for info | **fetch** (a query flowing upstream) |
| Limiter's sticky note ("oldest visible: 9:47") | **bound** / **TakeState** |
| First-time setup when you open a conversation | **Hydration** |
| Every-change-after | **Advance** |
| The result that hits your sidebar/thread | **RowChange** sent to the view-syncer |
| Two workers blocking on each other | **Deadlock** (in Rust: `Mutex` held across a downstream call) |
| Clock icon | A poke (a delta sent over WebSocket) hasn't arrived |

Once these terms click, jump to **`ivm-deep-dive.md`** for the file paths and
code-level mechanics.
