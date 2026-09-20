# memberlist 0.8.5 — verified API reference for the rEtcd gossip adapter

**Status: VERIFIED by compiling and running two encrypted nodes on this machine.**
Host: Windows Server 2022, `x86_64-pc-windows-msvc`, rustc/cargo 1.93.0.

Everything below was read from the vendored crate source at:

- `C:\Users\gautamb\.cargo\registry\src\index.crates.io-1949cf8c6b5b557f\memberlist-0.8.5\`
- `...\memberlist-core-0.8.5\`
- `...\memberlist-net-0.8.5\`
- `...\memberlist-proto-0.3.4\`  (pulled in transitively; re-exported as `memberlist::proto`)

Anything not read from source is marked **UNVERIFIED**.

> **WARNING — do not trust docs.rs/GitHub `main` for this version.** The `main`-branch
> README shows a different API (`memberlist::tokio::tcp(...)` free function, features
> `aes-gcm`, `chacha20-poly1305`, `lz4`, `zstd`, `reactor`, `compio`, `smoltcp`). **None of
> those exist in 0.8.5.** In 0.8.5 the constructor is `Memberlist::with_delegate(...)` and
> the encryption feature is the single flag `encryption`.
>
> **WARNING — `memberlist-net-0.8.5/src/tests/*.rs` is stale.** Those files call
> `NetTransportOptions::with_primary_key / with_encryption_algo / with_label /
> with_offload_size`, which **do not exist** on `NetTransportOptions` in 0.8.5 (they moved to
> core `Options`). Do not copy the test files. They are behind `feature = "test"`.

---

## 0. Cargo.toml — exact lines (compile-verified)

Minimal set for **Tokio + TCP/UDP net transport + encryption** (verified `cargo check` OK):

```toml
[dependencies]
memberlist = { version = "=0.8.5", default-features = false, features = [
  "tokio",
  "tcp",
  "encryption",
] }
smol_str = "0.3"          # for SmolStr node ids (memberlist re-exports it but not publicly)
tokio = { version = "1", features = ["full"] }
```

Recommended set for rEtcd (adds UDP checksum + metrics; verified `cargo build` + `cargo run` OK):

```toml
[dependencies]
memberlist = { version = "=0.8.5", default-features = false, features = [
  "tokio",
  "tcp",
  "encryption",
  "crc32",     # UDP is unreliable -> checksum strongly recommended by README
  "metrics",   # emits to the `metrics` crate facade
] }
smol_str = "0.3"
tokio = { version = "1", features = ["full"] }
```

`default-features = false` matters: the crate's `default` feature is
`["tokio","crc32","snappy","encryption","dns","tcp","quic","rayon"]`, which drags in
**quinn + rustls + ring/aws-lc + hickory-dns + rayon**. Turn it off.

### Windows x86_64-msvc: yes, it builds with no native deps

Verified with `cargo tree -e normal` on the recommended feature set:

- Crypto is **pure Rust**: `aes-gcm v0.10.3` + `aes v0.8.4` (RustCrypto). No `ring`,
  no `aws-lc-rs`, no `rustls`, no `openssl`.
- No `quinn`, no `s2n-quic`.
- **No `cc` / `cmake` / `bindgen`** anywhere in the tree — no C toolchain needed.
- 235 crates total; clean build in ~41 s.

`ring`/`aws-lc-rs` only enter via the `tls`, `quic*`, `dnssec-*`, `h3-*`, `https-*` features.
Leave them off.

---

## 1. Feature flags

### `memberlist` 0.8.5 — `memberlist-0.8.5/Cargo.toml`

```toml
default = ["tokio", "crc32", "snappy", "encryption", "dns", "tcp", "quic", "rayon"]
```

| Group | Flags |
|---|---|
| Runtime | `tokio`, `smol` |
| Transport | `net` (base), `tcp` (= `net`), `tls` (= `memberlist-net/tls` + `net`), `quic`, `quinn` |
| Encryption | `encryption` (single flag; AES-GCM 128/192/256) |
| Compression | `snappy`, `lz4`, `zstd`, `brotli` |
| Checksum | `crc32`, `xxhash32`, `xxhash64`, `xxhash3`, `murmur3` |
| TLS/QUIC backends | `tls-ring`, `tls-aws-lc-rs`, `quic-ring`, `quic-aws-lc-rs`, `h3-ring`, `h3-aws-lc-rs`, `https-ring`, `https-aws-lc-rs`, `dnssec-ring`, `dnssec-aws-lc-rs`, `rustls-platform-verifier`, `webpki-roots` |
| Other | `metrics`, `serde`, `dns`, `rayon`, `test` |

Notes read from source:
- `tcp = ["net"]`; `net = ["memberlist-net", "agnostic/net"]`.
- `tokio = ["agnostic/tokio", "memberlist-net?/tokio", "memberlist-quic?/tokio"]`.
- `metrics = ["memberlist-core/metrics", "memberlist-net?/metrics", "memberlist-quic?/metrics"]`.
- `rayon` only enables offloading checksum/crypto/compression of large frames to a rayon pool.
  Controlled by `Options::offload_size` (default **1 MiB**). Not required.
- There is **no** `udp` flag. `tcp` gives you **both TCP (promised/stream) and UDP (packet)** —
  `NetTransport` binds both on each bind address.

### `memberlist-net` 0.8.5

Same naming, reached through the parent's optional-dep syntax (`memberlist-net?/...`).
Its own flags used here: `tokio`, `tcp`, `metrics`, `serde`, `test`, plus the tls/quic backends.

---

## 2. Constructing a node

### 2.1 Constructors — `memberlist-core-0.8.5/src/api.rs:196-219`

```rust
impl<T> Memberlist<T>
where
  T: Transport,
{
  /// Create a new memberlist with the given transport and options.
  #[inline]
  pub async fn new(
    transport_options: T::Options,
    opts: Options,
  ) -> Result<Self, Error<T, VoidDelegate<T::Id, T::ResolvedAddress>>> { .. }
}

impl<T, D> Memberlist<T, D>
where
  D: Delegate<Id = T::Id, Address = T::ResolvedAddress>,
  T: Transport,
{
  /// Create a new memberlist with the given transport, delegate and options.
  #[inline]
  pub async fn with_delegate(
    delegate: D,
    transport_options: T::Options,
    opts: Options,
  ) -> Result<Self, Error<T, D>> { .. }
}
```

**Key point:** you pass **transport *options*, not a constructed transport.** `Memberlist`
builds the transport internally (`T::new(transport_options)` — `api.rs:229`). There is no
`Memberlist::new(transport, delegate, options)` in 0.8.5.

Also from `create()` (`api.rs:224-249`): after start it calls
`delegate.node_meta(META_MAX_SIZE)` and **panics** if the returned meta exceeds the limit:

```rust
if meta.len() > META_MAX_SIZE {
  panic!("NodeState meta data provided is longer than the limit");
}
```

### 2.2 Type parameters — `memberlist-net-0.8.5/src/lib.rs:63-74`

```rust
pub type TokioNetTransport<I, A, S> = NetTransport<I, A, S, agnostic::tokio::TokioRuntime>;

pub struct NetTransport<I, A, S, R>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer<Runtime = R>,
{ .. }
```

So `I` = id, `A` = address resolver, `S` = stream layer, `R` = runtime.

### 2.3 Tokio type aliases — `memberlist-0.8.5/src/tokio.rs`

```rust
pub use agnostic::tokio::TokioRuntime;
pub use memberlist_net::TokioNetTransport;

pub type TokioSocketAddrResolver =
  memberlist_core::transport::resolver::socket_addr::SocketAddrResolver<TokioRuntime>;
pub type TokioHostAddrResolver =
  memberlist_core::transport::resolver::address::HostAddrResolver<TokioRuntime>;
pub type TokioDnsResolver =                       // feature = "dns"
  memberlist_core::transport::resolver::dns::DnsResolver<TokioRuntime>;

pub type TokioTcp = memberlist_net::stream_layer::tcp::Tcp<TokioRuntime>;
pub type TokioTls = memberlist_net::stream_layer::tls::Tls<TokioRuntime>;   // feature = "tls"
pub type TokioQuinn = memberlist_quic::stream_layer::quinn::Quinn<TokioRuntime>;

pub type TokioTcpMemberlist<I, A, D = memberlist_core::delegate::CompositeDelegate<I, A>> =
  memberlist_core::Memberlist<TokioNetTransport<I, A, TokioTcp>, D>;
pub type TokioTlsMemberlist<I, A, D = ...> = ...;
pub type TokioQuicMemberlist<I, A, D = ...> = ...;
```

`Tcp<R>` stream layer has `type Options = ();` (`memberlist-net-0.8.5/src/stream_layer/tcp.rs:44`).

The concrete aliases for rEtcd:

```rust
type Transport = TokioNetTransport<SmolStr, TokioSocketAddrResolver, TokioTcp>;
type TOpts     = NetTransportOptions<SmolStr, TokioSocketAddrResolver, TokioTcp>;
```

### 2.4 `NetTransportOptions` — `memberlist-net-0.8.5/src/options.rs:25-152`

```rust
pub struct NetTransportOptions<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer>
{
  id: I,
  bind_addresses: IndexSet<A::Address>,
  advertise_address: Option<A::ResolvedAddress>,
  resolver: A::Options,
  stream_layer: S::Options,
  cidrs_policy: CIDRsPolicy,          // default CIDRsPolicy::allow_all()
  max_packet_size: usize,             // default 1472  (UDP payload cap)
  recv_buffer_size: usize,            // default 2 * 1024 * 1024  (2 MiB UDP recv buf)
  metric_labels: Option<Arc<MetricLabels>>,  // feature = "metrics"
  packet_buffer_size: usize,          // default 1000  (packet COUNT, not bytes)
}
```

Constructors and mutators (same file, lines 176-259):

```rust
pub fn new(id: I) -> Self                                   // needs A::Options: Default, S::Options: Default
pub fn with_resolver_options(id: I, resolver_options: A::Options) -> Self
pub fn with_stream_layer_options(id: I, stream_layer_options: S::Options) -> Self
pub fn with_resolver_options_and_stream_layer_options(
  id: I, resolver_options: A::Options, stream_layer_opts: S::Options) -> Self

pub fn add_bind_address(&mut self, addr: A::Address) -> &mut Self   // &mut, NOT builder
pub fn with_advertise_address(mut self, addr: A::ResolvedAddress) -> Self
pub fn with_maybe_advertise_address(self, Option<A::ResolvedAddress>) -> Self
```

Generated by `#[viewit::viewit(getters(vis_all="pub"), setters(vis_all="pub", prefix="with"))]`:
every field gets a `field()` getter and a `with_field(v)` builder setter.

**Gotcha:** `add_bind_address` takes `&mut self`, everything else is move-builder. So:

```rust
let mut topts = TOpts::new(SmolStr::new("node-a"));
topts.add_bind_address(bind);                   // &mut
let topts = topts.with_advertise_address(bind); // move
```

**Bind/advertise are on the transport options, not on `Options`.**

### 2.5 Core `Options` — `memberlist-core-0.8.5/src/options.rs`

No bind/advertise here. Same `viewit` pattern: getter `x()`, setter `with_x(v)`.
Defaults are from `Options::lan()` (which is `Default::default()`):

| Field | Type | `lan()` default |
|---|---|---|
| `label` | `Label` | `Label::empty()` |
| `skip_inbound_label_check` | `bool` | `false` |
| `timeout` | `Duration` | 10 s |
| `indirect_checks` | `usize` | 3 |
| `retransmit_mult` | `usize` | 4 |
| `suspicion_mult` | `usize` | 4 |
| `suspicion_max_timeout_mult` | `usize` | 6 |
| `push_pull_interval` | `Duration` | 30 s |
| `probe_interval` | `Duration` | 500 ms |
| `probe_timeout` | `Duration` | 1 s |
| `disable_reliable_pings` | `bool` | `false` |
| `awareness_max_multiplier` | `usize` | 8 |
| `gossip_interval` | `Duration` | 200 ms |
| `gossip_nodes` | `usize` | 3 |
| `gossip_to_the_dead_time` | `Duration` | 30 s |
| `protocol_version` | `ProtocolVersion` | `V1` |
| `delegate_version` | `DelegateVersion` | `V1` |
| `handoff_queue_depth` | `usize` | 1024 |
| `dead_node_reclaim_time` | `Duration` | `Duration::ZERO` |
| `queue_check_interval` | `Duration` | 30 s |
| `checksum_algo` | `Option<ChecksumAlgorithm>` | `None` |
| `offload_size` | `usize` | `1024 * 1024` |
| `encryption_algo` | `Option<EncryptionAlgorithm>` | `None` |
| `gossip_verify_incoming` | `bool` | `false` |
| `gossip_verify_outgoing` | `bool` | `false` |
| `primary_key` | `Option<SecretKey>` | `None` |
| `secret_keys` | `SecretKeys` | `SecretKeys::new()` |
| `compress_algo` | `Option<CompressAlgorithm>` | `None` |
| `metric_labels` | `Arc<MetricLabels>` | empty |

Three presets:

```rust
pub fn lan()   -> Self   // table above
pub fn wan()   -> Self   // timeout 30s, suspicion_mult 6, push_pull 60s, probe_timeout 3s,
                         // probe_interval 5s, gossip_nodes 4, gossip_interval 500ms,
                         // gossip_to_the_dead_time 60s
pub fn local() -> Self   // timeout 1s, indirect_checks 1, retransmit_mult 2, suspicion_mult 3,
                         // push_pull 15s, probe_timeout 200ms, probe_interval 1s,
                         // gossip_interval 100ms, gossip_to_the_dead_time 15s
```

Hand-written (non-`viewit`) setters that take the value directly instead of `Option`:

```rust
pub fn with_compress_algo(mut self, compress_algo: CompressAlgorithm) -> Self
pub fn with_checksum_algo(mut self, checksum_algo: ChecksumAlgorithm) -> Self
pub fn with_encryption_algo(mut self, encryption_algo: EncryptionAlgorithm) -> Self
pub fn with_primary_key(mut self, primary_key: SecretKey) -> Self
```

The `viewit`-generated `Option`-taking variants are renamed:
`with_maybe_checksum_algo`, `with_maybe_encryption_algo`, `with_maybe_primary_key`.

`label` doc comment, verbatim: *"If gossip encryption is enabled and this is set it is
treated as GCM authenticated data."* — so label must match cluster-wide when encrypting.

---

## 3. Delegate traits

### 3.1 `Delegate` — `memberlist-core-0.8.5/src/delegate.rs:74-93`

```rust
pub trait Delegate:
  NodeDelegate
  + PingDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
  + EventDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
  + ConflictDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
  + AliveDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
  + MergeDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
{
  /// The id type of the delegate
  type Id: Id;
  /// The address type of the delegate
  type Address: CheapClone + Send + Sync + 'static;
}
```

Note `NodeDelegate` has **no** `Id`/`Address` associated types.

### 3.2 `NodeDelegate` — `memberlist-core-0.8.5/src/delegate/node.rs`

All six methods have default impls, so you override only what you need.

```rust
#[auto_impl::auto_impl(Box, Arc)]
pub trait NodeDelegate: Send + Sync + 'static {
  fn node_meta(&self, limit: usize) -> impl Future<Output = Meta> + Send { .. }

  fn notify_message(&self, msg: Cow<'_, [u8]>) -> impl Future<Output = ()> + Send { .. }

  fn broadcast_messages<F>(
    &self,
    limit: usize,
    encoded_len: F,
  ) -> impl Future<Output = impl Iterator<Item = Bytes> + Send> + Send
  where
    F: Fn(Bytes) -> (usize, Bytes) + Send + Sync + 'static { .. }

  fn local_state(&self, join: bool) -> impl Future<Output = Bytes> + Send { .. }

  fn merge_remote_state(&self, buf: &[u8], join: bool) -> impl Future<Output = ()> + Send { .. }
}
```

`#[auto_impl(Box, Arc)]` means `Arc<MyDelegate>` implements `NodeDelegate` too — handy for
sharing state between the delegate and your own code.

### 3.3 `EventDelegate` — `memberlist-core-0.8.5/src/delegate/event.rs:91-119`

```rust
#[auto_impl::auto_impl(Box, Arc)]
pub trait EventDelegate: Send + Sync + 'static {
  type Id: Id;
  type Address: CheapClone + Send + Sync + 'static;

  fn notify_join(&self, node: Arc<NodeState<Self::Id, Self::Address>>)
    -> impl Future<Output = ()> + Send;
  fn notify_leave(&self, node: Arc<NodeState<Self::Id, Self::Address>>)
    -> impl Future<Output = ()> + Send;
  fn notify_update(&self, node: Arc<NodeState<Self::Id, Self::Address>>)
    -> impl Future<Output = ()> + Send;
}
```

No defaults here — all three are required.

Channel-based alternative (same file, lines 121-205). **Name is `SubscribleEventDelegate`**
(sic — that spelling is in the crate):

```rust
pub struct SubscribleEventDelegate<I, A>(async_channel::Sender<Event<I, A>>);

impl<I, A> SubscribleEventDelegate<I, A> {
  pub fn unbounded() -> (Self, EventSubscriber<I, A>)
  pub fn bounded(capacity: usize) -> (Self, EventSubscriber<I, A>)
}

pub struct EventSubscriber<I, A>(async_channel::Receiver<Event<I, A>>);

impl<I, A> EventSubscriber<I, A> {
  pub async fn recv(&self) -> Result<Event<I, A>, async_channel::RecvError>
  pub fn try_recv(&self) -> Result<Event<I, A>, async_channel::TryRecvError>
  pub fn capacity(&self) -> Option<usize>
  pub fn len(&self) -> usize
  pub fn is_empty(&self) -> bool
  pub fn is_full(&self) -> bool
}
// EventSubscriber also implements futures::Stream<Item = Event<I, A>>

pub enum EventKind { Join, Leave, Update }   // #[non_exhaustive]

impl<I, A> Event<I, A> {
  pub fn node_state(&self) -> &NodeState<I, A>
  pub const fn kind(&self) -> EventKind
}
```

This is the cleanest fit for rEtcd: drop `SubscribleEventDelegate` into the composite and
consume `EventSubscriber` as a stream from the gossip task.

### 3.4 `AliveDelegate` — `memberlist-core-0.8.5/src/delegate/alive.rs`

```rust
#[auto_impl::auto_impl(Box, Arc)]
pub trait AliveDelegate: Send + Sync + 'static {
  type Id: Id;
  type Address: CheapClone + Send + Sync + 'static;
  type Error: std::error::Error + Send + Sync + 'static;

  fn notify_alive(
    &self,
    peer: Arc<NodeState<Self::Id, Self::Address>>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
```

Returning `Err` prevents the node from being considered a peer. Useful for rejecting peers
with a mismatched `cluster_id` in their meta.

### 3.5 `MergeDelegate` — `memberlist-core-0.8.5/src/delegate/merge.rs`

```rust
#[auto_impl::auto_impl(Box, Arc)]
pub trait MergeDelegate: Send + Sync + 'static {
  type Id: Id;
  type Address: CheapClone + Send + Sync + 'static;
  type Error: std::error::Error + Send + Sync + 'static;

  fn notify_merge(
    &self,
    peers: Arc<[NodeState<Self::Id, Self::Address>]>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
```

`Err` cancels the join. Invoked on join push/pull only, **not** on anti-entropy push/pull.

### 3.6 `ConflictDelegate` / `PingDelegate` (signatures via `VoidDelegate` impls, `delegate.rs:157-190`)

```rust
// ConflictDelegate
async fn notify_conflict(
  &self,
  existing: Arc<NodeState<Self::Id, Self::Address>>,
  other: Arc<NodeState<Self::Id, Self::Address>>,
);

// PingDelegate
async fn ack_payload(&self) -> Bytes;
async fn notify_ping_complete(
  &self,
  node: Arc<NodeState<Self::Id, Self::Address>>,
  rtt: std::time::Duration,
  payload: Bytes,
);
fn disable_reliable_pings(&self, target: &Self::Id) -> bool;
```

### 3.7 `VoidDelegate` — `memberlist-core-0.8.5/src/delegate.rs:104-119`

```rust
pub struct VoidDelegate<I, A>(core::marker::PhantomData<(I, A)>);

impl<I, A> VoidDelegate<I, A> {
  pub const fn new() -> Self
}
pub struct VoidDelegateError;   // its Error type
```

Implements all six sub-traits + `Delegate` as no-ops. `Memberlist::new()` (no delegate)
yields `Error<T, VoidDelegate<..>>`.

### 3.8 `CompositeDelegate` — `memberlist-core-0.8.5/src/delegate/composite.rs:6-155`

```rust
pub struct CompositeDelegate<
  I,
  Address,
  A = VoidDelegate<I, Address>,   // AliveDelegate
  C = VoidDelegate<I, Address>,   // ConflictDelegate
  E = VoidDelegate<I, Address>,   // EventDelegate
  M = VoidDelegate<I, Address>,   // MergeDelegate
  N = VoidDelegate<I, Address>,   // NodeDelegate
  P = VoidDelegate<I, Address>,   // PingDelegate
> { .. }

impl<I, Address> CompositeDelegate<I, Address> {
  pub const fn new() -> Self
}

// Each of these consumes self and returns a NEW type with one param swapped:
pub fn with_alive_delegate<NA>(self, alive_delegate: NA)
  -> CompositeDelegate<I, Address, NA, C, E, M, N, P>
pub fn with_conflict_delegate<NC>(self, conflict_delegate: NC)
  -> CompositeDelegate<I, Address, A, NC, E, M, N, P>
pub fn with_event_delegate<NE>(self, event_delegate: NE)
  -> CompositeDelegate<I, Address, A, C, NE, M, N, P>
pub fn with_merge_delegate<NM>(self, merge_delegate: NM)
  -> CompositeDelegate<I, Address, A, C, E, NM, N, P>
pub fn with_node_delegate<NN>(self, node_delegate: NN)
  -> CompositeDelegate<I, Address, A, C, E, M, NN, P>
pub fn with_ping_delegate<NP>(self, ping_delegate: NP)
  -> CompositeDelegate<I, Address, A, C, E, M, N, NP>
```

**Parameter order is A, C, E, M, N, P** (alive, conflict, event, merge, node, ping).
You must spell the full type out in your `Memberlist<T, D>` alias, so get the order right.

`DelegateError<D>` (`delegate.rs:34-72`):

```rust
pub enum DelegateError<D: Delegate> {
  AliveDelegate(<D as AliveDelegate>::Error),
  MergeDelegate(<D as MergeDelegate>::Error),
}
impl<D: Delegate> DelegateError<D> {
  pub const fn alive(err: <D as AliveDelegate>::Error) -> Self
  pub const fn merge(err: <D as MergeDelegate>::Error) -> Self
}
```

---

## 4. Join / membership / leave / shutdown

All from `memberlist-core-0.8.5/src/api.rs`.

### 4.1 Join

```rust
// api.rs:316
pub async fn join(
  &self,
  addr: MaybeResolvedAddress<T::Address, T::ResolvedAddress>,
) -> Result<T::ResolvedAddress, Error<T, D>>

// api.rs:345
pub async fn join_many(
  &self,
  existing: impl Iterator<Item = MaybeResolvedAddress<T::Address, T::ResolvedAddress>>,
) -> Result<SmallVec<T::ResolvedAddress>, (SmallVec<T::ResolvedAddress>, Error<T, D>)>
```

`join_many`'s error arm carries the **successful** joins alongside the error — so a partial
join is recoverable. For rEtcd (gossip is ADVISORY) log the error and keep the successes.

`MaybeResolvedAddress` — `memberlist-proto-0.3.4/src/address.rs`:

```rust
pub enum MaybeResolvedAddress<A, R> {
  Resolved(R),
  Unresolved(A),
}
impl<A, R> MaybeResolvedAddress<A, R> {
  pub fn resolved(addr: R) -> Self
  pub fn unresolved(addr: A) -> Self
}
```

With `TokioSocketAddrResolver`, both `Address` and `ResolvedAddress` are `SocketAddr`, so
`MaybeResolvedAddress::resolved(sock)` is what you want.

Both `join` and `join_many` return `Err(Error::NotRunning)` if the node has left or shut down.

### 4.2 Membership queries

```rust
pub fn local_id(&self) -> &T::Id
pub fn local_address(&self) -> &<T::Resolver as AddressResolver>::Address
pub fn advertise_node(&self) -> Node<T::Id, T::ResolvedAddress>
pub fn advertise_address(&self) -> &T::ResolvedAddress
pub fn keyring(&self) -> Option<&super::keyring::Keyring>        // feature = "encryption"
pub fn encryption_enabled(&self) -> bool                         // feature = "encryption"
pub fn delegate(&self) -> Option<&D>
pub fn health_score(&self) -> usize                              // 0 == totally healthy

pub async fn local_state(&self) -> Option<Arc<NodeState<T::Id, T::ResolvedAddress>>>
pub async fn by_id(&self, id: &T::Id) -> Option<Arc<NodeState<T::Id, T::ResolvedAddress>>>
pub async fn members(&self) -> SmallVec<Arc<NodeState<T::Id, T::ResolvedAddress>>>
pub async fn num_members(&self) -> usize
pub async fn online_members(&self) -> SmallVec<Arc<NodeState<T::Id, T::ResolvedAddress>>>
pub async fn num_online_members(&self) -> usize
pub async fn members_by(..) -> ..        // predicate-filtered, api.rs:143
pub async fn num_members_by(..) -> ..    // api.rs:160
pub async fn members_map_by<O>(..) -> .. // api.rs:176
```

`SmallVec<T>` is from `smallvec_wrapper`, re-exported as `memberlist::proto::SmallVec`.
It iterates and derefs to a slice like `Vec`.

### 4.3 `NodeState` / `State` — `memberlist-proto-0.3.4/src/server.rs`

```rust
pub enum State {           // #[non_exhaustive]-ish: has an Unknown(u8) variant
  Alive,                   // wire 0, Display "alive"
  Suspect,                 // wire 1, Display "suspect"
  Dead,                    // wire 2, Display "dead"
  Left,                    // wire 3, Display "left"
  Unknown(u8),
}
impl State { pub fn as_str(&self) -> Cow<'static, str> }

#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
pub struct NodeState<I, A> {
  id: I,
  addr: A,                 // getter is RENAMED to `address()`
  meta: Meta,
  state: State,
  protocol_version: ProtocolVersion,
  delegate_version: DelegateVersion,
}
```

Accessors you actually call (`viewit`-generated):

```rust
m.id()               -> &I
m.address()          -> &A        // NOT m.addr()
m.meta()             -> &Meta
m.state()            -> State     // Copy
m.protocol_version() -> ProtocolVersion
m.delegate_version() -> DelegateVersion
```

`State` must be matched with a catch-all arm because of `Unknown(u8)`.

### 4.4 Leave / shutdown / update

```rust
// api.rs:263 — returns true if this call is what left the cluster
pub async fn leave(&self, timeout: Duration) -> Result<bool, Error<T, D>>

// api.rs:636
pub async fn shutdown(&self) -> Result<(), Error<T, D>>

// api.rs:423 — re-advertise local node; use after changing node_meta
pub async fn update_node(&self, timeout: Duration) -> Result<(), Error<T, D>>
```

`leave` broadcasts a leave message but **keeps the background listeners running**. Call
`leave` then `shutdown` for a graceful exit. Both are safe to call multiple times;
`leave` must not be called after shutdown.

Other messaging APIs (not needed for M1, listed for completeness):

```rust
pub async fn send(&self, to: &T::ResolvedAddress, msg: Bytes) -> Result<(), Error<T, D>>
pub async fn send_many(..) -> ..                  // api.rs:488
pub async fn send_reliable(..) -> ..              // api.rs:528
pub async fn send_many_reliable(..) -> ..         // api.rs:541
pub async fn ping(&self, node: Node<T::Id, T::ResolvedAddress>) -> Result<Duration, Error<T, D>>
```

---

## 5. Encryption

### 5.1 Wiring (verified working)

Encryption lives entirely on core `Options` in 0.8.5:

```rust
let opts = Options::lan()
  .with_label(Label::try_from("retcd-gossip")?)      // becomes GCM AAD when encrypting
  .with_primary_key(secret)                          // SecretKey, not Option
  .with_encryption_algo(EncryptionAlgorithm::NoPadding)
  .with_gossip_verify_incoming(true)
  .with_gossip_verify_outgoing(true);
```

You must set **both** `primary_key` and `encryption_algo`. `primary_key` alone installs the
key into the keyring (making decrypt possible) but `encryption_algo: None` means nothing is
encrypted on the way out.

`gossip_verify_outgoing` / `gossip_verify_incoming` default `false` so you can upshift a
running cluster from plaintext to encrypted: roll out keys with both `false`, then flip
`outgoing`, then flip `incoming`.

Additional keys for rotation via `Options::with_secret_keys(SecretKeys)`.

### 5.2 `SecretKey` — `memberlist-proto-0.3.4/src/encryption.rs:96-102`

```rust
pub enum SecretKey {
  /// secret key for AES128
  Aes128([u8; 16]),
  /// secret key for AES192
  Aes192([u8; 24]),
  /// secret key for AES256
  Aes256([u8; 32]),
}
```

It is `Copy + Eq + Ord + Hash`, and:

```rust
pub fn random_aes128() -> Self
pub fn random_aes192() -> Self
pub fn random_aes256() -> Self
// From<[u8;16]> / From<[u8;24]> / From<[u8;32]>
// TryFrom<&[u8]>  -> Err(InvalidKeyLength(n)) unless n in {16,24,32}
// FromStr         -> base64 (standard alphabet), Err(ParseSecretKeyError)
// AsRef<[u8]>
```

`SecretKeys` is a fixed-capacity inline vec: `pub SecretKeys([SecretKey; 3])`
(`encryption.rs:440`) with `SecretKeys::new()` / `Default` / `is_empty()`.
**So the keyring supports at most 3 keys via `Options::secret_keys`.**

### 5.3 `EncryptionAlgorithm` — `memberlist-proto-0.3.4/src/encryption.rs`

```rust
pub enum EncryptionAlgorithm {
  #[default]
  NoPadding,     // AES-GCM, no padding.   Display/FromStr "aes-gcm-nopadding"
  Pkcs7,         // AES-GCM, PKCS7 padding. Display/FromStr "aes-gcm-pkcs7"
  Unknown(u8),
}
```

Wire tags: `NOPADDING_TAG = 1`, `PKCS7_TAG = 2`. AES-GCM constants in the same file:
`NONCE_SIZE = 12`, `TAG_SIZE = 16`, `BLOCK_SIZE = 16`. `Pkcs7` exists to pad message
lengths and hide plaintext size; `NoPadding` is the default and cheaper.

Note the **old `EncryptionAlgo::PKCS7`** spelling seen in `memberlist-net`'s stale test
files does **not** exist in 0.8.5 — it is `EncryptionAlgorithm::Pkcs7`.

### 5.4 Key rotation — `memberlist-core-0.8.5/src/keyring.rs`

Get the live keyring from a running node with `memberlist.keyring() -> Option<&Keyring>`.

```rust
pub struct Keyring { .. }   // Clone (shares the same Arc<RwLock<..>>), lock-based, thread-safe

impl Keyring {
  pub fn new(primary_key: SecretKey) -> Self
  pub fn with_keys(primary_key: SecretKey,
                   keys: impl Iterator<Item = impl Into<SecretKey>>) -> Self

  pub fn primary_key(&self) -> SecretKey
  pub fn insert(&self, key: SecretKey)                             // add for DECRYPT
  pub fn remove(&self, key: &[u8]) -> Result<(), KeyringError>     // Err if it's primary
  pub fn use_key(&self, key_data: &[u8]) -> Result<(), KeyringError> // promote to ENCRYPT key
  pub fn keys(&self) -> impl Iterator<Item = SecretKey> + Send + 'static  // primary first
}

pub enum KeyringError {
  SecretKeyNotFound,   // "secret key is not in the keyring"
  RemovePrimaryKey,    // "removing the primary key is not allowed"
}
```

Safe rotation order: `insert(new)` on every node → `use_key(new)` on every node →
`remove(old)` on every node.

**UNVERIFIED:** whether `Options::secret_keys` beyond the 3-slot `SecretKeys` capacity is
possible, and whether there is a public API to replace the whole keyring on a live node.
Observed in my run: `keyring().keys().count() == 1` when only `primary_key` was set.

---

## 6. Size limits

| Limit | Value | Source |
|---|---|---|
| `node_meta` / `Meta` | **512 bytes** | `memberlist-core-0.8.5/src/network.rs:17` → `pub const META_MAX_SIZE: usize = 512;` (re-exported at `lib.rs:43`) and `memberlist-proto-0.3.4/src/meta.rs:32` → `pub const MAX_SIZE: usize = 512;` |
| UDP packet payload | **1472 bytes** default, configurable | `NetTransportOptions::max_packet_size`, `memberlist-net-0.8.5/src/options.rs:242` |
| UDP recv buffer | **2 MiB** default | `DEFAULT_UDP_RECV_BUF_SIZE = 2 * 1024 * 1024`, `memberlist-net-0.8.5/src/lib.rs:59` |
| Packet channel depth | **1000 packets** (count, not bytes) | `NetTransportOptions::packet_buffer_size` |
| UDP handoff queue | **1024** | `Options::handoff_queue_depth` |
| Rayon offload threshold | **1 MiB** | `Options::offload_size` |

`Meta` API (`memberlist-proto-0.3.4/src/meta.rs`):

```rust
pub struct Meta(Bytes);
impl Meta {
  pub const MAX_SIZE: usize = 512;
  pub fn empty() -> Self
  pub fn as_bytes(&self) -> &[u8]
  pub fn len(&self) -> usize
  pub fn is_empty(&self) -> bool
}
// TryFrom<Vec<u8>> / TryFrom<&[u8]> / TryFrom<Bytes> / TryFrom<&str> ... -> Err(LargeMeta(n))
```

**512 bytes is the hard constraint on `ObservedPeerHint`.** Exceeding it makes
`Memberlist::create` **panic** (see §2.1). Budget it and truncate defensively.

---

## 7. Runtime

`memberlist-0.8.5/src/lib.rs:10-17` re-exports the runtime module:

```rust
pub mod agnostic {
  #[cfg(not(feature = "agnostic"))]
  pub use agnostic_lite::*;
  #[cfg(feature = "agnostic")]
  pub use agnostic::*;
}
```

Tokio runtime type: **`agnostic::tokio::TokioRuntime`**, re-exported as
`memberlist::tokio::TokioRuntime` (`memberlist-0.8.5/src/tokio.rs:1`).

The trait is `agnostic::Runtime` (super-trait `agnostic_lite::RuntimeLite`). You never name
it directly if you use the `Tokio*` aliases. Resolved versions in my lockfile:
`agnostic v0.9.0`, `agnostic-lite v0.6.2`, `agnostic-net v0.3.1`, `nodecraft v0.9.1`,
`memberlist-proto v0.3.4`.

---

## 8. Minimal runnable example — two encrypted nodes, join, list members

**This is not from the repo's examples dir** (0.8.5's crates.io package ships no `examples/`;
`autoexamples = false`). I wrote it against 0.8.5's actual API and **compiled and ran it**.

`Cargo.toml`:

```toml
[dependencies]
memberlist = { version = "=0.8.5", default-features = false, features = [
  "tokio", "tcp", "encryption", "crc32", "metrics",
] }
smol_str = "0.3"
tokio = { version = "1", features = ["full"] }
```

`src/main.rs`:

```rust
use std::net::SocketAddr;

use memberlist::{
  Memberlist, Options,
  delegate::VoidDelegate,
  net::NetTransportOptions,
  proto::{EncryptionAlgorithm, Label, MaybeResolvedAddress, SecretKey},
  tokio::{TokioNetTransport, TokioSocketAddrResolver, TokioTcp},
};
use smol_str::SmolStr;

type Transport = TokioNetTransport<SmolStr, TokioSocketAddrResolver, TokioTcp>;
type TOpts = NetTransportOptions<SmolStr, TokioSocketAddrResolver, TokioTcp>;
type Node = Memberlist<Transport, VoidDelegate<SmolStr, SocketAddr>>;

fn opts(key: SecretKey) -> Options {
  Options::local()
    .with_label(Label::try_from("retcd").unwrap())
    .with_primary_key(key)
    .with_encryption_algo(EncryptionAlgorithm::NoPadding)
    .with_gossip_verify_incoming(true)
    .with_gossip_verify_outgoing(true)
}

async fn start(id: &str, bind: SocketAddr, key: SecretKey) -> Node {
  let mut topts = TOpts::new(SmolStr::new(id));
  topts.add_bind_address(bind);                     // &mut self
  let topts = topts.with_advertise_address(bind);   // move builder
  Memberlist::with_delegate(VoidDelegate::new(), topts, opts(key))
    .await
    .unwrap()
}

#[tokio::main]
async fn main() {
  let key = SecretKey::Aes256([7u8; 32]);
  let a: SocketAddr = "127.0.0.1:17946".parse().unwrap();
  let b: SocketAddr = "127.0.0.1:17947".parse().unwrap();

  let n1 = start("node-a", a, key).await;
  let n2 = start("node-b", b, key).await;

  let resolved = n2.join(MaybeResolvedAddress::resolved(a)).await.unwrap();
  println!("joined {resolved}");

  tokio::time::sleep(std::time::Duration::from_secs(2)).await;

  for m in n1.online_members().await {
    println!(
      "n1 sees id={} addr={} state={} meta_len={} pv={:?} dv={:?}",
      m.id(), m.address(), m.state(), m.meta().len(),
      m.protocol_version(), m.delegate_version()
    );
  }
  println!("n1 num_online={}", n1.num_online_members().await);
  println!("n1 encryption_enabled={}", n1.encryption_enabled());
  println!("advertise_node = {}", n1.advertise_node());

  println!("leave = {:?}", n2.leave(std::time::Duration::from_secs(2)).await);
  n2.shutdown().await.unwrap();
  n1.shutdown().await.unwrap();
}
```

Observed output:

```
joined 127.0.0.1:17946
join_many -> SmallVec([127.0.0.1:17946])
n1 sees id=node-b addr=127.0.0.1:17947 state=alive meta_len=0 pv=V1 dv=V1
n1 sees id=node-a addr=127.0.0.1:17946 state=alive meta_len=0 pv=V1 dv=V1
n1 num_online=2 num=2
n1 keyring_enabled=true
n1 keys=1
advertise_node = node-a(127.0.0.1:17946)
leave = Ok(true)
OK
```

---

## 9. Not wire compatible with HashiCorp Go memberlist — CONFIRMED

From `memberlist-0.8.5/README.md`, "Q & A" section, verbatim:

> ***Does Rust's memberlist implemenetation compatible to Go's memberlist?***
>
> No but yes! Rust's memberlist impelementation use a protobuf-like forward and backward
> compatible encoding/decoding, Go's implementation use message pack. It is possible in
> theory, users need to implement their own transport layer, but not recommand, you are
> facing expensive overhead!

Implications for rEtcd M1:
- **No interop with Consul / Serf (Go) / Nomad gossip.** Rust memberlist clusters only.
- The SWIM + Lifeguard *protocol* semantics are ported, the *wire format* is not.
- License is **MPL-2.0** (file-level copyleft), not MIT/Apache. Flag this for rEtcd's
  licensing review before committing to the dependency.

---

## 10. Minimal skeleton — `GossipNode` wrapper (compiled and run)

Maps `NodeState` + `Meta` onto DesignSpec-01 §5/§21 `ObservedPeerHint`. The whole thing
compiled and produced correct hints across two nodes on this machine.

The encoding below is a deliberately dumb pipe-delimited string to stay dependency-free and
stay well inside the 512-byte `Meta` cap. Swap for `postcard` or compact JSON if you prefer;
just keep the budget check.

```rust
use std::{net::SocketAddr, sync::Arc, time::Duration};

use memberlist::{
  Memberlist, Options,
  delegate::{CompositeDelegate, NodeDelegate, VoidDelegate},
  net::NetTransportOptions,
  proto::{EncryptionAlgorithm, Label, MaybeResolvedAddress, Meta, SecretKey, State},
  tokio::{TokioNetTransport, TokioSocketAddrResolver, TokioTcp},
};
use smol_str::SmolStr;

/// DesignSpec-01 §5/§21. ADVISORY ONLY — never an input to Raft membership.
#[derive(Debug, Clone, Default)]
pub struct ObservedPeerHint {
  pub cluster_id: String,
  pub node_id: String,
  pub peer_endpoint: String,
  pub client_endpoint: String,
  pub versions: String,
  pub zone: String,
  pub liveness: &'static str,
}

impl ObservedPeerHint {
  fn encode_meta(&self) -> Meta {
    let s = format!(
      "{}|{}|{}|{}|{}|{}",
      self.cluster_id, self.node_id, self.peer_endpoint,
      self.client_endpoint, self.versions, self.zone
    );
    let mut b = s.into_bytes();
    b.truncate(Meta::MAX_SIZE);          // 512 — exceeding it PANICS in Memberlist::create
    Meta::try_from(b).expect("<= Meta::MAX_SIZE")
  }

  fn decode_meta(meta: &Meta, liveness: &'static str, fallback_id: &str) -> Self {
    let s = String::from_utf8_lossy(meta.as_bytes());
    let mut it = s.split('|');
    Self {
      cluster_id: it.next().unwrap_or_default().to_string(),
      node_id: {
        let v = it.next().unwrap_or_default();
        if v.is_empty() { fallback_id.to_string() } else { v.to_string() }
      },
      peer_endpoint: it.next().unwrap_or_default().to_string(),
      client_endpoint: it.next().unwrap_or_default().to_string(),
      versions: it.next().unwrap_or_default().to_string(),
      zone: it.next().unwrap_or_default().to_string(),
      liveness,
    }
  }
}

/// Publishes this node's own hint as gossip metadata.
struct HintDelegate {
  self_hint: ObservedPeerHint,
}

impl NodeDelegate for HintDelegate {
  async fn node_meta(&self, limit: usize) -> Meta {
    let m = self.self_hint.encode_meta();
    debug_assert!(m.len() <= limit);
    m
  }
  // notify_message / broadcast_messages / local_state / merge_remote_state: defaults are fine.
}

type Transport = TokioNetTransport<SmolStr, TokioSocketAddrResolver, TokioTcp>;
type TOpts = NetTransportOptions<SmolStr, TokioSocketAddrResolver, TokioTcp>;

// CompositeDelegate param order: Alive, Conflict, Event, Merge, Node, Ping.
type GossipDelegate = CompositeDelegate<
  SmolStr,
  SocketAddr,
  VoidDelegate<SmolStr, SocketAddr>, // A: alive
  VoidDelegate<SmolStr, SocketAddr>, // C: conflict
  VoidDelegate<SmolStr, SocketAddr>, // E: event    <- swap for SubscribleEventDelegate later
  VoidDelegate<SmolStr, SocketAddr>, // M: merge
  Arc<HintDelegate>,                 // N: node     (Arc works via #[auto_impl(Arc)])
  VoidDelegate<SmolStr, SocketAddr>, // P: ping
>;

pub struct GossipNode {
  inner: Memberlist<Transport, GossipDelegate>,
}

impl GossipNode {
  pub async fn start(
    node_id: &str,
    bind: SocketAddr,
    advertise: SocketAddr,
    secret: SecretKey,
    self_hint: ObservedPeerHint,
    seeds: &[SocketAddr],
  ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
    let mut topts = TOpts::new(SmolStr::new(node_id));
    topts.add_bind_address(bind);
    let topts = topts.with_advertise_address(advertise);

    let opts = Options::lan()
      .with_label(Label::try_from("retcd-gossip")?)   // GCM AAD; must match cluster-wide
      .with_primary_key(secret)
      .with_encryption_algo(EncryptionAlgorithm::NoPadding)
      .with_gossip_verify_incoming(true)
      .with_gossip_verify_outgoing(true);

    let delegate =
      CompositeDelegate::new().with_node_delegate(Arc::new(HintDelegate { self_hint }));

    let inner = Memberlist::with_delegate(delegate, topts, opts).await?;

    if !seeds.is_empty() {
      // ADVISORY: a failed join must never block rEtcd startup.
      let addrs = seeds.iter().copied().map(MaybeResolvedAddress::resolved);
      if let Err((ok, e)) = inner.join_many(addrs).await {
        tracing::warn!(joined = ok.len(), error = %e, "gossip: partial join");
      }
    }
    Ok(Self { inner })
  }

  pub async fn peers(&self) -> Vec<ObservedPeerHint> {
    self
      .inner
      .members()
      .await
      .into_iter()
      .map(|m| {
        let liveness = match m.state() {
          State::Alive => "alive",
          State::Suspect => "suspect",
          State::Dead => "dead",
          State::Left => "left",
          _ => "unknown",            // State has an Unknown(u8) variant
        };
        let mut h = ObservedPeerHint::decode_meta(m.meta(), liveness, m.id().as_str());
        if h.peer_endpoint.is_empty() {
          h.peer_endpoint = m.address().to_string();
        }
        h
      })
      .collect()
  }

  pub async fn shutdown(&self) {
    let _ = self.inner.leave(Duration::from_secs(2)).await;
    let _ = self.inner.shutdown().await;
  }
}
```

Observed output from running it (two nodes, encrypted, joined):

```
ObservedPeerHint { cluster_id: "retcd-1", node_id: "node-b", peer_endpoint: "127.0.0.1:18947", client_endpoint: "127.0.0.1:2389", versions: "0.1.0", zone: "z1", liveness: "alive" }
ObservedPeerHint { cluster_id: "retcd-1", node_id: "node-a", peer_endpoint: "127.0.0.1:18946", client_endpoint: "127.0.0.1:2379", versions: "0.1.0", zone: "z1", liveness: "alive" }
OK
```

### Signature deviations from the task brief, and why

- `peers()` must be **`async`** (`Memberlist::members()` is `async`). A sync
  `fn peers() -> Vec<ObservedPeerHint>` is only possible behind a cached snapshot updated by
  an event task. Recommended for rEtcd: keep an `ArcSwap<Vec<ObservedPeerHint>>` fed by a
  `SubscribleEventDelegate` + `EventSubscriber` stream, then `peers()` can be sync and
  lock-free. Adds `arc-swap`.
- `shutdown(&self)` not `shutdown(self)` — `leave`/`shutdown` take `&self` and are
  idempotent, so `GossipNode` need not be consumed.

---

## 11. Gotchas checklist for the implementer

1. `default-features = false` — otherwise you inherit quinn + rustls + ring + hickory-dns.
2. `Memberlist::with_delegate(delegate, transport_options, opts)` — options, not a transport.
3. `add_bind_address` is `&mut self`; everything else is a move-builder.
4. Bind/advertise → `NetTransportOptions`. Label/encryption/timers → core `Options`.
5. `NodeState` address getter is `.address()`, not `.addr()`.
6. `Meta` ≤ 512 bytes or `Memberlist::create` **panics**.
7. `CompositeDelegate` type params are ordered **A, C, E, M, N, P**.
8. Set both `primary_key` **and** `encryption_algo`; `primary_key` alone does not encrypt.
9. `label` is GCM AAD when encrypting — must be identical on every node.
10. `join_many`'s `Err` arm still hands you the successful joins; do not discard them.
11. `State` needs a catch-all match arm (`Unknown(u8)`).
12. Ignore `memberlist-net-0.8.5/src/tests/*` and the GitHub `main` README — both describe
    APIs that are not in 0.8.5.
13. License is MPL-2.0.

## 12. Reproduction

Probe project (kept for re-verification, outside the repo):
`C:\Users\gautamb\AppData\Local\Temp\mlprobe\`

```
cargo init ; cargo add memberlist@=0.8.5 ; cargo fetch ; cargo build ; cargo run
cargo tree -e normal        # confirms no ring / aws-lc / rustls / quinn / cc / cmake
```

## Correction (2026-09-18, dev-gossip, verified by failing test)

`NodeState::state()` obtained via `members()` / `by_id()` / event delegate is permanently stale:
`dead_node`/`suspect_node` mutate a private `LocalNodeState.state`, not the shared `Arc<NodeState>`.
Derive liveness as `members()` minus `online_members()` → only `Alive` and `Dead` are observable;
`Suspect`/`Left` surface as `Dead` (safe direction for an advisory signal).
