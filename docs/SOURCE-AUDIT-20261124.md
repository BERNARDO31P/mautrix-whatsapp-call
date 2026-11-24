# Source audit and hardening candidate — 2026-11-24

## Decision and scope

**Candidate for rebuild and staging acceptance, not a verified production release.** The operator reports that the existing release works. No production service, account, database, room, GitHub branch or published release was changed during this review. Do not substitute this candidate for that working release without passing the release gate below.

The independently available source was the full Library archive `mautrix-whatsapp-call.zip`, SHA-256 `76c59061f93cf1c34340dd91748d8ad1f2a88b5a80d982066d533ccebe68a4eb`. It was imported as local baseline `7cc19fc`; the preceding documentation patch was applied as baseline `b9f4609`. These local commit IDs are not GitHub commit IDs. The archive has 462 tracked files before the new documentation and hardening additions. The pinned revisions are listed in `build/versions.env`. The R25 correctness report and R26 source/patch archive were used for cross-reference. Historical repair descriptions were checked against the assembled project paths. The public repository was inspected through the web, but cloning/downloading its complete current HEAD and fetching compiler/dependency inputs from this execution environment failed. Consequently this audit does not claim a byte-identical fresh upstream comparison, exhaustive review of every unchanged third-party implementation, new dependency vulnerability scan or live end-to-end certification.

A review of source and reachable regression paths covered the connector, call engine integration, call admission and lifetime, business restrictions, wire protocol, Rust worker, media lifecycle, identity management, deployment tooling, build inputs, packaging and operator documentation. New changes below are made in the actual assembled source, not only in an audit document.

## Confirmed findings and corrections

Severity reflects potential impact, not a claim that the existing deployment was exploited or that every failure occurred in live use.

| ID | Severity | Finding in baseline | Candidate correction and verification |
| --- | --- | --- | --- |
| A01 | High | Runtime packaging copied the deployment tree wholesale. A locally used tree could include generated registrations, credentials, backups and worker crypto state. | `build/runtime-layout.py` defines the exact shipped file inventory. Stage only regular allowlisted files; validate archive paths, types, duplicate entries, complete manifest and matching binary copies. Docker contexts also use allowlists. Synthetic runtime fixture passed. |
| A02 | High | Hostname substitution recursively searched deployment contents, including persistent private state. | `render-hostname.py` only renders four shipped templates, validates all first, preserves file mode/owner and refuses mismatched hostnames/symlinks. Repeated rendering, private files and mismatch fixtures passed. |
| A03 | High | Broad permission normalization could make arbitrary private local files readable by other users. | Installer normalizes only known shipped paths; generated and worker-state files remain private; symlink guards added. Fixture passed. |
| A04 | High | Installer failure restored enabled calls even after partial changes; restart could retain obsolete bind-mounted files. | Serialize installs with a lock; stop worker/provisioner before live mutations; on failure stop both, leave saved setting untouched; recreate containers that consume replaced files and verify restored setting/readiness. Failure trap and setting fixtures passed; real Docker fault injection remains required. |
| A05 | High | Credential recovery accepted ambiguous or malformed surviving values; generating replacement passphrase when preparation evidence survives can strand encrypted stores. | Reconcile all matching credential sources before generation; reject inconsistent copies, malformed values, symlinks and missing passphrase when old identity evidence exists. YAML serialization protects unusual scalar contents. Repeated preparation and conflict fixtures passed. |
| A06 | Medium | Preflight matched a particular indentation style instead of YAML semantics. Several helpers could accept duplicate keys or expose source lines in parse errors. | Use parsed YAML, duplicate-explicit-key rejection, sanitized diagnostics, atomic writes and durable backups. Existing transport conflicts are refused rather than silently replaced. YAML and preflight fixtures passed. |
| A07 | High | Cancellation during a Rust frame write could leave part of its header/body in the stream; later messages would be misframed. Go also continued after a failed write. | One bounded Rust writer task owns each stream; canceled queued frames can be skipped, started frames finish or poison/close stream. Go refuses subsequent writes after failure. Two Go wire regressions passed with race detector. Two Rust writer tests added but not run here. |
| A08 | Medium | Go and Rust differed in certain wire identifier length checks. | Align Go with Rust limits; reject invalid identifiers and broken writer return counts. Go wire tests passed. |
| A09 | High | Concurrent capability probes and PN/LID alias cache ordering could overwrite a still-valid explicit negative with a weaker fallback. | Lock atomic cache alias updates; retain strongest unexpired negative, return effective result, periodically prune expired entries, isolate login keys. Two independent race-tested cache fixtures passed. Integration with actual connector still needs pinned full build. |
| A10 | Medium | Routing via the preferred LID could lose the original PN when reverse mapping was unavailable. Ended manager entries could accumulate. | Retain original PN among candidates, prefer LID where present, prune ended calls. Source correction, full connector compile/real portal cases pending. |
| A11 | High | Media producer cancellation could detach the FFmpeg stdout task; a queued publication event could restart a muted track; early return could leave media gate active. | Abort-on-drop encoder output, close media gate on all exits, stop/await per-track producer before mute control and require unmuted current state before starting. Rust source correction; cancellation and rapid-toggle live tests required. |
| A12 | High | Join deadline did not cover every room-open/SFU-connect wait. Slot creation could fail after persisting ownership without the ordinary cleanup path. | Apply existing join deadline to room opening and SFU connection, bounded close on join failure; route failed slot creation through ownership cleanup. Preserve existing ownership checks. Additional room eligibility check before answer. Rust build and fault-injection pending. |
| A13 | High | The cold call path could open/create a new crypto store when the prepared store was missing. | Require a nonempty existing crypto DB before initializing the call account, then retain the SDK cross-signing/device checks. Provisioning entry point remains able to create a new store. Rust build pending; verify existing prepared stores with exact pinned SDK. |
| A14 | High | Python authenticated HTTP requests followed redirects; Rust provisioning allowed the internal-HTTP flag to disable HTTPS certificate validation. | Disable redirects on appservice login/scanning; preserve TLS certificate checks for HTTPS, restrict insecure flag to explicit HTTP. Redirect fixture passed. Rust provisioning build pending. |
| A15 | High | Manual namespace resolution used a partial line parser, and automatic provisioning trusted potentially broad registration regexes and stale room-cache evidence. | Parse real YAML; restrict ghost match to configured distinct local username template; verify functional bot and authoritative WhatsApp DM metadata before preparing a new ghost. Positive cache only bypasses rechecking for an already prepared identity. Inline config, broad namespace and portal-kind self-tests passed. |
| A16 | High | Manual direct-chat sync placed the human access token in curl arguments and could infer a DM solely from membership. | Use a private temporary header file, reject CR/LF, HTTPS, bounded requests, require encrypted portal metadata and configured bot. Automatic migration also uses HTTPS and bounded requests. Classification self-tests passed; real m.direct race still possible. |
| A17 | Medium | Source packaging copied a directory with a denylist; old cleanup could affect unrelated files. | Source packaging uses clean committed Git tree; checks forbidden tracked state/artifacts. Cleanup only selects complete old project release pairs. Fixture passed. |
| A18 | Medium | Runtime ZIP validation/extraction could diverge between parsers; manifest coverage was not exact. | Validate and extract using one Python ZIP reader, reject paths/types outside inventory, extract only to empty temporary directory, verify all SHA-256 entries. Check binaries are matching AMD64 ELF and expected executable modes. Fixture passed; synthetic ELF header is not evidence of successful binary build. |
| A19 | Medium | Documentation could imply credentials can be regenerated, fail-closed installation is a rollback, or JWT full access is a general federation control. | Updated complete install/operations/releasing/testing guides, actual pinned JWT distinction, trust boundaries, upgrade/recovery and release acceptance requirements. No production secrets included. |

## R25 findings: status cross-reference

This is the review of the previously reported defects against the current assembled snapshot. “Present” means corresponding source guard/repair is visible, not proof of interoperability. R26 historical test totals are not counted as tests of this candidate.

| R25 | Topic | Current disposition |
| --- | --- | --- |
| 1 | Group bot absent during seeding | Own-account inclusion present; extend deadline in A12. |
| 2 | Dial before admission | Encrypted invitation, roster and device admission present; integrated timing test required. |
| 3 | Replacement device | Device/member/generation pinning present; A12 revalidates preanswer room. |
| 4 | Stale invite/decline | Occupied-slot and relation checks present; exercise competing invitations. |
| 5 | Early WhatsApp handshake | Latching/accept wait present; test both event orderings. |
| 6 | Slot/session cleanup | Durable leases and room reservation present; A12 closes more failure paths. |
| 7 | Terminal signalling | Common cleanup and timeout paths present; EOF/disconnect/cancel combinations require live tests. |
| 8 | Legacy slot | Canonical lowercase slot and compatibility handlers present; client-version acceptance required. |
| 9 | Business restriction | Negative evidence retained on reconnect; A09 repairs remaining alias/update race. |
| 10 | PN/LID divergence | Alias discovery present; A10 retains original peer during lookup failure. |
| 11 | Duplicate track task | Publication task registry present; A07/A11 address cancellation and stale events. |
| 12 | Early media controls | Setup state buffering present; A11 strengthens ordering. |
| 13 | Call mode versus camera | Separate state present; test pending upgrade/cancel race. |
| 14 | FFmpeg restart EOF | Stdin release present; A11 stops detached output task. |
| 15 | DM promoted into group | Portal binding and promotion refusal present; promotion is not transparently migrated to group. |
| 16 | Group identity/lifetime | Group JID/participant controls present; verify partial rejection/roster updates. |
| 17 | Aggregate media | Per-source selection/mixing present; verify multi-party mute and video failover. |
| 18 | Keyframe/timing | Keyframe recovery and per-picture timing present; verify on real clients and loss. |
| 19 | Messaging dispatch stalls | Off-path control handling present; A07 avoids corrupted subsequent frames. |
| 20 | Malformed media | Input checks present; A08 aligns wire identifier limits. |
| 21 | Control-room misclassification | Portal/management-room distinction present; fixtures pass. |
| 22 | Wrong portal permissions | Bridge metadata/ownership record present; A15/A16 narrow helpers. |
| 23 | Restore calls setting | Normalization and ping/pong present; A04 changes partial failure to fail closed. |
| 24 | YAML loss | Semantic merge present; A06 rejects ambiguity and conflicting existing transports. |
| 25 | Credential persistence | Reconciliation present; A02/A03/A05 harden scope and restoration. |
| 26 | Interrupted device | Shared lock, atomic session and matching marker present; A13 avoids empty store recreation. |
| 27 | Provenance | Pinned source/input hashes present; A01/A17/A18 harden packaging; no fresh toolchain verification here. |

## Upstream contracts and documentation reviewed

- [Element JWT service v0.9.0 README](https://github.com/element-hq/lk-jwt-service/blob/v0.9.0/README.md): exact tagged documentation confirms registration file, local homeserver, bind address, membership lookup scope, proxy prefix, webhook and restricted standalone-token membership conditions. The later [main README](https://github.com/element-hq/lk-jwt-service) documents features not necessarily available in this pin. Do not assume its newer remote-publisher allowlist works with v0.9.0. The supplied internal webhook uses the matching API key; ensure delivery. Keep room auto-creation disabled.
- [LiveKit firewall documentation](https://docs.livekit.io/transport/self-hosting/ports-firewall/): the configured TCP 7880 proxy, TCP 7881 ICE fallback and UDP 7882 multiplexed media match documented options. Public ICE reachability, NAT, external IP discovery and restricted-network TURN are deployment-specific, not guaranteed by configuration.
- [Synapse configuration manual](https://element-hq.github.io/synapse/latest/usage/configuration/config_documentation.html#matrix_rtc): stable `m.livekit` transport with WebSocket URL and legacy `livekit` compatibility. The current documentation is newer than some client/service implementations, so verify the actual supported stable/unstable routes on the installed versions; keep known working pin combinations.
- [Matrix appservice API](https://spec.matrix.org/latest/application-service-api/): namespace, identity assertion, registration, token and device boundaries. Application-service tokens are high privilege; the main worker does not require the WhatsApp registration. The explicit double-puppet appservice still has broad local-user authority by design; protect it.
- [mautrix v26.11 config](https://docs.mau.fi/configs/mautrix-whatsapp/v26.11.html) and [double puppeting](https://docs.mau.fi/bridges/general/double-puppeting.html): compare real appservice templates and automatic double-puppet configuration. Project-specific VoIP keys belong to the patched bridge, not ordinary upstream configuration.
- [Tokio AsyncWriteExt](https://docs.rs/tokio/1.53.1/tokio/io/trait.AsyncWriteExt.html): multi-write operations are not safely canceled and restarted from byte zero. Keep one owner of frame writes; a timed-out partially transmitted frame invalidates the connection. The reader already preserves incremental offsets.
- [Docker bind mounts](https://docs.docker.com/engine/storage/bind-mounts/) and [Compose up](https://docs.docker.com/reference/cli/docker/compose/up/): recreate bind-file consumers after atomic replacement; restart alone may retain a mount to the old file. Never imply that a container restart rolls back durable state.
- [Python urllib request](https://docs.python.org/3/library/urllib.request.html): redirect handler behavior, authentication boundary. The provisioner accepts only its configured server and no redirects.
- [MSC4143](https://github.com/matrix-org/matrix-spec-proposals/pull/4143), related MatrixRTC proposals and local compatibility code were used to assess room, slot and membership boundaries. WhatsApp's call protocol is not a fully documented public contract; source reconstruction and real peer acceptance remain necessary.

## Executed checks on candidate

| Check | Result |
| --- | --- |
| Python project fixtures | 13/13 pass. |
| Go media bus and lifecycle | 9/9 pass with `-race`. |
| Go wire protocol | 9/9 pass with `-race`, including failed-write/identifier regressions. |
| Go standalone availability cache | 2/2 pass with `-race`. |
| Resolver, automatic provisioner, direct-chat sync, migration self-tests | 4/4 pass. |
| Three shell source guard suites | Pass; guards are not runtime tests. |
| Source-tree SHA-256, shell syntax, Python compilation, YAML parse | Pass. |
| Actual pinned full Go connector/meowcaller compile and tests | Not run: Go 1.23.2 only, pinned Go 1.27.4 and external dependencies inaccessible. |
| Actual Rust full compile/test, including new writer tests | Not run: no Rust/Cargo, pinned LLVM/WebRTC/dependency inputs inaccessible. |
| Actual Docker images, Synapse configuration start, native runtime package | Not run: Docker/runtime absent. |
| Live WhatsApp/Element X/LiveKit integration | Not run; operator's successful observations belong to the original deployed release. |

## Remaining risks and release gate

1. Compile all changed code using the pinned bootstrap and full tests. Do not publish a runtime built with `MAUTRIX_WHATSAPP_CALL_SKIP_TESTS=1`. In particular verify Rust ownership/cancellation and SDK API compatibility; the new tests are source only until compiled. Verify actual ELF binaries and release ZIP, not synthetic fixtures.
2. Use a non-production Matrix stack with compatible pinned images and real authorized test accounts. Verify both calls directions, audio/video, accept/decline/cancel/timeout/hangup on both sides, camera/mic toggles, delayed callbacks, repeated calls and normal encrypted message bridging. Test audio/video upgrade and cancellation, portrait orientation, keyframes and RTP timing, loss/reconnect and abrupt peer/worker/bridge restart. Group tests: partial rejection, additions, muted participants, competing callers, source failover, and confirm refused DM-to-group promotion cannot leak media into the DM.
3. Exercise authorization adversarially: wrong user/device, replaced membership, unencrypted room, stale or occupied slot, changed room participants, explicit business disallow, cloud/hosted identity, unknown/timeout fallback, PN/LID aliases, self calling, forged bridge metadata, non-member JWT request. Verify that a known restriction cannot be overridden by a weaker probe.
4. Test interrupted install at each phase with saved calls enabled/disabled, credential conflict/missing copy, restore matching crypto store, modified bind-file mounts, pre-existing transport config, stale room cache, concurrent manual migration, and real Docker health/readiness. Fail-closed shutdown is not a rollback. The trap cannot guarantee stop if Docker itself fails or the process is killed; check runtime state manually.
5. Matrix account data writes still replace the full `m.direct` object and cannot be made compare-and-swap through the current endpoint. Avoid simultaneous editors. Room state may change between verification and a later network send; the bridge revalidates at key transitions, but only adversarial integration testing can establish the residual window. Slot cleanup and delayed leave depend on server delivery, and worker restart may interrupt calls. The per-account SDK client cache and probe scheduling need load testing at scale. No TURN is shipped; confirm restricted networks separately. The bridge terminates media encryption on both protocol legs; it is not transparent WhatsApp-to-Matrix end-to-end encryption. Container image tags and dependency vulnerabilities require a fresh security/provenance check when publishing. A checksum supplied by the same untrusted publisher is not a signature.
6. Keep the existing working release intact until this candidate passes all above. Build from a separate branch/checkout, not from production. Compare the exact public GitHub tree before merging: the saved snapshot predates some public README/build edits. A correction patch generated from the saved snapshot is not guaranteed to apply to newer public commits without reconciliation.

See [TESTING.md](TESTING.md), [RELEASING.md](RELEASING.md), [INSTALLATION.md](../deployment/mautrix-whatsapp-call/INSTALLATION.md) and [OPERATIONS.md](../deployment/mautrix-whatsapp-call/OPERATIONS.md) for runnable commands and the detailed acceptance checklist.
