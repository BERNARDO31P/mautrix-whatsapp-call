# Upstream sources and build dependencies

The repository contains modified source based on these pinned upstream revisions:

- mautrix-whatsapp v0.2611.0: `64e67416013b475d29e80e0a736d2b9afeb6e8cc`
- meowcaller: `c48c3e2a243c672c942cbf0c941d14161720d540`
- matrix-rust-rtc: `52af3d1f4dc6af5c92ce5f8c64418fd97a8c4f57`
- matrix-rust-sdk: `56bc59112822e7eb25943a95e02502855cdeef2d`

Source checksums are verified by `build/versions.env` and `build/verify-source.sh`.

Pinned toolchain inputs:

- Go 1.27.4 — SHA-256 `7226736c2339719cda28fbf3a54d71aa63bcefe376e98cef00034268dab24840`
- Rust 1.100.0
- LLVM/Clang 21.1.8 — SHA-256 `b3b7f2801d15d50736acea3c73982994d025b01c2f035b91ae3b49d1b575732b`
- LiveKit WebRTC `webrtc-a545de4` — SHA-256 `2ece73b9a72a2cacd73d0c62ac3a414e202f37885c3c8f49dddb43c1443ac0bd`
- libopus 1.5.2 — SHA-256 `65c1d2f78b9f2fb20082c38cbe47c951ad5839345876e46941612ee87f9a7ce1`
- webrtc-sys 0.3.51
- libwebrtc crate 0.3.54

See [BUILD.md](BUILD.md) for build instructions.
