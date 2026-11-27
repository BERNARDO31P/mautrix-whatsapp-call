# Licensing and provenance

This repository vendors the complete source required for the call bridge:

- `bridge/` is based on mautrix-whatsapp, distributed upstream under GNU AGPL v3 with upstream exceptions. Its upstream license files remain in `bridge/` and are also summarized under `licenses/`.
- `bridge/third_party/meowcaller/` is based on meowcaller and retains its MIT license.
- `rtc/` is based on matrix-rust-rtc, distributed upstream under GNU AGPL v3 with an upstream commercial-license option. Its upstream license files remain in `rtc/`.
- The negotiated standard Opus path links Xiph.Org libopus 1.5.2; see `licenses/OPUS-BSD.txt`. The upstream source archive is checksum-pinned and built by `build/prepare-opus.sh`.
- Go, Rust, LLVM, LiveKit/WebRTC and other build dependencies are downloaded by bootstrap and retain their own upstream licenses; they are not committed here.

Pinned upstream commits and toolchain versions are recorded in `docs/PROVENANCE.md` and `build/versions.env`.
