FROM debian:trixie-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg libsqlite3-0 python3 python3-yaml \
    && rm -rf /var/lib/apt/lists/*

COPY mautrix-whatsapp-call-worker /usr/local/bin/mautrix-whatsapp-call-worker
COPY mautrix-whatsapp-call-provision /usr/local/bin/mautrix-whatsapp-call-provision
COPY session_files.py /usr/local/lib/mautrix-whatsapp-call/session_files.py
COPY provision-ghost.py /usr/local/lib/mautrix-whatsapp-call/provision-ghost.py
COPY auto-provision-whatsapp-ghosts.py /usr/local/lib/mautrix-whatsapp-call/auto-provision-whatsapp-ghosts.py

RUN chmod 0755 /usr/local/bin/mautrix-whatsapp-call-worker \
    /usr/local/bin/mautrix-whatsapp-call-provision \
    /usr/local/lib/mautrix-whatsapp-call/provision-ghost.py \
    /usr/local/lib/mautrix-whatsapp-call/auto-provision-whatsapp-ghosts.py

WORKDIR /var/lib/mautrix-whatsapp-call
