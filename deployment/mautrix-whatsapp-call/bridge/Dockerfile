ARG MATRIXRTC_BRIDGE_BASE_IMAGE=dock.mau.dev/mautrix/whatsapp:v0.2611.0
FROM ${MATRIXRTC_BRIDGE_BASE_IMAGE}
USER root
COPY mautrix-whatsapp-call-bridge /usr/bin/mautrix-whatsapp
RUN chmod 0755 /usr/bin/mautrix-whatsapp
