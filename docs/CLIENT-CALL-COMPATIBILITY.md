# Client compatibility

The bridge uses MatrixRTC rather than legacy Matrix peer-to-peer calls. Element X Android 26.11.0 (embedded Element Call 0.28.2 and native Element Call Android 0.1.0-rc.9) is the reference client configuration. The embedded and native interfaces differ; end-to-end interoperability remains unverified. Element Web compatibility is not established.

Use `MATRIXRTC_INCOMING_MEMBERSHIP_FORMAT=room_state` for clients using legacy room-state memberships. MSC-format incoming membership is opt-in. Outgoing membership format is detected from the authenticated call membership.

## Call controls

| Feature | Client availability |
| --- | --- |
| Audio/video, answer, reject, hangup | Standard MatrixRTC controls |
| Microphone and camera | Standard MatrixRTC controls; media requires live testing |
| Camera selection | Local client function |
| WhatsApp video upgrade approval | No dedicated stock client control |
| Group invitations, raised hands, reactions and call links | Compatible client extensions; see below |
| Screen sharing and independent group media | Experimental, opt-in; not verified with stock clients |

Group calls default to aggregate media. `MATRIXRTC_EXPERIMENTAL_INDEPENDENT_GROUP_MEDIA=1` enables the experimental independent group media path. This does not imply support for arbitrary room participants, simultaneous camera/display or direct-call screen sharing.

## Provisioning API

The following controls are intended for compatible clients, not the stock Element X UI. Use the existing authenticated provisioning API and an owned `login_id`. The call extensions do not create ordinary chat messages. Provider submission or acknowledgement does not establish remote delivery.

## Authenticated call-link creation and preview

The provisioning API supports call-link creation and preview. It does not
add controls to the stock Element X interface or create a Matrix room. It is
registered only for the foreground bridge with experimental calling enabled
and an RTC gateway socket configured. It inherits the existing provisioning
API authentication and login permission checks, and requires an explicit
owned `login_id`. It does not change the reverse proxy or enable provisioning
if it was disabled. Do not expose the application service port directly.

Use the existing authenticated `/_matrix/provision` transport (HTTPS if remote),
with `user_id` and `login_id` query parameters and the existing Authorization
header. The two POST routes are:

- `/_matrix/provision/v3/whatsapp/call_link/create`: JSON `{"media":"audio"}`
  (or `"video"`). A provider-confirmed result is `{"media":"audio","url":"https://call.whatsapp.com/audio/TOKEN"}`.
- `/_matrix/provision/v3/whatsapp/call_link/preview`: JSON
  `{"media":"audio","link":"https://call.whatsapp.com/audio/TOKEN"}` (a
  raw token is also accepted). A confirmed result contains `media`,
  `approval_required` and `is_admin`; no token or creator identifiers are
  returned. The request never joins the call or publishes media.

Both routes accept only the specified JSON fields. Media must match the link.
Keep tokens in the JSON body, not the request URL, and do not send them in
ordinary Matrix messages. Responses are marked no-store. Valid call-link
protocol nodes in the pinned WhatsApp socket logger are redacted without
altering regular message logging. Infrastructure body dumps, malformed raw
frame diagnostics, client logs and any copied links are outside this narrow
redaction; disable sensitive body/frame tracing and handle the returned link
as a bearer secret. An ambiguous or cancelled creation is not automatically
retried and may have created a link on WhatsApp. A per-login in-flight guard
and connection lifetime cancellation apply to both operations.

Call-link join/leave and waiting-room controls require compatible clients and separate interoperability testing.

## Authenticated group participant invite and re-ring

The call-only provisioning extension inherits the existing provisioning
API authentication and requires explicit selection of a login owned by the
requesting Matrix user. No configuration, proxy, room or ordinary messaging
changes are made. With experimental calling enabled and an RTC gateway socket
configured, use the existing authenticated `/_matrix/provision` transport
with `user_id` and `login_id` and the existing Authorization header:

- `GET /_matrix/provision/v3/whatsapp/group_call/active`: returns `{"calls":[{"call_id":"CALLID","group":"GROUP@g.us"}]}` for active chat-bound group calls on that login (empty array if none).
- `POST /_matrix/provision/v3/whatsapp/group_call/participant`: JSON `{"call_id":"CALLID","target":"123456@lid","action":"invite"}` or `"action":"ring"`. The target must be a canonical non-device LID or phone JID (`@s.whatsapp.net`) of a current member. Each request addresses exactly one target and has an independent outcome; no bulk or implicit full-group invitation.

Invite requires a member absent from the current authoritative call roster;
ring requires a present non-connected member. The linked account must be a
current chat member and a connected participant in the active call. Self,
ambiguous aliases, nonmembers, direct calls, and stale call handles are
rejected. The implementation rechecks the exact call and roster following
device discovery, and uses a bounded connection-owned request with a per-login
in-flight guard. The existing automatic invitation path is unchanged.

HTTP 200 with `outcome: acknowledged` means a correlated WhatsApp offer ACK
was received, not that the target rang, joined or accepted. 409 indicates
an unavailable action/current-state conflict; 502 covers provider rejection
or other failure; 503 indicates the feature is unavailable; 504 denotes an
unknown outcome or timeout. No automatic retry. The linked account can lose
membership or the call can end during network submission; subsequent state
must be reconciled with the authoritative provider roster. Explicit actions
can also race the separate automatic path; do not retry an ambiguous action.
No new controls in stock Element X, no ad-hoc escalation, and no change to the
aggregate Matrix roster. Live ringing and acceptance require interoperability testing.

## Explicit ad-hoc group call selection

A compatible Matrix client can opt into an **unbound WhatsApp group offer**
by setting `"fi.mau.whatsapp.group_call_mode": "ad_hoc"` on the existing
trusted encrypted MatrixRTC invitation/notification, with `m.mentions.user_ids`
containing exactly 2–31 distinct WhatsApp ghost MXIDs and no room mention.
Use the existing bridged WhatsApp group room and its normal RTC membership,
notification, slot, device trust and preferred-login requirements. This is a
client extension, not a new control in stock Element X. No new rooms or
ordinary messages are created. The room remains a group MatrixRTC session
with an aggregate bridge participant: **this is not a private Matrix call**;
existing room membership and media admission rules continue to apply.

The bridge requires a current roster containing the linked account and every
selected target. Unknown, ambiguous, duplicate (including phone/LID aliases),
self, device-specific and unreachable targets fail rather than expanding to
the chat roster or silently dropping a target. The provider call has no
`group-jid`; its initial offer contains only the selected remote identities
and the local linked account. Existing cancellation and media handoff apply.
The bridge refuses invitation follow-ups and explicit chat-bound group
participant controls for this unbound call; a later provider update claiming
an unrelated chat binding terminates it. This does not prevent participants
from taking actions independently on WhatsApp or guarantee remote roster
membership cannot change after admission. 

The pinned MeowCaller public API supports initial ad-hoc group offers but
has no operation to initiate direct-to-group promotion of an existing call.
The bridge deliberately retains its fixed-room mode (provider-driven direct
promotion terminates the direct call); it does not silently redial or convert
a direct invitation. Ad-hoc invitations outside a bridged group room and
arbitrary cross-room target selection are not supported by this extension.

## Participant raised hands

A compatible Matrix client can read and control in-call WhatsApp group hands
through the existing authenticated provisioning transport. As for the other
call-only controls, select the explicitly owned `login_id` and authenticated
Matrix `user_id`. No ordinary Matrix reaction, synthetic ghost membership or
message event is emitted.

`POST /_matrix/provision/v3/whatsapp/group_call/hand` accepts one of:

- `{"call_id":"CALLID"}` — read the complete connected roster hand snapshot.
- `{"call_id":"CALLID","raised":true}` — submit a raise for the linked account.
- `{"call_id":"CALLID","raised":false}` — submit a lower for the linked account.

The snapshot includes `call_id`, `roster_transaction`, `hand_revision`,
`participants` (provider `participant_id`, `self`, `raised`), and `outcome`.
Clients must replace the entire prior snapshot rather than merging rows,
serialize polling per call, discard an older `hand_revision`, and clear the
snapshot when the call terminates or its roster is no longer available. The
revision advances on accepted roster and hand changes; repeated identical
provider hand state does not advance it. IDs are the authoritative WhatsApp
roster JIDs, not fabricated Matrix ghost IDs. No device aliases or arbitrary
remote target can be supplied to a mutation. No pre-authoritative/zero
transaction, ambiguous roster, disconnected local participant or ended call
is accepted. On roster departure or disconnection the stored hand is cleared;
continuous connected roster refresh preserves the hand. Unknown participants,
wrong call creator and unrelated sender envelopes cannot update group hands.

A successful mutation returns `outcome: submitted`, **not** a correlated
provider acknowledgement. The local hand is updated after successful submission;
this is not proof that peers saw it. A timeout, cancellation or changed roster
can leave the remote result unknown; refresh the snapshot, do not automatically
retry. The provider hand envelope has no membership-generation token: a late
remote event for a participant that has already departed and rejoined cannot
always be distinguished from a new event. No stock Element X hand UI or
MatrixRTC hand-event mirroring is added; this is a compatible-client API for
WhatsApp group calls, not impersonation of remote Matrix users. Live hand delivery remains unverified.

## Transient in-call reactions

A compatible client can poll or submit WhatsApp RTC app-data reactions using
`POST /_matrix/provision/v3/whatsapp/call/reaction`, with the same explicitly
owned `login_id` and authenticated Matrix user as other call controls.

- `{"call_id":"CALLID"}` obtains a stream ID and a bounded transient feed.
- `{"call_id":"CALLID","stream":"STREAM","after":123}` polls after a cursor.
- The same request with `"emoji":"👍"` submits the linked account's reaction;
  `"emoji":""` explicitly clears it. Omitting `emoji` never sends anything.

The response contains `stream`, `cursor`, `roster_transaction` (zero for
one-to-one), `reset`, `events` and `outcome`. Each event has a sequence,
provider `participant_id`, emoji and `removed` flag. Serialize polling per
call. Clear the local transient display when the stream or roster changes,
when `reset` is true, or when the call ends; consume only newer sequences.
Clear applies to the named participant, not the entire feed. Polling does not
consume the feed. Retention is ten seconds, at most 64 events and 256 recent
participant rate-limit records per call. This is deliberately lossy transient
UI data. Incoming reactions from the same participant are
rate limited; a clear after a displayed reaction is allowed, redundant clears
are suppressed. The provider's app-data transaction decoder rejects repeated
RTP reaction transactions. The bridge accepts only the current call's peer or
unique connected roster identity, excluding the linked account and its aliases.
Roster changes discard buffered reactions rather than risking misattribution.

Submission is serialized and limited to one action per second per call;
a stale stream cannot submit to a reused call ID. Success is `submitted`, not
remote acknowledgement. Cancellation stops further retransmission after the
current packet send returns. Unknown outcomes must not be automatically
retried. The feed never inserts a local submission as a received reaction;
there is no ordinary Matrix/WhatsApp message or reaction conversion.

The API accepts bounded Unicode symbol/emoji sequences (not arbitrary text);
it is not an exhaustive Unicode RGI emoji validator. Call-link discovery and
stock Element X reaction UI are not provided by this extension. Late provider
media whose original membership generation is not carried through the callback
cannot always be attributed across a leave/rejoin. Provider and client interoperability remains unverified.

## Read-only group device roster

A compatible client may POST `{"call_id":"CALLID"}` to
`/_matrix/provision/v3/whatsapp/group_call/roster` with its existing authenticated
provisioning session and explicit `login_id`. The endpoint is available only
for a current active group call owned by the linked login. It returns the
provider roster transaction, local media revision, and a complete sorted
replacement list of participant IDs, connection states and device IDs. Device
`microphone` is `muted`, `unmuted` or `unknown`; `camera` is `on`, `off` or
`unknown`. Poll sequentially and replace the snapshot; a missing participant
or device is no longer in the current snapshot. Unknown is not silence or a
camera-off indication. The local device can also be unknown.

Only the exact current connected device in a matching call-creator envelope
may supply a media observation. A roster refresh preserves an observation only
for continuous connected presence with the same exact device and known PID.
A removed/replaced device has no cached state. A change in the committed
roster during processing causes rejection rather than applying the old
observation to the new roster. Provider signalling has no reliable per-join
media-control sequence, so late remote notifications within a reused device
identity are not proven distinguishable. Bare identities without an exact
roster device and ambiguous aliases are not inferred. This read-only endpoint
exposes provider IDs only to the owning linked user; it does not send Matrix
messages, impersonate ghosts or modify ordinary messaging.

`media_representation` remains `aggregate`. This endpoint is **not** native
per-participant MatrixRTC membership, separate media tracks, or a stock
Element X group roster. The independent group media mode is experimental; live participant attribution remains unverified.
