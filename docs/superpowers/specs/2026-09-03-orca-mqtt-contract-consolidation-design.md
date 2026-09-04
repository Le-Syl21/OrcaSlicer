# OrcaCloud / OrcaSonar MQTT contract consolidation — design

**Date:** 2026-09-03
**Status:** Approved design, pre-implementation
**Repos touched by this task:** OrcaSlicer (implementation), plus a written conformance
checklist handed off to OrcaCloud and OrcaSonar (not edited here).

---

## 1. Context & problem

`OrcaPrinterAgent` talks to two backends that are meant to expose the *same*
printer API:

- **OrcaCloud** — cloud relay. Cloudflare Durable Object "broker-lite" with a
  hand-rolled MQTT 3.1.1 codec (`apps/gateway/src/lib/mqtt-codec.ts`,
  `apps/gateway/src/durable-objects/printer-shard.ts`).
- **OrcaSonar** — LAN hub. Real mochi MQTT broker fronted by a `/mqtt`
  WebSocket→TCP proxy (`internal/broker/broker.go`, `internal/httpws/server.go`).

A survey of all three codebases found the *payload and topic layer already ~90 %
identical* (Bambu-dialect JSON, `device/<id>/request` + `device/<id>/report`,
shared command set, numeric-string `sequence_id`), but the *transport mechanics
diverge*:

| Dimension | OrcaSonar LAN | OrcaCloud |
|---|---|---|
| Command send | client **PUBLISHes** `device/<id>/request` | viewers **receive-only**; commands via `POST /api/v1/printers/:id/commands` |
| Auth | MQTT CONNECT username/password | Bearer token on the HTTP upgrade |
| Endpoint | `ws://<host>:8280/mqtt` | `wss://<api>/api/v1/printers/{id}/mqtt` (and an aggregate `/printers/mqtt`) |
| Socket cardinality | 1 printer : 1 socket | aggregate socket, N printers, dynamic grant |
| Status stream | always on | demand-gated (`pushing.start` / `pushing.stop`) |
| Capability | retained `device/<id>/capability` | `info.get_capabilities` command, no retained |
| `sequence_id` bands | unenforced | enforced (OrcaSlicer must use 20000–29999) |
| Spec of record | `spec/protocol/orca_printer_comm_spec.md` (OPCP v1.1.0 + JSON schemas) | `doc/gateway/printer_mqtt_facade_2026-08-07.md` + dialect code |

Both sides already track the drift: OrcaSonar `API.md:244-301` diffs itself against
OrcaCloud's dialect code; OrcaCloud's facade doc has an explicit "OrcaSonar
adoption" section.

The client today mirrors the divergence — `send_message` (cloud) has historically
gone via REST while `send_message_to_printer` (LAN) publishes over MQTT — so the
`IPrinterAgent` split is a transport split, not just a routing switch.

**Goal:** one contract (OPCP v1.2.0) that both backends conform to, and an
OrcaSlicer client where LAN and cloud run *identical code* differing only by a
`Config` value.

---

## 2. Decisions

| # | Decision |
|---|---|
| D1 | **Contract merge + thin client shim.** Merge to a canonical spec; fix payload-level divergences server-side; the client keeps only a `Config`-sized shim for endpoint/auth/keepalive. |
| D2 | **This task ships:** the OPCP v1.2.0 spec text (authored here), the OrcaSlicer client implementation, and an enumerated conformance checklist for OrcaCloud & OrcaSonar. The other two repos are **not** edited in this task. |
| D3 | **Command transport is MQTT PUBLISH `device/<id>/request` on both cloud and LAN.** OrcaCloud gains client PUBLISH via change CH-1. |
| D4 | **Client structure: one `OrcaMqttConnection` class, two `Config`-only instances, routed by `OrcaPrinterAgent`.** The cloud instance uses the **1:1 per-printer** endpoint `/api/v1/printers/{id}/mqtt`, exactly like LAN. |
| D5 | **One socket per transport, selected printer only.** The pre-existing aggregate cloud MQTT socket is removed; unselected printers show announce/REST status on both transports (see §4.3). |
| D6 | **No back-compat.** OPCP v1.2.0 replaces v1.1.0 outright. REST `/commands` is not a required alias. No `/tunnel` bridge concerns. |
| D7 | **OrcaSonar's OPCP is the source of truth.** The contract is OrcaSonar's spec (with the additions in §5.1, most of which absorb behaviours OrcaCloud already ships); OrcaCloud conforms to it. |

---

## 3. OPCP v1.2.0 — the unified contract

### 3.1 Canonical core (already aligned; ratified here)

- **Transport:** MQTT 3.1.1 over WebSocket, binary frames, subprotocol `mqtt`,
  `cleanSession = 1`.
- **Topics:** `device/<dev_id>/request` (client → device),
  `device/<dev_id>/report` (device → client). `<dev_id>` is the identifier the
  client is already provisioned with — the cloud printer UUID on the cloud
  binding; the mDNS-advertised `device_id` (TXT `device_id=`, SSDP UDN
  `uuid:<device_id>`) on the LAN binding. The client never learns the id from
  message traffic.
- **Envelope:** exactly one top-level namespace key ∈
  `{pushing, info, print, system, camera, xcam, upgrade, files, event}`, plus
  `command` and `sequence_id` (decimal string, `^[0-9]+$`).
- **Result echo (single-phase):** same namespace + command + `sequence_id`, plus
  `result ∈ {success, fail}`, `reason` on `fail`, optional `errno`. No separate
  dialect-layer transport ack.
- **Status:** `<ns>.push_status` on the report topic; `msg` 0 = full, 1 = diff.
- **`info.get_version` reply:** `module[]` entries with `name / sw_ver / hw_ver / sn`.
- **Optional extended header** (adopt OrcaSonar spec §3.1 verbatim):
  `protocol_version`, `schema_version`, `sent_at_utc_ms`,
  `source{role, agent_id, transport}`. Receivers ignore unknown top-level keys.

### 3.2 Divergence resolutions

| Divergence | Resolution | Owner |
|---|---|---|
| Command send: REST vs PUBLISH | Client PUBLISHes `device/<id>/request` on both transports. | OrcaCloud CH-1 |
| `sequence_id` bands | Normative registry: OrcaSlicer **20000–29999**, dashboard 50000–59999, gateway-minted 70000–79999, status-mirror 90000+. Correlation is producer-scoped (match echoes against your own outstanding ids). | OPCP SPEC-2; client |
| Capability discovery | `info.get_capabilities` command is the REQUIRED path (returns the capability manifest). Retained `device/<id>/capability` is an OPTIONAL LAN optimization; clients MUST NOT depend on it. | client; OrcaSonar SN-2 |
| Status gating | Client issues `pushing.start` immediately after SUBSCRIBE and `pushing.stop` on deselect, on both transports. Always-streaming implementations accept both as `result:"success"` no-ops. | client; OrcaSonar SN-1; OrcaCloud CH-4 |
| Topic `<id>` | LAN topic id == advertised `device_id`, so the client subscribes the exact topic — **no `device/+/report` wildcard**. | OrcaSonar SN-3 |
| `system.set_settings` | Schema defined in OPCP (SPEC-5): curated toggles — camera, discovery, moonraker_compat. Unknown setting → `result:"fail"`, `errno = UNSUPPORTED_SETTING`. Not client-driven in this task. | OPCP SPEC-5; OrcaSonar SN-4 |
| Auth | Two mechanisms, both normative: (a) bearer token in the `Authorization` header of the WS upgrade (cloud); (b) MQTT CONNECT username/password (LAN local broker). The client `Config` carries whichever applies. | documented only |
| QoS | Client requests SUBSCRIBE QoS 1 and PUBLISH QoS 0–1; MUST tolerate a SUBACK that grants QoS 0. | client |
| Keepalive | `Config.keepalive_seconds` (default 60 LAN / 300 cloud). Binary PINGREQ. | client |
| Endpoint | LAN `ws://<host>:8280/mqtt`; cloud `wss://<api>/api/v1/printers/{id}/mqtt`. | `Config` |

Net effect: everything payload- and topic-level is identical on both transports;
the only per-transport variation is `Config` (URL + auth + keepalive) plus one
connect-time `pushing.start`.

---

## 4. OrcaSlicer client architecture

### 4.1 `OrcaMqttConnection` — the single transport class

Lives in its own translation unit (extracted from `OrcaCloudServiceAgent.cpp`,
where an earlier `OrcaCloudMqttConnection` / renamed `OrcaMqttConnection` still
sits). No LAN/cloud conditionals in the body.

```cpp
struct Config {
    std::string   url;                   // ws://host:8280/mqtt | wss://api/.../printers/{id}/mqtt
    bool          use_tls = false;       // derived from the URL scheme
    TokenProvider bearer_provider;       // cloud: Authorization: Bearer <jwt> on the WS upgrade
    std::string   username, password;    // LAN: MQTT CONNECT credentials
    // Precedence: if bearer_provider is set it is used for the WS upgrade and the
    // CONNECT username/password are omitted; otherwise CONNECT carries the creds.
    std::string   client_id;             // stable for the process run, unique per instance
    int           keepalive_seconds = 60;
};
```

API (used identically by both instances):

| Method | Behaviour |
|---|---|
| `bool start(Config, MessageHandler on_message, StateHandler on_state, CancellationHandler = {})` | spawns the worker thread; blocks (bounded, ~10 s) for the first CONNACK; returns the initial result |
| `void stop()` | idempotent; joins the worker |
| `bool send_request(const std::string& dev_id, const std::string& payload)` | PUBLISH `device/<dev_id>/request` (QoS 0/1); thread-safe via the write mutex; false when there is no CONNACKed session. **The uniform outbound seam.** |
| `bool subscribe(const std::string& dev_id)` / `unsubscribe(...)` | SUBSCRIBE / UNSUBSCRIBE `device/<dev_id>/report`; persistent set re-sent after every CONNACK |
| `bool is_connected() const` / `int last_connack_rc() const` | CONNACK state; rc 0 = accepted, 1..5 = MQTT refusal, -1 = no CONNACK this attempt |
| `MessageHandler(dev_id, payload)` | inbound: strips the `device/<id>/report` topic, hands up raw JSON, on the worker thread |

All protocol logic (topic construction, MQTT framing, reconnect/backoff, write
serialization, QoS-downgrade tolerance, PINGREQ) is internal. The only internal
branches are `use_tls` (TLS handshake + SNI) and bearer-vs-CONNECT-creds during
the handshake.

Reusable pieces from `salvage/orcasonar-lan-agent-2026-09-03` (the generalized
`Config`, `ws://` support in `parse_endpoint`, `last_connack_rc`, auth-reject
handling, static frame builders + their tests) are lifted in rather than
re-derived.

### 4.2 `OrcaPrinterAgent` — routing + lifecycle

- `std::unique_ptr<OrcaMqttConnection> lan_mqtt_connection` — owned here;
  lifecycle = LAN printer selection.
- Cloud per-printer `OrcaMqttConnection` — owned by `OrcaCloudServiceAgent`,
  reached via `get_orca_cloud_agent()->get_mqtt_connection()`.
- `OrcaMqttConnection* get_appropriate_mqtt_connection(bool is_lan)` — the one
  place that encodes the ownership split. Kept even though callers know their
  `is_lan` bit; it is the seam and it is tiny. Where a caller has only a
  `dev_id`, resolve via `DeviceManager::get_my_machine(dev_id)->is_lan_mode_printer()`.
- `enum CurrentConn { NONE, CLOUD, LAN } m_current_connection` — a label/gate,
  **not** a socket selector.

**Outbound collapse.** Both methods reduce to the same body:

```cpp
int OrcaPrinterAgent::send_message(dev_id, json, qos, flag)            // is_lan = false
int OrcaPrinterAgent::send_message_to_printer(dev_id, json, qos, flag) // is_lan = true
// ->
auto* conn = get_appropriate_mqtt_connection(is_lan);
if (!conn || dev_id.empty()) return BAMBU_NETWORK_ERR_INVALID_HANDLE;
return conn->send_request(dev_id, json) ? BAMBU_NETWORK_SUCCESS
                                        : BAMBU_NETWORK_ERR_CONNECTION_TO_SERVER_FAILED;
```

The `command_*` helpers keep branching only to build the payload, then call one
or the other.

**Inbound: remove `read_loop()`.** Each `OrcaMqttConnection` already delivers on
its worker thread via `MessageHandler`. The agent registers one handler per
connection that funnels to `on_message_fn`, marshalled onto the UI thread via
`queue_on_main_fn`. This is already the pattern `set_cloud_agent()` wires for the
cloud side (`set_printer_status_callback`); the LAN side gets the symmetric
wiring when `lan_mqtt_connection` is created. A single reader that "picks the
current connection" reintroduces a one-transport-at-a-time asymmetry and a
busy-spin; the per-connection callback is already uniform.

**Lifecycle — the post-connect sequence is byte-identical on both transports:**

| Trigger | LAN | Cloud |
|---|---|---|
| select | `connect_printer(dev_id, dev_ip, user, code, ssl)` | `set_user_selected_machine(dev_id)` |
| agent builds `Config` | `ws://<dev_ip>:8280/mqtt` + username/password | `wss://<api>/api/v1/printers/{dev_id}/mqtt` + `bearer_provider` |
| then — shared `on_connected(dev_id, conn)` | `subscribe(dev_id)` → `send_request(dev_id, pushing.start)` → `send_request(dev_id, pushall)` → `send_request(dev_id, info.get_version)` → `send_request(dev_id, info.get_capabilities)` | *the same five calls* |
| deselect / change | `disconnect_printer()` → `stop()` + reset | `send_request(prev, pushing.stop)` → `unsubscribe(prev)` → `stop()` / retarget |

**Threading & lifetime:**

- The blocking `start()` runs on a short-lived thread so the UI is never blocked
  (mirrors the salvaged WIP).
- A generation counter (atomic, captured by value into every handler lambda)
  makes a superseded connection's late `MessageHandler` / `StateHandler` calls
  no-ops.
- `~OrcaPrinterAgent` calls `stop()` on both connections (joining their workers)
  before any member is destroyed. No detached threads may touch `*this`.
- CONNACK rc 4/5 (bad credentials / not authorised) is terminal: report
  `ConnectStatusFailed`, do not retry (a retry storm would flap
  `ConnectStatusLost` → `set_selected_machine("")`).

### 4.3 One socket per transport — the selected printer only

The client opens exactly one MQTT socket at a time, for the **selected** printer,
identically on both transports:

- LAN: `ws://<dev_ip>:8280/mqtt` — opened on `connect_printer`, closed on
  `disconnect_printer`.
- Cloud: `wss://<api>/api/v1/printers/{dev_id}/mqtt` (the 1:1 per-printer
  binding, D4) — opened on `set_user_selected_machine(dev_id)`, closed on
  deselect. `OrcaCloudServiceAgent` owns the instance; `OrcaPrinterAgent`
  drives it via `get_mqtt_connection()`.

Unselected printers are never connected on either transport. They populate the
Device list from announce data alone — mDNS/SSDP `on_machine_alive` for LAN
hubs, the account REST list for cloud printers — exactly as unselected LAN hubs
already behave.

**This removes the pre-existing aggregate cloud MQTT socket**
(`wss://<api>/api/v1/printers/mqtt`, started today by
`OrcaCloudServiceAgent::connect_server()`). `is_server_connected()` falls back to
the REST health probe `connect_server()` already performs on the same 5 s
`refresh_connection()` tick.

**Behaviour change (needs product sign-off, tracked as O2):** unselected cloud
printers in the Device list lose their live status feed and show last-known /
REST status. This is the price of LAN/cloud symmetry and matches how unselected
LAN hubs already appear. If live multi-printer status is later required it is an
`OrcaCloudServiceAgent` concern (its own aggregate consumer), and it must not add
a second `device/<id>/report` stream for the already-connected selected printer.

---

## 5. Conformance checklist (hand-off)

**Framing (D7):** the target state is "cloud and LAN expose an identical API",
and **OrcaSonar's OPCP spec is the source of truth**. Read this section as:

- §5.1 — additions the OPCP spec (and therefore OrcaSonar's implementation)
  needs. Small, and mostly formalising behaviours OrcaCloud already ships
  (`pushing.start/stop`, `system.set_settings`, the `sequence_id` registry).
- §5.2 — OrcaCloud is the participant furthest from the contract (commands over
  REST, viewer PUBLISH forbidden, aggregate-only socket). These changes make it
  conform.
- §5.3 — OrcaSonar's own work: implement the §5.1 additions, plus one or two
  guarantees to make explicit.

### 5.1 OPCP spec additions → v1.2.0 (`OrcaSonar spec/protocol/orca_printer_comm_spec.md`)

| ID | Change |
|---|---|
| SPEC-1 | Add a normative **Transports** section: the two bindings from §3.1 (LAN local broker; cloud gateway). Both MUST accept client PUBLISH to `device/<id>/request`. |
| SPEC-2 | Normative `sequence_id` band registry (§3.2); correlation is producer-scoped. |
| SPEC-3 | `pushing.start` / `pushing.stop` are normative commands — "begin / stop streaming `push_status` to this subscriber". Always-streaming implementations MUST still return `result:"success"` (no-op). |
| SPEC-4 | `info.get_capabilities` is the REQUIRED capability path; retained `device/<id>/capability` is OPTIONAL and non-load-bearing. |
| SPEC-5 | Define the `system.set_settings` schema (curated toggles: camera, discovery, moonraker_compat); unknown setting → `result:"fail"`, `errno = UNSUPPORTED_SETTING`. |
| SPEC-6 | Errno registry: enumerate values already in use plus `UNSUPPORTED_COMMAND`, `UNSUPPORTED_SETTING`, `NOT_AUTHORIZED`. Align semantics (not numeric values) with the client's `ORCA_NETWORK_ERR_CMD_NOT_SUPPORTED` / `ORCA_NETWORK_ERR_CAP_NOT_AVAILABLE`. |
| SPEC-7 | Promote the extended envelope header (§3.1 of the OrcaSonar spec) to OPTIONAL on every message; receivers ignore unknown top-level keys. |
| SPEC-8 | Topic `<id>` MUST equal the id the client is provisioned with; no id discovery from traffic. |

### 5.2 OrcaCloud — conform to OPCP (`~/repos/OrcaCloud/apps/gateway`)

Assuming the OPCP contract (client PUBLISHes commands over a 1:1 MQTT-over-WS
socket, exactly as against OrcaSonar LAN), OrcaCloud must add:

| ID | Change | Acceptance |
|---|---|---|
| **CH-1** | `mqtt-viewer` sessions MAY `PUBLISH` to `device/{id}/request` (today → close 4403 in `printer-shard.ts::handleMqttPublish`). Relay viewer → connector via the existing `awaitConnectorReply` / `deliverToConnector` path. `/report` stays connector-only (anti-forgery preserved). | e2e: a viewer publishes `print.pause` `sequence_id` 20001 → connector receives it on `device/{id}/request` → the result echo fans back to that viewer. |
| CH-2 | Move operator-role authorization from the REST `/commands` route (`routes/printer.ts`) into the shard publish handler. READ set — `pushing.pushall`, `info.get_version`, `info.get_capabilities`, `files.list`, `files.metadata` — allowed for viewer / live-token; every other command requires an operator+ session. | live-token viewer publishing `print.stop` → 4403; JWT operator → success. |
| CH-3 | Provide a **1:1 per-printer** WS binding `GET /api/v1/printers/{id}/mqtt` that behaves like OrcaSonar's `/mqtt` (one printer per socket; subscribe the exact `device/{id}/report`; no `X-Orca-Printer-Ids`). If #948 ("shard-only routing") removed this facade path, re-adding it is the change — the client does not use the aggregate `/printers/mqtt`. | client connects, subscribes, publishes a command, receives the echo, with no aggregate-grant header. |
| CH-4 | `pushing.start` / `pushing.stop` over the viewer publish path arm / disarm the demand-mirror for that printer (today armed only by dashboard page polls, #984). Behaviour matches OPCP SPEC-3: on always-streaming backends it is a success no-op; OrcaCloud's mirror is genuinely demand-gated so it acts. | after a viewer `pushing.start`, `push_status` frames arrive on that viewer's `report` subscription; after `pushing.stop` (last viewer gone) they cease. |
| CH-5 | `info.get_capabilities` returns an OPCP capability manifest matching OrcaSonar's `orca_capability_manifest` schema (`protocol_version` ≥ 1.2.0), whether the connector is Bambu- or Klipper-backed. | manifest validates against the OPCP schema. |

### 5.3 OrcaSonar — implement the spec additions (`~/repos/OrcaSonar/internal`)

OrcaSonar owns the contract, so its work is small: land the §5.1 spec text, make
the LAN dispatcher match it, and turn two already-true facts into guarantees.

| ID | Change | Acceptance |
|---|---|---|
| **SN-1** | The LAN dispatcher accepts `pushing.start` / `pushing.stop` as `result:"success"` no-ops — the LAN broker always streams `push_status`, so these commands only need to be *recognised*, not fall through to "unsupported command". (Today they are handled only on the OrcaSonar→cloud connector path, not the LAN dispatcher.) `internal/protocol/dispatcher.go` specials + `internal/protocol/types.go` `SupportedCommands`. See O4: this is the "always-on, nothing to gate" reading, not a per-subscriber mirror. | golden: `{"pushing":{"command":"start","sequence_id":"20005"}}` → `{"pushing":{"command":"start","sequence_id":"20005","result":"success","errno":0}}`. |
| SN-2 | `info.get_capabilities` returns the manifest identically whether requested via command or read from the retained topic, and works before any `pushall`. | fresh connect → command → manifest with `protocol_version ≥ 1.2.0`. |
| SN-3 | Documented guarantee, plus a test, that topic `<id>` == advertised `device_id` (mDNS TXT `device_id=`, SSDP UDN). Lets the client drop the `device/+/report` wildcard. Likely already true (`internal/discovery/discovery.go`, `internal/config/config.go`). | client subscribing `device/<advertised-id>/report` receives reports. |
| SN-4 | `system.set_settings` → implement per SPEC-5, or return `result:"fail"`, `errno = UNSUPPORTED_SETTING` (not a bare "unsupported command", not a parse error / close). | unknown setting key → structured errno, session stays open. |
| SN-5 | Capability manifest `protocol_version` / `schema_version` → `1.2.0`. | manifest validates against the v1.2.0 schema. |
| SN-6 | Conformance test only (no code change expected): broker accepts client-id `orcaslicer-lan-<dev_id>-<hex>` and a QoS 1 SUBSCRIBE. | test passes. |

### 5.4 OrcaSlicer client (this repo — implemented, not enumerated)

Everything in §4, plus: uses `info.get_capabilities` (never the retained topic),
always issues `pushing.start` / `pushing.stop`, stays in `sequence_id` band
20000–29999, subscribes the exact report topic, tolerates a SUBACK that grants
QoS 0.

---

## 6. Testing & verification

### 6.1 Client unit tests (`tests/slic3rutils/`, Catch2)

- `OrcaMqttConnection` static frame builders — CONNECT (bearer and
  CONNECT-creds forms), SUBSCRIBE / UNSUBSCRIBE, PUBLISH, PINGREQ,
  remaining-length codec, `parse_endpoint` for `ws://` and `wss://`. Byte-level
  assertions.
- `send_request` produces topic `device/<id>/request` with a verbatim payload;
  inbound strips `device/<id>/report` → `(id, payload)`.
- `Config` selects the handshake path (auth mode; `use_tls` from scheme).
- `command_*` payload builders match OPCP shapes (string `sequence_id`, band
  20000–29999, single namespace key).
- The post-connect sequence emits exactly
  `subscribe → pushing.start → pushall → info.get_version → info.get_capabilities`,
  in that order.
- Generation guard: a superseded connection's late handler calls are no-ops.
- SUBACK granting QoS 0 when 1 was requested → still connected, messages still
  delivered.

### 6.2 Client integration (in-process MQTT-over-WS mock)

- **One parametrized test, two fixtures (LAN `Config` / cloud `Config`):**
  connect → CONNACK → subscribe → publish command → mock emits the result echo →
  assert `on_message_fn` fires with it. Identical assertions for both fixtures —
  this is the "exactly the same" proof.
- Reconnect: mock drops the socket → worker backs off → reconnects →
  subscriptions re-sent → `pushing.start` re-issued.
- Auth reject: CONNACK rc 4/5 → terminal `ConnectStatusFailed`, no retry storm.

### 6.3 Cross-repo conformance (run in those repos' CI, from §5 acceptance rows)

- OrcaCloud: extend `tests/e2e/lane-b-printers/printer_mqtt.e2e.test.ts` for
  CH-1 / CH-2 / CH-3 / CH-4 / CH-5 (each row's acceptance criterion in §5.2).
- OrcaSonar: golden JSONL fixtures for SN-1 / SN-2 / SN-4.

### 6.4 Manual smoke (record a log / screenshot for each)

RelWithDebInfo build →

- (a) real OrcaSonar hub on the LAN: discover, connect, Device tab populates,
  set nozzle temperature, observe the result echo;
- (b) OrcaCloud staging with a paired printer: select, same checks.

Both exercised through the *same* `OrcaMqttConnection` code path.

### 6.5 Gates before "done"

- New unit + integration tests green (ctest output as evidence).
- No regression in the existing `tests/slic3rutils` suites (printer-agent,
  plugin, qidi).
- LAN and cloud smoke each confirmed with a log/screenshot.
- One `cmake --build build` at the end.

---

## 7. Out of scope

- Editing OrcaCloud or OrcaSonar in this task (the checklist in §5 is the
  hand-off; those land as separate PRs in their own repos).
- A live multi-printer status feed for the Device list on cloud (would be its
  own `OrcaCloudServiceAgent` aggregate consumer — see §4.3 / O2). The printer
  agent connects the selected printer only.
- Reusing OrcaCloud's aggregate `/printers/mqtt` for the client (option B from
  brainstorming — rejected; it forces aggregate-grant semantics into
  `OrcaMqttConnection` that the LAN 1:1 case never needs).
- An `IPrinterTransport` abstraction above MQTT (option C — YAGNI while MQTT is
  the only transport).
- Camera streaming, filesystem / file transfer, filament sync, AMS mapping.
- Back-compat: the legacy `/tunnel` envelope plane, retaining REST `/commands`
  as a required alias, dual-schema acceptance on OrcaSonar.

---

## 8. Open items, risks & resolved questions

| # | Item |
|---|---|
| O1 | **Does OrcaCloud's 1:1 `/api/v1/printers/{id}/mqtt` still exist post-#948?** ("shard-only routing" made routing shard-backed.) Not a client fork any more — per CH-3 the client uses only the 1:1 endpoint, so if the facade path was removed, re-adding it *is* the OrcaCloud change. Just needs confirmation with the OrcaCloud team of whether CH-3 is "keep" or "re-add". |
| O2 | **Behaviour change: unselected cloud printers lose live status** (§4.3 — consequence of dropping the aggregate socket for LAN/cloud symmetry). They show last-known / REST status, matching unselected LAN hubs. Needs product sign-off before implementation. |
| O3 | **CONNECT-creds vs bearer through a fronting proxy.** If a deployment puts `wss://` in front of OrcaSonar, both auth inputs could be present. Precedence is fixed in §4.1 (`bearer_provider` set ⇒ bearer, CONNECT creds omitted); flagged only so the plan makes it a tested branch. |
| O4 | **Resolved.** `pushing.start` / `pushing.stop` mean "begin / stop streaming `push_status` to me". On OrcaCloud the mirror is genuinely demand-gated so the commands act; on OrcaSonar LAN the broker always streams, so they are recognised-and-succeed no-ops (SN-1). Not the "MQTT subscription granularity" reading — the client still SUBSCRIBEs `device/<id>/report` explicitly on both. |
| O5 | **`sequence_id` band collisions.** The client must never emit outside 20000–29999, including for any gateway-minted flow it triggers (print jobs). Audit every `sequence_id` source in `OrcaPrinterAgent`. |
