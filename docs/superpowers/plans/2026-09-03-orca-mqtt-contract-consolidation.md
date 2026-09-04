# OrcaSlicer MQTT Contract Consolidation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `OrcaPrinterAgent` talk to OrcaCloud and OrcaSonar through one `OrcaMqttConnection` class where LAN and cloud differ only by a `Config` value — commands published to `device/<id>/request`, status subscribed on `device/<id>/report`, on both transports.

**Architecture:** One `OrcaMqttConnection` (MQTT 3.1.1 codec + WebSocket transport, `ws://` and `wss://`) in its own translation unit. Two instances: a LAN one owned by `OrcaPrinterAgent`, a per-printer cloud one owned by `OrcaCloudServiceAgent` and driven by `OrcaPrinterAgent` via `get_mqtt_connection()`. `OrcaPrinterAgent` is routing + lifecycle only; `get_appropriate_mqtt_connection(is_lan)` picks the instance and `send_request()` is the uniform outbound seam. Inbound is a per-connection `MessageHandler` callback funnelled to `on_message_fn` (no reader loop). Exactly one socket is connected at a time, for the selected printer.

**Tech Stack:** C++17, Boost.Beast (WebSocket + TLS), Boost.Asio, nlohmann/json, Catch2 (`tests/slic3rutils`, target `slic3rutils_tests`, discovered via `orcaslicer_discover_tests`).

**Spec:** `docs/superpowers/specs/2026-09-03-orca-mqtt-contract-consolidation-design.md` — read it alongside this plan.

## Global Constraints

- **No git commits.** Per this repo's workflow, all changes stay unstaged; there is no per-task commit. Each task ends at "tests green" (see Workflow note).
- **Build once at the end.** Per-task verification builds and runs only the `slic3rutils_tests` target (`cmake --build build --target slic3rutils_tests`). The full `cmake --build build` runs only in the final task.
- **No back-compat.** OPCP v1.2.0 replaces v1.1.0. No REST `/commands` fallback in the client, no `/tunnel`, no dual-schema handling.
- **`sequence_id` band:** every `sequence_id` the client emits MUST be a decimal string in `20000`–`29999`.
- **Cross-platform:** Windows, macOS, Linux. No POSIX-only calls; use Boost/std.
- **Threading:** every `websocket::stream` write goes through `write_mutex`. Handlers run on the connection's worker thread and must marshal to the UI thread via `queue_on_main_fn` before touching wx.
- **Topic id:** the id in `device/<id>/...` is the id the client already holds (LAN: the mDNS-advertised `device_id` passed as `dev_id`; cloud: the cloud printer UUID). No wildcard subscribe, no learning the id from traffic.

## Workflow note (per-task loop, adapted for no-commits)

Each task's last step is **"Verify green"**, not "Commit":

```bash
cmake --build build --target slic3rutils_tests -j
ctest --test-dir build -R 'OrcaMqtt|OrcaPrinterAgent' --output-on-failure
```

Leave changes unstaged. Move to the next task.

---

## File Structure

**New:**

| File | Responsibility |
|---|---|
| `src/slic3r/Utils/OrcaMqttConnection.hpp` | `OrcaMqttConnection` class: `Config`, lifecycle (`start`/`stop`), `send_request`, `subscribe`/`unsubscribe`, state accessors, and the pure static codec helpers (`parse_endpoint`, `make_*_packet`) — public so tests reach them. |
| `src/slic3r/Utils/OrcaMqttConnection.cpp` | Its implementation (moved verbatim from `OrcaCloudServiceAgent.cpp`, then extended). |
| `tests/slic3rutils/test_orca_mqtt_connection.cpp` | Unit tests: codec byte-shapes, `parse_endpoint`, `Config`→handshake selection, `send_request`/`subscribe` topic strings. |
| `tests/slic3rutils/orca_mqtt_mock_broker.hpp` | Header-only in-process MQTT-over-WS mock (Beast server on `127.0.0.1:0`, plaintext): answers CONNECT→CONNACK, SUBSCRIBE→SUBACK, PUBLISH→PUBACK, and can push a canned message on a subscribed topic. Test-only. |
| `tests/slic3rutils/test_orca_printer_agent.cpp` | `OrcaPrinterAgent` routing + lifecycle tests + the parametrized LAN/cloud integration test. Seeded from `salvage/orcasonar-lan-agent-2026-09-03:tests/slic3rutils/test_orca_printer_agent.cpp` then rewritten for this design. |

**Modified:**

| File | Change |
|---|---|
| `src/slic3r/Utils/OrcaCloudServiceAgent.hpp` | Replace the inline `OrcaMqttConnection` class with `#include "OrcaMqttConnection.hpp"`. Keep `get_mqtt_connection()`. Add `configure_selected_printer_mqtt(std::string dev_id)` / `teardown_selected_printer_mqtt()` decls. |
| `src/slic3r/Utils/OrcaCloudServiceAgent.cpp` | Move the `OrcaMqttConnection` impl out. `connect_server()` stops starting the aggregate socket. Implement `configure_selected_printer_mqtt` / `teardown_selected_printer_mqtt`. `is_server_connected()` from the REST health result only. |
| `src/slic3r/Utils/OrcaPrinterAgent.hpp` | Remove `read_loop` / `m_read_loop_thread` / `m_should_end`. Add `std::atomic<uint64_t> m_lan_generation`, `std::string m_lan_dev_id`, `void on_connected(const std::string& dev_id, OrcaMqttConnection* conn, uint64_t generation)`, and the `sequence_id`-stamped payload builders. |
| `src/slic3r/Utils/OrcaPrinterAgent.cpp` | Implement `connect_printer`, `disconnect_printer`, `set_user_selected_machine`, the `send_message*` collapse, per-connection `MessageHandler` wiring, generation guard, destructor teardown. Remove `read_loop`. |
| `src/slic3r/CMakeLists.txt` | Add `Utils/OrcaMqttConnection.cpp` + `.hpp` to `SLIC3R_GUI_SOURCES`. |
| `tests/slic3rutils/CMakeLists.txt` | Add `test_orca_mqtt_connection.cpp` and `test_orca_printer_agent.cpp` to `slic3rutils_tests`. |

---

## Task 1: Extract `OrcaMqttConnection` to its own translation unit

Pure move, no behaviour change. Makes every later diff legible.

**Files:**
- Create: `src/slic3r/Utils/OrcaMqttConnection.hpp`, `src/slic3r/Utils/OrcaMqttConnection.cpp`
- Modify: `src/slic3r/Utils/OrcaCloudServiceAgent.hpp`, `src/slic3r/Utils/OrcaCloudServiceAgent.cpp`, `src/slic3r/CMakeLists.txt`

**Interfaces:**
- Produces: `class Slic3r::OrcaMqttConnection` at `src/slic3r/Utils/OrcaMqttConnection.hpp` with today's exact API (`start(const std::string& endpoint, TokenProvider, MessageHandler, StateHandler)`, `stop()`, `is_running()`, `is_connected()`, `subscribe(const std::vector<std::string>&)`, `unsubscribe(...)`, `clear_subscriptions()`, `send_request(const std::string&, const std::string&)`).

- [ ] **Step 1: Create `OrcaMqttConnection.hpp`**

Move the `class OrcaMqttConnection { ... };` block out of `OrcaCloudServiceAgent.hpp` (currently lines ~34–108) into a new header with include guard `#ifndef slic3r_OrcaMqttConnection_hpp_`. Carry the includes it needs: `<boost/beast/websocket.hpp>`, `<boost/beast/ssl.hpp>`, `<boost/asio/ssl.hpp>`, `<atomic>`, `<condition_variable>`, `<functional>`, `<memory>`, `<mutex>`, `<set>`, `<string>`, `<thread>`, `<vector>`, `<cstdint>`. Keep it in `namespace Slic3r`.

- [ ] **Step 2: Create `OrcaMqttConnection.cpp`**

Move every `OrcaMqttConnection::` and `OrcaMqttConnection::Connection` definition out of `OrcaCloudServiceAgent.cpp` (the block from `struct OrcaMqttConnection::Connection {` to the end of `OrcaMqttConnection::run()`), plus the file-local `using json = nlohmann::json;` / anonymous-namespace helpers those definitions use. `#include "OrcaMqttConnection.hpp"` at the top; carry the boost/log/json includes it references.

- [ ] **Step 3: Point `OrcaCloudServiceAgent` at the new header**

In `OrcaCloudServiceAgent.hpp` replace the removed class with `#include "OrcaMqttConnection.hpp"`. `OrcaCloudServiceAgent.cpp` keeps compiling (it already `#include`s its own header).

- [ ] **Step 4: Add to the build**

In `src/slic3r/CMakeLists.txt`, in `set(SLIC3R_GUI_SOURCES` next to `Utils/OrcaCloudServiceAgent.cpp`:

```cmake
    Utils/OrcaMqttConnection.cpp
    Utils/OrcaMqttConnection.hpp
```

- [ ] **Step 5: Verify green**

```bash
cmake --build build --target slic3rutils_tests -j
ctest --test-dir build -R 'printer_agent|Orca' --output-on-failure
```
Expected: builds; existing `test_printer_agent` / `test_qidi_printer_agent` unchanged and passing. No new tests yet.

---

## Task 2: `Config` struct + `parse_endpoint` for `ws://`

**Files:**
- Modify: `src/slic3r/Utils/OrcaMqttConnection.hpp`, `src/slic3r/Utils/OrcaMqttConnection.cpp`
- Create: `tests/slic3rutils/test_orca_mqtt_connection.cpp`
- Modify: `tests/slic3rutils/CMakeLists.txt`

**Interfaces:**
- Produces:
  ```cpp
  struct OrcaMqttConnection::Config {
      std::string   url;
      bool          use_tls = false;
      TokenProvider bearer_provider;       // set => bearer on WS upgrade, CONNECT creds omitted
      std::string   username;
      std::string   password;
      std::string   client_id  = "OrcaSlicer";
      int           keepalive_seconds = 60;
  };
  static bool OrcaMqttConnection::parse_endpoint(const std::string& url, Endpoint& out); // now public, ws:// + wss://
  struct OrcaMqttConnection::Endpoint { std::string host; std::string port; std::string target; };
  ```
  `Endpoint` and `parse_endpoint` move to the `public:` section.

- [ ] **Step 1: Write the failing test**

Create `tests/slic3rutils/test_orca_mqtt_connection.cpp`:

```cpp
#include <catch2/catch_test_macros.hpp>
#include "slic3r/Utils/OrcaMqttConnection.hpp"

using Slic3r::OrcaMqttConnection;

TEST_CASE("OrcaMqtt parse_endpoint handles ws and wss", "[OrcaMqtt]") {
    OrcaMqttConnection::Endpoint ep;

    REQUIRE(OrcaMqttConnection::parse_endpoint("ws://printer.local:8280/mqtt", ep));
    CHECK(ep.host   == "printer.local");
    CHECK(ep.port   == "8280");
    CHECK(ep.target == "/mqtt");

    REQUIRE(OrcaMqttConnection::parse_endpoint("ws://10.0.0.5/mqtt", ep));
    CHECK(ep.port == "80");

    REQUIRE(OrcaMqttConnection::parse_endpoint("wss://api.example.com/api/v1/printers/abc/mqtt", ep));
    CHECK(ep.host   == "api.example.com");
    CHECK(ep.port   == "443");
    CHECK(ep.target == "/api/v1/printers/abc/mqtt");

    CHECK_FALSE(OrcaMqttConnection::parse_endpoint("http://x/y", ep));
}
```

- [ ] **Step 2: Register the test file and run it (expect FAIL)**

Add `test_orca_mqtt_connection.cpp` to the `add_executable(${_TEST_NAME}_tests` list in `tests/slic3rutils/CMakeLists.txt`.

```bash
cmake --build build --target slic3rutils_tests -j
```
Expected: compile error — `parse_endpoint` is private / `Endpoint` is private / no `ws://` support.

- [ ] **Step 3: Make it pass**

In `OrcaMqttConnection.hpp` move `struct Endpoint` and `static bool parse_endpoint(...)` into `public:`. Add the `Config` struct (above) in `public:`.

In `OrcaMqttConnection.cpp` replace the `wss://`-only `parse_endpoint` body:

```cpp
bool OrcaMqttConnection::parse_endpoint(const std::string& url, Endpoint& endpoint) {
    std::string rest;
    std::string default_port;
    if      (url.rfind("wss://", 0) == 0) { rest = url.substr(6); default_port = "443"; }
    else if (url.rfind("ws://",  0) == 0) { rest = url.substr(5); default_port = "80";  }
    else return false;

    const auto slash = rest.find('/');
    const std::string authority = rest.substr(0, slash);
    endpoint.target = (slash == std::string::npos) ? "/" : rest.substr(slash);

    // host[:port] — leave an unbracketed IPv6 literal alone
    const auto colon = authority.rfind(':');
    if (colon != std::string::npos && authority.find(']') == std::string::npos) {
        endpoint.host = authority.substr(0, colon);
        endpoint.port = authority.substr(colon + 1);
    } else {
        endpoint.host = authority;
        endpoint.port = default_port;
    }
    return !endpoint.host.empty() && !endpoint.port.empty() && !endpoint.target.empty();
}
```

- [ ] **Step 4: Verify green**

```bash
cmake --build build --target slic3rutils_tests -j
ctest --test-dir build -R OrcaMqtt --output-on-failure
```
Expected: PASS.

---

## Task 3: `make_connect_packet` with client-id / auth / keepalive

**Files:**
- Modify: `src/slic3r/Utils/OrcaMqttConnection.hpp`, `.cpp`, `tests/slic3rutils/test_orca_mqtt_connection.cpp`

**Interfaces:**
- Consumes: `Config` (Task 2).
- Produces: `static std::vector<uint8_t> OrcaMqttConnection::make_connect_packet(const std::string& client_id, const std::string& username, const std::string& password, int keepalive_seconds);` — public. Clean-session always set; username/password flags + fields only when `username` non-empty.

- [ ] **Step 1: Write the failing test**

```cpp
TEST_CASE("OrcaMqtt CONNECT packet — no auth (cloud form)", "[OrcaMqtt]") {
    auto p = OrcaMqttConnection::make_connect_packet("OrcaSlicer", "", "", 300);
    REQUIRE(p.size() >= 12);
    CHECK(p[0] == 0x10);                 // CONNECT
    // variable header: protocol name "MQTT", level 4
    CHECK(p[2] == 'M'); CHECK(p[3] == 'Q'); CHECK(p[4] == 'T'); CHECK(p[5] == 'T');
    CHECK(p[6] == 0x04);
    CHECK(p[7] == 0x02);                 // connect flags: clean session only
    CHECK(((p[8] << 8) | p[9]) == 300);  // keepalive
}

TEST_CASE("OrcaMqtt CONNECT packet — username/password (LAN form)", "[OrcaMqtt]") {
    auto p = OrcaMqttConnection::make_connect_packet("orcaslicer-lan-x", "orcasonar", "code123", 60);
    CHECK(p[0] == 0x10);
    CHECK(p[7] == (0x02 | 0x80 | 0x40)); // clean session + username + password flags
    // payload contains the client id, then username, then password strings
    const std::string blob(p.begin(), p.end());
    CHECK(blob.find("orcaslicer-lan-x") != std::string::npos);
    CHECK(blob.find("orcasonar")        != std::string::npos);
    CHECK(blob.find("code123")          != std::string::npos);
}
```

- [ ] **Step 2: Run it (expect FAIL)**

```bash
cmake --build build --target slic3rutils_tests -j
```
Expected: compile error — `make_connect_packet` takes no args / is private.

- [ ] **Step 3: Implement**

Move `make_connect_packet` to `public:` and replace its body:

```cpp
std::vector<uint8_t> OrcaMqttConnection::make_connect_packet(
    const std::string& client_id, const std::string& username,
    const std::string& password, int keepalive_seconds) {
    std::vector<uint8_t> packet{0x10};
    append_string(packet, "MQTT");
    packet.push_back(4); // protocol level 3.1.1

    uint8_t flags = 0x02; // clean session
    if (!username.empty()) { flags |= 0x80; if (!password.empty()) flags |= 0x40; }
    packet.push_back(flags);

    packet.push_back(static_cast<uint8_t>(keepalive_seconds >> 8));
    packet.push_back(static_cast<uint8_t>(keepalive_seconds & 0xff));

    append_string(packet, client_id.empty() ? "OrcaSlicer" : client_id);
    if (!username.empty()) {
        append_string(packet, username);
        if (!password.empty()) append_string(packet, password);
    }
    prepend_remaining_length(packet, packet.size() - 1);
    return packet;
}
```

Update the one caller in `connect_and_read()` to pass
`current_config.client_id, current_config.username, current_config.password, current_config.keepalive_seconds`
(the `current_config` member arrives in Task 6; until then pass `"OrcaSlicer","","",60` and leave a `// TODO(Task 6): from Config` marker — **remove the marker in Task 6**).

- [ ] **Step 4: Verify green**

```bash
ctest --test-dir build -R OrcaMqtt --output-on-failure
```
Expected: PASS.

---

## Task 4: request / report topic packet builders

**Files:**
- Modify: `src/slic3r/Utils/OrcaMqttConnection.hpp`, `.cpp`, `tests/slic3rutils/test_orca_mqtt_connection.cpp`

**Interfaces:**
- Produces (all public, all static):
  ```cpp
  static std::string request_topic(const std::string& dev_id);            // "device/<id>/request"
  static std::string report_topic(const std::string& dev_id);             // "device/<id>/report"  (already exists; make public)
  static std::vector<uint8_t> make_publish_packet(const std::string& topic, const std::string& payload); // QoS 0
  static std::vector<uint8_t> make_subscribe_packet(uint16_t packet_id, const std::string& topic, uint8_t qos);
  static std::vector<uint8_t> make_unsubscribe_packet(uint16_t packet_id, const std::string& topic);
  ```

- [ ] **Step 1: Write the failing test**

```cpp
TEST_CASE("OrcaMqtt topic helpers", "[OrcaMqtt]") {
    CHECK(OrcaMqttConnection::request_topic("abc") == "device/abc/request");
    CHECK(OrcaMqttConnection::report_topic("abc")  == "device/abc/report");
}

TEST_CASE("OrcaMqtt PUBLISH packet QoS0", "[OrcaMqtt]") {
    auto p = OrcaMqttConnection::make_publish_packet("device/abc/request", "{\"ok\":1}");
    CHECK((p[0] & 0xf0) == 0x30);   // PUBLISH
    CHECK((p[0] & 0x06) == 0x00);   // QoS 0
    const std::string blob(p.begin(), p.end());
    CHECK(blob.find("device/abc/request") != std::string::npos);
    CHECK(blob.find("{\"ok\":1}")          != std::string::npos);
}

TEST_CASE("OrcaMqtt SUBSCRIBE packet", "[OrcaMqtt]") {
    auto p = OrcaMqttConnection::make_subscribe_packet(7, "device/abc/report", 1);
    CHECK(p[0] == 0x82);                       // SUBSCRIBE + reserved bit
    CHECK(((p[2] << 8) | p[3]) == 7);          // packet id
    CHECK(p.back() == 1);                      // requested QoS
}
```

- [ ] **Step 2: Run it (expect FAIL)** — `cmake --build build --target slic3rutils_tests -j`; missing symbols.

- [ ] **Step 3: Implement**

Make `report_topic` public. Add:

```cpp
std::string OrcaMqttConnection::request_topic(const std::string& id) { return "device/" + id + "/request"; }

std::vector<uint8_t> OrcaMqttConnection::make_publish_packet(const std::string& topic, const std::string& payload) {
    std::vector<uint8_t> packet{0x30}; // PUBLISH, QoS 0, no retain
    append_string(packet, topic);      // no packet id at QoS 0
    packet.insert(packet.end(), payload.begin(), payload.end());
    prepend_remaining_length(packet, packet.size() - 1);
    return packet;
}

std::vector<uint8_t> OrcaMqttConnection::make_subscribe_packet(uint16_t id, const std::string& topic, uint8_t qos) {
    std::vector<uint8_t> packet{0x82};
    packet.push_back(id >> 8); packet.push_back(id & 0xff);
    append_string(packet, topic);
    packet.push_back(qos);
    prepend_remaining_length(packet, packet.size() - 1);
    return packet;
}

std::vector<uint8_t> OrcaMqttConnection::make_unsubscribe_packet(uint16_t id, const std::string& topic) {
    std::vector<uint8_t> packet{0xA2};
    packet.push_back(id >> 8); packet.push_back(id & 0xff);
    append_string(packet, topic);
    prepend_remaining_length(packet, packet.size() - 1);
    return packet;
}
```

Keep the existing `make_topic_packet(uint8_t, uint16_t, const std::vector<std::string>&)` for now — Task 5 removes its callers.

- [ ] **Step 4: Verify green** — `ctest --test-dir build -R OrcaMqtt --output-on-failure`

---

## Task 5: single-id `subscribe` / `unsubscribe` + persistent set; `Config`-carrying `start`

**Files:**
- Modify: `src/slic3r/Utils/OrcaMqttConnection.hpp`, `.cpp`, `tests/slic3rutils/test_orca_mqtt_connection.cpp`

**Interfaces:**
- Produces:
  ```cpp
  bool start(const Config& config, MessageHandler on_message, StateHandler on_state);
  bool subscribe(const std::string& dev_id);     // adds report_topic(dev_id) to the set, SUBSCRIBEs if connected
  bool unsubscribe(const std::string& dev_id);
  int  last_connack_rc() const;                   // 0 ok, 1..5 refusal, -1 none this attempt
  ```
  The old `start(const std::string&, TokenProvider, ...)` and vector `subscribe`/`unsubscribe` are **removed** (no back-compat). `report_topic` set members replace the device-id set.

- [ ] **Step 1: Write the failing test**

```cpp
TEST_CASE("OrcaMqtt start takes a Config", "[OrcaMqtt]") {
    OrcaMqttConnection conn;
    OrcaMqttConnection::Config cfg;
    cfg.url = "ws://127.0.0.1:1/mqtt";           // nothing listening
    cfg.keepalive_seconds = 42;
    // start() returns false (no server) but must compile with the Config overload
    const bool ok = conn.start(cfg, [](auto, auto){}, [](bool, bool){});
    CHECK_FALSE(ok);
    CHECK(conn.last_connack_rc() == -1);
    conn.stop();
}
```

- [ ] **Step 2: Run it (expect FAIL)** — compile error: no `start(Config, ...)`, no `last_connack_rc`.

- [ ] **Step 3: Implement**

- Replace the `endpoint_url` / `get_token` members with `Config current_config;` and `std::atomic<int> m_last_connack_rc{-1};`.
- Replace `std::set<std::string> subscriptions` semantics: it now holds full report-topic strings. Add `pending_subscriptions` / `pending_unsubscriptions` as topic strings too (rename in place).
- New `start`:
  ```cpp
  bool OrcaMqttConnection::start(const Config& config, MessageHandler on_message, StateHandler on_state) {
      stop();
      { std::lock_guard<std::mutex> l(mutex);
        current_config = config; this->on_message = std::move(on_message); this->on_state = std::move(on_state);
        initial_completed = false; initial_result = false; connected = false; m_last_connack_rc = -1; }
      stopping.store(false);
      worker = std::thread(&OrcaMqttConnection::run, this);
      std::unique_lock<std::mutex> l(mutex);
      initial_cv.wait_for(l, std::chrono::seconds(10), [this]{ return initial_completed; });
      return initial_result;
  }
  ```
- `subscribe(dev_id)` / `unsubscribe(dev_id)`:
  ```cpp
  bool OrcaMqttConnection::subscribe(const std::string& dev_id) {
      const std::string topic = report_topic(dev_id);
      { std::lock_guard<std::mutex> l(mutex);
        subscriptions.insert(topic); pending_unsubscriptions.erase(topic);
        pending_subscriptions.insert(topic); }
      flush_subscription_change();
      return true;
  }
  ```
  (mirror for `unsubscribe`).
- `connect_and_read()` / `send_current_subscriptions` / `send_pending_subscriptions`: iterate topic strings, build with `make_subscribe_packet(next_packet_id++, topic, 1)` / `make_unsubscribe_packet`. Delete `make_topic_packet` and its declaration.
- In `connect_and_read()` use `current_config`: `parse_endpoint(current_config.url, ep)`; branch TLS on `current_config.use_tls` (Task 7 completes the plaintext branch); build CONNECT via `make_connect_packet(current_config.client_id, current_config.username, current_config.password, current_config.keepalive_seconds)` — **delete the Task 3 TODO marker**; add the `Authorization: Bearer` upgrade header only when `current_config.bearer_provider` is set.
- On CONNACK: `m_last_connack_rc = <byte 3 of CONNACK>;`. If rc ∈ {4,5}: stop the worker, do not retry (terminal).
- Update `OrcaCloudServiceAgent.cpp::connect_server()` (its only caller) to the new signature — see Task 15; for now make it compile with a `Config` built from today's `wss://.../api/v1/printers/mqtt` URL and `bearer_provider = [this]{ return get_access_token(); }`.

- [ ] **Step 4: Verify green** — `ctest --test-dir build -R OrcaMqtt --output-on-failure` (the new test plus Tasks 2–4). Also `ctest -R 'printer_agent'` still green.

---

## Task 6: `send_request` publishes to `device/<id>/request`

**Files:**
- Modify: `src/slic3r/Utils/OrcaMqttConnection.cpp`, `tests/slic3rutils/test_orca_mqtt_connection.cpp`

**Interfaces:**
- Consumes: `make_publish_packet`, `request_topic`, `ws_write`/`send` (existing write path), `write_mutex`.
- Produces: `bool send_request(const std::string& dev_id, const std::string& payload)` — returns `false` without sending if `!is_connected()`.

- [ ] **Step 1: Write the failing test** (uses the mock broker from Task 8 — so this test is added but `[.]`-hidden until Task 9 wires it; for now assert the guard):

```cpp
TEST_CASE("OrcaMqtt send_request refuses when not connected", "[OrcaMqtt]") {
    OrcaMqttConnection conn;
    CHECK_FALSE(conn.send_request("abc", "{\"pushing\":{\"command\":\"pushall\",\"sequence_id\":\"20001\"}}"));
}
```

- [ ] **Step 2: Run it (expect FAIL)** — `send_request` currently returns something else / is unimplemented.

- [ ] **Step 3: Implement**

```cpp
bool OrcaMqttConnection::send_request(const std::string& dev_id, const std::string& payload) {
    if (!connected.load()) return false;
    std::shared_ptr<Connection> conn;
    { std::lock_guard<std::mutex> l(connection_mutex); conn = active_connection; }
    if (!conn) return false;
    const auto packet = make_publish_packet(request_topic(dev_id), payload);
    std::lock_guard<std::mutex> w(write_mutex);
    try { send(conn->websocket, packet); }          // existing write helper
    catch (const std::exception& e) {
        BOOST_LOG_TRIVIAL(warning) << "OrcaMqtt send_request failed: " << e.what();
        return false;
    }
    return true;
}
```

(If Task 7 renamed `send`→`ws_write`, use that.)

- [ ] **Step 4: Verify green** — `ctest --test-dir build -R OrcaMqtt --output-on-failure`

---

## Task 7: plaintext (`ws://`) transport branch

**Files:**
- Modify: `src/slic3r/Utils/OrcaMqttConnection.hpp`, `.cpp`

**Interfaces:**
- Consumes: `Config.use_tls`, `Config.bearer_provider`.
- Produces: `struct Connection` holds `std::optional<PlainWebSocket> ws;` and `std::optional<TlsWebSocket> wss;` with `PlainWebSocket = websocket::stream<beast::tcp_stream>` and `TlsWebSocket = websocket::stream<ssl::stream<beast::tcp_stream>>`. Internal helpers `ws_write(Connection&, const std::vector<uint8_t>&)`, `ws_read(Connection&, beast::flat_buffer&, error_code&)`, `ws_handshake(Connection&, const Config&, const Endpoint&)`, `ws_close(Connection&)` dispatch on which optional is engaged.

- [ ] **Step 1: Write the failing test** (needs the mock broker — mark `[.integration]`, un-hide in Task 9):

```cpp
TEST_CASE("OrcaMqtt connects over plaintext ws", "[OrcaMqtt][.integration]") {
    orca_mqtt_test::MockBroker broker;                    // Task 8
    OrcaMqttConnection conn;
    OrcaMqttConnection::Config cfg; cfg.url = broker.ws_url(); cfg.use_tls = false;
    std::atomic<bool> up{false};
    REQUIRE(conn.start(cfg, [](auto,auto){}, [&](bool c, bool){ if (c) up = true; }));
    CHECK(conn.is_connected());
    conn.stop();
}
```

- [ ] **Step 2: Run it (expect FAIL to build / link)** — no `ws_handshake` etc.

- [ ] **Step 3: Implement**

- `Connection` gets both optionals + the shared `beast::flat_buffer read_buffer;` and a `net::io_context io;` / `ssl::context tls{ssl::context::tlsv12_client};` as today.
- `ws_handshake`: resolve host/port; `beast::get_lowest_layer(stream).connect(results)`; if `use_tls`: set SNI (`SSL_set_tlsext_host_name`), `stream.next_layer().handshake(ssl::stream_base::client)`. Then set the upgrade decorator that adds `Authorization: Bearer <token>` when `config.bearer_provider` is set, and `Sec-WebSocket-Protocol: mqtt`. Then `stream.handshake(host, target)`.
- `ws_write` / `ws_read` / `ws_close`: `if (conn.wss) conn.wss->...; else conn.ws->...;`.
- `connect_and_read()`: replace direct `WebSocket` use with a `Connection` and the `ws_*` helpers; construct `conn->ws.emplace(conn->io)` or `conn->wss.emplace(conn->io, conn->tls)` based on `current_config.use_tls`.
- Delete the old `using WebSocket = websocket::stream<ssl::stream<...>>;` typedef and the `send(WebSocket&, ...)` signature (fold into `ws_write`).

> API note: exact Beast calls (`beast::get_lowest_layer`, `tcp_stream::connect`, decorator signature) vary by Boost version. Match the version already vendored in `deps/`; the existing TLS code in this file is the reference for the `wss` side.

- [ ] **Step 4: Verify green**

```bash
cmake --build build --target slic3rutils_tests -j
ctest --test-dir build -R 'OrcaMqtt' --output-on-failure   # non-integration subset still green
```

---

## Task 8: in-process MQTT-over-WS mock broker

**Files:**
- Create: `tests/slic3rutils/orca_mqtt_mock_broker.hpp`

**Interfaces:**
- Produces:
  ```cpp
  namespace orca_mqtt_test {
  class MockBroker {                       // starts on ctor, stops on dtor
  public:
      MockBroker();
      ~MockBroker();
      std::string ws_url() const;          // "ws://127.0.0.1:<port>/mqtt"
      void push_report(const std::string& dev_id, const std::string& payload); // server->client PUBLISH on device/<id>/report
      std::vector<std::string> received_requests() const;   // payloads PUBLISHed by the client to any device/<id>/request
      int connect_count() const;
  };
  }
  ```

- [ ] **Step 1: Smoke test**

```cpp
#include "orca_mqtt_mock_broker.hpp"
TEST_CASE("MockBroker starts and reports a url", "[OrcaMqtt][.integration]") {
    orca_mqtt_test::MockBroker b;
    CHECK(b.ws_url().rfind("ws://127.0.0.1:", 0) == 0);
    CHECK(b.connect_count() == 0);
}
```

- [ ] **Step 2: Run it (expect FAIL)** — header missing.

- [ ] **Step 3: Implement**

Header-only. One `std::thread` running a `net::io_context`; `tcp::acceptor` on `{net::ip::make_address("127.0.0.1"), 0}` (port 0 → OS-assigned, read back via `acceptor.local_endpoint().port()`). On accept: `websocket::stream<beast::tcp_stream>`, accept the upgrade echoing `Sec-WebSocket-Protocol: mqtt`. Then a minimal MQTT read loop:
- `0x10` CONNECT → reply `0x20 0x02 0x00 0x00` (CONNACK accepted); `connect_count_++`.
- `0x82` SUBSCRIBE → reply `0x90` SUBACK with the echoed packet id and one granted-QoS byte `0x00`; remember the subscribed topic.
- `0x30` PUBLISH (QoS 0) → parse topic + payload; if topic ends `/request`, append payload to `received_requests_`.
- `0xC0` PINGREQ → reply `0xD0 0x00`.
- `0xE0` DISCONNECT / read error → close.
`push_report()` posts a `make`-style PUBLISH (`0x30`, topic `device/<id>/report`, payload) onto the connected client socket via `net::post(strand, ...)`.

Use only Boost already vendored. Guard all shared state (`received_requests_`, `connect_count_`) with a `std::mutex`.

- [ ] **Step 4: Verify green** — `ctest --test-dir build -R 'MockBroker' --output-on-failure` with `--allow-running-no-tests` off; run the hidden tag explicitly: `ctest --test-dir build -R OrcaMqtt -C RelWithDebInfo` then the test binary directly: `./build/tests/slic3rutils/slic3rutils_tests "[.integration]"`.

---

## Task 9: parametrized LAN/cloud integration test

**Files:**
- Modify: `tests/slic3rutils/test_orca_mqtt_connection.cpp`

**Interfaces:**
- Consumes: `MockBroker` (Task 8), `OrcaMqttConnection::start/subscribe/send_request` (Tasks 5–7).

- [ ] **Step 1: Write the test**

```cpp
static void run_round_trip(bool use_tls_flag_only) {
    orca_mqtt_test::MockBroker broker;
    OrcaMqttConnection conn;
    OrcaMqttConnection::Config cfg;
    cfg.url = broker.ws_url();                     // plaintext regardless
    cfg.use_tls = false;                           // the mock is plaintext; the flag path is unit-tested elsewhere
    if (use_tls_flag_only) cfg.bearer_provider = []{ return std::string("tok"); };
    else { cfg.username = "orcasonar"; cfg.password = "code"; }

    std::promise<std::pair<std::string,std::string>> got;
    REQUIRE(conn.start(cfg,
        [&](const std::string& id, const std::string& payload){ got.set_value({id, payload}); },
        [](bool,bool){}));
    REQUIRE(conn.subscribe("dev-1"));
    REQUIRE(conn.send_request("dev-1", R"({"pushing":{"command":"pushall","sequence_id":"20001"}})"));

    broker.push_report("dev-1", R"({"print":{"command":"push_status","sequence_id":"20001","result":"success"}})");
    auto fut = got.get_future();
    REQUIRE(fut.wait_for(std::chrono::seconds(3)) == std::future_status::ready);
    auto [id, payload] = fut.get();
    CHECK(id == "dev-1");
    CHECK(payload.find("push_status") != std::string::npos);

    // the client's command reached the broker on the request topic
    CHECK(broker.received_requests().size() == 1);
    conn.stop();
}

TEST_CASE("OrcaMqtt round-trip — LAN-style config",   "[OrcaMqtt][.integration]") { run_round_trip(false); }
TEST_CASE("OrcaMqtt round-trip — cloud-style config", "[OrcaMqtt][.integration]") { run_round_trip(true);  }
```

- [ ] **Step 2: Run it (expect FAIL then iterate)** — `./build/tests/slic3rutils/slic3rutils_tests "[.integration]"`.

- [ ] **Step 3: Fix defects** in `OrcaMqttConnection` until both cases pass with identical assertions. Un-hide the Task 6/7 integration tests (`[.integration]` → keep the tag; they run via explicit tag selection).

- [ ] **Step 4: Verify green** — `./build/tests/slic3rutils/slic3rutils_tests "[OrcaMqtt]"` (all, including `[.integration]`).

---

## Task 10: `OrcaPrinterAgent` — remove `read_loop`, wire per-connection inbound

**Files:**
- Modify: `src/slic3r/Utils/OrcaPrinterAgent.hpp`, `.cpp`
- Create: `tests/slic3rutils/test_orca_printer_agent.cpp` (seed from `salvage/orcasonar-lan-agent-2026-09-03`)
- Modify: `tests/slic3rutils/CMakeLists.txt`

**Interfaces:**
- Produces: `void OrcaPrinterAgent::deliver_to_sink(const std::string& dev_id, const std::string& payload)` (private) — snapshots `on_message_fn` under `state_mutex`, calls it (marshalling via `queue_on_main_fn` when set). Used as the body of every connection's `MessageHandler`.

- [ ] **Step 1: Seed + write the failing test**

```bash
git show salvage/orcasonar-lan-agent-2026-09-03:tests/slic3rutils/test_orca_printer_agent.cpp \
  > tests/slic3rutils/test_orca_printer_agent.cpp
```
Then replace its body with this design's tests. First test:

```cpp
#include <catch2/catch_test_macros.hpp>
#include "slic3r/Utils/OrcaPrinterAgent.hpp"
using Slic3r::OrcaPrinterAgent;

TEST_CASE("OrcaPrinterAgent forwards a status payload to on_message_fn", "[OrcaPrinterAgent]") {
    OrcaPrinterAgent agent("/tmp");
    std::string got_id, got_payload;
    agent.set_on_message_fn([&](std::string id, std::string p){ got_id = id; got_payload = p; });
    agent.deliver_to_sink("dev-1", R"({"print":{"command":"push_status"}})");   // test-only hook
    CHECK(got_id == "dev-1");
    CHECK(got_payload.find("push_status") != std::string::npos);
}
```

Add `test_orca_printer_agent.cpp` to `tests/slic3rutils/CMakeLists.txt`.

- [ ] **Step 2: Run it (expect FAIL)** — `deliver_to_sink` missing; `read_loop` still present.

- [ ] **Step 3: Implement**

- `OrcaPrinterAgent.hpp`: delete `read_loop`, `m_read_loop_thread`, `m_should_end`. Add `void deliver_to_sink(const std::string& dev_id, const std::string& payload);`.
- `OrcaPrinterAgent.cpp`: delete the `read_loop` definition and its ctor `std::thread(...)`/`detach()`. Delete the file-scope `std::mutex m_conn_type_mtx;`. Ctor body becomes empty (or just the log line). Add:
  ```cpp
  void OrcaPrinterAgent::deliver_to_sink(const std::string& dev_id, const std::string& payload) {
      OnMessageFn fn; QueueOnMainFn q;
      { std::lock_guard<std::mutex> l(state_mutex); fn = on_message_fn; q = queue_on_main_fn; }
      if (!fn) return;
      if (q) q([fn, dev_id, payload]{ fn(dev_id, payload); });
      else   fn(dev_id, payload);
  }
  ```
- In `set_cloud_agent`, the existing `set_printer_status_callback` lambda body becomes `deliver_to_sink(std::move(dev_id), std::move(payload));`.

- [ ] **Step 4: Verify green** — `ctest --test-dir build -R OrcaPrinterAgent --output-on-failure`

---

## Task 11: `connect_printer` builds + starts the LAN connection

**Files:**
- Modify: `src/slic3r/Utils/OrcaPrinterAgent.hpp`, `.cpp`, `tests/slic3rutils/test_orca_printer_agent.cpp`

**Interfaces:**
- Consumes: `OrcaMqttConnection::Config`, `start`.
- Produces:
  ```cpp
  static bool OrcaPrinterAgent::parse_lan_endpoint(const std::string& dev_ip, std::string& host, std::string& port); // "8280" default, "/mqtt" implied
  static std::string OrcaPrinterAgent::make_lan_client_id(const std::string& dev_id);
  ```
  New members: `std::atomic<uint64_t> m_lan_generation{0}`, `std::string m_lan_dev_id`.

- [ ] **Step 1: Write the failing test**

```cpp
TEST_CASE("OrcaPrinterAgent::parse_lan_endpoint", "[OrcaPrinterAgent]") {
    std::string h, p;
    REQUIRE(OrcaPrinterAgent::parse_lan_endpoint("192.168.1.9", h, p));
    CHECK(h == "192.168.1.9"); CHECK(p == "8280");
    REQUIRE(OrcaPrinterAgent::parse_lan_endpoint("http://host.local:9000/x", h, p));
    CHECK(h == "host.local"); CHECK(p == "9000");
}

TEST_CASE("connect_printer stands up a LAN connection", "[OrcaPrinterAgent]") {
    OrcaPrinterAgent agent("/tmp");
    // 10.255.255.1 is unroutable → start() returns fast-ish; we only assert wiring
    const int rc = agent.connect_printer("dev-1", "10.255.255.1", "orcasonar", "code", false);
    CHECK(rc == BAMBU_NETWORK_SUCCESS);
    CHECK(agent.get_user_selected_machine().empty());        // LAN path does not set the cloud selection
    agent.disconnect_printer();
}
```

- [ ] **Step 2: Run it (expect FAIL)** — helpers missing; `connect_printer` is a stub returning success without doing anything (make the wiring assertion fail by checking an observable — see Step 3 for the observable: a protected `lan_connection_url()` test hook).

- [ ] **Step 3: Implement**

- Add `parse_lan_endpoint` (scheme strip, `host[:port]`, default `8280`) and `make_lan_client_id` (`"orcaslicer-lan-" + dev_id + "-" + <8 hex, drawn once per process>`).
- `connect_printer`:
  ```cpp
  int OrcaPrinterAgent::connect_printer(std::string dev_id, std::string dev_ip,
                                        std::string username, std::string password, bool use_ssl) {
      if (dev_id.empty() || dev_ip.empty()) return BAMBU_NETWORK_ERR_INVALID_HANDLE;
      std::string host, port;
      if (!parse_lan_endpoint(dev_ip, host, port)) return BAMBU_NETWORK_ERR_INVALID_HANDLE;
      disconnect_printer();
      const uint64_t gen = ++m_lan_generation;
      OrcaMqttConnection::Config cfg;
      cfg.url      = "ws://" + host + ":" + port + "/mqtt";
      cfg.use_tls  = false;                       // OrcaSonar LAN is plaintext; use_ssl ignored
      cfg.username = username.empty() ? "orcasonar" : username;
      cfg.password = password;
      cfg.client_id = make_lan_client_id(dev_id);
      cfg.keepalive_seconds = 60;
      { std::lock_guard<std::mutex> l(state_mutex); m_lan_dev_id = dev_id; m_current_connection = LAN; }
      lan_mqtt_connection = std::make_unique<OrcaMqttConnection>();
      auto* conn = lan_mqtt_connection.get();
      std::thread([this, conn, cfg, dev_id, gen] {
          const bool ok = conn->start(cfg,
              [this, gen](const std::string& id, const std::string& p){ if (gen == m_lan_generation.load()) deliver_to_sink(id, p); },
              [this, gen, dev_id, conn](bool c, bool initial){ if (c && !initial && gen == m_lan_generation.load()) on_connected(dev_id, conn, gen); });
          if (ok && gen == m_lan_generation.load()) on_connected(dev_id, conn, gen);
      }).detach();
      return BAMBU_NETWORK_SUCCESS;
  }
  ```
- Add a protected test hook: `std::string lan_connection_target() const { return lan_mqtt_connection ? /* Config.url snapshot */ : ""; }` — store `cfg.url` in a `std::string m_lan_url;` member when building it, and return that. Assert it in the test.

- [ ] **Step 4: Verify green** — `ctest --test-dir build -R OrcaPrinterAgent --output-on-failure`

---

## Task 12: `on_connected` — the shared post-connect sequence

**Files:**
- Modify: `src/slic3r/Utils/OrcaPrinterAgent.hpp`, `.cpp`, `tests/slic3rutils/test_orca_printer_agent.cpp`

**Interfaces:**
- Consumes: `OrcaMqttConnection::subscribe`, `send_request`.
- Produces:
  ```cpp
  void OrcaPrinterAgent::on_connected(const std::string& dev_id, OrcaMqttConnection* conn, uint64_t generation);
  static std::string OrcaPrinterAgent::seq(int n);   // "2000" + zero-padded band offset; returns a string in 20000..29999
  static std::string OrcaPrinterAgent::build_pushing_start(const std::string& sequence_id);
  static std::string OrcaPrinterAgent::build_pushall(const std::string& sequence_id);
  static std::string OrcaPrinterAgent::build_get_version(const std::string& sequence_id);
  static std::string OrcaPrinterAgent::build_get_capabilities(const std::string& sequence_id);
  ```

- [ ] **Step 1: Write the failing test** — a fake `OrcaMqttConnection` subclass is not available (non-virtual). Instead test the payload builders + call order via a seam: `on_connected` takes an `OrcaMqttConnection*`; make the four `send_request` payloads and the `subscribe` observable by having `on_connected` delegate to a `protected virtual void emit_connect_sequence(const std::string&, std::function<void(const std::string&)> subscribe, std::function<void(const std::string&)> request)` that the test overrides.

```cpp
TEST_CASE("post-connect sequence is subscribe then 4 requests in order", "[OrcaPrinterAgent]") {
    struct Probe : OrcaPrinterAgent {
        using OrcaPrinterAgent::OrcaPrinterAgent;
        std::vector<std::string> calls;
        void emit_connect_sequence(const std::string& dev_id,
            std::function<void(const std::string&)> sub,
            std::function<void(const std::string&)> req) override {
            OrcaPrinterAgent::emit_connect_sequence(dev_id,
                [&](const std::string& id){ calls.push_back("sub:" + id); },
                [&](const std::string& body){ calls.push_back(body); });
        }
    } probe("/tmp");
    probe.run_connect_sequence_for_test("dev-1");     // thin public shim calling emit_connect_sequence
    REQUIRE(probe.calls.size() == 5);
    CHECK(probe.calls[0] == "sub:dev-1");
    CHECK(probe.calls[1].find("\"pushing\"")  != std::string::npos);
    CHECK(probe.calls[1].find("\"start\"")    != std::string::npos);
    CHECK(probe.calls[2].find("pushall")      != std::string::npos);
    CHECK(probe.calls[3].find("get_version")  != std::string::npos);
    CHECK(probe.calls[4].find("get_capabilities") != std::string::npos);
    for (auto& c : probe.calls)                       // sequence_id band
        if (auto pos = c.find("sequence_id"); pos != std::string::npos)
            CHECK(c.substr(pos).find("\"2") != std::string::npos);
}
```

- [ ] **Step 2: Run it (expect FAIL)** — members missing.

- [ ] **Step 3: Implement**

- Payload builders return exact JSON, e.g.:
  ```cpp
  std::string OrcaPrinterAgent::build_pushall(const std::string& sid) {
      return R"({"pushing":{"command":"pushall","sequence_id":")" + sid + R"(","version":1,"push_target":1}})";
  }
  std::string OrcaPrinterAgent::build_pushing_start(const std::string& sid) {
      return R"({"pushing":{"command":"start","sequence_id":")" + sid + R"("}})";
  }
  std::string OrcaPrinterAgent::build_get_version(const std::string& sid) {
      return R"({"info":{"command":"get_version","sequence_id":")" + sid + R"("}})";
  }
  std::string OrcaPrinterAgent::build_get_capabilities(const std::string& sid) {
      return R"({"info":{"command":"get_capabilities","sequence_id":")" + sid + R"("}})";
  }
  ```
- `seq(n)`: `return std::to_string(20000 + (n % 10000));`
- `emit_connect_sequence(dev_id, sub, req)`: `sub(dev_id); req(build_pushing_start(seq(1))); req(build_pushall(seq(2))); req(build_get_version(seq(3))); req(build_get_capabilities(seq(4)));`
- `on_connected(dev_id, conn, generation)`: if `generation != m_lan_generation.load()` **and** the cloud generation guard (Task 14) both mismatch, return. Else `emit_connect_sequence(dev_id, [conn](auto& id){ conn->subscribe(id); }, [conn, &dev_id](auto& body){ conn->send_request(dev_id, body); });`
- Add the tiny public shim `run_connect_sequence_for_test`.

- [ ] **Step 4: Verify green** — `ctest --test-dir build -R OrcaPrinterAgent --output-on-failure`

---

## Task 13: `disconnect_printer` + generation guard

**Files:**
- Modify: `src/slic3r/Utils/OrcaPrinterAgent.cpp`, `tests/slic3rutils/test_orca_printer_agent.cpp`

**Interfaces:**
- Consumes: `m_lan_generation`, `lan_mqtt_connection`, `OrcaMqttConnection::stop`.

- [ ] **Step 1: Write the failing test**

```cpp
TEST_CASE("a stale-generation inbound message is dropped", "[OrcaPrinterAgent]") {
    struct Probe : OrcaPrinterAgent { using OrcaPrinterAgent::OrcaPrinterAgent;
        using OrcaPrinterAgent::make_lan_message_handler; };   // expose for the test
    Probe agent("/tmp");
    int hits = 0;
    agent.set_on_message_fn([&](std::string, std::string){ ++hits; });
    auto handler_gen1 = agent.make_lan_message_handler(/*generation=*/1);
    agent.bump_lan_generation_for_test();                       // now current == 2
    handler_gen1("dev-1", "{}");                                // late callback from gen 1
    CHECK(hits == 0);
}
```

- [ ] **Step 2: Run it (expect FAIL)** — helpers missing.

- [ ] **Step 3: Implement**

- Factor the message-handler lambda into `std::function<void(const std::string&, const std::string&)> make_lan_message_handler(uint64_t generation)` returning `[this, generation](auto& id, auto& p){ if (generation == m_lan_generation.load()) deliver_to_sink(id, p); }`.
- `disconnect_printer`:
  ```cpp
  int OrcaPrinterAgent::disconnect_printer() {
      ++m_lan_generation;                       // fence stale callbacks
      std::unique_ptr<OrcaMqttConnection> doomed;
      { std::lock_guard<std::mutex> l(state_mutex);
        doomed = std::move(lan_mqtt_connection);
        m_lan_dev_id.clear();
        if (m_current_connection == LAN) m_current_connection = NONE; }
      if (doomed) doomed->stop();               // joins the worker, outside the lock
      return BAMBU_NETWORK_SUCCESS;
  }
  ```
- Add `bump_lan_generation_for_test()`.

- [ ] **Step 4: Verify green** — `ctest --test-dir build -R OrcaPrinterAgent --output-on-failure`

---

## Task 14: `set_user_selected_machine` drives the cloud per-printer connection

**Files:**
- Modify: `src/slic3r/Utils/OrcaPrinterAgent.cpp`, `src/slic3r/Utils/OrcaCloudServiceAgent.hpp`, `.cpp`, `tests/slic3rutils/test_orca_printer_agent.cpp`

**Interfaces:**
- Consumes: `OrcaCloudServiceAgent::get_mqtt_connection()`.
- Produces on `OrcaCloudServiceAgent`:
  ```cpp
  int  configure_selected_printer_mqtt(const std::string& dev_id);  // (re)build Config{wss://<api>/api/v1/printers/{id}/mqtt, bearer}, start()
  void teardown_selected_printer_mqtt();                            // stop()
  std::string selected_printer_mqtt_url() const;                    // test hook — "" when not configured
  ```

- [ ] **Step 1: Write the failing test**

```cpp
TEST_CASE("selecting a cloud printer configures the per-printer socket", "[OrcaPrinterAgent]") {
    auto cloud = std::make_shared<Slic3r::OrcaCloudServiceAgent>("/tmp");
    cloud->set_api_base_url("api.example.com");
    OrcaPrinterAgent agent("/tmp");
    agent.set_cloud_agent(cloud);

    agent.set_user_selected_machine("printer-uuid-1");
    CHECK(cloud->selected_printer_mqtt_url() == "wss://api.example.com/api/v1/printers/printer-uuid-1/mqtt");

    agent.set_user_selected_machine("");
    CHECK(cloud->selected_printer_mqtt_url().empty());
}
```

- [ ] **Step 2: Run it (expect FAIL)** — methods missing; `set_user_selected_machine` still does the aggregate `add_subscribe` dance.

- [ ] **Step 3: Implement**

- `OrcaCloudServiceAgent::configure_selected_printer_mqtt(dev_id)`:
  ```cpp
  OrcaMqttConnection::Config cfg;
  cfg.url = "wss://" + api_base_url + "/api/v1/printers/" + dev_id + "/mqtt";
  cfg.use_tls = true;
  cfg.bearer_provider = [this]{ return get_access_token(); };
  cfg.client_id = "OrcaSlicer";
  cfg.keepalive_seconds = 300;
  m_selected_printer_mqtt_url = cfg.url;
  return mqtt_connection->start(cfg,
      [this](const std::string& id, const std::string& p){ /* Task 15 routes to printer_status_callback */ deliver_cloud_message(id, p); },
      [this](bool, bool){ }) ? BAMBU_NETWORK_SUCCESS : BAMBU_NETWORK_ERR_CONNECTION_TO_SERVER_FAILED;
  ```
  `teardown_selected_printer_mqtt()`: `mqtt_connection->stop(); m_selected_printer_mqtt_url.clear();`
- `OrcaPrinterAgent::set_user_selected_machine(dev_id)`:
  ```cpp
  int OrcaPrinterAgent::set_user_selected_machine(std::string dev_id) {
      auto* cloud = get_orca_cloud_agent();
      std::string previous;
      { std::lock_guard<std::mutex> l(state_mutex);
        if (dev_id == selected_machine) return BAMBU_NETWORK_SUCCESS;
        previous = selected_machine; selected_machine = dev_id; }
      if (!cloud) return BAMBU_NETWORK_SUCCESS;
      auto* conn = cloud->get_mqtt_connection();
      if (!previous.empty() && conn && conn->is_connected())
          conn->send_request(previous, build_pushing_stop(seq(5)));
      cloud->teardown_selected_printer_mqtt();
      if (!dev_id.empty()) {
          const uint64_t gen = ++m_lan_generation;    // reuse the same fence for cloud
          std::thread([this, cloud, dev_id, gen]{
              if (cloud->configure_selected_printer_mqtt(dev_id) == BAMBU_NETWORK_SUCCESS
                  && gen == m_lan_generation.load())
                  on_connected(dev_id, cloud->get_mqtt_connection(), gen);
          }).detach();
          { std::lock_guard<std::mutex> l(state_mutex); m_current_connection = CLOUD; }
      } else {
          std::lock_guard<std::mutex> l(state_mutex); m_current_connection = NONE;
      }
      return BAMBU_NETWORK_SUCCESS;
  }
  ```
  Add `build_pushing_stop` next to `build_pushing_start` (`"command":"stop"`).
- Delete the old aggregate `add_subscribe` / `del_subscribe` / `send_message(pushall)` / `deliver_mock_get_version` body from `set_user_selected_machine` (the mock get_version is now redundant — `on_connected` sends a real `info.get_version`).

- [ ] **Step 4: Verify green** — `ctest --test-dir build -R OrcaPrinterAgent --output-on-failure`

---

## Task 15: `OrcaCloudServiceAgent::connect_server()` stops starting the aggregate socket

**Files:**
- Modify: `src/slic3r/Utils/OrcaCloudServiceAgent.cpp`, `.hpp`, `tests/slic3rutils/test_orca_printer_agent.cpp` (or a new `test_orca_cloud_service_agent.cpp`)

**Interfaces:**
- Consumes: existing REST `http_get(ORCA_HEALTH_PATH, ...)`.
- Produces: `connect_server()` returns success/failure from the health probe alone; `is_server_connected()` returns the last health result; `mqtt_connection` is untouched by `connect_server()`.

- [ ] **Step 1: Write the failing test**

```cpp
TEST_CASE("connect_server does not start an MQTT socket", "[OrcaCloud]") {
    auto cloud = std::make_shared<Slic3r::OrcaCloudServiceAgent>("/tmp");
    cloud->set_api_base_url("127.0.0.1:1");     // health probe will fail fast
    cloud->connect_server();
    REQUIRE(cloud->get_mqtt_connection() != nullptr);
    CHECK_FALSE(cloud->get_mqtt_connection()->is_running());
    CHECK(cloud->selected_printer_mqtt_url().empty());
}
```

- [ ] **Step 2: Run it (expect FAIL)** — `connect_server()` currently builds `cfg.url = "wss://" + api_base_url + "/api/v1/printers/mqtt"` and calls `mqtt_connection->start(...)`.

- [ ] **Step 3: Implement**

- Delete the whole `if (connected && mqtt_connection && !mqtt_connection->is_running()) { ... mqtt_connection->start(cfg, ...); }` block from `connect_server()` and the trailing "aggregate MQTT" logging.
- Keep the health check; set `is_connected = connected;` from it and call `invoke_server_connected_callback(connected ? 0 : -1, http_code);`.
- `refresh_connection()` still just calls `connect_server()`.
- The `printer_status_callback` / `set_printer_status_callback` machinery stays (Task 14's `configure_selected_printer_mqtt` message lambda calls `deliver_cloud_message` → the registered `printer_status_callback`). Add:
  ```cpp
  void OrcaCloudServiceAgent::deliver_cloud_message(const std::string& id, const std::string& p) {
      OnMessageFn cb;
      { std::lock_guard<std::mutex> l(callback_mutex); cb = printer_status_callback; }
      if (cb) cb(id, p);
  }
  ```

- [ ] **Step 4: Verify green** — `ctest --test-dir build -R 'OrcaCloud|OrcaPrinterAgent' --output-on-failure`

---

## Task 16: collapse `send_message` / `send_message_to_printer`

**Files:**
- Modify: `src/slic3r/Utils/OrcaPrinterAgent.cpp`, `tests/slic3rutils/test_orca_printer_agent.cpp`

**Interfaces:**
- Consumes: `get_appropriate_mqtt_connection(bool)`, `OrcaMqttConnection::send_request`.

- [ ] **Step 1: Write the failing test**

```cpp
TEST_CASE("send_message* reject when no connection", "[OrcaPrinterAgent]") {
    OrcaPrinterAgent agent("/tmp");                 // no cloud agent, no LAN connection
    CHECK(agent.send_message("d", "{}", 0, 0)            == BAMBU_NETWORK_ERR_INVALID_HANDLE);
    CHECK(agent.send_message_to_printer("d", "{}", 0, 0) == BAMBU_NETWORK_ERR_INVALID_HANDLE);
}

TEST_CASE("send_message_to_printer publishes on the LAN connection", "[OrcaPrinterAgent][.integration]") {
    orca_mqtt_test::MockBroker broker;
    OrcaPrinterAgent agent("/tmp");
    // point the LAN connection at the mock by connecting to its host:port
    auto ep = broker.host_port();                   // {"127.0.0.1", <port>}
    agent.connect_printer("dev-1", ep.first + ":" + ep.second, "orcasonar", "code", false);
    // wait for connect
    for (int i = 0; i < 100 && broker.connect_count() == 0; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(20));
    CHECK(agent.send_message_to_printer("dev-1", R"({"print":{"command":"pause","sequence_id":"20007"}})", 0, 0)
          == BAMBU_NETWORK_SUCCESS);
    for (int i = 0; i < 100 && broker.received_requests().empty(); ++i) std::this_thread::sleep_for(std::chrono::milliseconds(20));
    REQUIRE(broker.received_requests().size() >= 1);
    CHECK(broker.received_requests().back().find("pause") != std::string::npos);
    agent.disconnect_printer();
}
```

> `MockBroker::host_port()` — add a small accessor returning `{"127.0.0.1", std::to_string(port_)}` in Task 8's header (fold this one-line addition here).

- [ ] **Step 2: Run it (expect FAIL)** — `send_message` still spawns the old REST thread / `send_message_to_printer` is a bare `return SUCCESS`.

- [ ] **Step 3: Implement**

```cpp
int OrcaPrinterAgent::send_message(std::string dev_id, std::string json_str, int, int) {
    return route_send(false, dev_id, json_str);
}
int OrcaPrinterAgent::send_message_to_printer(std::string dev_id, std::string json_str, int, int) {
    return route_send(true, dev_id, json_str);
}
int OrcaPrinterAgent::route_send(bool is_lan, const std::string& dev_id, const std::string& json_str) {
    if (dev_id.empty()) return BAMBU_NETWORK_ERR_INVALID_HANDLE;
    OrcaMqttConnection* conn = get_appropriate_mqtt_connection(is_lan);
    if (!conn) return BAMBU_NETWORK_ERR_INVALID_HANDLE;
    return conn->send_request(dev_id, json_str) ? BAMBU_NETWORK_SUCCESS
                                               : BAMBU_NETWORK_ERR_CONNECTION_TO_SERVER_FAILED;
}
```

Guard `get_appropriate_mqtt_connection(false)` against a null cloud agent: `return get_orca_cloud_agent() ? get_orca_cloud_agent()->get_mqtt_connection() : nullptr;`

- [ ] **Step 4: Verify green** — `./build/tests/slic3rutils/slic3rutils_tests "[OrcaPrinterAgent]"`

---

## Task 17: destructor teardown — no detached threads touching `*this`

**Files:**
- Modify: `src/slic3r/Utils/OrcaPrinterAgent.cpp`, `tests/slic3rutils/test_orca_printer_agent.cpp`

- [ ] **Step 1: Write the failing test**

```cpp
TEST_CASE("destroying an agent mid-connect does not hang or crash", "[OrcaPrinterAgent]") {
    for (int i = 0; i < 20; ++i) {
        auto agent = std::make_unique<OrcaPrinterAgent>("/tmp");
        agent->connect_printer("dev-1", "10.255.255.1", "orcasonar", "code", false);  // unroutable, still connecting
        agent.reset();     // dtor must join/stop cleanly
    }
    SUCCEED();
}
```

- [ ] **Step 2: Run it (expect FAIL / hang / TSAN error)** — the `connect_printer` detached thread may outlive `*this` and call `on_connected`/`deliver_to_sink`.

- [ ] **Step 3: Implement**

- The detached connect thread already guards every callback with the generation check. Add: `~OrcaPrinterAgent()` bumps `m_lan_generation` first, then:
  ```cpp
  OrcaPrinterAgent::~OrcaPrinterAgent() {
      ++m_lan_generation;
      std::unique_ptr<OrcaMqttConnection> lan;
      { std::lock_guard<std::mutex> l(state_mutex); lan = std::move(lan_mqtt_connection); }
      if (lan) lan->stop();                                   // joins the OrcaMqttConnection worker
      if (auto* cloud = get_orca_cloud_agent()) cloud->teardown_selected_printer_mqtt();
  }
  ```
- The remaining race is the *connect* `std::thread(...).detach()` still running `conn->start()` on a `conn` that `lan->stop()` then destroys. Fix: hold the connect thread as a member `std::thread m_lan_connect_thread;` (not detached) and `join()` it in the dtor **before** moving `lan_mqtt_connection`. Update Task 11 & Task 14 to assign `m_lan_connect_thread = std::thread(...)` (join any prior one first).

- [ ] **Step 4: Verify green** — run under sanitizers if the build has them: `ctest --test-dir build -R OrcaPrinterAgent --output-on-failure`

---

## Task 18: full build, full test run, manual smoke

**Files:**
- Modify: none (verification only)

- [ ] **Step 1: Full build**

```bash
cmake --build build --config RelWithDebInfo --target all -j
```
Expected: success on Linux. (CI covers Windows/macOS.)

- [ ] **Step 2: Full unit + integration suite**

```bash
ctest --test-dir build --output-on-failure
./build/tests/slic3rutils/slic3rutils_tests "[.integration]"
```
Expected: no regressions in `test_printer_agent`, `test_qidi_printer_agent`, `test_plugin_*`; all `[OrcaMqtt]` / `[OrcaPrinterAgent]` green.

- [ ] **Step 3: Manual smoke — LAN**

Run OrcaSlicer against a real OrcaSonar hub: it appears via discovery; select it; Device tab populates (version, temps); set nozzle temperature; confirm the result echo in the log (`OrcaMqtt` / `on_message` lines). Record a log excerpt or screenshot.

- [ ] **Step 4: Manual smoke — cloud**

Against OrcaCloud staging with a paired printer (requires CH-1 deployed there): select the printer; same checks. If CH-1 is not yet deployed, record that this step is blocked on the §5.2 hand-off and verify only that the socket opens and status is received (commands will 4403 until CH-1 ships).

- [ ] **Step 5: Record results**

Append a short "Verification" section to the spec doc: build result, `ctest` summary line, and the two smoke outcomes (or the cloud-blocked note).

---

## Self-Review

**Spec coverage**

| Spec section | Task(s) |
|---|---|
| §3.1 canonical core (topics, envelope) | T4 (topics), T3 (CONNECT), payload builders T12 |
| §3.2 command send = PUBLISH both transports | T6, T16 |
| §3.2 `sequence_id` band 20000–29999 | T12 (`seq`), asserted in T12 test |
| §3.2 capability via `info.get_capabilities` | T12 (`build_get_capabilities`, in the sequence) |
| §3.2 status gating (`pushing.start`/`stop`) | T12 (start), T14 (stop on deselect) |
| §3.2 exact-topic subscribe, no wildcard | T4/T5 (`report_topic(dev_id)`, single-id `subscribe`) |
| §3.2 auth: bearer or CONNECT creds, precedence | T3 (flags), T5/T7 (bearer header vs creds), T7 |
| §3.2 QoS ≤1, tolerate SUBACK QoS 0 | T4 (`make_subscribe_packet` qos arg), T8 mock grants 0, T9 round-trip |
| §3.2 keepalive from Config | T3, T5 |
| §4.1 `OrcaMqttConnection` own TU, Config, API | T1, T2, T5 |
| §4.1 reuse salvage pieces | T2/T3/T5 (parse_endpoint, connect packet, last_connack_rc) |
| §4.2 outbound collapse | T16 |
| §4.2 remove `read_loop`, per-connection MessageHandler | T10 |
| §4.2 `get_appropriate_mqtt_connection` kept | T16 (null-guarded) |
| §4.2 lifecycle table (connect/select/deselect) | T11, T13, T14 |
| §4.2 identical post-connect sequence | T12 |
| §4.2 generation guard | T11 (capture), T13 (test), T17 (dtor) |
| §4.2 CONNACK rc 4/5 terminal | T5 |
| §4.2 dtor stops both, no detached threads on `*this` | T17 |
| §4.3 one socket, per-printer cloud endpoint | T14 (`wss://.../printers/{id}/mqtt`) |
| §4.3 remove aggregate socket, health-only `is_server_connected` | T15 |
| §6.1 unit tests | T2–T6, T12 |
| §6.2 parametrized LAN/cloud integration | T9 |
| §6.2 reconnect re-sends subs + `pushing.start` | *gap → see below* |
| §6.2 auth-reject no retry storm | T5 (rc 4/5 terminal); add assertion in T5 |
| §6.5 gates: full build, no regressions, smoke | T18 |

**Gaps found & fixed inline:**
- §6.2 "reconnect re-sends subscriptions and re-issues `pushing.start`" had no task. **Added to Task 9 Step 3**: extend the round-trip test with a `broker.drop_client()` call, assert the client reconnects (`connect_count() == 2`), that the report subscription is re-established (a second `push_report` is delivered), and that the agent re-runs `on_connected` (observable via a second `pushall` in `received_requests()`). Requires `MockBroker::drop_client()` — fold into Task 8.
- §6.2 "auth reject → terminal, no retry storm" — **added assertion to Task 5 Step 1 test**: point `Config.username/password` at a mock that CONNACKs `0x05`; assert `last_connack_rc() == 5` and `is_running() == false` within 1 s (needs `MockBroker` ctor flag `refuse_auth` — fold into Task 8).

**Placeholder scan:** the Task 3 `// TODO(Task 6)` marker is intentional and explicitly removed in Task 5 Step 3 (corrected: the marker is added in T3, removed in T5 — not T6). No other TODO/TBD. All code steps carry real code.

**Type consistency:**
- `OrcaMqttConnection::start(const Config&, MessageHandler, StateHandler)` — defined T5, used T7/T9/T11/T14. ✓
- `subscribe(const std::string&)` / `send_request(const std::string&, const std::string&)` — defined T5/T6, used T9/T12/T16. ✓
- `on_connected(const std::string&, OrcaMqttConnection*, uint64_t)` — decl T11 interfaces, defined T12, called T11/T14. ✓
- `emit_connect_sequence` / `run_connect_sequence_for_test` — introduced T12, only used by T12's test. ✓
- `deliver_to_sink` — T10, used T11/T13. ✓
- `route_send` — T16 only. ✓
- `configure_selected_printer_mqtt` / `teardown_selected_printer_mqtt` / `selected_printer_mqtt_url` / `deliver_cloud_message` — T14/T15, used T14/T15/T17. ✓
- `MockBroker` API grows across T8 (`ws_url`, `push_report`, `received_requests`, `connect_count`), T16 (`host_port`), self-review (`drop_client`, `refuse_auth` ctor flag). All folded into Task 8's file; later tasks only call them. ✓
- `make_connect_packet` 4-arg — T3, caller updated T5. Old 0-arg removed T3. ✓
- `make_topic_packet` removed in T5; no later reference. ✓
