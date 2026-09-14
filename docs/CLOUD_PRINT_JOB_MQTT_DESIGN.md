# Cloud print job: MQTT-native design (not yet implemented)

`OrcaPrinterAgent::start_print` currently finalizes a cloud print job over HTTP
(`OrcaCloudServiceAgent::start_cloud_print_job`, `POST
/api/v1/printers/<dev_id>/print-jobs/<job_id>/start`). This document records
the MQTT-native alternative that was designed as the intended replacement, the
gap that blocks it today, and why it should not be built by adding fields to
`print.gcode_file`.

## Current (implemented) flow

1. `OrcaCloudServiceAgent::upload_gcode_via_cloud`
   - `POST print-jobs/uploads` -> `{job_id, upload_url, expires_at, max_bytes}`
   - `PUT upload_url` -> raw G-code straight to R2 (presigned, PUT-only, no
     bearer token; must not go through the `http_put` helper, which always
     prefixes `api_base_url` and attaches the cloud session's Authorization
     header).
2. `OrcaCloudServiceAgent::start_cloud_print_job`
   - `POST print-jobs/<job_id>/start` with `{filename, start}`.
   - The gateway HEAD-verifies the R2 object landed, mints a short-lived
     signed *download* URL (`createPrintJobDownloadToken` in
     `apps/gateway/src/services/printer-print-jobs.ts`), and relays a
     `print.project_file` command carrying that URL to the printer -
     **over the gateway's own relay connection to OrcaSonar, not
     OrcaSlicer's MQTT session.** OrcaSonar's `executeProjectFile`
     (`internal/cloud/printfile.go`) downloads from that URL and starts the
     print.

This works today and requires no changes to OrcaCloud or OrcaSonar.

## Why an MQTT-native version is desirable

The HTTP finalize call means `start_print`'s cloud path depends on the
client's REST session (auth token, network path to the gateway's HTTP API)
in addition to its MQTT session. An MQTT-native version would let OrcaSlicer
trigger the download-and-start entirely over the connection it already
maintains for every other printer command.

## The key finding: OrcaSonar already supports this, transport-agnostically

`print.project_file` is not tied to the HTTP `/start` route. OrcaSonar's
cloud MQTT message handler intercepts it purely by command name, before
routing to the generic per-namespace dispatcher:

```go
// internal/cloud/client.go, makeRequestHandler
if cmd.Namespace == "print" && cmd.Name == "project_file" {
    go c.handleProjectFile(ctx, cmd)   // downloads `url`, then starts if start=true
    return
}
```

This fires for **any** message that reaches OrcaSonar's own outbound cloud
MQTT session on `device/<dev_id>/request` - regardless of whether it was
published there by the gateway's internal relay (today's `/start` path) or
by a client publishing directly. OrcaSlicer's cloud MQTT connection already
publishes to that same topic for every other cloud command
(`OrcaPrinterAgent::route_send(is_lan=false, ...)` - `set_bed_temp`,
`ams_change_filament`, etc.), via the same relay-shard mechanism
(`apps/gateway/src/lib/relay-router.ts`). So **OrcaSlicer publishing
`print.project_file` itself, over its existing cloud MQTT connection, would
already reach `handleProjectFile` and trigger the identical download-then-
start behavior - with zero new code on OrcaSonar.**

(This only works over the cloud relay session. OrcaSonar's LAN-side/generic
dispatcher, `internal/bridge/klipper/adapter.go`, treats `print.project_file`
as an unmapped macro call - a no-op in practice. That's fine: R2 upload is a
cloud-only feature to begin with.)

## The one real gap: no GET-signed download URL is exposed to the client

`handleProjectFile` needs a URL it can `GET`. The `upload_url` returned by
`POST print-jobs/uploads` is presigned for `PUT` only - S3 SigV4 signatures
are bound to the HTTP method, so it cannot be reused for a download.

Minting a download URL/token already exists as a function
(`createPrintJobDownloadToken` in
`apps/gateway/src/services/printer-print-jobs.ts`) and the exact URL
template is already built in `dispatchPrintFileCommand`
(`apps/gateway/src/routes/printer.ts`) - it is simply never returned to the
API caller today, only used server-side when `/start` builds the relay
payload itself.

**Required OrcaCloud change:** have `POST print-jobs/uploads` (or a small
follow-up call) also mint and return a signed download URL alongside
`upload_url`, reusing `createPrintJobDownloadToken` + the existing URL
template. This is on the order of ~10 lines in an existing handler, not a new
permission model - the caller is already an authenticated, authorized user of
that printer, identically to who is authorized to call `/start` today.

No OrcaSonar change is required at all.

## Why this should NOT be built into `print.gcode_file` / `start_sdcard_print`

`print.gcode_file` (sent by `OrcaPrinterAgent::start_sdcard_print`) is the
generic "start this file that is already on the printer" primitive. It is
used by the LAN `start_local_print` path today, is meant to stay usable for
starting any file already on the SD card by filename alone, and is expected
to grow parameters unrelated to cloud upload (e.g. filament mapping) over
time.

Making `gcode_file` download-aware would require either:
- adding cloud-specific fields (`job_id`, a download `url`, ...) to a command
  that has nothing to do with cloud jobs in the LAN case, forcing all of them
  to be optional/unused most of the time, or
- giving OrcaSonar a side-channel registry of "filenames currently being
  downloaded" that `gcode_file`'s handler consults - solvable, but an
  orthogonal change with its own design questions (see "decoupled two-step
  option" below).

Neither is necessary: `print.project_file` already exists as a fully
separate, fully-working command for exactly the "not yet on the printer,
fetch it first" case, so the cloud upload flow does not need to touch
`gcode_file` at all.

## Intended MQTT-native design, once the gap above is closed

Replace the HTTP finalize step (`start_cloud_print_job`) with: build and
publish, over the cloud MQTT connection (`route_send(is_lan=false, ...)`),

```json
{"print": {"command": "project_file", "sequence_id": "...",
           "url": "<signed download URL from the uploads response>",
           "param": "<remote_gcode_name(params)>",
           "start": true}}
```

`start_sdcard_print` / `print.gcode_file` remains untouched and fully
decoupled.

### Decoupled two-step option

If a use case ever needs "download now, start later" as an explicit user
action (rather than upload-and-immediately-print), the same
`print.project_file` command already supports it via `start: false` (download
and store only - see `executeProjectFile`'s `start` handling in
`internal/cloud/printfile.go`). The later "start" action would then be a
perfectly ordinary `print.gcode_file` with `param: <filename>`, going through
the existing, generic `start_sdcard_print` unmodified. This still requires no
protocol changes beyond the download-URL gap above.

## Summary of gaps

| Component | Change needed |
|---|---|
| OrcaCloud (gateway) | Return a signed download URL from `POST print-jobs/uploads` (or a small sibling endpoint), reusing existing `createPrintJobDownloadToken` logic. |
| OrcaSonar | None. `print.project_file` handling already does exactly what's needed, transport-agnostically, on the cloud MQTT session. |
| OrcaSlicer (this repo) | Once the above lands: replace `start_cloud_print_job`'s HTTP call with a `print.project_file` publish over the cloud MQTT connection. `start_sdcard_print` stays untouched either way. |
