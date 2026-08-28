#include "OrcaPrinterAgent.hpp"
#include "NetworkAgentFactory.hpp"
#include "OrcaCloudServiceAgent.hpp"
#include <boost/log/trivial.hpp>
#include <thread>

namespace Slic3r {

const std::string OrcaPrinterAgent_VERSION = "0.0.1";

OrcaPrinterAgent::OrcaPrinterAgent(std::string log_dir) : log_dir(std::move(log_dir))
{
}

OrcaPrinterAgent::~OrcaPrinterAgent() = default;

void OrcaPrinterAgent::set_cloud_agent(std::shared_ptr<ICloudServiceAgent> cloud)
{
    BOOST_LOG_TRIVIAL(info) << "OrcaPrinterAgent::set_cloud_agent: cloud=" << (cloud ? cloud->get_id() : "<null>");
    {
        std::lock_guard<std::mutex> lock(state_mutex);
        m_cloud_agent = cloud;
        m_orca_cloud  = dynamic_cast<OrcaCloudServiceAgent*>(cloud.get());
    }
    if (!m_orca_cloud) {
        BOOST_LOG_TRIVIAL(warning) << "OrcaPrinterAgent::set_cloud_agent: cloud is not OrcaCloudServiceAgent";
        return;   // BBL provider active - nothing to bridge
    }

    // OrcaCloudServiceAgent owns the aggregate MQTT socket; it already strips the
    // device/<id>/report topic and hands us (dev_id, raw_json). Forward to the
    // standard sink. message_arrive_fn self-marshals to the UI thread via CallAfter,
    // so being called from the MQTT worker thread is fine.
    const int callback_result = m_orca_cloud->set_printer_status_callback([this](std::string dev_id, std::string payload) {
        BOOST_LOG_TRIVIAL(info) << "OrcaPrinterAgent: received cloud status dev_id=" << dev_id
                                << " payload_bytes=" << payload.size();
        OnMessageFn fn;
        {
            std::lock_guard<std::mutex> lock(state_mutex);
            fn = on_message_fn;
        }
        if (fn)
            fn(std::move(dev_id), std::move(payload));
        else
            BOOST_LOG_TRIVIAL(warning) << "OrcaPrinterAgent: cloud status has no registered on_message callback";
    });
    BOOST_LOG_TRIVIAL(info) << "OrcaPrinterAgent::set_cloud_agent: status callback result=" << callback_result;
}

// ============================================================================
// Communication - All Stubs
// ============================================================================

int OrcaPrinterAgent::send_message(std::string dev_id, std::string json_str, int qos, int flag)
{
    (void) qos;
    (void) flag;   // MQTT concepts; N/A for the REST command endpoint

    std::shared_ptr<ICloudServiceAgent> cloud;
    {
        std::lock_guard<std::mutex> lock(state_mutex);
        cloud = m_cloud_agent;
    }
    BOOST_LOG_TRIVIAL(info) << "OrcaPrinterAgent::send_message: dev_id=" << dev_id
                            << " payload_bytes=" << json_str.size() << " qos=" << qos << " flag=" << flag
                            << " cloud=" << (cloud ? cloud->get_id() : "<null>");
    if (!cloud || dev_id.empty()) {
        BOOST_LOG_TRIVIAL(warning) << "OrcaPrinterAgent::send_message: rejected due to missing cloud or device ID";
        return BAMBU_NETWORK_ERR_INVALID_HANDLE;
    }

    // Detached worker so the UI thread is never blocked on HTTP. Capture a shared_ptr
    // copy (keeps the cloud agent alive) - never `this`.
    std::thread([cloud, dev_id, body = std::move(json_str)]() {
        if (auto* orca = dynamic_cast<OrcaCloudServiceAgent*>(cloud.get())) {
            const int result = orca->send_printer_command(dev_id, body);
            BOOST_LOG_TRIVIAL(info) << "OrcaPrinterAgent::send_message: cloud command result=" << result
                                    << " dev_id=" << dev_id;
        } else {
            BOOST_LOG_TRIVIAL(error) << "OrcaPrinterAgent::send_message: cloud agent is not OrcaCloudServiceAgent";
        }
    }).detach();

    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::connect_printer(std::string dev_id, std::string dev_ip, std::string username, std::string password, bool use_ssl)
{
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::disconnect_printer()
{
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::send_message_to_printer(std::string dev_id, std::string json_str, int qos, int flag)
{
    return BAMBU_NETWORK_SUCCESS;
}

// ============================================================================
// Certificates - All Stubs
// ============================================================================

int OrcaPrinterAgent::check_cert()
{
    return BAMBU_NETWORK_SUCCESS;
}

void OrcaPrinterAgent::install_device_cert(std::string dev_id, bool lan_only)
{
}

// ============================================================================
// Discovery - Stub
// ============================================================================

bool OrcaPrinterAgent::start_discovery(bool start, bool sending)
{
    return true;
}

// ============================================================================
// Binding - All Stubs
// ============================================================================

int OrcaPrinterAgent::ping_bind(std::string ping_code)
{
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::bind_detect(std::string dev_ip, std::string sec_link, detectResult& detect)
{
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::bind(
    std::string dev_ip, std::string dev_id, std::string dev_model, std::string sec_link, std::string timezone, bool improved, OnUpdateStatusFn update_fn)
{
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::unbind(std::string dev_id)
{
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::request_bind_ticket(std::string* ticket)
{
    if (ticket)
        *ticket = "";
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::get_hms_snapshot(std::string dev_id, std::string file_name, std::function<void(std::string, int)> callback)
{
    // No BBL cloud snapshot source; report failure so the caller falls back.
    (void) dev_id;
    (void) file_name;
    (void) callback;
    return -1;
}

int OrcaPrinterAgent::set_server_callback(OnServerErrFn fn)
{
    std::lock_guard<std::mutex> lock(state_mutex);
    on_server_err_fn = fn;
    return BAMBU_NETWORK_SUCCESS;
}

// ============================================================================
// Machine Selection
// ============================================================================

std::string OrcaPrinterAgent::get_user_selected_machine()
{
    std::lock_guard<std::mutex> lock(state_mutex);
    return selected_machine;
}

int OrcaPrinterAgent::set_user_selected_machine(std::string dev_id)
{
    std::shared_ptr<ICloudServiceAgent> cloud;
    std::string previous;
    {
        std::lock_guard<std::mutex> lock(state_mutex);
        if (dev_id == selected_machine) {
            BOOST_LOG_TRIVIAL(info) << "OrcaPrinterAgent::set_user_selected_machine: unchanged dev_id=" << dev_id;
            return BAMBU_NETWORK_SUCCESS;
        }
        previous         = selected_machine;
        selected_machine = dev_id;
        cloud            = m_cloud_agent;
    }
    BOOST_LOG_TRIVIAL(info) << "OrcaPrinterAgent::set_user_selected_machine: previous=" << previous
                            << " new=" << dev_id << " cloud=" << (cloud ? cloud->get_id() : "<null>");
    if (!cloud) {
        BOOST_LOG_TRIVIAL(warning) << "OrcaPrinterAgent::set_user_selected_machine: no cloud agent";
        return BAMBU_NETWORK_SUCCESS;
    }

    // One report topic at a time. add_subscribe/del_subscribe only mutate a set and
    // wake the MQTT worker, so they are safe to call synchronously on the UI thread.
    if (!previous.empty()) {
        const int result = cloud->del_subscribe({previous});
        BOOST_LOG_TRIVIAL(info) << "OrcaPrinterAgent::set_user_selected_machine: unsubscribe dev_id=" << previous
                                << " result=" << result;
    }
    if (!dev_id.empty()) {
        const int result = cloud->add_subscribe({dev_id});
        BOOST_LOG_TRIVIAL(info) << "OrcaPrinterAgent::set_user_selected_machine: subscribe dev_id=" << dev_id
                                << " result=" << result;
        // Relay retains nothing: ask the printer for a full snapshot. Async inside
        // send_message; returns immediately.
        send_message(dev_id,
            R"({"pushing":{"command":"pushall","sequence_id":"20001","version":1,"push_target":1}})",
            0, 0);
        deliver_mock_get_version(dev_id);
    }
    return BAMBU_NETWORK_SUCCESS;
}

void OrcaPrinterAgent::deliver_mock_get_version(const std::string& dev_id)
{
    // The printer would answer an info.get_version request with its firmware/module
    // list; OrcaCloud does not relay that yet, so MachineObject::module_vers stays
    // empty and is_info_ready(check_version) never passes (StatusPanel bails, every
    // field renders N/A). Synthesize the reply and push it through the same sink as
    // real report messages so parse_json handles it identically. Remove once the
    // backend answers info.get_version on device/<id>/report.
    OnMessageFn fn;
    {
        std::lock_guard<std::mutex> lock(state_mutex);
        fn = on_message_fn;
    }
    if (!fn)
        return;
    static const std::string kMockGetVersion =
        R"({"info":{"command":"get_version","sequence_id":"0","module":[)"
        R"({"name":"ota","product_name":"OrcaCloud Printer","hw_ver":"","sw_ver":"01.00.00.00","sn":""}]}})";
    BOOST_LOG_TRIVIAL(info) << "OrcaPrinterAgent: delivering mock info.get_version for dev_id=" << dev_id;
    fn(dev_id, kMockGetVersion);
}

// ============================================================================
// Agent Information
// ============================================================================
AgentInfo OrcaPrinterAgent::get_agent_info_static()
{
    return AgentInfo{ORCA_PRINTER_AGENT_ID, "Orca", OrcaPrinterAgent_VERSION, "Orca Printer Communication Protocol Agent"};
}

// ============================================================================
// Print Job Operations - All Stubs
// ============================================================================

int OrcaPrinterAgent::start_print(PrintParams params, OnUpdateStatusFn update_fn, WasCancelledFn cancel_fn, OnWaitFn wait_fn)
{
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::start_local_print_with_record(PrintParams      params,
                                                    OnUpdateStatusFn update_fn,
                                                    WasCancelledFn   cancel_fn,
                                                    OnWaitFn         wait_fn)
{
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::start_send_gcode_to_sdcard(PrintParams params, OnUpdateStatusFn update_fn, WasCancelledFn cancel_fn, OnWaitFn wait_fn)
{
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::start_local_print(PrintParams params, OnUpdateStatusFn update_fn, WasCancelledFn cancel_fn)
{
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::start_sdcard_print(PrintParams params, OnUpdateStatusFn update_fn, WasCancelledFn cancel_fn)
{
    return BAMBU_NETWORK_SUCCESS;
}

// ============================================================================
// Callback Registration
// ============================================================================

int OrcaPrinterAgent::set_on_ssdp_msg_fn(OnMsgArrivedFn fn)
{
    std::lock_guard<std::mutex> lock(state_mutex);
    on_ssdp_msg_fn = fn;
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::set_on_printer_connected_fn(OnPrinterConnectedFn fn)
{
    std::lock_guard<std::mutex> lock(state_mutex);
    on_printer_connected_fn = fn;
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::set_on_subscribe_failure_fn(GetSubscribeFailureFn fn)
{
    std::lock_guard<std::mutex> lock(state_mutex);
    on_subscribe_failure_fn = fn;
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::set_on_message_fn(OnMessageFn fn)
{
    std::lock_guard<std::mutex> lock(state_mutex);
    on_message_fn = fn;
    BOOST_LOG_TRIVIAL(info) << "OrcaPrinterAgent::set_on_message_fn: callback=" << (fn ? "set" : "clear");
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::set_on_user_message_fn(OnMessageFn fn)
{
    std::lock_guard<std::mutex> lock(state_mutex);
    on_user_message_fn = fn;
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::set_on_local_connect_fn(OnLocalConnectedFn fn)
{
    std::lock_guard<std::mutex> lock(state_mutex);
    on_local_connect_fn = fn;
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::set_on_local_message_fn(OnMessageFn fn)
{
    std::lock_guard<std::mutex> lock(state_mutex);
    on_local_message_fn = fn;
    return BAMBU_NETWORK_SUCCESS;
}

int OrcaPrinterAgent::set_queue_on_main_fn(QueueOnMainFn fn)
{
    std::lock_guard<std::mutex> lock(state_mutex);
    queue_on_main_fn = fn;
    return BAMBU_NETWORK_SUCCESS;
}

} // namespace Slic3r
