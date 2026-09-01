#include "WebRtcMediaController.hpp"

#include "AVVideoDecoder.hpp"

#include <rtc/common.hpp>
#include <rtc/rtc.hpp>

#include <mutex>
#include <variant>
#include <wx/mstream.h>

#include <boost/log/trivial.hpp>

#include <cstring>

namespace {
void init_rtc_logger_once()
{
    static std::once_flag flag;
    std::call_once(flag, [] {
        rtc::InitLogger(rtc::LogLevel::Verbose, [](rtc::LogLevel level, std::string message) {
            BOOST_LOG_TRIVIAL(info) << "[rtc:" << static_cast<int>(level) << "] " << message;
        });
    });
}
} // namespace

extern "C" {
#include <libavcodec/avcodec.h>
}

namespace Slic3r { namespace GUI {

WebRtcMediaController::WebRtcMediaController(std::function<void(const wxImage&, wxSize)> frame_sink,
                                             std::function<void(Status)> on_status)
    : m_frame_sink(std::move(frame_sink))
    , m_on_status(std::move(on_status))
{
}

WebRtcMediaController::~WebRtcMediaController()
{
    StopSession();
}

void WebRtcMediaController::report(Status status)
{
    status.epoch = m_epoch.load();
    BOOST_LOG_TRIVIAL(info) << "WebRTC: report kind=" << static_cast<int>(status.kind)
                            << " code=" << static_cast<int>(status.code) << " epoch=" << status.epoch;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (status.kind == Status::Connecting)
            m_state = static_cast<wxMediaState>(4);
        else if (status.kind == Status::Playing)
            m_state = wxMEDIASTATE_PLAYING;
        else
            m_state = static_cast<wxMediaState>(3);
    }
    if (m_on_status)
        m_on_status(status);
}

void WebRtcMediaController::StartSession(std::unique_ptr<ICameraSignalingChannel> channel)
{
    // Tear down any previous attempt WITHOUT notifying: the Stopped that would
    // otherwise be delivered (async, via CallAfter) races the new attempt's
    // Connecting and makes the consumer cancel a session that is mid-connect.
    teardown(false);
    if (!channel)
        return;

    m_epoch.fetch_add(1);
    m_alive.store(true);
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_signaling = std::move(channel);
        m_chunk_queue.clear();
        m_nal_queue.clear();
        m_pending_candidates.clear();
        m_remote_description_set = false;
        m_video_size = wxDefaultSize;
        m_has_frame = false;
        m_last_frame_time = {};
    }

    ICameraSignalingChannel* signaling = nullptr;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        signaling = m_signaling.get();
    }
    signaling->on_ready = [this](std::vector<CameraIceServer> servers) {
        if (m_alive.load())
            on_ready(std::move(servers));
    };
    signaling->on_answer = [this](std::string sdp) {
        if (m_alive.load())
            on_answer(std::move(sdp));
    };
    signaling->on_ice = [this](std::string candidate, std::string mid) {
        if (m_alive.load())
            on_ice(std::move(candidate), std::move(mid));
    };
    signaling->on_unavailable = [this](CameraUnavailableReason reason, std::string detail) {
        if (m_alive.load())
            on_unavailable(reason, std::move(detail));
    };

    m_decode_thread = std::thread([this] { decode_loop(); });
    report({Status::Connecting});
    signaling->open();
}

void WebRtcMediaController::StopSession()
{
    teardown(true);
}

void WebRtcMediaController::teardown(bool notify)
{
    const bool was_alive = m_alive.exchange(false);
    if (!was_alive && !m_decode_thread.joinable())
        return;

    m_cond.notify_all();
    std::unique_ptr<ICameraSignalingChannel> signaling;
    std::shared_ptr<rtc::PeerConnection> peer_connection;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        signaling = std::move(m_signaling);
        peer_connection = std::move(m_peer_connection);
        m_data_channel.reset();
        m_video_track.reset();
    }
    if (signaling)
        signaling->close();
    if (peer_connection)
        peer_connection->close();
    if (m_decode_thread.joinable())
        m_decode_thread.join();
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_chunk_queue.clear();
        m_nal_queue.clear();
    }
    if (was_alive && notify)
        report({Status::Stopped});
}

wxMediaState WebRtcMediaController::GetState()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_state;
}

wxSize WebRtcMediaController::GetVideoSize() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_video_size;
}

void WebRtcMediaController::bind_data_channel(const std::shared_ptr<rtc::DataChannel>& dc)
{
    const std::string label = dc->label();
    dc->onOpen([this, label] { BOOST_LOG_TRIVIAL(info) << "WebRTC: data channel '" << label << "' open"; });
    dc->onClosed([this, label] { BOOST_LOG_TRIVIAL(info) << "WebRTC: data channel '" << label << "' closed"; });
    dc->onError([label](std::string e) {
        BOOST_LOG_TRIVIAL(warning) << "WebRTC: data channel '" << label << "' error: " << e;
    });
    dc->onMessage(
        [this](rtc::binary data) {
            if (m_alive.load())
                enqueue_chunk(std::vector<std::byte>(data.begin(), data.end()));
        },
        [](rtc::string) {});
}

void WebRtcMediaController::on_ready(std::vector<CameraIceServer> servers)
{
    init_rtc_logger_once();
    rtc::Configuration configuration;
    for (const CameraIceServer& server : servers) {
        try {
            rtc::IceServer ice_server(server.urls);
            ice_server.username = server.username;
            ice_server.password = server.credential;
            configuration.iceServers.emplace_back(std::move(ice_server));
        } catch (const std::exception& e) {
            BOOST_LOG_TRIVIAL(warning) << "WebRTC: invalid ICE server: " << e.what();
        }
    }
    configuration.iceServers.emplace_back("stun:stun.cloudflare.com:3478");
    configuration.iceServers.emplace_back("stun:stun.l.google.com:19302");

    auto peer_connection = std::make_shared<rtc::PeerConnection>(std::move(configuration));
    BOOST_LOG_TRIVIAL(info) << "WebRTC: creating peer connection with " << configuration.iceServers.size()
                            << " ice servers";
    peer_connection->onLocalDescription([this](rtc::Description description) {
        if (!m_alive.load())
            return;
        const std::string sdp(description);
        BOOST_LOG_TRIVIAL(info) << "WebRTC: local description ready (" << description.typeString()
                                << "), OFFER SDP:\n" << sdp;
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_signaling)
            m_signaling->send_offer(sdp);
    });
    peer_connection->onLocalCandidate([this](rtc::Candidate candidate) {
        if (!m_alive.load())
            return;
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_signaling)
            m_signaling->send_ice(std::string(candidate), candidate.mid());
    });
    peer_connection->onStateChange([this](rtc::PeerConnection::State state) {
        BOOST_LOG_TRIVIAL(info) << "WebRTC: peer state -> " << static_cast<int>(state);
        if (!m_alive.load())
            return;
        if (state == rtc::PeerConnection::State::Failed || state == rtc::PeerConnection::State::Disconnected)
            report({Status::Failed, Status::ICE_FAILED});
    });
    peer_connection->onGatheringStateChange([](rtc::PeerConnection::GatheringState state) {
        BOOST_LOG_TRIVIAL(info) << "WebRTC: gathering state -> " << static_cast<int>(state);
    });

    // Accept a DataChannel opened by the remote peer (OrcaSonar may create the
    // "camera" channel from its side rather than answering the one we offer).
    peer_connection->onDataChannel([this](std::shared_ptr<rtc::DataChannel> dc) {
        BOOST_LOG_TRIVIAL(info) << "WebRTC: remote opened data channel '" << dc->label() << "'";
        bind_data_channel(dc);
        std::lock_guard<std::mutex> lock(m_mutex);
        m_data_channel = std::move(dc);
    });

    rtc::DataChannelInit init;
    init.reliability.unordered = true;
    init.reliability.maxPacketLifeTime = std::chrono::milliseconds(350);
    auto data_channel = peer_connection->createDataChannel("camera", init);
    if (data_channel)
        bind_data_channel(data_channel);

    // NOTE: the H.264 RTP RecvOnly track is intentionally NOT added to the offer
    // yet. OrcaSonar answers application-only (no m=video), which makes
    // libdatachannel renegotiate and send a second offer that OrcaSonar rejects
    // with "webrtc.unavailable: error". Re-add the video m-line (enqueue_nal /
    // the decode_loop NAL branch are already in place) once OrcaSonar answers it.

    std::shared_ptr<rtc::PeerConnection> peer_for_description;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!m_alive.load())
            return;
        m_peer_connection = std::move(peer_connection);
        peer_for_description = m_peer_connection;
        m_data_channel = std::move(data_channel);
    }
    if (peer_for_description)
        peer_for_description->setLocalDescription();
}

void WebRtcMediaController::on_answer(std::string sdp)
{
    std::shared_ptr<rtc::PeerConnection> peer_connection;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        peer_connection = m_peer_connection;
    }
    BOOST_LOG_TRIVIAL(info) << "WebRTC: applying remote answer (" << sdp.size() << " bytes), ANSWER SDP:\n" << sdp;
    if (!peer_connection)
        return;
    try {
        peer_connection->setRemoteDescription(rtc::Description(sdp, "answer"));
    } catch (const std::exception& e) {
        BOOST_LOG_TRIVIAL(warning) << "WebRTC: setRemoteDescription failed: " << e.what();
        report({Status::Failed, Status::ICE_FAILED});
        return;
    }

    // Flush any remote candidates that arrived before the answer.
    std::vector<std::pair<std::string, std::string>> pending;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_remote_description_set = true;
        pending.swap(m_pending_candidates);
    }
    BOOST_LOG_TRIVIAL(info) << "WebRTC: remote description set, flushing " << pending.size()
                            << " buffered candidate(s)";
    for (const auto& c : pending) {
        try {
            peer_connection->addRemoteCandidate(rtc::Candidate(c.first, c.second));
        } catch (const std::exception& e) {
            BOOST_LOG_TRIVIAL(warning) << "WebRTC: addRemoteCandidate (buffered) failed: " << e.what();
        }
    }
}

void WebRtcMediaController::on_ice(std::string candidate, std::string mid)
{
    std::shared_ptr<rtc::PeerConnection> peer_connection;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!m_remote_description_set) {
            m_pending_candidates.emplace_back(std::move(candidate), std::move(mid));
            return;
        }
        peer_connection = m_peer_connection;
    }
    if (!peer_connection)
        return;
    try {
        peer_connection->addRemoteCandidate(rtc::Candidate(candidate, mid));
    } catch (const std::exception& e) {
        BOOST_LOG_TRIVIAL(warning) << "WebRTC: addRemoteCandidate failed: " << e.what();
    }
}

void WebRtcMediaController::on_unavailable(CameraUnavailableReason reason, std::string detail)
{
    BOOST_LOG_TRIVIAL(warning) << "WebRTC camera unavailable: " << detail;
    Status::Code code = Status::UNAVAILABLE_ERROR;
    if (reason == CameraUnavailableReason::Busy)
        code = Status::UNAVAILABLE_BUSY;
    else if (reason == CameraUnavailableReason::Disabled)
        code = Status::UNAVAILABLE_DISABLED;
    else if (reason == CameraUnavailableReason::Closed)
        code = Status::SIGNALING_CLOSED;
    report({Status::Failed, code});
}

void WebRtcMediaController::enqueue_chunk(std::vector<std::byte> chunk)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_chunk_queue.size() >= 8)
        m_chunk_queue.pop_front();
    m_chunk_queue.emplace_back(std::move(chunk));
    m_cond.notify_one();
}

void WebRtcMediaController::enqueue_nal(std::vector<std::byte> nal)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_nal_queue.size() >= 4)
        m_nal_queue.pop_front();
    m_nal_queue.emplace_back(std::move(nal));
    m_cond.notify_one();
}

void WebRtcMediaController::deliver_jpeg(std::vector<std::byte> jpeg)
{
    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_last_frame_time != std::chrono::steady_clock::time_point{} &&
            now - m_last_frame_time < std::chrono::milliseconds(33))
            return;
        m_last_frame_time = now;
    }
    wxMemoryInputStream stream(jpeg.data(), jpeg.size());
    wxImage image;
    if (!image.LoadFile(stream, wxBITMAP_TYPE_JPEG)) {
        report({Status::Failed, Status::DECODE_ERROR});
        return;
    }
    bool first_frame = false;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_video_size = image.GetSize();
        first_frame = !m_has_frame;
        m_has_frame = true;
    }
    if (m_frame_sink)
        m_frame_sink(image, image.GetSize());
    if (first_frame)
        report({Status::Playing});
}

void WebRtcMediaController::decode_loop()
{
    WebRtcFrameAssembler assembler;
    assembler.on_frame = [this](std::vector<std::byte> jpeg) { deliver_jpeg(std::move(jpeg)); };
    AVCodecParameters parameters{};
    parameters.codec_type = AVMEDIA_TYPE_VIDEO;
    parameters.codec_id = AV_CODEC_ID_H264;
    AVVideoDecoder decoder;
    bool decoder_open = false;

    int stall_polls = 0;
    std::unique_lock<std::mutex> lock(m_mutex);
    while (m_alive.load()) {
        const bool woke = m_cond.wait_for(lock, std::chrono::seconds(2), [this] {
            return !m_alive.load() || !m_chunk_queue.empty() || !m_nal_queue.empty();
        });
        if (!m_alive.load())
            break;
        if (!woke && !m_has_frame) {
            const int pc_state = m_peer_connection ? static_cast<int>(m_peer_connection->state()) : -1;
            std::string dc = "none";
            if (m_data_channel)
                dc = "label='" + m_data_channel->label() + "' open=" +
                     (m_data_channel->isOpen() ? "1" : "0");
            lock.unlock();
            BOOST_LOG_TRIVIAL(info) << "WebRTC: waiting for frames; peer_state=" << pc_state
                                    << " data_channel=" << dc;
            if (++stall_polls >= 8) { // ~16s connected with no frame -> give up so the UI can retry
                report({Status::Failed, Status::TIMEOUT});
                lock.lock();
                break;
            }
            lock.lock();
            continue;
        }
        stall_polls = 0;
        if (!m_chunk_queue.empty()) {
            auto chunk = std::move(m_chunk_queue.front());
            m_chunk_queue.pop_front();
            lock.unlock();
            assembler.feed(chunk.data(), chunk.size());
            lock.lock();
        } else if (!m_nal_queue.empty()) {
            auto nal = std::move(m_nal_queue.front());
            m_nal_queue.pop_front();
            lock.unlock();
            if (!decoder_open)
                decoder_open = decoder.open(parameters) == 0;
            if (decoder_open) {
                AVPacket* packet = av_packet_alloc();
                if (packet && av_new_packet(packet, static_cast<int>(nal.size())) == 0) {
                    std::memcpy(packet->data, nal.data(), nal.size());
                    if (decoder.decode(*packet) == 0) {
                        wxImage image;
                        if (decoder.toWxImage(image, wxDefaultSize)) {
                            {
                                std::lock_guard<std::mutex> frame_lock(m_mutex);
                                m_video_size = image.GetSize();
                            }
                            if (m_frame_sink)
                                m_frame_sink(image, image.GetSize());
                            report({Status::Playing});
                        }
                    }
                }
                av_packet_free(&packet);
            }
            lock.lock();
        }
    }
}

}} // namespace Slic3r::GUI
