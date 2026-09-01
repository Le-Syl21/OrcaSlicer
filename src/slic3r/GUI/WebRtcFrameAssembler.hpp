#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

namespace Slic3r { namespace GUI {

class WebRtcFrameAssembler {
public:
    using Frame = std::vector<std::byte>;

    // The wire protocol uses a 10-byte header followed by one JPEG slice:
    // version, flags, chunk index, chunk count, and frame id, all in network
    // byte order where applicable.
    static constexpr std::size_t HeaderSize = 10;
    static constexpr std::size_t MaxChunkPayload = 16000;
    static constexpr std::size_t MaxChunkCount = 4096;
    static constexpr std::size_t MaxFrameSize = 8 * 1024 * 1024;

    std::function<void(Frame)> on_frame;

    void feed(const std::byte* data, std::size_t len);
    void reset();

private:
    void discard();

    bool m_active = false;
    std::uint32_t m_frame_id = 0;
    std::uint16_t m_chunk_count = 0;
    std::size_t m_received_chunks = 0;
    std::size_t m_total_size = 0;
    std::vector<Frame> m_chunks;
    std::vector<bool> m_received;
};

}} // namespace Slic3r::GUI
