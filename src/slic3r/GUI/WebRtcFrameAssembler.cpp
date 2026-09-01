#include "WebRtcFrameAssembler.hpp"

#include <algorithm>
#include <limits>

namespace Slic3r { namespace GUI {

void WebRtcFrameAssembler::discard()
{
    m_active = false;
    m_frame_id = 0;
    m_chunk_count = 0;
    m_received_chunks = 0;
    m_total_size = 0;
    m_chunks.clear();
    m_received.clear();
}

void WebRtcFrameAssembler::reset()
{
    discard();
}

void WebRtcFrameAssembler::feed(const std::byte* data, std::size_t len)
{
    if (data == nullptr || len < HeaderSize)
        return;

    if (std::to_integer<unsigned int>(data[0]) != 1)
        return;

    const std::uint16_t chunk_index = static_cast<std::uint16_t>((std::to_integer<unsigned int>(data[2]) << 8) |
                                                                  std::to_integer<unsigned int>(data[3]));
    const std::uint16_t chunk_count = static_cast<std::uint16_t>((std::to_integer<unsigned int>(data[4]) << 8) |
                                                                  std::to_integer<unsigned int>(data[5]));
    const std::uint32_t frame_id = (static_cast<std::uint32_t>(std::to_integer<unsigned int>(data[6])) << 24) |
                                   (static_cast<std::uint32_t>(std::to_integer<unsigned int>(data[7])) << 16) |
                                   (static_cast<std::uint32_t>(std::to_integer<unsigned int>(data[8])) << 8) |
                                   static_cast<std::uint32_t>(std::to_integer<unsigned int>(data[9]));
    const std::size_t payload_size = len - HeaderSize;

    if (chunk_count == 0 || chunk_count > MaxChunkCount || chunk_index >= chunk_count ||
        payload_size > MaxChunkPayload)
        return;

    if (!m_active || frame_id > m_frame_id) {
        discard();
        m_active = true;
        m_frame_id = frame_id;
        m_chunk_count = chunk_count;
        m_chunks.resize(chunk_count);
        m_received.assign(chunk_count, false);
    } else if (frame_id < m_frame_id) {
        return;
    } else if (chunk_count != m_chunk_count) {
        discard();
        return;
    }

    Frame& chunk = m_chunks[chunk_index];
    if (m_received[chunk_index])
        return;

    if (m_total_size > MaxFrameSize || payload_size > MaxFrameSize - m_total_size) {
        discard();
        return;
    }

    chunk.assign(data + HeaderSize, data + len);
    m_received[chunk_index] = true;
    m_total_size += payload_size;
    ++m_received_chunks;

    if (m_received_chunks != m_chunk_count)
        return;

    Frame frame;
    frame.reserve(m_total_size);
    for (const Frame& slice : m_chunks)
        frame.insert(frame.end(), slice.begin(), slice.end());
    if (on_frame)
        on_frame(std::move(frame));
    discard();
}

}} // namespace Slic3r::GUI
