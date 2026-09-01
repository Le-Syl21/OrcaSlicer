#include <catch2/catch_all.hpp>

#include <slic3r/GUI/WebRtcFrameAssembler.hpp>

#include <cstdint>
#include <cstddef>
#include <initializer_list>
#include <vector>

using Slic3r::GUI::WebRtcFrameAssembler;

static std::vector<std::byte> make_chunk(std::uint32_t frame_id,
                                         std::uint16_t index,
                                         std::uint16_t count,
                                         std::initializer_list<unsigned int> payload)
{
    std::vector<std::byte> result(WebRtcFrameAssembler::HeaderSize + payload.size());
    result[0] = std::byte{1};
    result[1] = std::byte{0};
    result[2] = std::byte{static_cast<unsigned char>(index >> 8)};
    result[3] = std::byte{static_cast<unsigned char>(index)};
    result[4] = std::byte{static_cast<unsigned char>(count >> 8)};
    result[5] = std::byte{static_cast<unsigned char>(count)};
    result[6] = std::byte{static_cast<unsigned char>(frame_id >> 24)};
    result[7] = std::byte{static_cast<unsigned char>(frame_id >> 16)};
    result[8] = std::byte{static_cast<unsigned char>(frame_id >> 8)};
    result[9] = std::byte{static_cast<unsigned char>(frame_id)};
    for (std::size_t i = 0; i < payload.size(); ++i)
        result[WebRtcFrameAssembler::HeaderSize + i] = std::byte{static_cast<unsigned char>(payload.begin()[i])};
    return result;
}

TEST_CASE("WebRTC frame assembler joins chunks in order", "[webrtc][unit]")
{
    WebRtcFrameAssembler assembler;
    std::vector<std::byte> frame;
    assembler.on_frame = [&frame](std::vector<std::byte> value) { frame = std::move(value); };

    const auto second = make_chunk(7, 1, 2, {'C', 'D'});
    const auto first = make_chunk(7, 0, 2, {'A', 'B'});
    assembler.feed(second.data(), second.size());
    assembler.feed(first.data(), first.size());

    REQUIRE(frame.size() == 4);
    CHECK(std::to_integer<char>(frame[0]) == 'A');
    CHECK(std::to_integer<char>(frame[3]) == 'D');
}

TEST_CASE("WebRTC frame assembler discards stale and malformed chunks", "[webrtc][unit]")
{
    WebRtcFrameAssembler assembler;
    int frames = 0;
    assembler.on_frame = [&frames](std::vector<std::byte>) { ++frames; };

    const auto stale = make_chunk(1, 0, 2, {'A'});
    const auto current = make_chunk(2, 0, 1, {'B'});
    const auto stale_tail = make_chunk(1, 1, 2, {'C'});
    assembler.feed(stale.data(), stale.size());
    assembler.feed(current.data(), current.size());
    assembler.feed(stale_tail.data(), stale_tail.size());

    CHECK(frames == 1);

    auto malformed = make_chunk(3, 0, 0, {'X'});
    assembler.feed(malformed.data(), malformed.size());
    CHECK(frames == 1);
}
