#include "nexus_core.h"
#include <string.h>

#if defined(_WIN32)
#include <windows.h>
#else
#include <sys/time.h>
#endif

static uint16_t be16(const uint8_t *p) {
    return ((uint16_t)p[0] << 8) | (uint16_t)p[1];
}

static uint32_t be32(const uint8_t *p) {
    return ((uint32_t)p[0] << 24) |
           ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8) |
           (uint32_t)p[3];
}

static void put16(uint8_t *p, uint16_t v) {
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)v;
}

static void put32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

uint8_t nexus_core_channel_for_type(uint8_t frame_type) {
    switch (frame_type) {
    case NEXUS_CORE_TYPE_VIDEO:
        return NEXUS_CORE_CHANNEL_VIDEO;
    case NEXUS_CORE_TYPE_INPUT:
        return NEXUS_CORE_CHANNEL_INPUT;
    case NEXUS_CORE_TYPE_KNOCK:
    case NEXUS_CORE_TYPE_KEEPALIVE:
    case NEXUS_CORE_TYPE_PATH_CHALLENGE:
    case NEXUS_CORE_TYPE_PATH_RESPONSE:
        return NEXUS_CORE_CHANNEL_PROBE;
    default:
        return NEXUS_CORE_CHANNEL_CONTROL;
    }
}

uint16_t nexus_core_flags_for_type(uint8_t frame_type) {
    switch (frame_type) {
    case NEXUS_CORE_TYPE_VIDEO:
        return NEXUS_CORE_FLAG_FRAGMENT;
    case NEXUS_CORE_TYPE_INPUT:
    case NEXUS_CORE_TYPE_SYNC_KEYFRAME:
    case NEXUS_CORE_TYPE_SIGNAL:
        return NEXUS_CORE_FLAG_ACK_ELICITING | NEXUS_CORE_FLAG_RELIABLE;
    case NEXUS_CORE_TYPE_KNOCK:
    case NEXUS_CORE_TYPE_KEEPALIVE:
    case NEXUS_CORE_TYPE_PATH_CHALLENGE:
        return NEXUS_CORE_FLAG_ACK_ELICITING | NEXUS_CORE_FLAG_PROBE;
    case NEXUS_CORE_TYPE_PATH_RESPONSE:
        return NEXUS_CORE_FLAG_PROBE;
    default:
        return 0;
    }
}

uint32_t nexus_core_now_ms(void) {
#if defined(_WIN32)
    return (uint32_t)GetTickCount64();
#else
    struct timeval tv;
    if (gettimeofday(&tv, NULL) != 0) {
        return 0;
    }
    return (uint32_t)(((uint64_t)tv.tv_sec * 1000u) + ((uint64_t)tv.tv_usec / 1000u));
#endif
}

uint32_t nexus_core_session_stream_id(const uint8_t *data, size_t len, int is_operator) {
    uint64_t number = 0;
    int has_digit = 0;
    uint32_t hash = 0x811c9dc5u;

    if (!data) {
        return is_operator ? 1u : 0u;
    }

    for (size_t i = 0; i < len; i++) {
        uint8_t b = data[i];
        hash ^= (uint32_t)b;
        hash *= 0x01000193u;

        if (b >= '0' && b <= '9') {
            has_digit = 1;
            number = (number * 10u) + (uint64_t)(b - '0');
            number &= 0xffffffffu;
        }
    }

    uint32_t base = (has_digit ? (uint32_t)number : hash) & 0xfffffffeu;
    return is_operator ? base + 1u : base;
}

int nexus_core_decode(const uint8_t *data, size_t len, nexus_core_header *out) {
    if (!data || !out || len < NEXUS_CORE_HEADER_LEN) {
        return -1;
    }
    if (data[0] != 'N' || data[1] != 'X') {
        return -2;
    }
    if (data[2] != NEXUS_CORE_VERSION) {
        return -3;
    }

    uint8_t header_len = data[6];
    uint16_t payload_len = be16(data + 28);
    if (header_len < NEXUS_CORE_HEADER_LEN || len < (size_t)header_len + payload_len) {
        return -4;
    }

    out->version = data[2];
    out->frame_type = data[3];
    out->flags = be16(data + 4);
    out->header_len = header_len;
    out->channel_id = data[7];
    out->stream_id = be32(data + 8);
    out->seq_num = be32(data + 12);
    out->ack_num = be32(data + 16);
    out->ack_bits = be32(data + 20);
    out->timestamp_ms = be32(data + 24);
    out->payload_len = payload_len;
    out->path_id = be16(data + 30);
    return 0;
}

int nexus_core_encode(const nexus_core_header *header, uint8_t *out, size_t len) {
    if (!header || !out || len < NEXUS_CORE_HEADER_LEN) {
        return -1;
    }

    out[0] = 'N';
    out[1] = 'X';
    out[2] = NEXUS_CORE_VERSION;
    out[3] = header->frame_type;
    put16(out + 4, header->flags);
    out[6] = NEXUS_CORE_HEADER_LEN;
    out[7] = header->channel_id;
    put32(out + 8, header->stream_id);
    put32(out + 12, header->seq_num);
    put32(out + 16, header->ack_num);
    put32(out + 20, header->ack_bits);
    put32(out + 24, header->timestamp_ms);
    put16(out + 28, header->payload_len);
    put16(out + 30, header->path_id);
    return NEXUS_CORE_HEADER_LEN;
}

int nexus_core_pack_frame(uint8_t frame_type,
                          uint32_t stream_id,
                          uint32_t seq_num,
                          uint32_t ack_num,
                          uint32_t ack_bits,
                          uint16_t path_id,
                          const uint8_t *payload,
                          size_t payload_len,
                          uint8_t *out,
                          size_t out_len) {
    if (!out || payload_len > 65535u || out_len < NEXUS_CORE_HEADER_LEN + payload_len) {
        return -1;
    }
    if (payload_len > 0 && !payload) {
        return -2;
    }

    nexus_core_header header;
    header.version = NEXUS_CORE_VERSION;
    header.frame_type = frame_type;
    header.flags = nexus_core_flags_for_type(frame_type);
    header.header_len = NEXUS_CORE_HEADER_LEN;
    header.channel_id = nexus_core_channel_for_type(frame_type);
    header.stream_id = stream_id;
    header.seq_num = seq_num;
    header.ack_num = ack_num;
    header.ack_bits = ack_bits;
    header.timestamp_ms = nexus_core_now_ms();
    header.payload_len = (uint16_t)payload_len;
    header.path_id = path_id;

    int header_len = nexus_core_encode(&header, out, out_len);
    if (header_len < 0) {
        return header_len;
    }
    if (payload_len > 0) {
        memcpy(out + header_len, payload, payload_len);
    }
    return header_len + (int)payload_len;
}

int nexus_core_pack_ack(uint32_t stream_id,
                        uint32_t seq_num,
                        uint32_t ack_num,
                        uint32_t ack_bits,
                        uint8_t *out,
                        size_t out_len) {
    return nexus_core_pack_frame(NEXUS_CORE_TYPE_ACK,
                                 stream_id,
                                 seq_num,
                                 ack_num,
                                 ack_bits,
                                 0,
                                 NULL,
                                 0,
                                 out,
                                 out_len);
}

int nexus_core_chunk_encode(const nexus_core_chunk_header *header, uint8_t *out, size_t len) {
    if (!header || !out || len < NEXUS_CORE_CHUNK_HEADER_LEN) {
        return -1;
    }
    put32(out, header->frame_seq);
    put16(out + 4, header->chunk_idx);
    put16(out + 6, header->total_chunks);
    put32(out + 8, header->frame_len);
    return NEXUS_CORE_CHUNK_HEADER_LEN;
}

int nexus_core_chunk_decode(const uint8_t *data, size_t len, nexus_core_chunk_header *out) {
    if (!data || !out || len < NEXUS_CORE_CHUNK_HEADER_LEN) {
        return -1;
    }
    out->frame_seq = be32(data);
    out->chunk_idx = be16(data + 4);
    out->total_chunks = be16(data + 6);
    out->frame_len = be32(data + 8);
    return 0;
}

void nexus_core_ack_init(nexus_core_ack_window *window) {
    if (!window) {
        return;
    }
    window->newest = 0;
    window->mask = 0;
}

void nexus_core_ack_observe(nexus_core_ack_window *window, uint32_t seq_num) {
    if (!window || seq_num == 0) {
        return;
    }
    if (window->newest == 0) {
        window->newest = seq_num;
        window->mask = 0;
        return;
    }
    if (seq_num > window->newest) {
        uint32_t shift = seq_num - window->newest;
        window->mask = shift >= 32u ? 0u : (window->mask << shift) | (1u << (shift - 1u));
        window->newest = seq_num;
        return;
    }

    uint32_t delta = window->newest - seq_num;
    if (delta > 0 && delta <= 32u) {
        window->mask |= 1u << (delta - 1u);
    }
}

uint32_t nexus_core_ack_num(const nexus_core_ack_window *window) {
    return window ? window->newest : 0;
}

uint32_t nexus_core_ack_bits(const nexus_core_ack_window *window) {
    return window ? window->mask : 0;
}
