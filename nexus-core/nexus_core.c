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

uint8_t nexus_core_lane_for_kind(uint8_t kind) {
    switch (kind) {
    case NEXUS_CORE_KIND_MEDIA:
        return NEXUS_CORE_LANE_MEDIA;
    case NEXUS_CORE_KIND_ACTION:
        return NEXUS_CORE_LANE_ACTION;
    case NEXUS_CORE_KIND_HELLO:
    case NEXUS_CORE_KIND_PULSE:
    case NEXUS_CORE_KIND_TRACE:
    case NEXUS_CORE_KIND_TRACE_REPLY:
        return NEXUS_CORE_LANE_TRACE;
    default:
        return NEXUS_CORE_LANE_COMMAND;
    }
}

uint16_t nexus_core_flags_for_kind(uint8_t kind) {
    switch (kind) {
    case NEXUS_CORE_KIND_MEDIA:
        return NEXUS_CORE_FLAG_PARTIAL;
    case NEXUS_CORE_KIND_ACTION:
    case NEXUS_CORE_KIND_REFRESH:
    case NEXUS_CORE_KIND_CONTROL:
        return NEXUS_CORE_FLAG_RECEIPT_WANTED | NEXUS_CORE_FLAG_ORDERED;
    case NEXUS_CORE_KIND_HELLO:
    case NEXUS_CORE_KIND_PULSE:
    case NEXUS_CORE_KIND_TRACE:
        return NEXUS_CORE_FLAG_RECEIPT_WANTED | NEXUS_CORE_FLAG_TRACE;
    case NEXUS_CORE_KIND_TRACE_REPLY:
        return NEXUS_CORE_FLAG_TRACE;
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

uint32_t nexus_core_pair_stream_id(uint32_t stream_id) {
    return stream_id ^ 1u;
}

int nexus_core_route_action(uint8_t kind) {
    switch (kind) {
    case NEXUS_CORE_KIND_HELLO:
        return NEXUS_CORE_ROUTE_HELLO_REPLY;
    case NEXUS_CORE_KIND_CONTROL:
    case NEXUS_CORE_KIND_MEDIA:
    case NEXUS_CORE_KIND_ACTION:
    case NEXUS_CORE_KIND_PULSE:
    case NEXUS_CORE_KIND_REFRESH:
    case NEXUS_CORE_KIND_RECEIPT:
    case NEXUS_CORE_KIND_GAP:
    case NEXUS_CORE_KIND_TRACE:
    case NEXUS_CORE_KIND_TRACE_REPLY:
        return NEXUS_CORE_ROUTE_RELAY;
    default:
        return NEXUS_CORE_ROUTE_DROP;
    }
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
    uint16_t body_len = be16(data + 28);
    if (header_len < NEXUS_CORE_HEADER_LEN || len < (size_t)header_len + body_len) {
        return -4;
    }

    out->version = data[2];
    out->kind = data[3];
    out->flags = be16(data + 4);
    out->header_len = header_len;
    out->lane_id = data[7];
    out->stream_id = be32(data + 8);
    out->seq_num = be32(data + 12);
    out->receipt_num = be32(data + 16);
    out->receipt_bits = be32(data + 20);
    out->timestamp_ms = be32(data + 24);
    out->body_len = body_len;
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
    out[3] = header->kind;
    put16(out + 4, header->flags);
    out[6] = NEXUS_CORE_HEADER_LEN;
    out[7] = header->lane_id;
    put32(out + 8, header->stream_id);
    put32(out + 12, header->seq_num);
    put32(out + 16, header->receipt_num);
    put32(out + 20, header->receipt_bits);
    put32(out + 24, header->timestamp_ms);
    put16(out + 28, header->body_len);
    put16(out + 30, header->path_id);
    return NEXUS_CORE_HEADER_LEN;
}

int nexus_core_pack_frame(uint8_t kind,
                          uint32_t stream_id,
                          uint32_t seq_num,
                          uint32_t receipt_num,
                          uint32_t receipt_bits,
                          uint16_t path_id,
                          const uint8_t *body,
                          size_t body_len,
                          uint8_t *out,
                          size_t out_len) {
    if (!out || body_len > 65535u || out_len < NEXUS_CORE_HEADER_LEN + body_len) {
        return -1;
    }
    if (body_len > 0 && !body) {
        return -2;
    }

    nexus_core_header header;
    header.version = NEXUS_CORE_VERSION;
    header.kind = kind;
    header.flags = nexus_core_flags_for_kind(kind);
    header.header_len = NEXUS_CORE_HEADER_LEN;
    header.lane_id = nexus_core_lane_for_kind(kind);
    header.stream_id = stream_id;
    header.seq_num = seq_num;
    header.receipt_num = receipt_num;
    header.receipt_bits = receipt_bits;
    header.timestamp_ms = nexus_core_now_ms();
    header.body_len = (uint16_t)body_len;
    header.path_id = path_id;

    int header_len = nexus_core_encode(&header, out, out_len);
    if (header_len < 0) {
        return header_len;
    }
    if (body_len > 0) {
        memcpy(out + header_len, body, body_len);
    }
    return header_len + (int)body_len;
}

int nexus_core_pack_receipt(uint32_t stream_id,
                        uint32_t seq_num,
                        uint32_t receipt_num,
                        uint32_t receipt_bits,
                        uint8_t *out,
                        size_t out_len) {
    return nexus_core_pack_frame(NEXUS_CORE_KIND_RECEIPT,
                                stream_id,
                                seq_num,
                                receipt_num,
                                receipt_bits,
                                0,
                                NULL,
                                0,
                                out,
                                out_len);
}

int nexus_core_pack_hello_reply(const nexus_core_header *request, uint8_t *out, size_t out_len) {
    if (!request) {
        return -1;
    }
    return nexus_core_pack_frame(NEXUS_CORE_KIND_HELLO,
                                request->stream_id,
                                request->seq_num,
                                request->seq_num,
                                0,
                                request->path_id,
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

void nexus_core_receipt_init(nexus_core_receipt_window *window) {
    if (!window) {
        return;
    }
    window->newest = 0;
    window->mask = 0;
}

void nexus_core_receipt_observe(nexus_core_receipt_window *window, uint32_t seq_num) {
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

uint32_t nexus_core_receipt_num(const nexus_core_receipt_window *window) {
    return window ? window->newest : 0;
}

uint32_t nexus_core_receipt_bits(const nexus_core_receipt_window *window) {
    return window ? window->mask : 0;
}
