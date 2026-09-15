#ifndef NEXUS_CORE_H
#define NEXUS_CORE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define NEXUS_CORE_VERSION 1
#define NEXUS_CORE_HEADER_LEN 32
#define NEXUS_CORE_CHUNK_HEADER_LEN 12
#define NEXUS_CORE_MAX_CHUNK_BODY 1180

#define NEXUS_CORE_KIND_HELLO 0x01
#define NEXUS_CORE_KIND_CONTROL 0x02
#define NEXUS_CORE_KIND_MEDIA 0x03
#define NEXUS_CORE_KIND_ACTION 0x04
#define NEXUS_CORE_KIND_PULSE 0x05
#define NEXUS_CORE_KIND_REFRESH 0x06
#define NEXUS_CORE_KIND_RECEIPT 0x07
#define NEXUS_CORE_KIND_GAP 0x08
#define NEXUS_CORE_KIND_TRACE 0x09
#define NEXUS_CORE_KIND_TRACE_REPLY 0x0A
#define NEXUS_CORE_KIND_P2P_PROBE 0x0B
#define NEXUS_CORE_KIND_P2P_PUNCH 0x0C

#define NEXUS_CORE_FLAG_RECEIPT_WANTED (1u << 0)
#define NEXUS_CORE_FLAG_ORDERED (1u << 1)
#define NEXUS_CORE_FLAG_PARTIAL (1u << 2)
#define NEXUS_CORE_FLAG_ANCHOR (1u << 3)
#define NEXUS_CORE_FLAG_TRACE (1u << 4)
#define NEXUS_CORE_FLAG_DIRECT (1u << 5)
#define NEXUS_CORE_FLAG_ENCRYPTED (1u << 6)

#define NEXUS_CORE_LANE_COMMAND 0
#define NEXUS_CORE_LANE_MEDIA 1
#define NEXUS_CORE_LANE_ACTION 2
#define NEXUS_CORE_LANE_TRACE 3

#define NEXUS_CORE_ROUTE_DROP 0
#define NEXUS_CORE_ROUTE_HELLO_REPLY 1
#define NEXUS_CORE_ROUTE_RELAY 2

typedef struct nexus_core_header {
    uint8_t version;
    uint8_t kind;
    uint16_t flags;
    uint8_t header_len;
    uint8_t lane_id;
    uint32_t stream_id;
    uint32_t seq_num;
    uint32_t receipt_num;
    uint32_t receipt_bits;
    uint32_t timestamp_ms;
    uint16_t body_len;
    uint16_t path_id;
} nexus_core_header;

typedef struct nexus_core_chunk_header {
    uint32_t frame_seq;
    uint16_t chunk_idx;
    uint16_t total_chunks;
    uint32_t frame_len;
} nexus_core_chunk_header;

typedef struct nexus_core_receipt_window {
    uint32_t newest;
    uint32_t mask;
} nexus_core_receipt_window;

uint8_t nexus_core_lane_for_kind(uint8_t kind);
uint16_t nexus_core_flags_for_kind(uint8_t kind);
uint32_t nexus_core_now_ms(void);
uint32_t nexus_core_session_stream_id(const uint8_t *data, size_t len, int is_operator);
uint32_t nexus_core_pair_stream_id(uint32_t stream_id);
int nexus_core_route_action(uint8_t kind);

int nexus_core_decode(const uint8_t *data, size_t len, nexus_core_header *out);
int nexus_core_encode(const nexus_core_header *header, uint8_t *out, size_t len);
int nexus_core_pack_frame(uint8_t kind,
                          uint32_t stream_id,
                          uint32_t seq_num,
                          uint32_t receipt_num,
                          uint32_t receipt_bits,
                          uint16_t path_id,
                          const uint8_t *body,
                          size_t body_len,
                          uint8_t *out,
                          size_t out_len);
int nexus_core_pack_receipt(uint32_t stream_id,
                        uint32_t seq_num,
                        uint32_t receipt_num,
                        uint32_t receipt_bits,
                        uint8_t *out,
                        size_t out_len);
int nexus_core_pack_hello_reply(const nexus_core_header *request, uint8_t *out, size_t out_len);
int nexus_core_pack_trace_reply(uint32_t stream_id, uint32_t seq_num, uint32_t ip, uint16_t port, uint8_t *out, size_t out_len);
int nexus_core_pack_p2p_probe(uint32_t stream_id, uint32_t token, uint8_t *out, size_t out_len);
int nexus_core_pack_p2p_punch(uint32_t stream_id, uint32_t token, uint8_t *out, size_t out_len);

int nexus_core_chunk_encode(const nexus_core_chunk_header *header, uint8_t *out, size_t len);
int nexus_core_chunk_decode(const uint8_t *data, size_t len, nexus_core_chunk_header *out);

void nexus_core_receipt_init(nexus_core_receipt_window *window);
void nexus_core_receipt_observe(nexus_core_receipt_window *window, uint32_t seq_num);
uint32_t nexus_core_receipt_num(const nexus_core_receipt_window *window);
uint32_t nexus_core_receipt_bits(const nexus_core_receipt_window *window);

uint32_t nexus_core_estimate_rtt(uint32_t send_ts, uint32_t now_ts);
float nexus_core_estimate_loss(uint32_t receipt_bits);
int nexus_core_encrypt_payload(const uint8_t *key, size_t key_len, uint32_t nonce, const uint8_t *in, size_t in_len, uint8_t *out);
int nexus_core_decrypt_payload(const uint8_t *key, size_t key_len, uint32_t nonce, const uint8_t *in, size_t in_len, uint8_t *out);

#ifdef __cplusplus
}
#endif

#endif
