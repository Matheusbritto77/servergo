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
#define NEXUS_CORE_MAX_CHUNK_PAYLOAD 1180

#define NEXUS_CORE_TYPE_KNOCK 0x01
#define NEXUS_CORE_TYPE_SIGNAL 0x02
#define NEXUS_CORE_TYPE_VIDEO 0x03
#define NEXUS_CORE_TYPE_INPUT 0x04
#define NEXUS_CORE_TYPE_KEEPALIVE 0x05
#define NEXUS_CORE_TYPE_SYNC_KEYFRAME 0x06
#define NEXUS_CORE_TYPE_ACK 0x07
#define NEXUS_CORE_TYPE_NACK 0x08
#define NEXUS_CORE_TYPE_PATH_CHALLENGE 0x09
#define NEXUS_CORE_TYPE_PATH_RESPONSE 0x0A

#define NEXUS_CORE_FLAG_ACK_ELICITING (1u << 0)
#define NEXUS_CORE_FLAG_RELIABLE (1u << 1)
#define NEXUS_CORE_FLAG_FRAGMENT (1u << 2)
#define NEXUS_CORE_FLAG_KEYFRAME (1u << 3)
#define NEXUS_CORE_FLAG_PROBE (1u << 4)
#define NEXUS_CORE_FLAG_DIRECT_PATH (1u << 5)

#define NEXUS_CORE_CHANNEL_CONTROL 0
#define NEXUS_CORE_CHANNEL_VIDEO 1
#define NEXUS_CORE_CHANNEL_INPUT 2
#define NEXUS_CORE_CHANNEL_PROBE 3

typedef struct nexus_core_header {
    uint8_t version;
    uint8_t frame_type;
    uint16_t flags;
    uint8_t header_len;
    uint8_t channel_id;
    uint32_t stream_id;
    uint32_t seq_num;
    uint32_t ack_num;
    uint32_t ack_bits;
    uint32_t timestamp_ms;
    uint16_t payload_len;
    uint16_t path_id;
} nexus_core_header;

typedef struct nexus_core_chunk_header {
    uint32_t frame_seq;
    uint16_t chunk_idx;
    uint16_t total_chunks;
    uint32_t frame_len;
} nexus_core_chunk_header;

typedef struct nexus_core_ack_window {
    uint32_t newest;
    uint32_t mask;
} nexus_core_ack_window;

uint8_t nexus_core_channel_for_type(uint8_t frame_type);
uint16_t nexus_core_flags_for_type(uint8_t frame_type);
uint32_t nexus_core_now_ms(void);
uint32_t nexus_core_session_stream_id(const uint8_t *data, size_t len, int is_operator);

int nexus_core_decode(const uint8_t *data, size_t len, nexus_core_header *out);
int nexus_core_encode(const nexus_core_header *header, uint8_t *out, size_t len);
int nexus_core_pack_frame(uint8_t frame_type,
                          uint32_t stream_id,
                          uint32_t seq_num,
                          uint32_t ack_num,
                          uint32_t ack_bits,
                          uint16_t path_id,
                          const uint8_t *payload,
                          size_t payload_len,
                          uint8_t *out,
                          size_t out_len);
int nexus_core_pack_ack(uint32_t stream_id,
                        uint32_t seq_num,
                        uint32_t ack_num,
                        uint32_t ack_bits,
                        uint8_t *out,
                        size_t out_len);

int nexus_core_chunk_encode(const nexus_core_chunk_header *header, uint8_t *out, size_t len);
int nexus_core_chunk_decode(const uint8_t *data, size_t len, nexus_core_chunk_header *out);

void nexus_core_ack_init(nexus_core_ack_window *window);
void nexus_core_ack_observe(nexus_core_ack_window *window, uint32_t seq_num);
uint32_t nexus_core_ack_num(const nexus_core_ack_window *window);
uint32_t nexus_core_ack_bits(const nexus_core_ack_window *window);

#ifdef __cplusplus
}
#endif

#endif
