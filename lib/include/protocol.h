#pragma once

#include <cstdint>

/* Values used for protocol ID and type. */
#define POLI_PROTOCOL_ID 42

/* Segment types */
#define TYPE_SYN     1
#define TYPE_SYN_ACK 2
#define TYPE_ACK     3
#define TYPE_DATA    4
#define TYPE_FIN     5
#define TYPE_FIN_ACK 6

/* Header for Data segments. Must be used in your implementation as it is. */
struct __attribute__((packed)) poli_tcp_data_hdr {
    uint8_t protocol_id;
    uint8_t conn_id;
    uint8_t type;
    uint16_t seq_num;
    uint16_t len;
};

/* Header for Control segments. Must be used in your implementation as it is. */
struct __attribute__((packed)) poli_tcp_ctrl_hdr {
    uint8_t protocol_id;
    uint8_t conn_id;
    uint8_t type;
    uint16_t ack_num;
    uint16_t recv_window;
};