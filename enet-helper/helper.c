/*
 * enet-helper: a real C ENet endpoint used as the oracle in the Rust
 * interop tests (lenet-tokio/tests). Talks raw ENet over a UDP socket so
 * the Rust side only ever sees vanilla ENet wire traffic.
 *
 * Modes:
 *   server <port> [--checksum]      echo server: on CONNECT replies with
 *                                   data "1"; on RECEIVE echoes the packet
 *                                   back on the same channel; on
 *                                   DISCONNECT prints "DISCONNECT".
 *   client <host> <port> <msg>      connect, send reliable msg, print echo
 *                                   (or server events), disconnect with
 *                                   data 42.
 *
 * Prints one line per notable event; exits 0 on success.
 */
#include <enet/enet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Real entry point; called by the Rust main (see src/main.rs). */
int enet_helper_main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: enet-helper server <port> [--checksum]\n"
                        "       enet-helper client <host> <port> <message>\n");
        return 2;
    }
    if (enet_initialize() != 0) {
        fprintf(stderr, "enet_initialize failed\n");
        return 2;
    }

    int checksum = 0;
    if (strcmp(argv[1], "server") == 0) {
        if (argc > 3 && strcmp(argv[3], "--checksum") == 0) checksum = 1;
        ENetAddress addr = { .host = ENET_HOST_ANY, .port = (enet_uint16)atoi(argv[2]) };
        ENetHost *host = enet_host_create(&addr, 16, 2, 0, 0);
        if (!host) { fprintf(stderr, "enet_host_create failed\n"); return 2; }
        if (checksum) host->checksum = enet_crc32;
        printf("READY\n"); fflush(stdout);

        ENetEvent ev;
        int running = 1;
        while (running) {
            if (enet_host_service(host, &ev, 100) <= 0) continue;
            switch (ev.type) {
            case ENET_EVENT_TYPE_CONNECT:
                printf("CONNECT data=%u\n", ev.data); fflush(stdout);
                break;
            case ENET_EVENT_TYPE_RECEIVE: {
                printf("RECEIVE ch=%u len=%u ", ev.channelID,
                       (unsigned)ev.packet->dataLength);
                fwrite(ev.packet->data, 1, ev.packet->dataLength, stdout);
                printf("\n"); fflush(stdout);
                /* echo back, reliable */
                ENetPacket *echo = enet_packet_create(ev.packet->data,
                                                      ev.packet->dataLength,
                                                      ENET_PACKET_FLAG_RELIABLE);
                enet_peer_send(ev.peer, ev.channelID, echo);
                break;
            }
            case ENET_EVENT_TYPE_DISCONNECT:
                printf("DISCONNECT data=%u\n", ev.data); fflush(stdout);
                running = 0;
                break;
            default:
                break;
            }
        }
        enet_host_destroy(host);
        enet_deinitialize();
        printf("SERVER_DONE\n");
        return 0;
    }

    if (strcmp(argv[1], "client") == 0) {
        if (argc < 5) { fprintf(stderr, "client needs <host> <port> <message>\n"); return 2; }
        ENetAddress addr = { .host = ENET_HOST_ANY, .port = 0 };
        ENetHost *host = enet_host_create(&addr, 1, 2, 0, 0);
        if (!host) { fprintf(stderr, "enet_host_create failed\n"); return 2; }
        ENetAddress target;
        enet_address_set_host(&target, argv[2]);
        target.port = (enet_uint16)atoi(argv[3]);
        ENetPeer *peer = enet_host_connect(host, &target, 2, 0);
        if (!peer) { fprintf(stderr, "enet_host_connect failed\n"); return 2; }

        ENetEvent ev;
        int connected = 0, echoed = 0;
        for (;;) {
            int r = enet_host_service(host, &ev, 5000);
            if (r <= 0) {
                fprintf(stderr, "client timeout (connected=%d echoed=%d)\n", connected, echoed);
                return 1;
            }
            if (ev.type == ENET_EVENT_TYPE_CONNECT) {
                printf("CONNECT\n"); fflush(stdout);
                connected = 1;
                ENetPacket *pkt = enet_packet_create(argv[4], strlen(argv[4]),
                                                     ENET_PACKET_FLAG_RELIABLE);
                enet_peer_send(peer, 0, pkt);
            } else if (ev.type == ENET_EVENT_TYPE_RECEIVE) {
                printf("RECEIVE %.*s\n", (int)ev.packet->dataLength,
                       (char *)ev.packet->data);
                fflush(stdout);
                if (connected && !echoed) {
                    echoed = 1;
                    enet_peer_disconnect(peer, 42);
                }
            } else if (ev.type == ENET_EVENT_TYPE_DISCONNECT) {
                printf("DISCONNECT data=%u\n", ev.data); fflush(stdout);
                enet_host_destroy(host);
                enet_deinitialize();
                printf("CLIENT_DONE\n");
                return (connected && echoed) ? 0 : 1;
            }
        }
    }

    fprintf(stderr, "unknown mode: %s\n", argv[1]);
    return 2;
}
