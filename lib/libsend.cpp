#include <pthread.h>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdint>
#include <cassert>
#include <unistd.h>
#include <poll.h>
#include <sys/timerfd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <algorithm>
#include <queue>

#include "lib.h"
#include "utils.h"
#include "protocol.h"

using namespace std;

/* Shared state (defined in libcommon.cpp) */
extern std::map<int, struct connection *> cons;

extern struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
extern struct pollfd timer_fds[MAX_CONNECTIONS];
extern int fdmax;

/* Global sender parameters */
static int sender_speed_mbps = 1;
static int sender_delay_ms = 0;
static int sender_window_segments = 1;

#define MAX_RING_SIZE 16384

struct InelCircular {
    send_segment segmente[MAX_RING_SIZE];
    int cap = 0;
    int coada = 0;
    int dimensiune = 0;
};

static std::map<int, InelCircular> inele_buffer;
static std::map<int, uint16_t> ultimul_ack;
static std::map<int, int> duplicate_ack;

struct PachetTx {
    int sockfd;
    char data[MAX_SEGMENT_SIZE];
    size_t len;
    struct sockaddr_in dest;
};

static std::queue<PachetTx> coada_io;
static pthread_mutex_t io_mtx = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t io_cond = PTHREAD_COND_INITIALIZER;

void* thread_io_worker(void* arg) {
    (void)arg;
    while (1) {
        pthread_mutex_lock(&io_mtx);
        while (coada_io.empty()) {
            pthread_cond_wait(&io_cond, &io_mtx);
        }
        PachetTx p = coada_io.front();
        coada_io.pop();
        pthread_mutex_unlock(&io_mtx);

        sendto(p.sockfd, p.data, p.len, 0, (struct sockaddr *)&p.dest, sizeof(p.dest));
    }
    return NULL;
}

struct GardianMutex {
    pthread_mutex_t* mutex_ptr;
    GardianMutex(pthread_mutex_t* m) : mutex_ptr(m) { pthread_mutex_lock(mutex_ptr); }
    ~GardianMutex() { pthread_mutex_unlock(mutex_ptr); }
};

static struct connection *aloca_conexiune()
{
    struct connection *noua_conexiune = new connection();
    assert(noua_conexiune);

    noua_conexiune->sockfd = -1;
    noua_conexiune->conn_id = 0;
    noua_conexiune->max_window_seq = 1;

    noua_conexiune->send_base = 0;
    noua_conexiune->next_seq = 0;
    noua_conexiune->remote_recv_window = 65535;
    noua_conexiune->sender_buffer_bytes = 0;
    noua_conexiune->sender_buffer_limit = 0;
    noua_conexiune->speed_mbps = 0;
    noua_conexiune->delay_ms = 0;

    noua_conexiune->recv_base = 0;
    noua_conexiune->recv_buffer_limit = 0;
    noua_conexiune->recv_buffer_bytes = 0;

    noua_conexiune->established = 0;
    noua_conexiune->timer_fd_index = -1;

    memset(&noua_conexiune->servaddr, 0, sizeof(noua_conexiune->servaddr));
    pthread_mutex_init(&noua_conexiune->con_lock, NULL);

    return noua_conexiune;
}

static int calculeaza_fereastra(int viteza_mbps, int intarziere_ms)
{
    double timp_rtt_sec               = (2.0 * intarziere_ms) / 1000.0;
    double latime_banda_Bps           = viteza_mbps * 1000000.0 / 8.0;
    int    produs_intarziere_banda_octeti = (int)(latime_banda_Bps * timp_rtt_sec);

    int fereastra_segmente =
        (int)ceil((double)produs_intarziere_banda_octeti / MAX_DATA_SIZE);

    if (fereastra_segmente < 24)
        fereastra_segmente = 24;
    if (fereastra_segmente > 512)
        fereastra_segmente = 512;

    return fereastra_segmente;
}

static void trimite_segment_date(struct connection *conexiune, send_segment &segment_curent)
{
    char pachet_retea[MAX_SEGMENT_SIZE];

    poli_tcp_data_hdr antet_date;
    antet_date.protocol_id = POLI_PROTOCOL_ID;
    antet_date.conn_id     = (uint8_t)conexiune->conn_id;
    antet_date.type        = TYPE_DATA;
    antet_date.seq_num     = htons(segment_curent.seq);
    antet_date.len         = htons((uint16_t)segment_curent.len);

    memcpy(pachet_retea, &antet_date, sizeof(antet_date));
    memcpy(pachet_retea + sizeof(antet_date), segment_curent.data, segment_curent.len);

    pthread_mutex_lock(&io_mtx);
    PachetTx p;
    p.sockfd = conexiune->sockfd;
    memcpy(p.data, pachet_retea, sizeof(antet_date) + segment_curent.len);
    p.len = sizeof(antet_date) + segment_curent.len;
    p.dest = conexiune->servaddr;
    coada_io.push(p);
    pthread_cond_signal(&io_cond);
    pthread_mutex_unlock(&io_mtx);
}

static void incearca_trimitere_segmente_noi(struct connection *conexiune)
{
    if (!conexiune->established)
        return;

    int segmente_destinatie = conexiune->remote_recv_window / MAX_DATA_SIZE;
    if (segmente_destinatie <= 0 && conexiune->remote_recv_window > 0)
        segmente_destinatie = 1;

    int fereastra_efectiva = std::min(conexiune->max_window_seq, segmente_destinatie);

    if (fereastra_efectiva <= 0)
        return;

    int segmente_in_zbor = 0;
    int octeti_in_zbor = 0;

    InelCircular &inel = inele_buffer[conexiune->conn_id];
    int idx = inel.cap;

    for (int i = 0; i < inel.dimensiune; ++i) {
        send_segment &segment_curent = inel.segmente[idx];
        if (segment_curent.sent) {
            segmente_in_zbor++;
            octeti_in_zbor += segment_curent.len;
        } else {
            if (segmente_in_zbor >= fereastra_efectiva)
                break;
            if (octeti_in_zbor + segment_curent.len > conexiune->remote_recv_window)
                break;

            trimite_segment_date(conexiune, segment_curent);
            segment_curent.sent = true;
            segmente_in_zbor++;
            octeti_in_zbor += segment_curent.len;
        }
        idx = (idx + 1) % MAX_RING_SIZE;
    }
}

static void proceseaza_confirmare(struct connection *conexiune, poli_tcp_ctrl_hdr *antet_control)
{
    uint16_t numar_confirmare   = ntohs(antet_control->ack_num);
    uint16_t fereastra_receptie = ntohs(antet_control->recv_window);

    conexiune->remote_recv_window = fereastra_receptie;

    InelCircular &inel = inele_buffer[conexiune->conn_id];

    if (numar_confirmare == ultimul_ack[conexiune->conn_id]) {
        duplicate_ack[conexiune->conn_id]++;
        if (duplicate_ack[conexiune->conn_id] == 3) {
            if (inel.dimensiune > 0) {
                trimite_segment_date(conexiune, inel.segmente[inel.cap]);
            }
        }
    } else {
        ultimul_ack[conexiune->conn_id] = numar_confirmare;
        duplicate_ack[conexiune->conn_id] = 0;
    }

    while (inel.dimensiune > 0 && inel.segmente[inel.cap].seq < numar_confirmare) {
        conexiune->sender_buffer_bytes -= inel.segmente[inel.cap].len;
        inel.cap = (inel.cap + 1) % MAX_RING_SIZE;
        inel.dimensiune--;
    }

    conexiune->send_base = numar_confirmare;
}

static void proceseaza_expirare_timp(struct connection *conexiune)
{
    int segmente_retransmise = 0;
    InelCircular &inel = inele_buffer[conexiune->conn_id];
    int idx = inel.cap;

    for (int i = 0; i < inel.dimensiune; ++i) {
        send_segment &segment_curent = inel.segmente[idx];
        if (segment_curent.sent) {
            trimite_segment_date(conexiune, segment_curent);
            segmente_retransmise++;
        }
        
        if (segmente_retransmise >= conexiune->max_window_seq * 2)
            break;
        idx = (idx + 1) % MAX_RING_SIZE;
    }
}

static void trimite_segmente_toate_conexiunile()
{
    for (auto const& [id, conexiune_curenta] : cons) {
        GardianMutex guard(&conexiune_curenta->con_lock);
        incearca_trimitere_segmente_noi(conexiune_curenta);
    }
}

static void proceseaza_pachet_primit(int rezultat_receptie, char *buffer_pachet, int id_conexiune)
{
    if (rezultat_receptie == -14)
        return;

    auto it = cons.find(id_conexiune);
    if (id_conexiune < 0 || it == cons.end())
        return;

    struct connection *conexiune_curenta = it->second;
    GardianMutex guard(&conexiune_curenta->con_lock);

    if (rezultat_receptie == -1) {
        proceseaza_expirare_timp(conexiune_curenta);
    } else if (rezultat_receptie >= (int)sizeof(poli_tcp_ctrl_hdr)) {
        poli_tcp_ctrl_hdr *antet_pachet = (poli_tcp_ctrl_hdr *)buffer_pachet;

        if (antet_pachet->protocol_id == POLI_PROTOCOL_ID &&
            antet_pachet->type == TYPE_ACK) {
            proceseaza_confirmare(conexiune_curenta, antet_pachet);
        }
    }

    incearca_trimitere_segmente_noi(conexiune_curenta);
}

void *sender_handler(void *arg)
{
    (void)arg;
    char buffer_pachet[MAX_SEGMENT_SIZE];

    while (1) {
        trimite_segmente_toate_conexiunile();

        if (cons.empty()) {
            usleep(1000);
            continue;
        }

        int id_conexiune = -1;
        int rezultat_receptie;

        rezultat_receptie = recv_message_or_timeout(buffer_pachet, MAX_SEGMENT_SIZE, &id_conexiune);

        proceseaza_pachet_primit(rezultat_receptie, buffer_pachet, id_conexiune);
    }

    return NULL;
}

int send_data(int conn_id, char *buffer, int len)
{
    auto it = cons.find(conn_id);
    if (it == cons.end())
        return -1;

    struct connection *con = it->second;
    GardianMutex guard(&con->con_lock);

    int available = con->sender_buffer_limit - con->sender_buffer_bytes;

    if (available < len) {
        return -1;
    }

    InelCircular &inel = inele_buffer[con->conn_id];
    int accepted = 0;

    while (accepted < len) {
        int chunk = std::min((int)MAX_DATA_SIZE, len - accepted);

        if (inel.dimensiune >= MAX_RING_SIZE) {
            break;
        }

        send_segment &seg = inel.segmente[inel.coada];
        seg.seq   = con->next_seq++;
        seg.len   = chunk;
        seg.sent  = false;
        seg.acked = false;
        memcpy(seg.data, buffer + accepted, chunk);

        inel.coada = (inel.coada + 1) % MAX_RING_SIZE;
        inel.dimensiune++;

        con->sender_buffer_bytes += chunk;
        accepted += chunk;
    }

    return accepted;
}

static void configureaza_socket_initial(struct connection *con, uint32_t ip, uint16_t port)
{
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    assert(con->sockfd >= 0);

    struct timeval tv;
    tv.tv_sec  = 0;
    tv.tv_usec = 100000; /* 100 ms */
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    memset(&con->servaddr, 0, sizeof(con->servaddr));
    con->servaddr.sin_family      = AF_INET;
    con->servaddr.sin_addr.s_addr = ip;
    con->servaddr.sin_port        = port; 
}

static int executa_strangere_de_mana(struct connection *con)
{
    poli_tcp_ctrl_hdr syn;
    memset(&syn, 0, sizeof(syn));
    syn.protocol_id = POLI_PROTOCOL_ID;
    syn.conn_id     = 0;
    syn.type        = TYPE_SYN;
    syn.ack_num     = htons(0);
    syn.recv_window = htons(0);

    char buffer_retea[MAX_SEGMENT_SIZE];
    int id_conexiune_rezultat = -1;

    while (id_conexiune_rezultat < 0) {
        sendto(con->sockfd, &syn, sizeof(syn), 0,
               (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

        struct sockaddr_in de_la;
        socklen_t lungime_de_la = sizeof(de_la);
        int octeti_primiti = recvfrom(con->sockfd, buffer_retea, sizeof(buffer_retea), 0,
                         (struct sockaddr *)&de_la, &lungime_de_la);

        if (octeti_primiti < (int)(sizeof(poli_tcp_ctrl_hdr) + sizeof(uint16_t)))
            continue;

        poli_tcp_ctrl_hdr *synack = (poli_tcp_ctrl_hdr *)buffer_retea;

        if (synack->protocol_id != POLI_PROTOCOL_ID ||
            synack->type != TYPE_SYN_ACK)
            continue;

        id_conexiune_rezultat = synack->conn_id;

        uint16_t port_date_retea;
        memcpy(&port_date_retea, buffer_retea + sizeof(poli_tcp_ctrl_hdr), sizeof(uint16_t));

        con->servaddr.sin_port = port_date_retea; 
    }

    return id_conexiune_rezultat;
}

static void trimite_confirmari_finale(struct connection *con, int id_conexiune)
{
    poli_tcp_ctrl_hdr ack_pkt;
    memset(&ack_pkt, 0, sizeof(ack_pkt));
    ack_pkt.protocol_id = POLI_PROTOCOL_ID;
    ack_pkt.conn_id     = (uint8_t)id_conexiune;
    ack_pkt.type        = TYPE_ACK;
    ack_pkt.ack_num     = htons(0);
    ack_pkt.recv_window = htons(0);

    for (int i = 0; i < 5; i++) {
        sendto(con->sockfd, &ack_pkt, sizeof(ack_pkt), 0,
               (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
        usleep(1000); 
    }

    struct timeval tv;
    tv.tv_sec  = 0;
    tv.tv_usec = 0;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

static void seteaza_parametri_conexiune(struct connection *con, int id_conexiune)
{
    con->conn_id             = id_conexiune;
    con->send_base           = 0;
    con->next_seq            = 0;
    con->remote_recv_window  = 65535;
    con->sender_buffer_bytes = 0;
    con->max_window_seq      = sender_window_segments;
    con->speed_mbps          = sender_speed_mbps;
    con->delay_ms            = sender_delay_ms;
    con->established         = 1;

    int limita_buffer = sender_window_segments * MAX_DATA_SIZE * 16;
    if (limita_buffer < 1024 * 1024)
        limita_buffer = 1024 * 1024;

    con->sender_buffer_limit = limita_buffer;
}

static void inregistreaza_descriptori(struct connection *con, int id_conexiune)
{
    int index_curent = fdmax;

    data_fds[index_curent].fd     = con->sockfd;
    data_fds[index_curent].events = POLLIN;

    timer_fds[index_curent].fd    = timerfd_create(CLOCK_REALTIME, 0);
    timer_fds[index_curent].events = POLLIN;
    assert(timer_fds[index_curent].fd >= 0);

    int timeout_ms = 35;
    struct itimerspec spec;
    spec.it_value.tv_sec     = timeout_ms / 1000;
    spec.it_value.tv_nsec    = (timeout_ms % 1000) * 1000000L;
    spec.it_interval.tv_sec  = timeout_ms / 1000;
    spec.it_interval.tv_nsec = (timeout_ms % 1000) * 1000000L;
    timerfd_settime(timer_fds[index_curent].fd, 0, &spec, NULL);

    con->timer_fd_index = index_curent;
    fdmax++;

    cons.insert({id_conexiune, con});
}

int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    struct connection *con = aloca_conexiune();

    /* // This can be used to set a timer on a socket 
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } */

    /* We will send the SYN on 8031. Then we will receive a SYN-ACK with the connection
     * port. We can use con->sockfd for both cases, but we will need to update server_addr
     * with the port received via SYN-ACK */

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    configureaza_socket_initial(con, ip, port);

    int conn_id = executa_strangere_de_mana(con);

    trimite_confirmari_finale(con, conn_id);

    seteaza_parametri_conexiune(con, conn_id);

    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on our connection */
    inregistreaza_descriptori(con, conn_id);

    DEBUG_PRINT("Connection established! conn_id=%d\n", conn_id);
    return conn_id;
}


void init_sender(int speed, int delay)
{
    sender_speed_mbps = speed;
    sender_delay_ms = delay;
    sender_window_segments = calculeaza_fereastra(speed, delay);

    DEBUG_PRINT("Initializare sender: viteza=%d Mbps, delay=%d ms, window=%d segments\n",
                speed, delay, sender_window_segments);

    pthread_t primul_thread;
    int result = pthread_create(&primul_thread, NULL, sender_handler, NULL);
    assert(result == 0);

    pthread_t io_thread;
    int result_io = pthread_create(&io_thread, NULL, thread_io_worker, NULL);
    assert(result_io == 0);
}