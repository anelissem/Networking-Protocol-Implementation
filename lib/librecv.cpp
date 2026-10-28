#include <pthread.h>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <unistd.h>
#include <poll.h>
#include <sys/timerfd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <algorithm>
#include <map>
#include <deque>

#include "lib.h"
#include "utils.h"
#include "protocol.h"

using namespace std;

extern std::map<int, struct connection *> cons;

extern struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
extern struct pollfd timer_fds[MAX_CONNECTIONS];
extern int fdmax;

/* Receiver globals */
static int socket_ascultare = -1;
static int limita_buffer_receptie_global = 0;
static int urmatorul_id_conexiune = 1;

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

static void trimite_confirmare(struct connection *conexiune)
{
    poli_tcp_ctrl_hdr confirmare;
    memset(&confirmare, 0, sizeof(confirmare));

    int spatiu_disponibil = conexiune->recv_buffer_limit - conexiune->recv_buffer_bytes;
    if (spatiu_disponibil < 0)  spatiu_disponibil = 0;
    if (spatiu_disponibil > 65535) spatiu_disponibil = 65535;

    confirmare.protocol_id = POLI_PROTOCOL_ID;
    confirmare.conn_id     = (uint8_t)conexiune->conn_id;
    confirmare.type        = TYPE_ACK;
    confirmare.ack_num     = htons(conexiune->recv_base);
    confirmare.recv_window = htons((uint16_t)spatiu_disponibil);

    sendto(conexiune->sockfd, &confirmare, sizeof(confirmare), 0,
           (struct sockaddr *)&conexiune->servaddr, sizeof(conexiune->servaddr));
}

static void proceseaza_segment_date(struct connection *conexiune, char *pachet_retea, int lungime_pachet)
{
    if (lungime_pachet < (int)sizeof(poli_tcp_data_hdr))
        return;

    poli_tcp_data_hdr *antet = (poli_tcp_data_hdr *)pachet_retea;

    uint16_t numar_secventa = ntohs(antet->seq_num);
    uint16_t lungime_date = ntohs(antet->len);

    if (lungime_date > MAX_DATA_SIZE)
        return;
    if (lungime_pachet < (int)(sizeof(poli_tcp_data_hdr) + lungime_date))
        return;

    if (numar_secventa < conexiune->recv_base) {
        trimite_confirmare(conexiune);
        return;
    }

    int spatiu_disponibil = conexiune->recv_buffer_limit - conexiune->recv_buffer_bytes;

    if (spatiu_disponibil < (int)lungime_date) {
        trimite_confirmare(conexiune);
        return;
    }

    int segmente_fereastra = spatiu_disponibil / MAX_DATA_SIZE;
    if (segmente_fereastra <= 0 && spatiu_disponibil > 0)
        segmente_fereastra = 1;

    if (numar_secventa >= (uint16_t)(conexiune->recv_base + segmente_fereastra)) {
        trimite_confirmare(conexiune);
        return;
    }

    /* Salvăm segmentul nou utilizând std::map pentru simplitate și corectitudine */
    recv_segment seg;
    seg.seq = numar_secventa;
    seg.len = lungime_date;
    memcpy(seg.data, pachet_retea + sizeof(poli_tcp_data_hdr), lungime_date);

    if (conexiune->recv_window.find(numar_secventa) == conexiune->recv_window.end()) {
        conexiune->recv_window[numar_secventa] = seg;
        conexiune->recv_buffer_bytes += lungime_date;
    }

    /* Mutăm pachetele secvențiale în buffer-ul aplicației folosind std::deque */
    while (conexiune->recv_window.count(conexiune->recv_base)) {
        recv_segment &s = conexiune->recv_window[conexiune->recv_base];

        for (int i = 0; i < s.len; i++) {
            conexiune->app_recv_buffer.push_back(s.data[i]);
        }

        conexiune->recv_buffer_bytes -= s.len;
        conexiune->recv_window.erase(conexiune->recv_base);
        conexiune->recv_base++;
    }

    trimite_confirmare(conexiune);
}

void *handle_receiver(void *arg)
{
    (void)arg; 

    char buffer_segment[MAX_SEGMENT_SIZE];

    DEBUG_PRINT("Start receiver handler\n");

    while (1) {
        int id_conexiune = -1;
        int rezultat_receptie;

        do {
            rezultat_receptie = recv_message_or_timeout(buffer_segment, MAX_SEGMENT_SIZE, &id_conexiune);
        } while (rezultat_receptie == -14);
        if (rezultat_receptie <= 0)
            continue;

        if (id_conexiune < 0 || cons.find(id_conexiune) == cons.end())
            continue;

        struct connection *conexiune_curenta = cons[id_conexiune];
        pthread_mutex_lock(&conexiune_curenta->con_lock);

        poli_tcp_data_hdr *antet = (poli_tcp_data_hdr *)buffer_segment;

        if (antet->protocol_id == POLI_PROTOCOL_ID &&
            antet->type == TYPE_DATA) {
            proceseaza_segment_date(conexiune_curenta, buffer_segment, rezultat_receptie);
        }
        /* Handle segment received from the sender. We use this between locks
        as to not have synchronization issues with the recv_data calls which are
        on the main thread */

        pthread_mutex_unlock(&conexiune_curenta->con_lock);
    }

    return NULL;
}

int recv_data(int conn_id, char *buffer, int len)
{
    if (cons.find(conn_id) == cons.end())
        return -1;

    struct connection *conexiune_curenta = cons[conn_id];

    while (1) {
        pthread_mutex_lock(&conexiune_curenta->con_lock);

        int available = conexiune_curenta->app_recv_buffer.size();

        if (available > 0) {
            int to_read = std::min(len, available);

            for (int i = 0; i < to_read; i++) {
                buffer[i] = conexiune_curenta->app_recv_buffer.front();
                conexiune_curenta->app_recv_buffer.pop_front();
            }

            trimite_confirmare(conexiune_curenta);
            /* We will write code here as to not have sync problems with recv_handler */
            pthread_mutex_unlock(&conexiune_curenta->con_lock);
            return to_read;
        }

        pthread_mutex_unlock(&conexiune_curenta->con_lock);
        usleep(1000); 
    }
}

static void asteapta_cerere_syn(struct sockaddr_in &adresa_client, socklen_t &lungime_client)
{
    char buffer_retea[MAX_SEGMENT_SIZE];
    while (1) {
        int octeti_primiti = recvfrom(socket_ascultare, buffer_retea, sizeof(buffer_retea), 0,
                                      (struct sockaddr *)&adresa_client, &lungime_client);
        if (octeti_primiti < (int)sizeof(poli_tcp_ctrl_hdr))
            continue;

        poli_tcp_ctrl_hdr *pachet_syn = (poli_tcp_ctrl_hdr *)buffer_retea;
        if (pachet_syn->protocol_id == POLI_PROTOCOL_ID && pachet_syn->type == TYPE_SYN)
            break;
    }
}

static void creeaza_si_leaga_socket_date(struct connection *conexiune, uint16_t &port_ales)
{
    conexiune->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    assert(conexiune->sockfd >= 0);

    struct sockaddr_in adresa_locala;
    memset(&adresa_locala, 0, sizeof(adresa_locala));
    adresa_locala.sin_family      = AF_INET;
    adresa_locala.sin_addr.s_addr = INADDR_ANY;
    adresa_locala.sin_port        = 0; 
    assert(bind(conexiune->sockfd, (struct sockaddr *)&adresa_locala,
                sizeof(adresa_locala)) == 0);

    socklen_t lungime_adresa = sizeof(adresa_locala);
    getsockname(conexiune->sockfd, (struct sockaddr *)&adresa_locala, &lungime_adresa);
    port_ales = adresa_locala.sin_port; 
}

static void trimite_syn_ack(struct sockaddr_in &adresa_client, socklen_t lungime_client, int id_conexiune, uint16_t port_ales, char* buffer_synack)
{
    poli_tcp_ctrl_hdr *synack = (poli_tcp_ctrl_hdr *)buffer_synack;
    memset(synack, 0, sizeof(poli_tcp_ctrl_hdr));
    synack->protocol_id = POLI_PROTOCOL_ID;
    synack->conn_id     = (uint8_t)id_conexiune;
    synack->type        = TYPE_SYN_ACK;
    synack->ack_num     = htons(0);
    synack->recv_window = htons(0);

    memcpy(buffer_synack + sizeof(poli_tcp_ctrl_hdr), &port_ales, sizeof(uint16_t));

    sendto(socket_ascultare, buffer_synack, sizeof(poli_tcp_ctrl_hdr) + sizeof(uint16_t), 0,
           (struct sockaddr *)&adresa_client, lungime_client);
}

static void asteapta_confirmare_finala(struct connection *conexiune, struct sockaddr_in &adresa_client, socklen_t lungime_client, char* buffer_synack)
{
    struct timeval tv;
    tv.tv_sec  = 0;
    tv.tv_usec = 50000; 
    setsockopt(conexiune->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    char buffer_retea[MAX_SEGMENT_SIZE];
    bool confirmare_primita = false;
    while (!confirmare_primita) {
        struct sockaddr_in de_la;
        socklen_t lungime_de_la = sizeof(de_la);
        int octeti_primiti = recvfrom(conexiune->sockfd, buffer_retea, sizeof(buffer_retea), 0,
                                      (struct sockaddr *)&de_la, &lungime_de_la);

        if (octeti_primiti < (int)sizeof(poli_tcp_ctrl_hdr)) {
            sendto(socket_ascultare, buffer_synack, sizeof(poli_tcp_ctrl_hdr) + sizeof(uint16_t), 0,
                   (struct sockaddr *)&adresa_client, lungime_client);
            continue;
        }

        poli_tcp_ctrl_hdr *pachet_control = (poli_tcp_ctrl_hdr *)buffer_retea;

        if (pachet_control->protocol_id != POLI_PROTOCOL_ID)
            continue;

        if (pachet_control->type == TYPE_ACK) {
            confirmare_primita = true;
        } else if (pachet_control->type == TYPE_DATA) {
            confirmare_primita = true;
            proceseaza_segment_date(conexiune, buffer_retea, octeti_primiti);
        }
    }

    tv.tv_sec  = 0;
    tv.tv_usec = 0;
    setsockopt(conexiune->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

static void inregistreaza_descriptori_receptie(struct connection *conexiune, int id_conexiune)
{
    int index_curent = fdmax;

    data_fds[index_curent].fd     = conexiune->sockfd;
    data_fds[index_curent].events = POLLIN;

    timer_fds[index_curent].fd     = timerfd_create(CLOCK_REALTIME, 0);
    timer_fds[index_curent].events = POLLIN;
    assert(timer_fds[index_curent].fd >= 0);

    struct itimerspec spec;
    spec.it_value.tv_sec     = 1;
    spec.it_value.tv_nsec    = 0;
    spec.it_interval.tv_sec  = 1;
    spec.it_interval.tv_nsec = 0;
    timerfd_settime(timer_fds[index_curent].fd, 0, &spec, NULL);

    conexiune->timer_fd_index = index_curent;
    fdmax++;

    cons.insert({id_conexiune, conexiune});
}

int wait4connect(uint32_t ip, uint16_t port)
{
    /* TODO: Implement the Three Way Handshake on the receiver part. This blocks
     * until a connection is established. */

    (void)ip;   
    (void)port; 

    assert(socket_ascultare >= 0);

    struct sockaddr_in adresa_client;
    socklen_t lungime_client = sizeof(adresa_client);

    asteapta_cerere_syn(adresa_client, lungime_client);

    /* This can be used to set a timer on a socket, useful once we received a
     * SYN. You may want to disable by setting the time to 0 (tv_sec = 0,
     * tv_usec = 0)
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } */

    /* Receive SYN on the connection socket. Create a new socket and bind it to
     * the chosen port. Send the data port number via SYN-ACK to the client */

    struct connection *conexiune_noua = aloca_conexiune();
    int id_conexiune = urmatorul_id_conexiune++;
    conexiune_noua->conn_id = id_conexiune;
    conexiune_noua->servaddr = adresa_client;

    uint16_t port_ales;
    creeaza_si_leaga_socket_date(conexiune_noua, port_ales);

    char buffer_synack[sizeof(poli_tcp_ctrl_hdr) + sizeof(uint16_t)];
    trimite_syn_ack(adresa_client, lungime_client, id_conexiune, port_ales, buffer_synack);

    conexiune_noua->recv_base          = 0;
    conexiune_noua->recv_buffer_limit  = limita_buffer_receptie_global;
    conexiune_noua->recv_buffer_bytes  = 0;
    conexiune_noua->established        = 1;

    asteapta_confirmare_finala(conexiune_noua, adresa_client, lungime_client, buffer_synack);

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on a connection */

    inregistreaza_descriptori_receptie(conexiune_noua, id_conexiune);

    DEBUG_PRINT("Connection accepted! conn_id=%d, data_port=%u\n",
                id_conexiune, ntohs(port_ales));

    return id_conexiune;
}

void init_receiver(int recv_buffer_bytes)
{
    if (recv_buffer_bytes < 128 * 1024) {
        recv_buffer_bytes = 128 * 1024;
    }

    limita_buffer_receptie_global = recv_buffer_bytes;

     /* TODO: Create the connection socket and bind it to 8031 */
    socket_ascultare = socket(AF_INET, SOCK_DGRAM, 0);
    assert(socket_ascultare >= 0);

    int opt = 1;
    setsockopt(socket_ascultare, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in adresa_ascultare;
    memset(&adresa_ascultare, 0, sizeof(adresa_ascultare));
    adresa_ascultare.sin_family      = AF_INET;
    adresa_ascultare.sin_addr.s_addr = INADDR_ANY;
    adresa_ascultare.sin_port        = htons(8032);

    assert(bind(socket_ascultare, (struct sockaddr *)&adresa_ascultare, sizeof(adresa_ascultare)) == 0);

    DEBUG_PRINT("init_receiver: listening on port 8032, buffer=%d bytes\n",
                recv_buffer_bytes);

    pthread_t thread_receptie;
    int ret = pthread_create(&thread_receptie, NULL, handle_receiver, NULL);
    assert(ret == 0);
}