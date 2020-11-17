
/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020 Redis Labs
 *
 * RedisRaft is dual licensed under the GNU Affero General Public License version 3
 * (AGPLv3) or the Redis Source Available License (RSAL).
 */

#include <time.h>

#include "redisraft.h"
#include "hiredis/adapters/libuv.h"

#include <assert.h>

#define CONN_LOG(level, conn, fmt, ...) \
    LOG(level, "{conn:%lu} " fmt, conn ? conn->id : 0, ##__VA_ARGS__)

#define CONN_LOG_ERROR(conn, fmt, ...) CONN_LOG(LOGLEVEL_ERROR, conn, fmt, ##__VA_ARGS__)
#define CONN_LOG_INFO(conn, fmt, ...) CONN_LOG(LOGLEVEL_INFO, conn, fmt, ##__VA_ARGS__)
#define CONN_LOG_VERBOSE(conn, fmt, ...) CONN_LOG(LOGLEVEL_VERBOSE, conn, fmt, ##__VA_ARGS__)
#define CONN_LOG_DEBUG(conn, fmt, ...) CONN_LOG(LOGLEVEL_DEBUG, conn, fmt, ##__VA_ARGS__)

#ifdef ENABLE_TRACE
#define CONN_TRACE(conn, fmt, ...) \
    LOG(LOGLEVEL_DEBUG, "%s:%d {conn:%d} " fmt, \
            __FILE__, __LINE__, \
            (conn) ? (conn)->id : 0, \
            ##__VA_ARGS__)
#else
#define CONN_TRACE(conn, fmt, ...) do {} while (0)
#endif

typedef enum ConnState {
    CONN_DISCONNECTED,
    CONN_RESOLVING,
    CONN_CONNECTING,
    CONN_CONNECTED,
    CONN_CONNECT_ERROR
} ConnState;

static const char *ConnStateStr[] = {
    "disconnected",
    "resolving",
    "connecting",
    "connected",
    "connect_error"
};

typedef enum ConnFlags {
    CONN_TERMINATING    = 1 << 0
} ConnFlags;

typedef enum ConnCallbackStatus {
    CONN_CALLBACK_CONNECTED,
    CONN_CALLBACK_CONNECT_FAILED,
    CONN_CALLBACK_
} ConnCallbackStatus;

struct Connection;  /* Forward declaration */

/* A connection represents a single outgoing Redis connection, such as the
 * one used to communicate with another node.
 *
 * Essentially it is a wrapper around a hiredis asyncRedisContext, providing
 * additional capabilities such as handling asynchronous DNS resolution,
 * dropped connections and re-connects, etc.
 */

typedef struct Connection {
    unsigned long id;
    ConnState state;
    ConnFlags flags; 
    NodeAddr addr;
    char ipaddr[INET6_ADDRSTRLEN+1];    /* Node's resolved IP */
    redisAsyncContext *rc;              /* hiredis async context */
    uv_getaddrinfo_t uv_resolver;       /* libuv resolver context */
    RedisRaftCtx *rr;                   /* Pointer back to redis_raft */
    long long last_connected_time;      /* Last connection time */
    unsigned int connect_oks;           /* Successful connects */
    unsigned int connect_errors;        /* Connection errors since last connection */
    void *privdata;

    ConnectionCallbackFunc connect_callback;
    ConnectionCallbackFunc idle_callback;

    /* Linkage to global connections list */
    LIST_ENTRY(Connection) entries;
} Connection;

/* A list of all connections */
static LIST_HEAD(conn_list, Connection) conn_list = LIST_HEAD_INITIALIZER(conn_list);

/* Create a new connection.
 *
 * The new connection is created in an idle state, so if it has an idle
 * callback it should be called shortly after.
 */

Connection *ConnCreate(RedisRaftCtx *rr, void *privdata, ConnectionCallbackFunc idle_cb)
{
    static long long id = 0;

    Connection *conn = RedisModule_Calloc(1, sizeof(Connection));

    LIST_INSERT_HEAD(&conn_list, conn, entries);
    conn->rr = rr;
    conn->privdata = privdata;
    conn->idle_callback = idle_cb;
    conn->id = ++id;

    CONN_TRACE(conn, "Connection created.");

    return conn;
}

/* Free a connection object.
 *
 * FIXME: When shoukd it be called?
 */
static void ConnFree(Connection *conn)
{
    if (!conn) {
        return;
    }

    CONN_TRACE(conn, "Connection freed.");

    /* FIXME: What about rc? */
    LIST_REMOVE(conn, entries);
    RedisModule_Free(conn);
}

/* A callback we register on hiredis to make sure ConnFree gets called when
 * the async context is freed. FIXME: Do we need it?
 */
static void connDataCleanupCallback(void *privdata)
{
    Connection *conn = (Connection *) privdata;

    CONN_TRACE(conn, "connDataCleanupCallback: flags=%d, rc=%p\n",
         conn ? conn->flags : 0,
         conn ? conn->rc : NULL);

    /* If we got called hiredis is tearing down the context, make sure
     * we drop the reference to it.
     */
    conn->rc = NULL;

    /* If connection was not flagged for async termination, don't clean it up. It
     * may get reused or cleaned up at a later stage.
     */
    if (!(conn->flags & CONN_TERMINATING)) {
        return;
    }

    ConnFree(conn);
}

/* Request async termination of a connection. The connection will be closed and
 * in the next iteration.
 */

void ConnAsyncTerminate(Connection *conn)
{
    CONN_TRACE(conn, "ConnAsyncTerminate called.");

    conn->flags |= CONN_TERMINATING;
}

/* Connect callback (hiredis).
 *
 * This callback will always be called as a result of a prior redisAsyncConnect()
 * from handleResolved(), with state CONN_CONNECTING.
 *
 * Depending on status, state transitions to CONN_CONNECTED or CONN_CONNECT_ERROR.
 * User callback is called in both cases.
 */

static void handleConnected(const redisAsyncContext *c, int status)
{
    Connection *conn = (Connection *) c->data;

    CONN_TRACE(conn, "handleConnected: status=%d\n", status);

    if (status == REDIS_OK) {
        conn->state = CONN_CONNECTED;
        conn->connect_oks++;
        conn->last_connected_time = RedisModule_Milliseconds();
    } else {
        conn->state = CONN_CONNECT_ERROR;
        conn->rc = NULL;
        conn->connect_errors++;
    }

    /* If connection was flagged for termination between connection attempt
     * and now, we don't call the connect callback.
     */
    /* If we're terminating, abort now */
    if (conn->flags & CONN_TERMINATING) {
        return;
    }

    /* Call explicit connect callback (even if failed) */
    if (conn->connect_callback) {
        conn->connect_callback(conn);
    }
}

/* Disconnect callback (hiredis).
 *
 */
static void handleDisconnected(const redisAsyncContext *c, int status)
{
    Connection *conn = (Connection *) c->data;

    CONN_TRACE(conn, "handleDisconnected: rc=%p\n",
        conn ? conn->rc : NULL);

    if (conn) {
        conn->state = CONN_DISCONNECTED;
        conn->rc = NULL;    /* FIXME: Need this? */
    }
}

/* Callback for uv_getaddrinfo.
 */
static void handleResolved(uv_getaddrinfo_t *resolver, int status, struct addrinfo *res)
{
    Connection *conn = uv_req_get_data((uv_req_t *)resolver);

    CONN_TRACE(conn, "handleResolved: flags=%d, state=%s, rc=%p\n",
        conn->flags,
        ConnStateStr[conn->state],
        conn->rc);

    /* If flagged for terminated in the meanwhile, drop now.
     */
    if (conn->flags & CONN_TERMINATING) {
        conn->state = CONN_DISCONNECTED;
        uv_freeaddrinfo(res);
        return;
    }

    if (status < 0) {
        CONN_LOG_ERROR(conn, "Failed to resolve '%s': %s\n", conn->addr.host, uv_strerror(status));
        conn->state = CONN_CONNECT_ERROR;
        conn->connect_errors++;
        uv_freeaddrinfo(res);
        return;
    }

    uv_ip4_name((struct sockaddr_in *) res->ai_addr, conn->ipaddr, sizeof(conn->ipaddr)-1);
    uv_freeaddrinfo(res);

    /* Initiate connection */
    if (conn->rc != NULL) {
        redisAsyncFree(conn->rc);
    }
    conn->rc = redisAsyncConnect(conn->ipaddr, conn->addr.port);
    if (conn->rc->err) {
        conn->state = CONN_CONNECT_ERROR;
        conn->connect_errors++;

        redisAsyncFree(conn->rc);
        conn->rc = NULL;
        return;
    }

    conn->rc->data = conn;
    conn->rc->dataCleanup = connDataCleanupCallback;
    conn->state = CONN_CONNECTING;
    conn->flags &= ~CONN_TERMINATING;

    redisLibuvAttach(conn->rc, conn->rr->loop);
    redisAsyncSetConnectCallback(conn->rc, handleConnected);
    redisAsyncSetDisconnectCallback(conn->rc, handleDisconnected);
}

RRStatus ConnConnect(Connection *conn, const NodeAddr *addr, ConnectionCallbackFunc connect_callback)
{
    struct addrinfo hints = {
        .ai_family = PF_INET,
        .ai_socktype = SOCK_STREAM,
        .ai_protocol = IPPROTO_TCP,
        .ai_flags = 0
    };

    CONN_TRACE(conn, "ConnConnect: connecting %s:%u.", addr->host, addr->port);

    assert(ConnIsIdle(conn));

    conn->addr = *addr;

    conn->state = CONN_RESOLVING;
    conn->connect_callback = connect_callback;
    uv_req_set_data((uv_req_t *)&conn->uv_resolver, conn);
    int r = uv_getaddrinfo(conn->rr->loop, &conn->uv_resolver, handleResolved,
            conn->addr.host, NULL, &hints);
    if (r) {
        conn->state = CONN_CONNECT_ERROR;
        return RR_ERROR;
    }

    return RR_OK;
}

/* An idle state is one that will not transition automatically to another
 * state, unless actively mutated.
 */

bool ConnIsIdle(Connection *conn)
{
    return (conn->state == CONN_DISCONNECTED || conn->state == CONN_CONNECT_ERROR);
}

bool ConnIsConnected(Connection *conn)
{
    return (conn->state == CONN_CONNECTED && !(conn->flags & CONN_TERMINATING));
}

void *ConnGetPrivateData(Connection *conn)
{
    return conn->privdata;
}

RedisRaftCtx *ConnGetRedisRaftCtx(Connection *conn)
{
    return conn->rr;
}

redisAsyncContext *ConnGetRedisCtx(Connection *conn)
{
    return conn->rc;
}

void HandleIdleConnections(RedisRaftCtx *rr)
{
    if (rr->state == REDIS_RAFT_LOADING)
        return;

    Connection *conn, *tmp;
    LIST_FOREACH_SAFE(conn, &conn_list, entries, tmp) {
        /* Idle connections are either terminating and should be reaped,
         * or waiting for an idle callback which can re-connect them.
         */
        if (ConnIsIdle(conn)) {
            if (conn->flags & CONN_TERMINATING) {
                LIST_REMOVE(conn, entries);
                if (conn->rc) {
                    /* Note: redisAsyncFree will call the connDataCleanupCallback
                     * callback which will free the Conn structure!
                     */
                    redisAsyncContext *ac = conn->rc;
                    conn->rc = NULL;
                    redisAsyncFree(ac);
                } else {
                    ConnFree(conn);
                }
            } else {
                if (conn->idle_callback) {
                    conn->idle_callback(conn);
                }
            }
        }
    }
}

