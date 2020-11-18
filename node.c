/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020 Redis Labs
 *
 * RedisRaft is dual licensed under the GNU Affero General Public License version 3
 * (AGPLv3) or the Redis Source Available License (RSAL).
 */

#include <time.h>
#include <string.h>
#include <assert.h>

#include "redisraft.h"

/* State of the RAFT.CLUSTER JOIN operation.
 *
 * The address list is initialized by RAFT.CLUSTER JOIN, but it may grow if RAFT.NODE ADD
 * requests are sent to follower nodes that reply -MOVED.
 *
 * We use a fake Node structure to simplify and reuse connection management code.
 */

static LIST_HEAD(node_list, Node) node_list = LIST_HEAD_INITIALIZER(node_list);

typedef struct JoinState {
    NodeAddrListElement *addr;
    NodeAddrListElement *addr_iter;
    Connection *conn;
} JoinState;

static void clearPendingResponses(Node *node)
{
    node->pending_raft_response_num = 0;
    node->pending_proxy_response_num = 0;

    while (!STAILQ_EMPTY(&node->pending_responses)) {
        PendingResponse *resp = STAILQ_FIRST(&node->pending_responses);
        STAILQ_REMOVE_HEAD(&node->pending_responses, entries);
        RedisModule_Free(resp);
    }
}

static void handleNodeConnect(Connection *conn)
{
    Node *node = (Node *) ConnGetPrivateData(conn);

    if (ConnIsConnected(conn)) {
        clearPendingResponses(node);
        NODE_TRACE(node, "Node connection established.\n");
    }
}

void nodeIdleCallback(Connection *conn)
{
    Node *node = ConnGetPrivateData(conn);
    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);

    raft_node_t *raft_node = raft_get_node(rr->raft, node->id);
    if (raft_node != NULL && raft_node_is_active(raft_node)) {
        ConnConnect(node->conn, &node->addr, handleNodeConnect);
    }
}

void NodeFree(Node *node)
{
    if (!node) {
        return;
    }

    clearPendingResponses(node);

    LIST_REMOVE(node, entries);
    RedisModule_Free(node);
}

static void nodeFreeCallback(void *privdata)
{
    Node *node = (Node *) privdata;
    NodeFree(node);
}

Node *NodeInit(RedisRaftCtx *rr, int id, const NodeAddr *addr)
{
    Node *node = RedisModule_Calloc(1, sizeof(Node));
    STAILQ_INIT(&node->pending_responses);

    node->id = id;
    node->rr = rr;

    strcpy(node->addr.host, addr->host);
    node->addr.port = addr->port;

    LIST_INSERT_HEAD(&node_list, node, entries);
    node->conn = ConnCreate(node->rr, node, nodeIdleCallback, nodeFreeCallback);

    return node;
}

bool NodeAddrParse(const char *node_addr, size_t node_addr_len, NodeAddr *result)
{
    char buf[32] = { 0 };
    char *endptr;
    unsigned long l;

    /* Split */
    const char *colon = node_addr + node_addr_len;
    while (colon > node_addr && *colon != ':') {
        colon--;
    }
    if (*colon != ':') {
        return false;
    }

    /* Get port */
    int portlen = node_addr_len - (colon + 1 - node_addr);
    if (portlen >= sizeof(buf) || portlen < 1) {
        return false;
    }

    strncpy(buf, colon + 1, portlen);
    l = strtoul(buf, &endptr, 10);
    if (*endptr != '\0' || l < 1 || l > 65535) {
        return false;
    }
    result->port = l;

    /* Get addr */
    int addrlen = colon - node_addr;
    if (addrlen >= sizeof(result->host)) {
        addrlen = sizeof(result->host)-1;
    }
    memcpy(result->host, node_addr, addrlen);
    result->host[addrlen] = '\0';

    return true;
}

/* Compare two NodeAddr sructs */
bool NodeAddrEqual(const NodeAddr *a1, const NodeAddr *a2)
{
    return (a1->port == a2->port && !strcmp(a1->host, a2->host));
}

/* Add a NodeAddrListElement to a chain of elements.  If an existing element with the same
 * address already exists, nothing is done.  The addr pointer provided is copied into newly
 * allocated memory, caller should free addr if necessary.
 */
void NodeAddrListAddElement(NodeAddrListElement **head, const NodeAddr *addr)
{
    while (*head != NULL) {
        if (NodeAddrEqual(&(*head)->addr, addr)) {
            return;
        }

        head = &(*head)->next;
    }

    *head = RedisModule_Calloc(1, sizeof(NodeAddrListElement));
    (*head)->addr = *addr;
}

/* Concat a NodeAddrList to another NodeAddrList */
void NodeAddrListConcat(NodeAddrListElement **head, const NodeAddrListElement *other)
{
    const NodeAddrListElement *e = other;

    while (e != NULL) {
        NodeAddrListAddElement(head, &e->addr);
        e = e->next;
    }
}

void NodeAddrListFree(NodeAddrListElement *head)
{
    NodeAddrListElement *t;

    while (head != NULL) {
        t = head->next;
        RedisModule_Free(head);
        head = t;
    }
}

/* Parse a -MOVED reply and update the returned address in addr.
 * Both standard Redis Cluster reply (with the hash slot) or the simplified
 * RedisRaft reply are supported.
 */
static bool parseMovedReply(const char *str, NodeAddr *addr)
{
    /* -MOVED 0 1.1.1.1:1 or -MOVED 1.1.1.1:1 */
    if (strlen(str) < 15 || strncmp(str, "MOVED ", 6))
        return false;

    const char *tok = str + 6;
    const char *tok2;

    /* Handle current or cluster-style -MOVED replies. */
    if ((tok2 = strchr(tok, ' ')) == NULL) {
        return NodeAddrParse(tok, strlen(tok), addr);
    } else {
        return NodeAddrParse(tok2, strlen(tok2), addr);
    }
}

void handleNodeAddResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Connection *conn = privdata;
    JoinState *state = ConnGetPrivateData(conn);
    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);

    redisReply *reply = r;

    if (!reply) {
        LOG_ERROR("RAFT.NODE ADD failed: connection dropped.\n");
    } else if (reply->type == REDIS_REPLY_ERROR) {
        /* -MOVED? */
        if (strlen(reply->str) > 6 && !strncmp(reply->str, "MOVED ", 6)) {
            NodeAddr addr;
            if (!parseMovedReply(reply->str, &addr)) {
                LOG_ERROR("RAFT.NODE ADD failed: invalid MOVED response: %s\n", reply->str);
            } else {
                LOG_VERBOSE("Join redirected to leader: %s:%d\n", addr.host, addr.port);
                NodeAddrListAddElement(&state->addr, &addr);
            }
        } else {
            LOG_ERROR("RAFT.NODE ADD failed: %s\n", reply->str);
        }
    } else if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 2) {
        LOG_ERROR("RAFT.NODE ADD invalid reply.\n");
    } else {
        LOG_INFO("Joined Raft cluster, node id: %lu, dbid: %.*s\n",
                reply->element[0]->integer,
                reply->element[1]->len, reply->element[1]->str);

        strncpy(rr->snapshot_info.dbid, reply->element[1]->str, reply->element[1]->len);
        rr->snapshot_info.dbid[RAFT_DBID_LEN] = '\0';

        rr->config->id = reply->element[0]->integer;

        HandleClusterJoinCompleted(rr);
        assert(rr->state == REDIS_RAFT_UP);

        ConnAsyncTerminate(conn);
    }

    redisAsyncDisconnect(c);
}


void sendNodeAddRequest(Connection *conn)
{
    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);

    /* Connection is not good?  Terminate and continue */
    if (!ConnIsConnected(conn)) {
        return;
    }

    if (redisAsyncCommand(ConnGetRedisCtx(conn), handleNodeAddResponse, conn,
        "RAFT.NODE %s %d %s:%u",
        "ADD",
        rr->config->id,
        rr->config->addr.host, rr->config->addr.port) != REDIS_OK) {

        ConnAsyncTerminate(conn);
    }
}


/* Track a new pending response for a request that was sent to the node.
 * This is used to track connection liveness and decide when it should be
 * dropped.
 */
void NodeAddPendingResponse(Node *node, bool proxy)
{
    static int response_id = 0;

    PendingResponse *resp = RedisModule_Calloc(1, sizeof(PendingResponse));
    resp->proxy = proxy;
    resp->request_time = RedisModule_Milliseconds();
    resp->id = ++response_id;

    if (proxy) {
        node->pending_proxy_response_num++;
    } else {
        node->pending_raft_response_num++;
    }
    STAILQ_INSERT_TAIL(&node->pending_responses, resp, entries);

    NODE_TRACE(node, "NodeAddPendingResponse: id=%d, type=%s, request_time=%lld\n",
            resp->id, proxy ? "proxy" : "raft", resp->request_time);
}

/* Acknowledge a response that has been received and remove it from the
 * node's list of pending responses.
 */
void NodeDismissPendingResponse(Node *node)
{
    PendingResponse *resp = STAILQ_FIRST(&node->pending_responses);
    STAILQ_REMOVE_HEAD(&node->pending_responses, entries);

    if (resp->proxy) {
        node->pending_proxy_response_num--;
    } else {
        node->pending_raft_response_num--;
    }

    NODE_TRACE(node, "NodeDismissPendingResponse: id=%d, type=%s, latency=%lld\n",
            resp->id, resp->proxy ? "proxy" : "raft",
            RedisModule_Milliseconds() - resp->request_time);

    RedisModule_Free(resp);
}

void HandleNodeStates(RedisRaftCtx *rr)
{
    if (rr->state == REDIS_RAFT_LOADING)
        return;

    /* Iterate nodes and find nodes that require reconnection */
    Node *node, *tmp;
    LIST_FOREACH_SAFE(node, &node_list, entries, tmp) {
        if (ConnIsConnected(node->conn) && !STAILQ_EMPTY(&node->pending_responses)) {
            PendingResponse *resp = STAILQ_FIRST(&node->pending_responses);
            long timeout;

            if (raft_is_leader(rr->raft)) {
                timeout = rr->config->raft_response_timeout;
            } else {
                timeout = resp->proxy ? rr->config->proxy_response_timeout : rr->config->raft_response_timeout;
            }

            if (timeout && resp->request_time + timeout < RedisModule_Milliseconds()) {
                NODE_TRACE(node, "Pending %s response timeout expired, reconnecting.\n",
                        resp->proxy ? "proxy" : "raft");
                ConnMarkDisconnected(node->conn);
            }
        }
    }
}

void joinFreeCallback(void *privdata)
{
    JoinState *state = (JoinState *) privdata;

    NodeAddrListFree(state->addr);
    RedisModule_Free(state);
}

void joinIdleCallback(Connection *conn)
{
    JoinState *state = ConnGetPrivateData(conn);

    /* Advance iterator, wrap around to start */
    if (state->addr_iter) {
        state->addr_iter = state->addr_iter->next;
    }
    if (!state->addr_iter) {
        state->addr_iter = state->addr;

        /* FIXME: If we iterated through the entire list, we currently continue
         * forever. This should be changed along with the change of configuration
         * interface, so once we've exahusted all addresses we fail the
         * join operation.
         */
    }

    LOG_VERBOSE("Joining cluster, connecting to %s:%u\n", state->addr_iter->addr.host, state->addr_iter->addr.port);

    /* Establish connection. We silently ignore errors here as we'll
     * just get iterated again in the future.
     */
    ConnConnect(state->conn, &state->addr_iter->addr, sendNodeAddRequest);
}

void InitiateJoinCluster(RedisRaftCtx *rr, const NodeAddrListElement *addr)
{
    JoinState *state = RedisModule_Calloc(1, sizeof(*state));
    state->conn = ConnCreate(rr, state, joinIdleCallback, joinFreeCallback);
    NodeAddrListConcat(&state->addr, addr);
}

