/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020 Redis Labs
 *
 * RedisRaft is dual licensed under the GNU Affero General Public License version 3
 * (AGPLv3) or the Redis Source Available License (RSAL).
 */

#include <string.h>

#include "redisraft.h"

static int addClusterSlotNodeReply(RedisRaftCtx *rr, RedisModuleCtx *ctx, raft_node_t *raft_node)
{
    Node *node = raft_node_get_udata(raft_node);
    NodeAddr *addr;
    char node_id[42];

    /* Stale nodes should not exist but we prefer to be defensive.
     * Our own node doesn't have a connection so we don't expect a Node object.
     */
    if (node) {
        addr = &node->addr;
    } else if (raft_get_my_node(rr->raft) == raft_node) {
        addr = &rr->config->addr;
    } else {
        return 0;
    }

    /* Create a three-element reply:
     * 1) Address
     * 2) Port
     * 3) Node ID
     */

    RedisModule_ReplyWithArray(ctx, 3);
    RedisModule_ReplyWithCString(ctx, addr->host);
    RedisModule_ReplyWithLongLong(ctx, addr->port);

    snprintf(node_id, sizeof(node_id) - 1, "%040x", raft_node_get_id(raft_node));
    RedisModule_ReplyWithCString(ctx, node_id);

    return 1;
}

static void addClusterSlotsReply(RedisRaftCtx *rr, RaftReq *req)
{
    int i;
    int alen;

    /* Make sure we have a leader, or return a -CLUSTERDOWN message */
    raft_node_t *leader_node = raft_get_current_leader_node(rr->raft);
    if (!leader_node) {
        RedisModule_ReplyWithError(req->ctx,
                "CLUSTERDOWN No raft leader");
        return;
    }

    /* FIXME currently not handling partitioning, so we assume we
     * own all hash slots.
     */

    RedisModule_ReplyWithArray(req->ctx, 1);        /* Nodes count */

    /* Dump Raft nodes now. Leader (master) first, followed by others */
    RedisModule_ReplyWithArray(req->ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModule_ReplyWithLongLong(req->ctx, 0);     /* Start slot */
    RedisModule_ReplyWithLongLong(req->ctx, 16383); /* End slot */

    alen = 2;
    alen += addClusterSlotNodeReply(rr, req->ctx, leader_node);
    for (i = 0; i < raft_get_num_nodes(rr->raft); i++) {
        raft_node_t *raft_node = raft_get_node_from_idx(rr->raft, i);
        if (raft_node_get_id(raft_node) == raft_get_current_leader(rr->raft) ||
                !raft_node_is_active(raft_node)) {
            continue;
        }

        alen += addClusterSlotNodeReply(rr, req->ctx, raft_node);
    }
    RedisModule_ReplySetArrayLength(req->ctx, alen);
}


void handleClusterCommand(RedisRaftCtx *rr, RaftReq *req)
{
    RaftRedisCommand *cmd = req->r.redis.cmds.commands[0];

    if (cmd->argc < 2) {
        RedisModule_WrongArity(req->ctx);
        goto exit;
    }

    size_t cmd_len;
    const char *cmd_str = RedisModule_StringPtrLen(cmd->argv[1], &cmd_len);

    if (cmd_len == 5 && !strncasecmp(cmd_str, "SLOTS", 5) && cmd->argc == 2) {
        addClusterSlotsReply(rr, req);
        goto exit;
    } else {
        RedisModule_ReplyWithError(req->ctx,
            "ERR Unknown subcommand ot wrong number of arguments.");
        goto exit;
    }

exit:
    RaftReqFree(req);
}
