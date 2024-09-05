package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class ClusterHandler extends RedisCommandHandler {
    ClusterHandler(InMemorySharedStore sharedStore) {
        super("Cluster", sharedStore);

        commands.put("ASKING",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "ASKING",
                        "Signals that a cluster client is following an -ASK redirect."
                )
        );
        commands.put("CLUSTER ADDSLOTS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER ADDSLOTS slot [slot ...]",
                        "Assigns new hash slots to a node."
                )
        );
        commands.put("CLUSTER ADDSLOTSRANGE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER ADDSLOTSRANGE start-slot end-slot [start-slot end-slot ...]",
                        "Assigns new hash slot ranges to a node."
                )
        );
        commands.put("CLUSTER BUMPEPOCH",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER BUMPEPOCH",
                        "Advances the cluster config epoch."
                )
        );
        commands.put("CLUSTER COUNT-FAILURE-REPORTS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER COUNT-FAILURE-REPORTS node-id",
                        "Returns the number of active failure reports active for a node."
                )
        );
        commands.put("CLUSTER COUNTKEYSINSLOT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER COUNTKEYSINSLOT slot",
                        "Returns the number of keys in a hash slot."
                )
        );
        commands.put("CLUSTER DELSLOTS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER DELSLOTS slot [slot ...]",
                        "Sets hash slots as unbound for a node."
                )
        );
        commands.put("CLUSTER DELSLOTSRANGE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER DELSLOTSRANGE start-slot end-slot [start-slot end-slot ...]",
                        "Sets hash slot ranges as unbound for a node."
                )
        );
        commands.put("CLUSTER FAILOVER",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER FAILOVER [FORCE | TAKEOVER]",
                        "Forces a replica to perform a manual failover of its master."
                )
        );
        commands.put("CLUSTER FLUSHSLOTS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER FLUSHSLOTS",
                        "Deletes all slots information from a node."
                )
        );
        commands.put("CLUSTER FORGET",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER FORGET node-id",
                        "Removes a node from the nodes table."
                )
        );
        commands.put("CLUSTER GETKEYSINSLOT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER GETKEYSINSLOT slot count",
                        "Returns the key names in a hash slot."
                )
        );
        commands.put("CLUSTER INFO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER INFO",
                        "Returns information about the state of a node."
                )
        );
        commands.put("CLUSTER KEYSLOT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER KEYSLOT key",
                        "Returns the hash slot for a key."
                )
        );
        commands.put("CLUSTER LINKS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER LINKS",
                        "Returns a list of all TCP links to and from peer nodes."
                )
        );
        commands.put("CLUSTER MEET",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER MEET ip port [cluster-bus-port]",
                        "Forces a node to handshake with another node."
                )
        );
        commands.put("CLUSTER MYID",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER MYID",
                        "Returns the ID of a node."
                )
        );
        commands.put("CLUSTER MYSHARDID",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER MYSHARDID",
                        "Returns the shard ID of a node."
                )
        );
        commands.put("CLUSTER NODES",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER NODES",
                        "Returns the cluster configuration for a node."
                )
        );
        commands.put("CLUSTER REPLICAS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER REPLICAS node-id",
                        "Lists the replica nodes of a master node."
                )
        );
        commands.put("CLUSTER REPLICATE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER REPLICATE node-id",
                        "Configure a node as replica of a master node."
                )
        );
        commands.put("CLUSTER RESET",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER RESET [HARD | SOFT]",
                        "Resets a node."
                )
        );
        commands.put("CLUSTER SAVECONFIG",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER SAVECONFIG",
                        "Forces a node to save the cluster configuration to disk."
                )
        );
        commands.put("CLUSTER SET-CONFIG-EPOCH",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER SET-CONFIG-EPOCH config-epoch",
                        "Sets the configuration epoch for a new node."
                )
        );
        commands.put("CLUSTER SETSLOT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER SETSLOT slot",
                        "Binds a hash slot to a node."
                )
        );
        commands.put("CLUSTER SHARDS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER SHARDS",
                        "Returns the mapping of cluster slots to shards."
                )
        );
        commands.put("CLUSTER SLAVES",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER SLAVES node-id",
                        "Lists the replica nodes of a master node."
                )
        );
        commands.put("CLUSTER SLOTS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLUSTER SLOTS",
                        "Returns the mapping of cluster slots to nodes."
                )
        );
        commands.put("READONLY",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "READONLY",
                        "Enables read-only queries for a connection to a Redis Cluster replica node."
                )
        );
        commands.put("READWRITE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "READWRITE",
                        "Enables read-write queries for a connection to a Reids Cluster replica node."
                )
        );
    }
}
