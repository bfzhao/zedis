package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;

@Component
public class ServerHandler extends RedisCommandHandler {
    HashMap<String, String> config = new HashMap<>();

    ServerHandler(InMemorySharedStore sharedStore) {
        super("Server", sharedStore);
        config.put("SAVE", "3600 1 300 100 60 10000");
        config.put("APPENDONLY", "no");

        commands.put("ACL CAT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "ACL CAT [category]",
                        "Lists the ACL categories, or the commands inside a category."
                )
        );
        commands.put("ACL DELUSER",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "ACL DELUSER username [username ...]",
                        "Deletes ACL users, and terminates their connections."
                )
        );
        commands.put("ACL DRYRUN",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "ACL DRYRUN username command [arg [arg ...]]",
                        "Simulates the execution of a command by a user, without executing the command."
                )
        );
        commands.put("ACL GENPASS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "ACL GENPASS [bits]",
                        "Generates a pseudorandom, secure password that can be used to identify ACL users."
                )
        );
        commands.put("ACL GETUSER",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "ACL GETUSER username",
                        "Lists the ACL rules of a user."
                )
        );
        commands.put("ACL LIST",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "ACL LIST",
                        "Dumps the effective rules in ACL file format."
                )
        );
        commands.put("ACL LOAD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "ACL LOAD",
                        "Reloads the rules from the configured ACL file."
                )
        );
        commands.put("ACL LOG",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "ACL LOG [count | RESET]",
                        "Lists recent security events generated due to ACL rules."
                )
        );
        commands.put("ACL SAVE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "ACL SAVE",
                        "Saves the effective ACL rules in the configured ACL file."
                )
        );
        commands.put("ACL SETUSER",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "ACL SETUSER username [rule [rule ...]]",
                        "Creates and modifies an ACL user and its rules."
                )
        );
        commands.put("ACL USERS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "ACL USERS",
                        "Lists all ACL users."
                )
        );
        commands.put("ACL WHOAMI",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "ACL WHOAMI",
                        "Returns the authenticated username of the current connection."
                )
        );
        commands.put("BGREWRITEAOF",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BGREWRITEAOF",
                        "Asynchronously rewrites the append-only file to disk."
                )
        );
        commands.put("BGSAVE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BGSAVE [SCHEDULE]",
                        "Asynchronously saves the database(s) to disk."
                )
        );
        commands.put("COMMAND TEST",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "COMMAND TEST v1 v2 length key [key ...] [X|Y|Z] [A] [B b] [C c1 c2] [AA <SUM|AVG|MAX>] <BEFORE|AFTER> [message]",
                        "Returns a count of commands."
                        , Command.Part.ofValue("v1")
                        , Command.Part.ofValue("v2")
                        , Command.Part.ofFixedLengthListValue("length", "key")
                        , Command.Part.ofOptionChoice(
                                Command.Part.ofOptionNamedSimple("X"),
                                Command.Part.ofOptionNamedSimple("Y"),
                                Command.Part.ofOptionNamedSimple("Z"))
                        , Command.Part.ofOptionNamedSimple("A")
                        , Command.Part.ofOptionNamedSimple("B", "b1")
                        , Command.Part.ofOptionNamedSimple("C", "c1", "c2")
                        , Command.Part.ofOptionNamedTerms("AA", "SUM", "AVG", "MAX")
                        , Command.Part.ofTermValue(8, "BEFORE", "AFTER")
                        , Command.Part.ofOptionAnonymous("message")
                )
        );
        commands.put("COMMAND COUNT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "COMMAND COUNT",
                        "Returns a count of commands."
                )
        );
        commands.put("COMMAND DOCS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "COMMAND DOCS [command-name [command-name ...]]",
                        "Returns documentary information about one, multiple or all commands."
                )
        );
        commands.put("COMMAND GETKEYS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "COMMAND GETKEYS command [arg [arg ...]]",
                        "Extracts the key names from an arbitrary command."
                )
        );
        commands.put("COMMAND GETKEYSANDFLAGS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "COMMAND GETKEYSANDFLAGS command [arg [arg ...]]",
                        "Extracts the key names and access flags for an arbitrary command."
                )
        );
        commands.put("COMMAND INFO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "COMMAND INFO [command-name [command-name ...]]",
                        "Returns information about one, multiple or all commands."
                )
        );
        commands.put("COMMAND LIST",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "COMMAND LIST [FILTERBY ]",
                        "Returns a list of command names."
                )
        );
        commands.put("COMMAND",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "COMMAND",
                        "Returns detailed information about all commands."
                )
        );
        commands.put("CONFIG GET",
                new Command(
                        (ctx, args) -> {
                            String[] parameters = args.valueListDefault();
                            for (String p: parameters) {
                                if (config.containsKey(p.toUpperCase())) {
                                    return RespType.ofArray(p, config.get(p.toUpperCase()));
                                }
                            }
                            return RespType.ofError("not implemented");
                        },
                        "CONFIG GET parameter [parameter ...]",
                        "Returns the effective values of configuration parameters."
                        , Command.Part.ofListValue("parameter", "parameter")
                )
        );
        commands.put("CONFIG RESETSTAT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CONFIG RESETSTAT",
                        "Resets the server's statistics."
                )
        );
        commands.put("CONFIG REWRITE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CONFIG REWRITE",
                        "Persists the effective configuration to file."
                )
        );
        commands.put("CONFIG SET",
                new Command(
                        (ctx, args) -> {
                            String[] kv = args.valueListDefault();
                            if (kv.length % 2 != 0)
                                throw new IllegalArgumentException("k/v pair not matched");

                            for (int i = 0; i < kv.length / 2; i++) {
                                config.put(kv[i * 2].toUpperCase(), kv[i * 2 + 1]);
                            }
                            return RespType.OK();
                        },
                        "CONFIG SET parameter value [parameter value ...]",
                        "Sets configuration parameters in-flight."
                        , Command.Part.ofListValue("parameter", "value")
                )
        );
        commands.put("DBSIZE",
                new Command(
                        (ctx, args) -> RespType.ofLong(ctx.getStore().size()),
                        "DBSIZE",
                        "Returns the number of keys in the database."
                )
        );
        commands.put("FAILOVER",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FAILOVER [TO host port [FORCE]] [ABORT] [TIMEOUT milliseconds]",
                        "Starts a coordinated failover from a server to one of its replicas."
                )
        );
        commands.put("FLUSHALL",
                new Command(
                        (ctx, args) -> {
                            sharedStore.flushAll();
                            return RespType.OK();
                        },
                        "FLUSHALL [ASYNC | SYNC]",
                        "Removes all keys from all databases."
                )
        );
        commands.put("FLUSHDB",
                new Command(
                        (ctx, args) -> {
                            ctx.getStore().clear();
                            return RespType.OK();
                        },
                        "FLUSHDB [ASYNC | SYNC]",
                        "Remove all keys from the current database."
                )
        );
        commands.put("INFO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "INFO [section [section ...]]",
                        "Returns information and statistics about the server."
                )
        );
        commands.put("LASTSAVE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "LASTSAVE",
                        "Returns the Unix timestamp of the last successful save to disk."
                )
        );
        commands.put("LATENCY DOCTOR",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "LATENCY DOCTOR",
                        "Returns a human-readable latency analysis report."
                )
        );
        commands.put("LATENCY GRAPH",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "LATENCY GRAPH event",
                        "Returns a latency graph for an event."
                )
        );
        commands.put("LATENCY HISTOGRAM",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "LATENCY HISTOGRAM [command [command ...]]",
                        "Returns the cumulative distribution of latencies of a subset or all commands."
                )
        );
        commands.put("LATENCY HISTORY",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "LATENCY HISTORY event",
                        "Returns timestamp-latency samples for an event."
                )
        );
        commands.put("LATENCY LATEST",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "LATENCY LATEST",
                        "Returns the latest latency samples for all events."
                )
        );
        commands.put("LATENCY RESET",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "LATENCY RESET [event [event ...]]",
                        "Resets the latency data for one or more events."
                )
        );
        commands.put("LOLWUT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "LOLWUT [VERSION version]",
                        "Displays computer art and the Redis version"
                )
        );
        commands.put("MEMORY DOCTOR",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "MEMORY DOCTOR",
                        "Outputs a memory problems report."
                )
        );
        commands.put("MEMORY MALLOC-STATS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "MEMORY MALLOC-STATS",
                        "Returns the allocator statistics."
                )
        );
        commands.put("MEMORY PURGE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "MEMORY PURGE",
                        "Asks the allocator to release memory."
                )
        );
        commands.put("MEMORY STATS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "MEMORY STATS",
                        "Returns details about memory usage."
                )
        );
        commands.put("MEMORY USAGE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "MEMORY USAGE key [SAMPLES count]",
                        "Estimates the memory usage of a key."
                )
        );
        commands.put("MODULE LIST",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "MODULE LIST",
                        "Returns all loaded modules."
                )
        );
        commands.put("MODULE LOAD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "MODULE LOAD path [arg [arg ...]]",
                        "Loads a module."
                )
        );
        commands.put("MODULE LOADEX",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "MODULE LOADEX path [CONFIG name value [CONFIG name value ...]]  [ARGS args [args ...]]",
                        "Loads a module using extended parameters."
                )
        );
        commands.put("MODULE UNLOAD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "MODULE UNLOAD name",
                        "Unloads a module."
                )
        );
        commands.put("MONITOR",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "MONITOR",
                        "Listens for all requests received by the server in real-time."
                )
        );
        commands.put("PSYNC",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "PSYNC replicationid offset",
                        "An internal command used in replication."
                )
        );
        commands.put("REPLCONF",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "REPLCONF",
                        "An internal command for configuring the replication stream."
                )
        );
        commands.put("REPLICAOF",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "REPLICAOF",
                        "Configures a server as replica of another, or promotes it to a master."
                )
        );
        commands.put("RESTORE-ASKING",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "RESTORE-ASKING key ttl serialized-value [REPLACE] [ABSTTL]  [IDLETIME seconds] [FREQ frequency]",
                        "An internal command for migrating keys in a cluster."
                )
        );
        commands.put("ROLE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "ROLE",
                        "Returns the replication role."
                )
        );
        commands.put("SAVE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SAVE",
                        "Synchronously saves the database(s) to disk."
                )
        );
        commands.put("SHUTDOWN",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SHUTDOWN [NOSAVE | SAVE] [NOW] [FORCE] [ABORT]",
                        "Synchronously saves the database(s) to disk and shuts down the Redis server."
                )
        );
        commands.put("SLAVEOF",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SLAVEOF",
                        "Sets a Redis server as a replica of another, or promotes it to being a master."
                )
        );
        commands.put("SLOWLOG GET",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SLOWLOG GET [count]",
                        "Returns the slow log's entries."
                )
        );
        commands.put("SLOWLOG LEN",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SLOWLOG LEN",
                        "Returns the number of entries in the slow log."
                )
        );
        commands.put("SLOWLOG RESET",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SLOWLOG RESET",
                        "Clears all entries from the slow log."
                )
        );
        commands.put("SWAPDB",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SWAPDB index1 index2",
                        "Swaps two Redis databases."
                )
        );
        commands.put("SYNC",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SYNC",
                        "An internal command used in replication."
                )
        );
        commands.put("TIME",
                new Command(
                        (ctx, args) -> {
                            Instant instant = Instant.now();
                            return RespType.ofArray(Long.toString(instant.getEpochSecond()),
                                    Integer.toString(Instant.now().getNano()/1000));
                        },
                        "TIME",
                        "Returns the server time."
                )
        );
    }
}
