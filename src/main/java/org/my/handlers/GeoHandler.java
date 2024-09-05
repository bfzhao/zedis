package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class GeoHandler extends RedisCommandHandler {
    GeoHandler(InMemorySharedStore sharedStore) {
        super("Geo", sharedStore);

        commands.put("GEOADD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "GEOADD key [NX | XX] [CH] longitude latitude member [longitude latitude member ...]",
                        "Adds one or more members to a geospatial index. The key is created if it doesn't exist."
                )
        );
        commands.put("GEODIST",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "GEODIST key member1 member2 [M | KM | FT | MI]",
                        "Returns the distance between two members of a geospatial index."
                )
        );
        commands.put("GEOHASH",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "GEOHASH key [member [member ...]]",
                        "Returns members from a geospatial index as geohash strings."
                )
        );
        commands.put("GEOPOS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "GEOPOS key [member [member ...]]",
                        "Returns the longitude and latitude of members from a geospatial index."
                )
        );
        commands.put("GEORADIUS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "GEORADIUS key longitude latitude radius  [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC | DESC]  [STORE key | STOREDIST key]",
                        "Queries a geospatial index for members within a distance from a coordinate, optionally stores the result."
                )
        );
        commands.put("GEORADIUSBYMEMBER",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "GEORADIUSBYMEMBER key member radius [WITHCOORD]  [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC | DESC] [STORE key  | STOREDIST key]",
                        "Queries a geospatial index for members within a distance from a member, optionally stores the result."
                )
        );
        commands.put("GEORADIUSBYMEMBER_RO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "GEORADIUSBYMEMBER_RO key member radius  [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC | DESC]",
                        "Returns members from a geospatial index that are within a distance from a member."
                )
        );
        commands.put("GEORADIUS_RO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "GEORADIUS_RO key longitude latitude radius  [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC | DESC]",
                        "Returns members from a geospatial index that are within a distance from a coordinate."
                )
        );
        commands.put("GEOSEARCH",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "GEOSEARCH key | BYBOX width height> [ASC | DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST]  [WITHHASH]",
                        "Queries a geospatial index for members inside an area of a box or a circle."
                )
        );
        commands.put("GEOSEARCHSTORE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "GEOSEARCHSTORE destination source  | BYBOX width height> [ASC | DESC] [COUNT count  [ANY]] [STOREDIST]",
                        "Queries a geospatial index for members inside an area of a box or a circle, optionally stores the result."
                )
        );
    }
}
