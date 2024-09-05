package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class BitmapHandler extends RedisCommandHandler {
    BitmapHandler(InMemorySharedStore sharedStore) {
        super("Bitmap", sharedStore);

        commands.put("BITCOUNT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BITCOUNT key [start end [BYTE | BIT]]",
                        "Counts the number of set bits (population counting) in a string."
                )
        );
        commands.put("BITFIELD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BITFIELD key [GET encoding offset | [OVERFLOW ]  [GET encoding offset | [OVERFLOW ]  ...]]",
                        "Performs arbitrary bitfield integer operations on strings."
                )
        );
        commands.put("BITFIELD_RO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BITFIELD_RO key [GET encoding offset [GET encoding offset ...]]",
                        "Performs arbitrary read-only bitfield integer operations on strings."
                )
        );
        commands.put("BITOP",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BITOP destkey key [key ...]",
                        "Performs bitwise operations on multiple strings, and stores the result."
                )
        );
        commands.put("BITPOS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BITPOS key bit [start [end [BYTE | BIT]]]",
                        "Finds the first set (1) or clear (0) bit in a string."
                )
        );
        commands.put("GETBIT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "GETBIT key offset",
                        "Returns a bit value by offset."
                )
        );
        commands.put("SETBIT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SETBIT key offset value",
                        "Sets or clears the bit at offset of the string value. Creates the key if it doesn't exist."
                )
        );
    }
}
