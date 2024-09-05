package org.my.zedis;

import io.netty.buffer.ByteBuf;
import io.netty.util.ByteProcessor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

@lombok.Setter
@lombok.Getter
public class RespType {
    @lombok.Getter
    @lombok.AllArgsConstructor
    public enum Type {
        // RESP2
        Strings("+"),
        Errors("-"),
        Long(":"),
        BulkStrings("$"),
        Arrays("*"),

        // RESP3
        Nulls("_"),
        Booleans("#"),
        Doubles(","),
        BigNumbers("("),
        BulkErrors("!"),
        VerbatimStrings("="),
        Maps("%"),
        Sets("~"),
        Pushes(">"),
        ;

        private final String leadingByte;
    }

    private Type type;
    private Object value;

    private RespType(Type type, Object value) {
        this.type = type;
        this.value = value;
    }

    public static RespType ofString(String value) {
        return new RespType(Type.Strings, value);
    }

    public static RespType ofError(String value) {
        return new RespType(Type.Errors, value);
    }

    public static RespType ofLong(Long value) {
        return new RespType(Type.Long, value);
    }

    public static RespType ofLong(Integer value) {
        return new RespType(Type.Long, value);
    }

    public static RespType ofBulkString(String value) {
        return new RespType(Type.BulkStrings, value);
    }

    public static RespType ofArray(RespType... value) {
        return new RespType(Type.Arrays, value);
    }

    public static RespType ofArray(Long... value) {
        return RespType.ofArray(Arrays.stream(value).map(RespType::ofLong).toArray(RespType[]::new));
    }

    public static RespType ofArray(Integer... value) {
        return RespType.ofArray(Arrays.stream(value).map(RespType::ofLong).toArray(RespType[]::new));
    }

    public static RespType ofArray(String... value) {
        return value == null? emptyArray() : RespType.ofArray(Arrays.stream(value).map(RespType::ofBulkString).toArray(RespType[]::new));
    }

    public String asString() {
        assert (type == Type.Strings);
        return (String) value;
    }

    public String asError() {
        assert (type == Type.Errors);
        return (String) value;
    }

    public Integer asInteger() {
        assert (type == Type.Long);
        return (Integer) value;
    }

    public Integer toInteger() {
        assert (type == Type.Strings);
        return Integer.parseInt((String) value);
    }

    public String asBulkString() {
        assert (type == Type.BulkStrings);
        return (String) value;
    }

    public RespType[] asArray() {
        assert (type == Type.Arrays);
        return (RespType[]) value;
    }

    public static RespType decode(ByteBuf bytes) {
        byte prefix = bytes.readByte();
        switch (prefix) {
            case '+':
                return ofString(new String(readBytesUntilCRLF(bytes, -1)));
            case '-':
                return ofError(new String(readBytesUntilCRLF(bytes, -1)));
            case ':':
                return ofLong(Long.parseLong(new String(readBytesUntilCRLF(bytes, -1))));
            case '$':
                int sz = Integer.parseInt(new String(readBytesUntilCRLF(bytes, -1)));
                return ofBulkString(new String(readBytesUntilCRLF(bytes, sz)));
            case '*':
                int elementsCount = Integer.parseInt(new String(readBytesUntilCRLF(bytes, -1)));
                RespType[] list = new RespType[elementsCount];
                for (int i = 0; i < elementsCount; i++) {
                    list[i] = decode(bytes);
                }
                return ofArray(list);
            default:
                throw new IllegalArgumentException("Invalid RESP type: " + (char) prefix);
        }
    }

    private static byte[] readBytesUntilCRLF(ByteBuf byteBuf, int sz) {
        int crIndex = byteBuf.forEachByte(ByteProcessor.FIND_CR);
        if (crIndex >= 0 && crIndex + 1 < byteBuf.writerIndex() && byteBuf.getByte(crIndex + 1) == '\n') {
            if (sz != -1 && crIndex - byteBuf.readerIndex() != sz) {
                throw new IllegalArgumentException("Invalid RESP byte stream, bulk string size is mismatched");
            }

            byte[] bytes = new byte[crIndex - byteBuf.readerIndex()];
            byteBuf.readBytes(bytes);
            byteBuf.skipBytes(2); // Skip \r\n
            return bytes;
        } else {
            // Handle case where CRLF is not found
            return new byte[0];
        }
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(encodeImpl().getBytes());
        return outputStream.toByteArray();
    }

    public String encodeImpl() {
        switch (type) {
            case Strings:
            case Errors:
            case Long:
            case Doubles:
            case BigNumbers:
                assert(value != null);
                return type.getLeadingByte() + value + "\r\n";
            case BulkStrings:
                if (value == null) {
                    return type.getLeadingByte() + "-1\r\n";
                } else {
                    String bulkStringValue = (String) value;
                    return type.getLeadingByte() + bulkStringValue.getBytes().length + "\r\n" + bulkStringValue + "\r\n";
                }
            case Arrays:
                if (value == null) {
                    return type.getLeadingByte() + "-1\r\n";
                } else {
                    StringBuilder sb = new StringBuilder(type.getLeadingByte() + ((RespType[]) value).length + "\r\n");
                    for (Object element : ((RespType[]) value)) {
                        sb.append(((RespType) element).encodeImpl());
                    }
                    return sb.toString();
                }
            case Nulls:
                assert(value == null);
                return type.getLeadingByte() + "\r\n";
            case Booleans:
                return type.getLeadingByte() + (((Boolean)value)? "t" : "f") + "\r\n";
            case BulkErrors:
            case VerbatimStrings:
            case Maps:
            case Sets:
            case Pushes:
            default:
                throw new IllegalArgumentException("Invalid RESP type");
        }
    }

    @Override
    public String toString() {
        return encodeImpl()
                .replaceAll("\r", "\\\\r")
                .replaceAll("\n", "\\\\n");
    }

    // public value
    public static RespType OK() {
        return ofString("OK");
    }

    public static RespType NullBulkString() {
        return ofBulkString(null);
    }

    public static RespType emptyArray() {
        return ofArray(new String[0]);
    }
}
