package org.my;

import java.util.Map;

@lombok.Getter
@lombok.AllArgsConstructor
public class Context {
    private final String clientKey;
    private final Map<String, ValueWithTTL> store;
}
