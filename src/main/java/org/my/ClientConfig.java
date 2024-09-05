package org.my;

import java.util.concurrent.atomic.AtomicInteger;

@lombok.Getter
@lombok.Setter
public class ClientConfig {
    private static AtomicInteger MAX_ID = new AtomicInteger(0);
    private int id;
    private int db;
    private String connectionName;

    public static ClientConfig defaultConfig() {
        ClientConfig config = new ClientConfig();
        config.db = 0;
        config.id = MAX_ID.addAndGet(1);
        return config;
    }

    public void reset() {
    }
}
