package org.my;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class ConnectionManager {
    private final Map<String, ClientConfig> clientConfigMap = new HashMap<>();
    public void register(String remoteAddress) {
        clientConfigMap.put(remoteAddress, ClientConfig.defaultConfig());
    }

    public void remove(String remoteAddress) {
        clientConfigMap.remove(remoteAddress);
    }

    public void selectDb(String clientKey, int index) {
        clientConfigMap.get(clientKey).setDb(index);
    }

    public int getDb(String clientKey) {
        return clientConfigMap.get(clientKey).getDb();
    }

    public void reset(String clientKey) {
        clientConfigMap.get(clientKey).reset();
    }

    public String getName(String clientKey) {
        return clientConfigMap.get(clientKey).getConnectionName();
    }

    public void setName(String clientKey, String connectionName) {
        clientConfigMap.get(clientKey).setConnectionName(connectionName);
    }

    public Long getId(String clientKey) {
        return (long) clientConfigMap.get(clientKey).getId();
    }
}
