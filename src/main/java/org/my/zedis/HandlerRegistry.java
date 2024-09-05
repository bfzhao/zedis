package org.my.zedis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class HandlerRegistry {
    private final Map<String, RedisCommandHandler> handlers = new HashMap<>();

    @Autowired
    public HandlerRegistry(ApplicationContext context) {
        Map<String, RedisCommandHandler> m = context.getBeansOfType(RedisCommandHandler.class);
        for (RedisCommandHandler handler: m.values()) {
            handler.getCommands().forEach(x -> handlers.put(x, handler));
        }
    }

    public RedisCommandHandler getHandler(String name) {
        return handlers.get(name.toUpperCase());
    }
}
