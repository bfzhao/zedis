package org.my.zedis;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class ZedisRunner implements CommandLineRunner {
    private final ZedisServer zedis;

    public ZedisRunner(ZedisServer zedis) {
        this.zedis = zedis;
    }

    @Override
    public void run(String... args) throws Exception {
        zedis.run(args);
    }
}
