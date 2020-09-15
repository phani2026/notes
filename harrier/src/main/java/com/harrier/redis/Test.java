package com.harrier.redis;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class Test {

    public static void main(String[] args) {

        Config config = new Config();
        config.useSingleServer()
                .setAddress("127.0.0.1:6379");
        RedissonClient client = Redisson.create();


    }
}
