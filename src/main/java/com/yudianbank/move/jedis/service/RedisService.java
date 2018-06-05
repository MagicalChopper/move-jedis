package com.yudianbank.move.jedis.service;


import redis.clients.jedis.Jedis;

public interface RedisService {

    Jedis getResource();

    void returnResource(Jedis jedis);

    Object keys();

    void move(String key);
}
