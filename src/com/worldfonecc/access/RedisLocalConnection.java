/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.worldfonecc.access;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 *
 * @author MitsuyoRai
 */
public class RedisLocalConnection {

    private final static Logger logger = Logger.getLogger("CrmEvent");

    private static JedisPool jedisPool;

    private RedisLocalConnection() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        jedisPool = new JedisPool(poolConfig, "127.0.0.1", 6799, 60000, "worldfone-cloud");
    }

    @Override
    protected void finalize() {
        try {
            if (jedisPool != null) {
                jedisPool.close();
            }
            RedisLocalConnection.instance = null;
            super.finalize();
        } catch (Throwable ex) {
            logger.error("Redis Local Throwable", ex);
        }
    }

    private static volatile RedisLocalConnection instance;

    public static RedisLocalConnection getInstance() {
        RedisLocalConnection _instance = RedisLocalConnection.instance;
        if (_instance == null) {
            synchronized (RedisLocalConnection.class) {
                _instance = RedisLocalConnection.instance;
                if (_instance == null) {
                    RedisLocalConnection.instance = _instance = new RedisLocalConnection();
                }
            }
        }
        return _instance;
    }

    final public Set<String> keys(String pattern) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.keys(pattern);
        }
    }

    final public boolean exists(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(key);
        }
    }

    final public String get(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        }
    }

    final public String HGet(String key, String field) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hget(key, field);
        }
    }

    final public List<String> HMGet(String key, String... fields) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hmget(key, fields);
        }
    }

    final public Set<String> HKeys(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hkeys(key);
        }
    }

    final public String set(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.set(key, value);
        }
    }

    final public String set(String key, String value, int timeout) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.setex(key, timeout, value);
        }
    }

    final public Long expire(String key, int timeout) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.expire(key, timeout);
        }
    }

    final public Long HSet(String key, String field, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hset(key, field, value);
        }
    }

    final public String HMSet(String key, Map<String, String> hash) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hmset(key, hash);
        }
    }

    final public Long del(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.del(key);
        }
    }

    final public Long sAdd(String key, String... val) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.sadd(key, val);
        }
    }

    final public Long sMove(String src, String dst, String val) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.smove(src, dst, val);
        }
    }
}
