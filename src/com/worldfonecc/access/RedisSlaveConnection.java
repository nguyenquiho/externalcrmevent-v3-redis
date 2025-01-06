/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.worldfonecc.access;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.Set;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 *
 * @author MitsuyoRai
 */
public class RedisSlaveConnection {

    private final static Logger logger = Logger.getLogger("CrmEvent");

    private static String rHost = "127.0.0.1";
    private static int rPort = 6798;
    private static String rAuth = "worldfone-cloud";
    private static int rTimeout = 30000;
    private static JedisPool jedisPool;

    private RedisSlaveConnection() {
        try {
            File file1 = new File(System.getProperty("user.dir"));
            URL[] urls = {file1.toURI().toURL()};
            ClassLoader loader = new URLClassLoader(urls);
            ResourceBundle rb = ResourceBundle.getBundle("PBX", Locale.getDefault(), loader);
            if (rb.containsKey("redis.host")) {
                rHost = rb.getString("redis.host");
            }
            if (rb.containsKey("redis.port")) {
                rPort = Integer.parseInt(rb.getString("redis.port"));
            }
            if (rb.containsKey("redis.auth")) {
                rAuth = rb.getString("redis.auth");
            }
            if (rb.containsKey("redis.timeout")) {
                rTimeout = Integer.parseInt(rb.getString("redis.timeout"));
            }
        } catch (IOException e) {
            System.out.println(e);
        }
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        jedisPool = new JedisPool(poolConfig, rHost, rPort, rTimeout, rAuth);
    }

    @Override
    protected void finalize() {
        try {
            if (jedisPool != null) {
                jedisPool.close();
            }
            RedisSlaveConnection.instance = null;
            super.finalize();
        } catch (Throwable ex) {
            logger.error("Redis Throwable", ex);
        }
    }

    private static volatile RedisSlaveConnection instance;

    public static RedisSlaveConnection getInstance() {
        RedisSlaveConnection _instance = RedisSlaveConnection.instance;
        if (_instance == null) {
            synchronized (RedisSlaveConnection.class) {
                _instance = RedisSlaveConnection.instance;
                if (_instance == null) {
                    RedisSlaveConnection.instance = _instance = new RedisSlaveConnection();
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
}
