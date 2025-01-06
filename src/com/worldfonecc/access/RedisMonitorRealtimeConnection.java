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
import java.util.Map;
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
public class RedisMonitorRealtimeConnection {

    private final static Logger logger = Logger.getLogger("CrmEvent");

    private static String rHost = "192.168.16.230";
    private static int rPort = 30110;
    private static String rAuth = "pbxbc957bc3715e94e";
    private static int rTimeout = 30000;
    private static JedisPool jedisPool;

    private RedisMonitorRealtimeConnection() {
        try {
            File file1 = new File(System.getProperty("user.dir"));
            URL[] urls = {file1.toURI().toURL()};
            ClassLoader loader = new URLClassLoader(urls);
            ResourceBundle rb = ResourceBundle.getBundle("PBX", Locale.getDefault(), loader);
            if (rb.containsKey("redis_monitor.host")) {
                rHost = rb.getString("redis_monitor.host");
            }
            if (rb.containsKey("redis_monitor.port")) {
                rPort = Integer.parseInt(rb.getString("redis_monitor.port"));
            }
            if (rb.containsKey("redis_monitor.auth")) {
                rAuth = rb.getString("redis_monitor.auth");
            }
            if (rb.containsKey("redis_monitor.timeout")) {
                rTimeout = Integer.parseInt(rb.getString("redis_monitor.timeout"));
            }
        } catch (IOException e) {
            System.out.println(e);
        }
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        jedisPool = new JedisPool(poolConfig, rHost, rPort, rTimeout, rAuth);
    }

    @Override
    protected void finalize() {
        try {
            if (jedisPool != null) {
                jedisPool.close();
            }
            RedisMonitorRealtimeConnection.instance = null;
            super.finalize();
        } catch (Throwable ex) {
            logger.error("Redis Local Throwable", ex);
        }
    }

    private static volatile RedisMonitorRealtimeConnection instance;

    public static RedisMonitorRealtimeConnection getInstance() {
        RedisMonitorRealtimeConnection _instance = RedisMonitorRealtimeConnection.instance;
        if (_instance == null) {
            synchronized (RedisMonitorRealtimeConnection.class) {
                _instance = RedisMonitorRealtimeConnection.instance;
                if (_instance == null) {
                    RedisMonitorRealtimeConnection.instance = _instance = new RedisMonitorRealtimeConnection();
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

    final public boolean expire(String key, int timeout) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.expire(key, timeout) > 0;
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
