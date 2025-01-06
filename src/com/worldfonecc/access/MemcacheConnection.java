/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.worldfonecc.access;

import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;

/**
 *
 * @author MitsuyoRai
 */
public class MemcacheConnection {

    private static String MEMCACHED_CONFIG_FILE = System.getProperty("user.dir") + "/memcached_server_config.properties";

    private static String parse(String key) {
        try {
            File f = new File(MEMCACHED_CONFIG_FILE);
            if (f.exists()) {
                Properties pro = new Properties();
                FileInputStream in = new FileInputStream(f);
                pro.load(in);

                String p = pro.getProperty(key);
                return p;
            } else {
                System.out.println(MEMCACHED_CONFIG_FILE);
                System.out.println("File not found!");
                return null;
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    final String HOST = parse("HOST");
    final String WEIGHTS = parse("WEIGHTS");
    final String INIT_CONN = parse("INIT_CONN");
    final String MIN_CONN = parse("MIN_CONN");
    final String MAX_CONN = parse("MAX_CONN");
    final String MAINT_SLEEP = parse("MAINT_SLEEP");
    final String NAGLE = parse("NAGLE");
    final String MAX_IDLE = parse("MAX_IDLE");
    final String SOCKET_TO = parse("SOCKET_TO");
    final String SOCKET_CONNECT_TO = parse("SOCKET_CONNECT_TO");

    private MemCachedClient mcc = null;

    // Khởi tạo và kết nối đến Memcached server
    private MemcacheConnection() {
        SockIOPool pool = SockIOPool.getInstance();
        pool.setServers(HOST.split(","));
        pool.setWeights(getWeightsProperty(WEIGHTS));                  // 3,3
        pool.setInitConn(Integer.parseInt(INIT_CONN));                // 5
        pool.setMinConn(Integer.parseInt(MIN_CONN));                   // 5
        pool.setMaxConn(Integer.parseInt(MAX_CONN));                    // 250
        pool.setMaintSleep(Integer.parseInt(MAINT_SLEEP));              // 30
        pool.setNagle(Boolean.parseBoolean(NAGLE));                     // FALSE
        pool.setMaxIdle(Long.parseLong(MAX_IDLE));                    // 21600000
        pool.setSocketTO(Integer.parseInt(SOCKET_TO));                  // 3000
        pool.setSocketConnectTO(Integer.parseInt(SOCKET_CONNECT_TO));  // 0
        pool.initialize();
        
        mcc = new MemCachedClient();
    }

    // sử lý tách các weights ra set vào một mảng Integer
    private Integer[] getWeightsProperty(String weights) {
        String[] strWeights = weights.split(",");
        Integer[] intWeights = new Integer[strWeights.length];
        int i = 0;
        for (String strWeight : strWeights) {
            intWeights[i] = Integer.valueOf(strWeight);
            i++;
        }
        return intWeights;
    }

    private static volatile MemcacheConnection instance;

    public static MemcacheConnection getInstance() {
        MemcacheConnection _instance = MemcacheConnection.instance;
        if (_instance == null) {
            synchronized (MemcacheConnection.class) {
                _instance = MemcacheConnection.instance;
                if (_instance == null) {
                    MemcacheConnection.instance = _instance = new MemcacheConnection();
                }
            }
        }
        return _instance;
    }

    //Hàm get một Object từ Memcached server thông qua key.
    public Object get(String key) {
        try {
            return mcc.get(key);
        } catch (Throwable t) {
            return null;
        }
    }

    //Hàm set một Object vào Memcached server
    public void set(String key, Object value, Date date) {
        try {
            mcc.set(key, value, date);
        } catch (Throwable t) {
        }
    }
    //Hàm update một Object vào Memcached server

    public void update(String key, Object value) {
        try {
            mcc.replace(key, value);
        } catch (Throwable t) {
        }
    }

    //Hàm get một Object từ Memcached server thông qua key.
    public void delete(String key) {
        try {
            mcc.delete(key);
        } catch (Throwable t) {
        }
    }

}
