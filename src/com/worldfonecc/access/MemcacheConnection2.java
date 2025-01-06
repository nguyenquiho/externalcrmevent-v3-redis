/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.worldfonecc.access;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;

/**
 *
 * @author MitsuyoRai
 */
public class MemcacheConnection2 {

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

    private MemcachedClient mcc = null;// MemCachedClient();

    // Khởi tạo và kết nối đến Memcached server
    private MemcacheConnection2() {
        try {
            mcc = new MemcachedClient(new BinaryConnectionFactory(), AddrUtil.getAddresses(HOST));
        } catch (IOException ex) {
            System.out.println("Memcache connection error!!!!, " + ex.getMessage());
        }
    }

    private static volatile MemcacheConnection2 instance = null;

    public static MemcacheConnection2 getInstance() {
        MemcacheConnection2 _instance = MemcacheConnection2.instance;
        if (_instance == null) {
            synchronized (MemcacheConnection2.class) {
                _instance = MemcacheConnection2.instance;
                if (_instance == null) {
                    MemcacheConnection2.instance = _instance = new MemcacheConnection2();
                }
            }
        }
        return _instance;
    }

    //Hàm get một Object từ Memcached server thông qua key.
    public Object get(String key) {
        // System.out.println(this.getClass().getName() + " get(key) : " + key);
        //  System.out.println(mcc.get(key));
        Object obj = mcc.get(key);
        //if (obj == null) {
        //    System.out.println(this.getClass().getName() + " get : Object NULL");
        //} else {
        //   System.out.println(this.getClass().getName() + " get : " + obj.toString());
        // }
        return obj;
    }

    //Hàm set một Object vào Memcached server
    public void set(String key, Object value, int expireTime) {
        try {
            if (mcc.set(key, expireTime, value).get()) {
            } else {
                System.out.println(this.getClass().getName() + " set key error");
            }
        } catch (InterruptedException | ExecutionException ex) {
            System.out.println(this.getClass().getName() + " set key error: " + ex.getMessage());
        }
    }
    
    //Hàm delete Memcached server thông qua key.
    public void delete(String key) {
        try {
            mcc.delete(key);
        } catch (Throwable t) {
        }
    }
}
