/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.worldfonecc.queue;

import com.worldfonecc.access.MemcacheConnection;
import dk.safl.beanstemc.Beanstemc;
import dk.safl.beanstemc.BeanstemcException;
import java.io.IOException;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author cafes
 */
public class JobExternal {

    final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("CrmEvent");
    private JSONObject payload = new JSONObject();
    private boolean log2console = false;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
    private String queuehost = "192.168.16.60";
    static QueueClusterManager queueClusterManager = new QueueClusterManager();

    public JobExternal(String queuehost, boolean log2console) {
        this.queuehost = queuehost;
        this.log2console = log2console;
    }

    private void log(String message) {
        if (this.log2console) {
            System.out.println("---------------------------------------------");
            System.out.println(String.format("%s::[%s]::%s", sdf.format(System.currentTimeMillis()), Thread.currentThread().getName(), message));
        }
        logger.debug(message);
    }

    public void addParam(String name, String key) {
        try {
            if (payload == null) {
                payload = new JSONObject();
            }

            payload.put(name, key);
        } catch (JSONException ex) {
            ex.printStackTrace(System.out);
            logger.error(ex);
        }
    }

    public boolean QueuePut() {
        try {
            Object hostname = MemcacheConnection.getInstance().get("hostname");
            if (hostname == null) {
                Calendar cal = Calendar.getInstance();
                cal.add(Calendar.DAY_OF_MONTH, 1);
                hostname = InetAddress.getLocalHost().getHostName();
                MemcacheConnection.getInstance().set("hostname", hostname, cal.getTime());
            }
            payload.put("sipgw", hostname.toString());
            
            Beanstemc queue = null;
            while (queue == null) {
                queue = queueClusterManager.getQueueNode();
                if (queue == null) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        log(ie.getMessage());
                    }
                }
            }
            queue.use("externalcrm").put(payload.toString().getBytes(), 10);
            queue.close();
            
            log(payload.toString());
            
            return true;
        } catch (BeanstemcException | IOException | JSONException ex) {
            ex.printStackTrace(System.out);
            logger.error(ex);
        }
        return false;
    }
}
