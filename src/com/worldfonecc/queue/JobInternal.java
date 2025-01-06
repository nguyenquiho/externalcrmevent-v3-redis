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
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Calendar;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author Dai Le
 */
public class JobInternal {

    final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("CrmEvent");
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");

    private boolean log2console = false;

    private JSONObject payload;
    private static final String queuehost = "127.0.0.1";
    private long priority = Beanstemc.DEFAULT_PRIORITY;

    private static Beanstemc queue;

    public static Beanstemc getBeanstalkd() throws IOException {
        if (queue == null) {
            queue = createBeanstalkd();
        }
        return queue;
    }

    public static void closeBeanstalkd() {
        if (queue != null) {
            try {
                queue.close();
            } catch (IOException ex) {

            }
        }
        queue = null;
    }

    private static Beanstemc createBeanstalkd() throws IOException {
        return new Beanstemc(queuehost, 11300);
    }

    public JobInternal() {
        payload = new JSONObject();

        LocalTime now = LocalTime.now(ZoneId.systemDefault());
        priority = now.toSecondOfDay();
    }

    public JobInternal(boolean log2console) {
        this.log2console = log2console;

        payload = new JSONObject();

        LocalTime now = LocalTime.now(ZoneId.systemDefault());
        priority = now.toSecondOfDay();
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
        return QueuePut("internalevent", Beanstemc.DEFAULT_DELAY);
    }

    public boolean QueuePut(String queuename) {
        return QueuePut(queuename, Beanstemc.DEFAULT_DELAY);
    }

    public boolean QueuePut(String queuename, int delay) {
        try {
            Object hostname = MemcacheConnection.getInstance().get("hostname");
            if (hostname == null) {
                Calendar cal = Calendar.getInstance();
                cal.add(Calendar.DAY_OF_MONTH, 1);
                hostname = InetAddress.getLocalHost().getHostName();
                MemcacheConnection.getInstance().set("hostname", hostname, cal.getTime());
            }

            payload.put("sipgw", hostname.toString());
            synchronized (JobInternal.class) {
                getBeanstalkd().use(queuename).put(payload.toString().getBytes(), priority, delay);
            }
            log(payload.toString());
            return true;
        } catch (BeanstemcException | JSONException ex) {
            ex.printStackTrace(System.out);
            logger.error(ex);
        } catch (IOException ioex) {
            closeBeanstalkd();
            logger.error(ioex);
        }
        return false;
    }
}
