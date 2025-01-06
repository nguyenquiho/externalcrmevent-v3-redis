/*
 * Copyright © 2014 South Telecom
 */
package com.worldfonecc.queue;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 *
 * @author nguyenngocbinh
 */
public class QueueConfig {

    final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("CrmEvent");
    public String queuehosts = "192.168.16.60";

    public QueueConfig() {
        this.init();
    }

    private void init() {
        try {
            File file1 = new File(System.getProperty("user.dir"));
            URL[] urls = {file1.toURI().toURL()};
            ClassLoader loader = new URLClassLoader(urls);
            ResourceBundle rb = ResourceBundle.getBundle("PBX", Locale.getDefault(), loader);
            if (rb.containsKey("queue.ip")) {
                this.queuehosts = rb.getString("queue.ip");
            } else {
                logger.error("Missing queue.ip");
            }

        } catch (MalformedURLException ex) {
            logger.error(ex);
        }
    }
}
