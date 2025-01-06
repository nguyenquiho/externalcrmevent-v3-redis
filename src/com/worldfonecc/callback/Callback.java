/*
 * Copyright © 2014 South Telecom
 */
package com.worldfonecc.callback;

import com.worldfonecc.queue.JobExternal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;

/**
 *
 * @author nguyenngocbinh
 */
public class Callback {

    final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("CrmEvent");
    private static final AtomicLong idCounter = new AtomicLong();
    private List<NameValuePair> nvps = new ArrayList<>();
    private String callbackURL = null;
    private boolean log2console = false;
    private String queuehost = "192.168.16.60";

    public Callback() {

    }

    public Callback(String url, String queuehost, boolean log2console) {
        this.callbackURL = url;
        this.queuehost = queuehost;
        this.log2console = log2console;
    }

    public void setCallbackURL(String url) {
        this.callbackURL = url;
    }

    public void addParam(String name, String key) {
        if (nvps == null) {
            nvps = new ArrayList<>();
        }

        nvps.add(new BasicNameValuePair(name, key));
    }

    public boolean sendGet(long receivedTimestamp0) throws URISyntaxException {
        if (callbackURL.length() > 8) {
            URI uri = new URIBuilder(callbackURL).addParameters(nvps).build();
            JobExternal j = new JobExternal(this.queuehost, this.log2console);
            long timestamp = System.currentTimeMillis() / 1000;
            j.addParam("receivedTimestamp0", receivedTimestamp0 + "");
            j.addParam("receivedTimestamp", timestamp + "");
            j.addParam("url", uri.toString());
            j.addParam("method", "get");
            j.addParam("headers", "");
            j.addParam("postfields", "");
            return j.QueuePut();

        }
        return false;
    }

    class CallbackThreadUncaughtExceptionHanlder implements Thread.UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.error("Uncaught exception in CallbackVtiger Async", e);
        }
    }

}
