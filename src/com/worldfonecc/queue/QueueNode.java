/*
 * Copyright © 2014 South Telecom
 */
package com.worldfonecc.queue;

/**
 *
 * @author nguyenngocbinh
 */
public class QueueNode {

    private String host = "localhost";
    private int port = 11300;
    private long status = 0;

    public QueueNode(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public long getStatus() {
        return this.status;
    }

    public void setStatus(long status) {
        this.status = status;
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }
}
