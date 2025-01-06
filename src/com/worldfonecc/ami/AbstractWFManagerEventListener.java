/*
 * Copyright © 2014 South Telecom
 */
package com.worldfonecc.ami;

import org.asteriskjava.manager.ManagerEventListener;

/**
 *
 * @author nguyenngocbinh
 */
public abstract class AbstractWFManagerEventListener implements ManagerEventListener {

    public abstract void setHostname(String hostname);

    public abstract void setPort(int port);

    public abstract void setUsername(String username);

    public abstract void setPassword(String password);

    public abstract void setLog2consolse(boolean log2console);

    public abstract void setLogallevents(boolean logallevents);

    public abstract void setCurCDRtable(String curCDRtable);

    public abstract void setSupportedEvent(String[] events);

    public abstract void setQueuehost(String queuehost);

    public abstract void startup() throws Exception;

    public abstract void shutdown() throws Exception;

}
