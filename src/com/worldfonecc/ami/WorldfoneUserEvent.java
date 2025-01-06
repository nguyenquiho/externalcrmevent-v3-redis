/*
 * Copyright © 2014 South Telecom
 */
package com.worldfonecc.ami;

import org.asteriskjava.manager.event.UserEvent;

/**
 *
 * @author nguyenngocbinh
 */
public class WorldfoneUserEvent extends UserEvent {

    public WorldfoneUserEvent(Object source) {
        super(source);
    }

    private String monitorFilename;

    /**
     * Get the value of monitorFilename
     *
     * @return the value of monitorFilename
     */
    public String getMonitorFilename() {
        return monitorFilename;
    }

    /**
     * Set the value of monitorFilename
     *
     * @param monitorFilename new value of monitorFilename
     */
    public void setMonitorFilename(String monitorFilename) {
        this.monitorFilename = monitorFilename;
    }

}
