/*
 * Copyright © 2014 South Telecom
 */
package com.worldfonecc.ami;

import java.util.Arrays;
import org.asteriskjava.live.DefaultAsteriskServer;
import org.asteriskjava.manager.ManagerConnection;
import org.asteriskjava.manager.ManagerConnectionFactory;
import static org.asteriskjava.manager.ManagerConnectionState.CONNECTED;
import static org.asteriskjava.manager.ManagerConnectionState.RECONNECTING;
import org.asteriskjava.manager.ManagerEventListener;
import org.asteriskjava.manager.event.ManagerEvent;


/**
 *
 * @author nguyenngocbinh
 */
public class TestManagerEventListener extends AbstractWFManagerEventListener implements ManagerEventListener {

    final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("CrmEvent");

    private ManagerConnection managerConnection;
    private DefaultAsteriskServer asteriskServer;

    private String hostname = "localhost";
    private int port = 5038;
    private String username = "admin";
    private String password = "admin";
    private String curCDRtable = "curcdr_20";
    private boolean log2console = true;
    private boolean logallevents = true;
    private String[] supportedEvents = {};

    private void log(String message) {
        if (this.log2console) {
            System.out.println("---------------------------------------------");
            System.out.println(message);
            System.out.println("---------------------------------------------");
        }
        logger.debug(message);

    }

    @Override
    public void startup() throws Exception {
        log("Prepare startup Event Listener ");

        ManagerConnectionFactory factory = new ManagerConnectionFactory(hostname, port, username, password);
        this.managerConnection = factory.createManagerConnection();

        // connect to Asterisk and log in
        managerConnection.login();

        managerConnection.registerUserEventClass(WorldfoneUserEvent.class);

        // register for events listener
        managerConnection.addEventListener(this);

        asteriskServer = new DefaultAsteriskServer(managerConnection);

        log("Manager Event Listener has startup");

    }

    @Override
    public void shutdown() throws Exception {
        if (managerConnection != null) {
            //remove events listener
            managerConnection.removeEventListener(this);
            // and finally log off and disconnect
            if ((managerConnection.getState() == CONNECTED)
                    || (managerConnection.getState() == RECONNECTING)) {
                managerConnection.logoff();
            }
            log("Manager Event Listener has shutdown");
        }
    }
    private boolean filterEvent(ManagerEvent event) {
        return Arrays.asList(supportedEvents).contains(event.getClass().getSimpleName());
    }

    @Override
    public void onManagerEvent(ManagerEvent me) {
        Runnable task = () -> {
            if (filterEvent(me)) {
                if (this.logallevents) {
                    log(me.toString());
                }
            }
        };

        Thread thread = new Thread(task);
        thread.start();

    }

    @Override
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    @Override
    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public void setUsername(String username) {
        this.username = username;
    }

    @Override
    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public void setLog2consolse(boolean log2console) {
        this.log2console = log2console;
    }
    
    @Override
    public void setLogallevents(boolean logallevents) {
        this.logallevents = logallevents;
    }

    @Override
    public void setCurCDRtable(String curCDRtable) {
        this.curCDRtable = curCDRtable;
    }

    @Override
    public void setSupportedEvent(String[] events) {
        this.supportedEvents = events;
    }

    @Override
    public void setQueuehost(String queuehost) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
