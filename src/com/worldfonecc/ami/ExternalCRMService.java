/*
 * Copyright © 2014 South Telecom
 */
package com.worldfonecc.ami;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Locale;
import java.util.ResourceBundle;
import org.asteriskjava.manager.event.*;

/**
 *
 * @author nguyenngocbinh
 */
public class ExternalCRMService {

    static public String PIDFILE = "/externalcrmevent.pid";

    public static void touch(String filePath) {
        ExternalCRMService.touch(new File(filePath));
    }

    public static void touch(File file) {
        long timestamp = System.currentTimeMillis();
        ExternalCRMService.touch(file, timestamp);
    }

    public static void touch(File file, long timestamp) {
        try {
            if (!file.exists()) {
                new FileOutputStream(file).close();
            }
            file.setLastModified(timestamp);
        } catch (IOException e) {
        }
    }

    public static boolean delete(String filePath) {
        File file = new File(filePath);
        return file.delete();
    }

    public static boolean isFileExists(String filePath) {
        File file = new File(filePath);
        return file.exists();
    }

    public static void main(String[] args) throws Exception {
        ExternalCRMService.touch(System.getProperty("user.dir") + ExternalCRMService.PIDFILE);

        File file = new File(System.getProperty("user.dir"));
        URL[] urls = {file.toURI().toURL()};
        ClassLoader loader = new URLClassLoader(urls);
        ResourceBundle rb = ResourceBundle.getBundle("PBX", Locale.getDefault(), loader);
        /*
        <property name="hostname" value="${ami.ip}" />
        <property name="port" value="${ami.port}" />
        <property name="username" value="${ami.user}" />
        <property name="password" value="${ami.password}" />
        <property name="dbhost" value="${db.host}" />
        <property name="dbname" value="${db.name}" />
        <property name="dbuser" value="${db.user}" />
        <property name="dbpass" value="${db.pass}" />
         */
        String[][] events_multiconn = {
            {BridgeEvent.class.getSimpleName()},
            {DialEvent.class.getSimpleName()},
            {CdrEvent.class.getSimpleName(), HangupEvent.class.getSimpleName()},
            {NewStateEvent.class.getSimpleName(), VarSetEvent.class.getSimpleName()},
            {PeerStatusEvent.class.getSimpleName()},
            {TransferEvent.class.getSimpleName()},
            {ExtensionStatusEvent.class.getSimpleName()}
        };

        String[][] events_singleconn = {
            {
                CelEvent.class.getSimpleName(),
                BridgeEvent.class.getSimpleName(),
                DialEvent.class.getSimpleName(),
                CdrEvent.class.getSimpleName(),
                HangupRequestEvent.class.getSimpleName(),
                HangupEvent.class.getSimpleName(),
                NewStateEvent.class.getSimpleName(),
                OriginateResponseEvent.class.getSimpleName(),
                VarSetEvent.class.getSimpleName(),
                PeerStatusEvent.class.getSimpleName(),
                TransferEvent.class.getSimpleName(),
                ExtensionStatusEvent.class.getSimpleName(),
                MusicOnHoldEvent.class.getSimpleName(),
                QueueMemberStatusEvent.class.getSimpleName(),
                QueueMemberPauseEvent.class.getSimpleName()
            }
        };
        String[][] events;

        if (rb.containsKey("ami.multiconn") && "true".equalsIgnoreCase(rb.getString("ami.multiconn"))) {
            events = events_multiconn;
        } else {
            events = events_singleconn;
        }
        ManagerEventListenerThread[] mThread = new ManagerEventListenerThread[events.length];

        for (int i = 0; i < events.length; i++) {
            WFManagerEventListener evtListener;
            evtListener = new WFManagerEventListener();
            evtListener.setHostname(rb.getString("ami.ip"));
            evtListener.setPort(Integer.parseInt(rb.getString("ami.port")));
            evtListener.setUsername(rb.getString("ami.user"));
            evtListener.setPassword(rb.getString("ami.password"));
            if (rb.containsKey("curCDRtable")) {
                evtListener.setCurCDRtable(rb.getString("curCDRtable"));
            }
            if (rb.containsKey("log2console")) {
                evtListener.setLog2consolse(Boolean.valueOf(rb.getString("log2console")));
            }
            if (rb.containsKey("logallevents")) {
                evtListener.setLogallevents(Boolean.valueOf(rb.getString("logallevents")));
            }
            if (rb.containsKey("queuehost")) {
                evtListener.setQueuehost(rb.getString("queuehost"));
            }

            String[] supportedEvents = events[i];
            evtListener.setSupportedEvent(supportedEvents);

            mThread[i] = new ManagerEventListenerThread(evtListener);
            mThread[i].startup();
        }

        while (ExternalCRMService.isFileExists(System.getProperty("user.dir") + ExternalCRMService.PIDFILE)) {
            Thread.sleep(0, 1000);
        }
        for (ManagerEventListenerThread mThread1 : mThread) {
            mThread1.shutdown();
        }

    }
}
