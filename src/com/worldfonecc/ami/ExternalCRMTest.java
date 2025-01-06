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
import org.asteriskjava.manager.event.BridgeEvent;
import org.asteriskjava.manager.event.CdrEvent;
import org.asteriskjava.manager.event.DialEvent;
import org.asteriskjava.manager.event.HangupEvent;
import org.asteriskjava.manager.event.NewStateEvent;
import org.asteriskjava.manager.event.VarSetEvent;

/**
 *
 * @author nguyenngocbinh
 */
public class ExternalCRMTest {

    static public String PIDFILE = "/externalcrmeventtest.pid";

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
        ExternalCRMTest.touch(System.getProperty("user.dir") + ExternalCRMTest.PIDFILE);

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
        String[][] events = {
            {BridgeEvent.class.getSimpleName()},
            {DialEvent.class.getSimpleName()},
            {CdrEvent.class.getSimpleName(), HangupEvent.class.getSimpleName()},
            {NewStateEvent.class.getSimpleName(), VarSetEvent.class.getSimpleName()}
        };
        ManagerEventListenerThread[] mThread = new ManagerEventListenerThread[events.length];
        for (int i = 0; i < mThread.length; i++) {
            TestManagerEventListener evtListener;
            evtListener = new TestManagerEventListener();
            evtListener.setHostname(rb.getString("ami.ip"));
            evtListener.setPort(Integer.parseInt(rb.getString("ami.port")));
            evtListener.setUsername(rb.getString("ami.user"));
            evtListener.setPassword(rb.getString("ami.password"));
            evtListener.setLog2consolse(true);
            evtListener.setLogallevents(true);
            if (rb.containsKey("curCDRtable")) {
                evtListener.setCurCDRtable(rb.getString("curCDRtable"));
            }
            String[] supportedEvents = events[i];
            evtListener.setSupportedEvent(supportedEvents);
            mThread[i] = new ManagerEventListenerThread(evtListener);
            mThread[i].startup();
        }

        while (ExternalCRMTest.isFileExists(System.getProperty("user.dir") + ExternalCRMTest.PIDFILE)) {
            Thread.sleep(1);
        }
        for (ManagerEventListenerThread mThread1 : mThread) {
            mThread1.shutdown();
        }
    }
}
