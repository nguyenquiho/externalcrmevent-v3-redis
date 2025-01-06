/*
 * Copyright © 2014 South Telecom
 */
package com.worldfonecc.ami;

import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author nguyenngocbinh
 */
public class ManagerEventListenerThread {

    final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("CrmEvent");
    private static AtomicLong idCounter = new AtomicLong();
    private AbstractWFManagerEventListener managerEventListener;
    private Thread thread;
    private boolean daemon = true;

    public ManagerEventListenerThread() {
        super();
    }

    public ManagerEventListenerThread(AbstractWFManagerEventListener managerEventListener) {
        super();
        this.managerEventListener = managerEventListener;
    }

    /**
     * Sets the WFManagerEventListener to run.
     * <p>
     * This property must be set before starting the ManagerEventListenerThread
     * by calling startup.
     *
     * @param managerEventListener the WFManagerEventListener to run.
     */
    public void setManagerEventListener(AbstractWFManagerEventListener managerEventListener) {
        this.managerEventListener = managerEventListener;
    }

    /**
     * Marks the thread as either a daemon thread or a user thread.
     * <p>
     * Default is <code>true</code>.
     *
     * @param daemon if <code>false</code>, marks the thread as a user thread.
     * @see Thread#setDaemon(boolean)
     * @since 0.3
     */
    public void setDaemon(boolean daemon) {
        this.daemon = daemon;
    }

    /**
     * Starts the ManagerEventListener in its own thread.
     * <p>
     * Note: The ManagerEventListenerThread is designed to handle one
     * ManagerEventListener instance at a time so calling this method twice
     * without stopping the ManagerEventListener in between will result in a
     * RuntimeException.
     *
     * @throws IllegalStateException if the mandatory property
     * managerEventListener has not been set or the ManagerEventListener had
     * already been started.
     * @throws RuntimeException if the ManagerEventListener can't be started due
     * to IO problems, for example because the socket has already been bound by
     * another process.
     */
    public synchronized void startup() throws IllegalStateException, RuntimeException {
        if (managerEventListener == null) {
            throw new IllegalStateException("Mandatory property managerEventListener is not set.");
        }

        if (thread != null) {
            throw new IllegalStateException("ManagerEventListener is already started");
        }

        thread = createThread();
        thread.start();
    }

    protected Thread createThread() {
        Thread t;

        t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    managerEventListener.startup();

                } catch (Exception e) {
                    throw new RuntimeException("Exception running ManagerEventListener.", e);
                }
            }
        });
        t.setName("WFManagerEventListener-" + idCounter.getAndIncrement());
        t.setDaemon(daemon);
        t.setUncaughtExceptionHandler(new AMIThreadUncaughtExceptionHanlder());

        return t;
    }

    /**
     * Stops the {@link WFManagerEventListener}.
     * <p>
     * The WFManagerEventListener must have been started by calling
     * {@link #run()} before you can stop it.
     *
     * @see WFManagerEventListener#stop()
     * @throws IllegalStateException if the mandatory property
     * managerEventListener has not been set or the WFManagerEventListener had
     * already been shut down.
     */
    public synchronized void shutdown() throws IllegalStateException {
        if (managerEventListener == null) {
            throw new IllegalStateException("Mandatory property managerEventListener is not set.");
        }

        try {
            managerEventListener.shutdown();
        } catch (Exception e) {
            logger.error("Error when shutdown WFManagerEventListener: " + e.getMessage());

        }

        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for WFManagerEventListener to shutdown.");
            }
            thread = null;
            //thread.interrupt();
        }
    }

    class AMIThreadUncaughtExceptionHanlder implements Thread.UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.error("Uncaught exception in ManagerEventListenerThread", e);
        }
    }

}
