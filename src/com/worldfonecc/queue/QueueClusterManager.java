/*
 * Copyright © 2014 South Telecom
 */
package com.worldfonecc.queue;

import dk.safl.beanstemc.Beanstemc;
import java.io.IOException;
import java.util.ArrayList;

/**
 *
 * @author nguyenngocbinh
 */
public class QueueClusterManager {

    final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("CrmEvent");

    final static ArrayList<QueueNode> queueCluster = QueueClusterManager.initCluster();
    private static int currentIndex = -1;

    public QueueClusterManager() {

    }

    public void printQueues() {
        for (int i = 0; i < queueCluster.size(); i++) {
            System.out.printf("Server %d : host : %s port : %d status: %d", i, queueCluster.get(i).getHost(), queueCluster.get(i).getPort(), queueCluster.get(i).getStatus());
            System.out.println();
        }
    }

    private static ArrayList<QueueNode> initCluster() {
        ArrayList<QueueNode> queueCluster0 = new ArrayList<>();
        QueueConfig queueConfig = new QueueConfig();
        String[] hostArray = queueConfig.queuehosts.split(",");
        for (String host : hostArray) {
            String[] hostParams = host.split(":");
            int port = 11300;
            if (2 == hostParams.length) {
                try {
                    port = Integer.parseInt(hostParams[1]);
                } catch (NumberFormatException ne) {
                    port = 11300;
                }
            }
            QueueNode queue = new QueueNode(hostParams[0], port);
            queueCluster0.add(queue);
        }
        System.out.println("Init Cluster " + queueCluster0.toString());
        return queueCluster0;
    }

    public synchronized Beanstemc getQueueNode() throws IOException {
        if (queueCluster.isEmpty()) {
            return null;
        }
        currentIndex++;
        if (currentIndex >= queueCluster.size()) {
            currentIndex = 0;
        }
        int lastIndex = currentIndex;
        Beanstemc beanstalk = null;

        for (int i = 0; i < queueCluster.size(); i++) {
            currentIndex = lastIndex + i;
            if (currentIndex >= queueCluster.size()) {
                currentIndex = currentIndex % queueCluster.size();
            }

            if (queueCluster.get(currentIndex).getStatus() <= (System.currentTimeMillis() / 1000)) {
                try {
                    beanstalk = new Beanstemc(queueCluster.get(currentIndex).getHost(), queueCluster.get(currentIndex).getPort());
                    break;
                } catch (IOException ex) {
                    logger.error(ex);
                    queueCluster.get(currentIndex).setStatus((System.currentTimeMillis() / 1000) + 300);
                }
            }
        }

        if (beanstalk != null) {
            queueCluster.get(currentIndex).setStatus(0);
        }
        return beanstalk;
    }
}
