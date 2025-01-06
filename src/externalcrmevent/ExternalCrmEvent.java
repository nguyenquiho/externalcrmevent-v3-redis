/*
 * Copyright © 2014 South Telecom
 */
package externalcrmevent;

import java.util.Arrays;

/**
 *
 * @author nguyenngocbinh
 */
public class ExternalCrmEvent {

    /**
     * @param args the command line arguments
     */
    private void filterEvent(String event) {
        String[] supportedEvents = {"abc"};
        if (Arrays.asList(supportedEvents).contains(event)) {
            System.out.println(event + " is supported");
        } else {
            System.out.println(event + " is not supported");
        }
    }

    public static void main(String[] args) {
        ExternalCrmEvent crm = new ExternalCrmEvent();
        crm.filterEvent("abc");
        crm.filterEvent("dev");
        crm.filterEvent("123");
        crm.filterEvent("12");
        crm.filterEvent("1234");
    }

}
