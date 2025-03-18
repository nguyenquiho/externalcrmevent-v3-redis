/*
 * Copyright © 2014 South Telecom
 */
package com.worldfonecc.ami;

import com.worldfonecc.access.DBSelectionSet;
import com.worldfonecc.access.MemcacheConnection;
import com.worldfonecc.access.MemcacheConnection2;
import com.worldfonecc.callback.Callback;
import static com.worldfonecc.helpers.Common.diffTime;
import com.worldfonecc.queue.JobInternal;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.codec.binary.Base64;
import org.asteriskjava.live.DefaultAsteriskServer;
import org.asteriskjava.manager.ManagerConnection;
import org.asteriskjava.manager.ManagerConnectionFactory;
import static org.asteriskjava.manager.ManagerConnectionState.CONNECTED;
import static org.asteriskjava.manager.ManagerConnectionState.RECONNECTING;
import org.asteriskjava.manager.ManagerEventListener;
import org.asteriskjava.manager.event.BridgeEvent;
import org.asteriskjava.manager.event.CdrEvent;
import org.asteriskjava.manager.event.CelEvent;
import org.asteriskjava.manager.event.DialEvent;
import org.asteriskjava.manager.event.ExtensionStatusEvent;
import org.asteriskjava.manager.event.HangupEvent;
import org.asteriskjava.manager.event.HangupRequestEvent;
import org.asteriskjava.manager.event.LocalBridgeEvent;
import org.asteriskjava.manager.event.ManagerEvent;
import org.asteriskjava.manager.event.MusicOnHoldEvent;
import org.asteriskjava.manager.event.NewStateEvent;
import org.asteriskjava.manager.event.OriginateResponseEvent;
import org.asteriskjava.manager.event.PeerStatusEvent;
import org.asteriskjava.manager.event.QueueMemberPauseEvent;
import org.asteriskjava.manager.event.QueueMemberStatusEvent;
import org.asteriskjava.manager.event.TransferEvent;
import org.asteriskjava.manager.event.VarSetEvent;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author nguyenngocbinh
 * @deverloper lechidai
 */
public class WFManagerEventListener extends AbstractWFManagerEventListener implements ManagerEventListener {

    final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("CrmEvent");
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");

    private ManagerConnection managerConnection;
    private DefaultAsteriskServer asteriskServer;

    private String hostname = "127.0.0.1";
    private int port = 5038;
    private String username = "admin";
    private String password = "Admin@Stel7779";

    private boolean log2console = false;
    private String curCDRtable = "curcdr";
    private boolean logallevents = false;
    private String queuehost = "127.0.0.1";

    private String[] supportedEvents = new String[0];

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

    private void log(String message) {
        if (this.log2console) {
            System.out.println("---------------------------------------------");
            System.out.println(String.format("%s::[%s]::%s", sdf.format(System.currentTimeMillis()), Thread.currentThread().getName(), message));
        }
        logger.debug(message);
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
        this.queuehost = queuehost;
    }

    @Override
    public void startup() throws Exception {
        log("Prepare startup Event Listener ");
        ManagerConnectionFactory factory = new ManagerConnectionFactory(hostname, port, username, password);
        managerConnection = factory.createManagerConnection();
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
            if ((managerConnection.getState() == CONNECTED) || (managerConnection.getState() == RECONNECTING)) {
                managerConnection.logoff();
            }
            log("Manager Event Listener has shutdown");
        }
    }

    public void restart() throws Exception {
        shutdown();
        startup();
    }

    private boolean filterEvent(String eventName) {
        return Arrays.asList(supportedEvents).contains(eventName);
    }

    @Override
    public void onManagerEvent(ManagerEvent event) {
        Runnable task = () -> {
            String eventName = event.getClass().getSimpleName();
//            System.out.println("TIG " + eventName);

            if (filterEvent(eventName)) {
                long receivedTimestamp0 = System.currentTimeMillis() / 1000;
                switch (eventName) {
                    case "CelEvent":
                        handleCelEvent((CelEvent) event);
                        break;
                    case "ExtensionStatusEvent":
                        handleExtensionStatusEvent((ExtensionStatusEvent) event);
                        break;
                    case "DialEvent":
                        handleDialEvent((DialEvent) event, receivedTimestamp0);
                        break;
                    case "NewStateEvent":
                        handleNewStateEvent((NewStateEvent) event, receivedTimestamp0);
                        break;
                    case "BridgeEvent":
                        handleBridgeEvent((BridgeEvent) event, receivedTimestamp0);
                        break;
                    case "CdrEvent":
                        handleCdrEvent((CdrEvent) event, receivedTimestamp0);
                        break;
                    case "HangupEvent":
                        handleHangupEvent((HangupEvent) event, receivedTimestamp0);
                        break;
                    case "HangupRequestEvent":
                        handleHangupRequestEvent((HangupRequestEvent) event);
                        break;
                    case "VarSetEvent":
                        handleVarSetEvent((VarSetEvent) event);
                        handleSynchronization(receivedTimestamp0);
                        break;
                    case "OriginateResponseEvent":
                        handleOriginateResponseEvent((OriginateResponseEvent) event);
                        break;
                    case "PeerStatusEvent":
                        handlePeerStatusEvent((PeerStatusEvent) event, receivedTimestamp0);
                        break;
                    case "LocalBridgeEvent":
                        handleLocalBridgeEvent((LocalBridgeEvent) event);
                        break;
                    case "TransferEvent":
                        handleTransferEvent((TransferEvent) event);
                        break;
                    case "MusicOnHoldEvent":
                        handleMusicOnHoldEvent((MusicOnHoldEvent) event);
                        break;
                    case "QueueMemberStatusEvent":
                        handleQueueMemberStatus((QueueMemberStatusEvent) event, receivedTimestamp0);
                        break;
                    case "QueueMemberPauseEvent":
                        handleQueueMemberPauseEvent((QueueMemberPauseEvent) event);
                        break;
                }
                if (this.logallevents) {
                    log(String.format("EVENT NAME]: %s. [DETAIL]: %s", event.getClass().getSimpleName(), event.toString()));
                } else {
                    log(String.format("[PROCESS EVENT] %s:", event.getClass().getSimpleName()));
                }
            }
        };
        Thread thread = new Thread(task);
        thread.start();
    }

    private void handleSynchronization(long receivedTimestamp0) {
        Object oCache = MemcacheConnection.getInstance().get(this.getClass().getCanonicalName());
        if (oCache == null) {
            MemcacheConnection.getInstance().set(this.getClass().getCanonicalName(), 1, new Date(System.currentTimeMillis() + 300000)); //5 * 60 *1000 -> 5phut
            HashMap<String, String> customerList = DBSelectionSet.getInstance().getCustomerList();
            customerList.keySet().forEach((customer_code) -> {
                String version = DBSelectionSet.getInstance().getVersion(customer_code);
                if (version != null && ("4".equals(version) || "5".equals(version))) {
                    String calluuids = "";
                    List<JSONObject> CDRs = DBSelectionSet.getInstance().getCurCDR(customer_code, curCDRtable);
                    HashMap<String, String> site = DBSelectionSet.getInstance().getSiteByCustomer(customer_code);
                    ArrayList<String> dests = DBSelectionSet.getInstance().getDestBySite(site.get("site_id"), customer_code);
                    for (JSONObject CDR : CDRs) {
                        for (String dest : dests) {
                            try {
                                if ((CDR.has("src") && CDR.get("src").toString().toLowerCase().contains(dest.toLowerCase()))
                                        || (CDR.has("dst") && CDR.get("dst").toString().toLowerCase().contains(dest.toLowerCase()))
                                        || (CDR.has("srcchan") && CDR.get("srcchan").toString().toLowerCase().contains(dest.toLowerCase()))
                                        || (CDR.has("dstchan") && CDR.get("dstchan").toString().toLowerCase().contains(dest.toLowerCase()))) {
                                    calluuids = calluuids + ";" + CDR.get("srcuid").toString();
                                }
                            } catch (JSONException ex) {
                                Logger.getLogger(WFManagerEventListener.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }
                    }
                    if (!"".equals(calluuids)) {
                        calluuids = calluuids.substring(1);
                        try {
                            String url = site.get("method") + site.get("callback_url");
                            Callback c = new Callback(url, this.queuehost, this.log2console);
                            c.addParam("pbx_customer_code", site.get("customer_code"));
                            c.addParam("secret", site.get("secret"));
                            c.addParam("callstatus", "SyncCurCalls");
                            c.addParam("calluuids", calluuids);
                            c.addParam("version", version);
                            c.sendGet(receivedTimestamp0);
                        } catch (URISyntaxException ex) {
                            logger.error(ex);
                        }
                    }
                }
            });
        }
    }

    private void handleCelEvent(CelEvent ev) {
        try {
            JobInternal job = new JobInternal();
            job.addParam("event", "CEL");
            job.addParam("eventname", ev.getEventName());
            job.addParam("application", ev.getApplication());
            job.addParam("context", ev.getContext());
            job.addParam("accountcode", ev.getAccountCode());
            job.addParam("calleridnum", ev.getCallerIdNum());
            job.addParam("calleridname", ev.getCallerIdName());
            job.addParam("calleridani", ev.getCallerIDani());
            job.addParam("calleridrdnis", ev.getCallerIDrdnis());
            job.addParam("calleriddnid", ev.getCallerIDdnid());
            job.addParam("exten", ev.getExten());
            job.addParam("channel", ev.getChannel());
            job.addParam("eventtime", ev.getEventTime());
            job.addParam("uniqueid", ev.getUniqueID());
            job.addParam("linkedid", ev.getLinkedID());
            job.addParam("userfield", ev.getUserField());
            job.addParam("peer", ev.getPeer());
            job.addParam("peeraccount", ev.getPeerAccount());
            job.addParam("extra", ev.getExtra());
            job.addParam("curCDRtable", curCDRtable);
            job.QueuePut();
        } catch (Exception ex) {
            System.out.println(ex.getClass().getName() + ": " + ex.getMessage());
        }

        if ("CHAN_START".equalsIgnoreCase(ev.getEventName()) && !ev.getChannel().endsWith(";2")) {
            DBSelectionSet.getInstance().updateLastChannel(ev.getLinkedID(), ev.getChannel());
        } else if ("BRIDGE_ENTER".equalsIgnoreCase(ev.getEventName())
                && !ev.getChannel().endsWith(";2")
                && ("Dial".equals(ev.getApplication()) || "Queue".equals(ev.getApplication()))
                && ev.getPeer() != null
                && !"".equals(ev.getPeer())
                && ev.getPeer().startsWith("SIP")) {
            DBSelectionSet.getInstance().updateLastChannel(ev.getLinkedID(), ev.getPeer());
        } else if ("LINKEDID_END".equals(ev.getEventName())) {
            /* Gửi event cho Job CDR */
            try {
                JobInternal j = new JobInternal(this.log2console);
                j.addParam("event", "CDR");
                j.addParam("uniqueid", ev.getLinkedID());
                j.QueuePut("cdr_sync_event", 5);
            } catch (Exception e) {
                logger.error(e);
            }
            /* Gửi event cho Job CDR 2 */
            try {
                if (!ev.getLinkedID().equals(ev.getUniqueID())) {
                    JobInternal j2 = new JobInternal(this.log2console);
                    j2.addParam("event", "CDR");
                    j2.addParam("uniqueid", ev.getUniqueID());
                    j2.QueuePut("cdr_sync_event", 5);
                }
            } catch (Exception e) {
                logger.error(e);
            }

            DBSelectionSet.getInstance().delCustomercodeByCallid(ev.getLinkedID());
            DBSelectionSet.getInstance().deleteLastChannel(ev.getLinkedID());
        } else if (ev.getEventName().equalsIgnoreCase("HANGUP") && ev.getContext().contains("internal-crm") && ev.getChannel().endsWith(";2")) {
            /* Gửi event cho Job CDR */
            try {
                JobInternal j = new JobInternal(this.log2console);
                j.addParam("event", "CDR");
                j.addParam("uniqueid", ev.getUniqueID());
                j.QueuePut("cdr_sync_event", 5);
            } catch (Exception ex) {
                logger.error(ex);
            }
        }

    }

    private void handleNewStateEvent(NewStateEvent ev, long receivedTimestamp0) {
        if ("Ringing".equalsIgnoreCase(ev.getChannelStateDesc()) && DBSelectionSet.getInstance().checkNumber(ev.getConnectedLineNum())) {
            try {
                String datereceived = new SimpleDateFormat("yyyyMMdd'T'HHmmss").format(ev.getDateReceived());
                String[] temp1 = ev.getChannel().split("-");
                String[] temp2 = temp1[0].split("/");
                String customer_code = temp2[1].substring(0, 5);
                String version = DBSelectionSet.getInstance().getVersion(customer_code);
                if (!"1".equals(version) && DBSelectionSet.getInstance().checkExtension(ev.getCallerIdNum(), customer_code)) {
                    String parentcalluuid = DBSelectionSet.getInstance().getCalluuid(ev.getConnectedLineNum());
                    JSONObject jArray = null;
                    //Không lấy được data từ cache
                    HashMap<String, String> site = DBSelectionSet.getInstance().getSiteInfo(parentcalluuid, customer_code);
                    if (site.containsKey("params") && site.get("params") != null && !"".equals(site.get("params"))) {
                        jArray = new JSONObject(site.get("params"));
                    }
                    String calltype = site.get("calltype");
                    String url = site.get("method") + site.get("callback_url");
                    Callback c = new Callback(url, this.queuehost, this.log2console);
                    c.addParam("pbx_customer_code", site.get("customer_code"));
                    c.addParam("secret", site.get("secret"));
                    c.addParam("callstatus", "Dialing");
                    c.addParam("calluuid", ev.getUniqueId());
                    c.addParam("direction", "inbound");
                    c.addParam("callernumber", ev.getConnectedLineNum());
                    c.addParam("destinationnumber", ev.getCallerIdNum());
                    c.addParam("starttime", datereceived);
                    c.addParam("parentcalluuid", parentcalluuid);
                    c.addParam("calltype", calltype);
                    c.addParam("agentname", ev.getCallerIdName());
                    
                    // add header to 4x
                    HashMap<String, String> params_to_4x = DBSelectionSet.getInstance().getParamsTo4x(parentcalluuid, site.get("customer_code"));
                    if(params_to_4x != null){
                        String param_phonenumber = params_to_4x.get("param_phonenumber");
                        String param_name = params_to_4x.get("param_name");
                        String param_email = params_to_4x.get("param_email");
                        c.addParam("param_phonenumber", param_phonenumber);
                        c.addParam("param_name", param_name);
                        c.addParam("param_email", param_email);
                    }

                    if (jArray != null) {
                        if (jArray.has("diallist_id")) {
                            c.addParam("diallist_id", jArray.getString("diallist_id"));
                        }
                        if (jArray.has("dnis")) {
                            c.addParam("dnis", jArray.getString("dnis"));
                        }
                        if (jArray.has("try_count")) {
                            c.addParam("try_count", jArray.getString("try_count"));
                        }
                        if (jArray.has("dialid")) {
                            c.addParam("dialid", jArray.getString("dialid"));
                        }
                        if (jArray.has("dial_type")) {
                            c.addParam("dial_type", jArray.getString("dial_type"));
                        }
                    }
                    c.addParam("partner_id", "2");
                    c.addParam("version", version);
                    c.sendGet(receivedTimestamp0);

                    datereceived = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ev.getDateReceived());
                    DBSelectionSet.getInstance().logExtension(ev.getUniqueId(), "inbound", site.get("site_id"), parentcalluuid, datereceived, customer_code);
                }
            } catch (URISyntaxException | JSONException ex) {
                logger.error(ex);
            }
        }
    }

    private void handleDialEvent(DialEvent ev, long receivedTimestamp0) {
        if ("Begin".equals(ev.getSubEvent())) {
            String customer_code = "*";
            Pattern p = Pattern.compile("^SIP/(C([0-9]{4}))([0-9]+)-(.*)$");
            if (ev.getChannel() != null) {
                Matcher m = p.matcher(ev.getChannel());
                if (m.matches()) {
                    customer_code = m.group(1);
                }
                if (DBSelectionSet.getInstance().checkCall(ev.getUniqueId(), customer_code)) {
                    HashMap<String, String> site = DBSelectionSet.getInstance().getSiteInfo(ev.getUniqueId(), customer_code);
                    if (site.containsKey("customer_code")) {
                        String version = DBSelectionSet.getInstance().getVersion(site.get("customer_code"));
                        if ("1".equals(version)) {
                            try {
                                String url = site.get("method") + site.get("callback_url");
                                Callback c = new Callback(url, this.queuehost, this.log2console);
                                c.addParam("pbx_customer_code", site.get("customer_code"));
                                c.addParam("secret", site.get("secret"));
                                c.addParam("callstatus", "Dialing");
                                c.addParam("calluuid", ev.getUniqueId());
                                c.addParam("callernumber", ev.getCallerIdNum());
                                c.addParam("partner_id", "2");
                                
                                // add header to 4x
                                HashMap<String, String> params_to_4x = DBSelectionSet.getInstance().getParamsTo4x(ev.getUniqueId(), site.get("customer_code"));
                                if(params_to_4x != null){
                                    String param_phonenumber = params_to_4x.get("param_phonenumber");
                                    String param_name = params_to_4x.get("param_name");
                                    String param_email = params_to_4x.get("param_email");
                                    c.addParam("param_phonenumber", param_phonenumber);
                                    c.addParam("param_name", param_name);
                                    c.addParam("param_email", param_email);
                                }
                                
                                c.sendGet(receivedTimestamp0);
                            } catch (URISyntaxException ex) {
                                logger.error(ex);
                            }

                            String update_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ev.getDateReceived());
                            DBSelectionSet.getInstance().updateCall(ev.getUniqueId(), "Dialing", update_date, customer_code);
                        }
                    }
                }
            }
        }
    }

    private void handleVarSetEvent(VarSetEvent ev) {
        if ("MONITOR_FILENAME".equalsIgnoreCase(ev.getVariable()) || "customer_code".equalsIgnoreCase(ev.getVariable())) {
            JobInternal j = new JobInternal(this.log2console);
            String currentDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ev.getDateReceived());
            j.addParam("event", "VarSet");
            j.addParam("varname", ev.getVariable());
            j.addParam("uniqueid", ev.getUniqueId());
            j.addParam("datereceived", currentDateTime);
            j.addParam("value", ev.getValue());
            j.addParam("channel", ev.getChannel());
            j.addParam("curCDRtable", curCDRtable);
            j.QueuePut();

            if ("customer_code".equals(ev.getVariable())) {
                DBSelectionSet.getInstance().setCustomercodeByCallid(ev.getUniqueId(), ev.getValue());
            }
        } else if ("BLINDTRANSFER".equalsIgnoreCase(ev.getVariable())) {
            String channel = ev.getChannel();
            String value = ev.getValue();
            String uniqueid = ev.getUniqueId();
            DBSelectionSet.getInstance().insertBlindTransfer(channel, value, uniqueid);
        }
    }

    private void handleBridgeEvent(BridgeEvent ev, long receivedTimestamp0) {
        if (ev.isLink() && !ev.getChannel1().endsWith(";2")) {
            try {
                String answertime = new SimpleDateFormat("yyyyMMdd'T'HHmmss").format(ev.getDateReceived());
                String key_cache_answered = "PBX_Event_handleBridgeEvent_" + ev.getUniqueId1();
                Object oCache = MemcacheConnection.getInstance().get(key_cache_answered);

                String customer_code = "*";
                if (ev.getChannel1().contains("Local")) {
                    Pattern p = Pattern.compile("^Local/([0-9]+)@(C([0-9]+))-from-internal-(.*);1$");
                    Matcher m = p.matcher(ev.getChannel1());
                    if (m.matches()) {
                        customer_code = m.group(2);
                    }
                } else {
                    Pattern p = Pattern.compile("^SIP/(C([0-9]{4}))([0-9]+)-(.*)$");
                    Matcher m;
                    if (ev.getChannel1().contains("trunk")) {
                        m = p.matcher(ev.getChannel2());
                    } else {
                        m = p.matcher(ev.getChannel1());
                    }
                    if (m.matches()) {
                        customer_code = m.group(1);
                    } else {
                        customer_code = DBSelectionSet.getInstance().getCustomercodeByCallid(ev.getUniqueId1());
                        if (customer_code == null || "".equals(customer_code)) {
                            String ccode = DBSelectionSet.getInstance().getCustomercodeNumber(ev.getCallerId1());
                            if (ccode != null && !"".equals(ccode)) {
                                customer_code = ccode;
                            }
                        }
                    }
                }

                if (oCache == null && DBSelectionSet.getInstance().checkCallNotAnswer(ev.getUniqueId1(), customer_code)) {
                    MemcacheConnection.getInstance().set(key_cache_answered, ev.getUniqueId2(), new Date(System.currentTimeMillis() + 3000)); //2s
                    JSONObject jArray = null;
                    String update_params = null;
                    HashMap<String, String> site = DBSelectionSet.getInstance().getSiteInfo(ev.getUniqueId1(), customer_code);
                    if (site != null && site.get("params") != null && !site.get("params").isEmpty()) {
                        jArray = new JSONObject(site.get("params"));
                        jArray.put("destinationnumber", ev.getCallerId2());
                        jArray.put("callernumber", ev.getCallerId1());
                        update_params = jArray.toString();
                    }
                    if (site != null) {
                        String version = DBSelectionSet.getInstance().getVersion(site.get("customer_code"));
                        String url = site.get("method") + site.get("callback_url");
                        Callback c = new Callback(url, this.queuehost, this.log2console);
                        c.addParam("pbx_customer_code", site.get("customer_code"));
                        // add header to 4x
                        HashMap<String, String> params_to_4x = DBSelectionSet.getInstance().getParamsTo4x(ev.getUniqueId1(), site.get("customer_code"));
                        if(params_to_4x != null){
                            String param_phonenumber = params_to_4x.get("param_phonenumber");
                            String param_name = params_to_4x.get("param_name");
                            String param_email = params_to_4x.get("param_email");
                            c.addParam("param_phonenumber", param_phonenumber);
                            c.addParam("param_name", param_name);
                            c.addParam("param_email", param_email);
                        }
                        switch (version) {
                            case "1":
                                c.addParam("secret", site.get("secret"));
                                c.addParam("callstatus", "DialAnswer");
                                c.addParam("calluuid", ev.getUniqueId1());
                                c.addParam("destinationnumber", ev.getCallerId2());
                                break;
                            default:
                                c.addParam("secret", site.get("secret"));
                                c.addParam("callstatus", "DialAnswer");
                                c.addParam("calluuid", ev.getUniqueId1());
                                c.addParam("childcalluuid", ev.getUniqueId2());
                                c.addParam("answertime", answertime);
                                c.addParam("direction", site.get("direction"));
                                try {
                                    if ("inbound".equals(site.get("direction"))) {
                                        c.addParam("callernumber", ev.getCallerId1());
                                        c.addParam("destinationnumber", ev.getCallerId2());
                                        
                                        String channlel2 = ev.getChannel2();
                                        String[] chandest = channlel2.split("-");
                                        if (Pattern.matches("^SIP/" + site.get("customer_code") + "([0-9]+)$", chandest[0])) {
                                            String ext = chandest[0].replace("SIP/" + site.get("customer_code"), "");
                                            c.addParam("extension", ext); 
                                        } else if (Pattern.matches("^Local/([0-9]+)@" + site.get("customer_code") + "$", chandest[0])) {
                                            String ext = chandest[0].replaceAll("^Local/([0-9]+)@" + site.get("customer_code") + "$", "$1");
                                            c.addParam("extension", ext);
                                        }
                                    } else {
                                        String ext = "";
                                        String channlel1 = ev.getChannel1();
                                        String[] chansrc = channlel1.split("-");

                                        if (Pattern.matches("^SIP/" + site.get("customer_code") + "([0-9]+)$", chansrc[0])) {
                                            ext = chansrc[0].replace("SIP/" + site.get("customer_code"), "");
                                        } else if (Pattern.matches("^Local/([0-9]+)@" + site.get("customer_code") + "$", chansrc[0])) {
                                            ext = chansrc[0].replaceAll("^Local/([0-9]+)@" + site.get("customer_code") + "$", "$1");
                                        }
                                        c.addParam("callernumber", ev.getCallerId2());
                                        c.addParam("destinationnumber", ext);
                                        c.addParam("extension", ext);
                                    }
                                } catch (Exception ex) {
                                    System.out.println(ex.getMessage());
                                }
                                if (jArray != null) {
                                    if (jArray.has("diallist_id")) {
                                        c.addParam("diallist_id", jArray.getString("diallist_id"));
                                    }
                                    if (jArray.has("dnis")) {
                                        c.addParam("dnis", jArray.getString("dnis"));
                                    }
                                    if (jArray.has("try_count")) {
                                        c.addParam("try_count", jArray.getString("try_count"));
                                    }
                                    if (jArray.has("dialid")) {
                                        c.addParam("dialid", jArray.getString("dialid"));
                                    }
                                    if (jArray.has("dial_type")) {
                                        c.addParam("dial_type", jArray.getString("dial_type"));
                                    }
                                }
                                c.addParam("version", version);
                        }
                        c.addParam("partner_id", "2");
                        c.sendGet(receivedTimestamp0);

                        String answertime_update = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ev.getDateReceived());
                        if (update_params != null) {
                            DBSelectionSet.getInstance().updateCallDialAnswer(ev.getUniqueId1(), "DialAnswer", ev.getUniqueId2(), update_params, answertime_update, customer_code);
                        } else {
                            DBSelectionSet.getInstance().updateCallDialAnswer(ev.getUniqueId1(), "DialAnswer", ev.getUniqueId2(), answertime_update, customer_code);
                        }
                    }
                } else {
                    MemcacheConnection.getInstance().delete(key_cache_answered);
                }
            } catch (URISyntaxException | JSONException ex) {
                logger.error(ex);
            }
        }
    }

    private void handleLocalBridgeEvent(LocalBridgeEvent ev) {
        MemcacheConnection.getInstance().set("LocalBridgeEvent_" + ev.getUniqueId2(), ev.getUniqueId1(), new Date(System.currentTimeMillis() + 1800000));
    }

    private void handleHangupRequestEvent(HangupRequestEvent ev) {
        if (!ev.getChannel().endsWith(";2")) {
            String hangup_key = "hangupby_request_" + ev.getLinkedId();
            MemcacheConnection.getInstance().set(hangup_key, ev.getChannel(), new Date(System.currentTimeMillis() + 5000));
        }
    }

    private void handleHangupEvent(HangupEvent ev, long receivedTimestamp0) {
        try {
            String customer_code = "*";
            if (ev.getChannel().contains("Local")) {
                Pattern p = Pattern.compile("^Local/([0-9]+)@(C([0-9]+))-from-internal-(.*);1$");
                Matcher m = p.matcher(ev.getChannel());
                if (m.matches()) {
                    customer_code = m.group(2);
                }
            } else {
                Pattern p = Pattern.compile("^SIP/(C([0-9]{4}))([0-9]+)-(.*)$");
                Matcher m = p.matcher(ev.getChannel());
                if (m.matches()) {
                    customer_code = m.group(1);
                } else {
                    customer_code = DBSelectionSet.getInstance().getCustomercodeByCallid(ev.getUniqueId());
                    if (customer_code == null || "".equals(customer_code)) {
                        String ccode = DBSelectionSet.getInstance().getCustomercodeNumber(ev.getCallerIdNum());
                        if (ccode != null && !"".equals(ccode)) {
                            customer_code = ccode;
                        }
                    } else {
                        DBSelectionSet.getInstance().delCustomercodeByCallid(ev.getUniqueId());
                    }
                }
            }
            if (!"*".equals(customer_code)) {
                String childcalluuid = DBSelectionSet.getInstance().checkCallHangup(ev.getUniqueId(), customer_code);
                if (!"".equals(childcalluuid)) {
                    JSONObject jArray = null;
                    HashMap<String, String> site = DBSelectionSet.getInstance().getSiteInfo(ev.getUniqueId(), customer_code);
                    if (site != null && site.get("params") != null && !"".equals(site.get("params"))) {
                        jArray = new JSONObject(site.get("params"));
                    }
                    if (site != null) {
                        String update_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ev.getDateReceived());
                        String version = DBSelectionSet.getInstance().getVersion(site.get("customer_code"));
                        String datereceived = new SimpleDateFormat("yyyyMMdd'T'HHmmss").format(ev.getDateReceived());
                        String url = site.get("method") + site.get("callback_url");
                        
                        // add header to 4x
                        String param_phonenumber = null;
                        String param_name = null;
                        String param_email = null;
                        HashMap<String, String> params_to_4x = DBSelectionSet.getInstance().getParamsTo4x(ev.getUniqueId(), site.get("customer_code"));
                        if(params_to_4x != null){
                            param_phonenumber = params_to_4x.get("param_phonenumber");
                            param_name = params_to_4x.get("param_name");
                            param_email = params_to_4x.get("param_email");
                        }else{
//                            System.out.println("KKKKKKKKKKKKKKKKK");
                        }
//                        System.out.println("VERSIONNNNNNNN " + version);
//                        System.out.println("UNIQUEIDDDDDDD " + ev.getUniqueId());
//                        System.out.println("param_name " + param_name);
//                        System.out.println("param_email " + param_email);

                        if ("1".equals(version)) {
                            Callback c = new Callback(url, this.queuehost, this.log2console);
                            c.addParam("pbx_customer_code", site.get("customer_code"));
                            c.addParam("secret", site.get("secret"));
                            c.addParam("callstatus", "HangUp");
                            c.addParam("calluuid", ev.getUniqueId());
                            c.addParam("datereceived", datereceived);
                            c.addParam("causetxt", ev.getCause().toString());
                            c.addParam("partner_id", "2");
                            c.sendGet(receivedTimestamp0);
                        } else if (DBSelectionSet.getInstance().checkUuid(ev.getUniqueId(), customer_code)) {
                            Callback c = new Callback(url, this.queuehost, this.log2console);
                            c.addParam("pbx_customer_code", site.get("customer_code"));
                            c.addParam("secret", site.get("secret"));
                            c.addParam("callstatus", "Trim");
                            c.addParam("calluuid", ev.getUniqueId());
                            c.addParam("partner_id", "2");
                            c.addParam("version", version);
                            c.sendGet(receivedTimestamp0);
                        } else {
                            Callback c = new Callback(url, this.queuehost, this.log2console);
                            c.addParam("pbx_customer_code", site.get("customer_code"));
                            c.addParam("secret", site.get("secret"));
                            c.addParam("callstatus", "HangUp");
                            c.addParam("calluuid", ev.getUniqueId());
                            c.addParam("datereceived", datereceived);
                            c.addParam("causetxt", ev.getCause().toString());
                            if (Integer.parseInt(version) >= 4) {
                                byte[] encodedBytes = Base64.encodeBase64(site.get("connect_info").getBytes());
                                String context = new String(encodedBytes);
                                c.addParam("context", context);
                            }
                            c.addParam("direction", site.get("direction"));
                            if (jArray != null) {
                                if (jArray.has("diallist_id")) {
                                    c.addParam("diallist_id", jArray.getString("diallist_id"));
                                }
                                if (jArray.has("dnis")) {
                                    c.addParam("dnis", jArray.getString("dnis"));
                                }
                                if (jArray.has("try_count")) {
                                    c.addParam("try_count", jArray.getString("try_count"));
                                }
                                if (jArray.has("dialid")) {
                                    c.addParam("dialid", jArray.getString("dialid"));
                                }
                                if (jArray.has("dial_type")) {
                                    c.addParam("dial_type", jArray.getString("dial_type"));
                                }
                                if (jArray.has("callernumber")) {
                                    c.addParam("callernumber", jArray.getString("callernumber"));
                                }
                                if (jArray.has("destinationnumber")) {
                                    c.addParam("destinationnumber", jArray.getString("destinationnumber"));
                                }
                            }
                            
                            c.addParam("param_phonenumber", param_phonenumber);
                            c.addParam("param_name", param_name);
                            c.addParam("param_email", param_email);
                            
                            c.addParam("partner_id", "2");
                            c.addParam("version", version);
                            c.sendGet(receivedTimestamp0);

                            Object data_cache_transfer = MemcacheConnection2.getInstance().get("cache_blindtransfer_" + ev.getUniqueId());
                            if (data_cache_transfer == null) {
                                DBSelectionSet.getInstance().deleteCallLog(ev.getConnectedLineNum());
                                DBSelectionSet.getInstance().deleteCallLog(ev.getCallerIdNum());
                            }
                        }
                        DBSelectionSet.getInstance().updateCall(ev.getUniqueId(), "HangUp", update_date, customer_code);
                    }else{
//                        System.out.println("HIHIIIIIIIIIIIIII");
                    }
                }
            }
        } catch (URISyntaxException | JSONException ex) {
            logger.error(ex);
        }

        Object oCache = MemcacheConnection.getInstance().get("LocalBridgeEvent_" + ev.getUniqueId());
        if (oCache != null) {
            MemcacheConnection.getInstance().delete("LocalBridgeEvent_" + ev.getUniqueId());
        }
    }

    private void handleCdrEvent(CdrEvent ev, long receivedTimestamp0) {
        try {
            if (ev.getLastApplication() != null && ("Hangup".equalsIgnoreCase(ev.getLastApplication()) || "Playback".equalsIgnoreCase(ev.getLastApplication()))) {
                return;
            }
        } catch (NullPointerException ex0) {
            System.out.println("Loi gi do: " + ex0.getMessage());
        }

        try {
            String chan_start = DBSelectionSet.getInstance().getLastChannel(ev.getUniqueId());
            if (chan_start != null) {
                String chanel = ev.getChannel();
                String dstchanel = ev.getDestinationChannel();
                if (!chanel.equalsIgnoreCase(chan_start) && !dstchanel.equalsIgnoreCase(chan_start)) {
                    return;
                }
                DBSelectionSet.getInstance().deleteLastChannel(ev.getUniqueId());
            }
        } catch (Exception ex0) {
            System.out.println("Loi gi do: " + ex0.getMessage());
        }

        try {
            if (!ev.getChannel().endsWith(";2")) {
                String hangup_by = "";
                String hangup_key = "hangupby_request_" + ev.getUniqueId();
                Object cache_hangupby = MemcacheConnection.getInstance().get(hangup_key);
                if (cache_hangupby != null) {
                    if (cache_hangupby.toString().contains("trunk")) {
                        hangup_by = "customer";
                    } else {
                        hangup_by = "agent";
                    }
                }

                String customer_code = "*";
                if (ev.getDestinationContext().contains("queue")
                        || ev.getDestinationContext().contains("internal")
                        || ev.getDestinationContext().contains("ecrm")
                        || ev.getDestinationContext().contains("trunk_")
                        || ev.getDestinationContext().contains("lcr")) {
                    String[] dstctx = ev.getDestinationContext().split("-");
                    customer_code = dstctx[0];
                } else {
                    if (ev.getChannel().contains("Local")) {
                        Pattern p = Pattern.compile("^Local/([0-9]+)@(C([0-9]+))-from-internal-(.*);1$");
                        Matcher m = p.matcher(ev.getChannel());
                        if (m.matches()) {
                            customer_code = m.group(2);
                        }
                    } else {
                        Pattern p = Pattern.compile("^SIP/(C([0-9]{4}))([0-9]+)-(.*)$");
                        Matcher m;
                        if (ev.getChannel().contains("trunk")) {
                            m = p.matcher(ev.getDestinationChannel());
                        } else {
                            m = p.matcher(ev.getChannel());
                        }
                        if (m.matches()) {
                            customer_code = m.group(1);
                        }
                    }
                    if ("*".equals(customer_code)) {
                        customer_code = ev.getCustomercode();
                    }
                }

                HashMap<String, String> site = DBSelectionSet.getInstance().getSiteInfo(ev.getUniqueId(), customer_code);
                if (site != null) {
                    JSONObject jArray = null;
                    if (site.get("params") != null && !"".equals(site.get("params"))) {
                        jArray = new JSONObject(site.get("params"));
                    }

                    String version = DBSelectionSet.getInstance().getVersion(site.get("customer_code"));
                    String url = site.get("method") + site.get("callback_url");

                    String starttime, answertime, endtime, totalduration, billduration;
                    SimpleDateFormat printtime = new SimpleDateFormat("yyyyMMdd'T'HHmmss");
                    HashMap<String, String> ucCall = DBSelectionSet.getInstance().getUCurrentTime(ev.getUniqueId(), customer_code);
                    SimpleDateFormat _sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    try {
                        Date parsedStarttime = _sdf.parse(ucCall.get("starttime"));
                        starttime = printtime.format(parsedStarttime);
                    } catch (ParseException | NullPointerException ex) {
                        try {
                            starttime = printtime.format(ev.getStartTimeAsDate());
                        } catch (NullPointerException ex2) {
                            starttime = "";
                        }
                    }
                    try {
                        Date parsedAnswertime = _sdf.parse(ucCall.get("answertime"));
                        answertime = printtime.format(parsedAnswertime);
                    } catch (ParseException | NullPointerException ex) {
                        try {
                            answertime = printtime.format(ev.getAnswerTimeAsDate());
                        } catch (NullPointerException ex2) {
                            answertime = "";
                        }
                    }
                    try {
                        endtime = printtime.format(ev.getEndTimeAsDate());
                    } catch (NullPointerException ex) {
                        endtime = "";
                    }
                    totalduration = diffTime(printtime, endtime, starttime);
                    billduration = diffTime(printtime, endtime, answertime);

                    Callback c = new Callback(url, this.queuehost, this.log2console);
                    c.addParam("pbx_customer_code", site.get("customer_code"));
                    
                    // add header to 4x
                    String param_phonenumber = null;
                    String param_name = null;
                    String param_email = null;
                    HashMap<String, String> params_to_4x = DBSelectionSet.getInstance().getParamsTo4x(ev.getUniqueId(), site.get("customer_code"));
                    if(params_to_4x != null){
                        param_phonenumber = params_to_4x.get("param_phonenumber");
                        param_name = params_to_4x.get("param_name");
                        param_email = params_to_4x.get("param_email");
                        c.addParam("param_phonenumber", param_phonenumber);
                        c.addParam("param_name", param_name);
                        c.addParam("param_email", param_email);
                    }
                    
                    if (DBSelectionSet.getInstance().checkTransfer(ev.getUniqueId(), customer_code)) {
                        c.addParam("pbx_customer_code", site.get("customer_code"));
                        c.addParam("secret", site.get("secret"));
                        c.addParam("callstatus", "HangUp");
                        c.addParam("calluuid", ev.getUniqueId());
                        c.addParam("datereceived", endtime);
                        c.addParam("causetxt", "16");
                        c.addParam("partner_id", "2");
                        c.addParam("version", version);
                        c.sendGet(receivedTimestamp0);
                        c = new Callback(url, this.queuehost, this.log2console);
                        c.addParam("pbx_customer_code", site.get("customer_code"));
                    }
                    int iversion = Integer.parseInt(version);
                    switch (iversion) {
                        case 1:
                            c.addParam("secret", site.get("secret"));
                            c.addParam("callstatus", "CDR");
                            c.addParam("calluuid", ev.getUniqueId());
                            c.addParam("starttime", starttime);
                            c.addParam("answertime", answertime);
                            c.addParam("endtime", endtime);
                            c.addParam("totalduration", totalduration);
                            if ("1".equals(ucCall.get("dialing_start_time"))) {
                                c.addParam("billduration", billduration);
                            } else {
                                c.addParam("billduration", "0");
                            }
                            break;
                        case 3:
                            c.addParam("secret", site.get("secret"));
                            c.addParam("callstatus", "CDR");
                            c.addParam("calluuid", ev.getUniqueId());
                            c.addParam("starttime", starttime);
                            c.addParam("answertime", answertime);
                            c.addParam("endtime", endtime);
                            c.addParam("totalduration", totalduration);
                            if ("1".equals(ucCall.get("dialing_start_time"))) {
                                c.addParam("billduration", billduration);
                            } else {
                                c.addParam("billduration", "0");
                            }
                            c.addParam("version", version);
                            break;
                        case 4:
                        case 5:
                        default:
                            c.addParam("secret", site.get("secret"));
                            c.addParam("callstatus", "CDR");
                            c.addParam("calluuid", ev.getUniqueId());
                            c.addParam("starttime", starttime);
                            c.addParam("answertime", answertime);
                            c.addParam("endtime", endtime);
                            c.addParam("totalduration", totalduration);
                            if ("1".equals(ucCall.get("dialing_start_time"))) {
                                c.addParam("billduration", billduration);
                                if ("ANSWERED".equalsIgnoreCase(ev.getDisposition())) {
                                    if (ev.getExtdisposition() != null && !"ANSWERED".equals(ev.getExtdisposition())) {
                                        c.addParam("disposition", ev.getExtdisposition());
                                    } else {
                                        c.addParam("disposition", ev.getDisposition());
                                    }
                                    c.addParam("monitorfilename", ev.getMonitorfilename() + ".mp3");
                                } else {
                                    c.addParam("disposition", ev.getDisposition());
                                }
                            } else {
                                if ("inbound".equals(site.get("direction"))) {
                                    c.addParam("billduration", billduration);
                                } else {
                                    c.addParam("billduration", "0");
                                }
                                if ("ANSWERED".equalsIgnoreCase(ev.getDisposition())) {
                                    c.addParam("disposition", "NO ANSWER");
                                } else {
                                    c.addParam("disposition", ev.getDisposition());
                                }
                            }
                            c.addParam("direction", site.get("direction"));
                            if (jArray != null) {
                                if (jArray.has("diallist_id")) {
                                    c.addParam("diallist_id", jArray.getString("diallist_id"));
                                }
                                if (jArray.has("dnis")) {
                                    c.addParam("dnis", jArray.getString("dnis"));
                                }
                                if (jArray.has("try_count")) {
                                    c.addParam("try_count", jArray.getString("try_count"));
                                }
                                if (jArray.has("dialid")) {
                                    c.addParam("dialid", jArray.getString("dialid"));
                                }
                                if (jArray.has("dial_type")) {
                                    c.addParam("dial_type", jArray.getString("dial_type"));
                                }
                                if (jArray.has("callernumber")) {
                                    c.addParam("callernumber", jArray.getString("callernumber"));
                                }
                                if (jArray.has("destinationnumber")) {
                                    c.addParam("destinationnumber", jArray.getString("destinationnumber"));
                                }
                            }
                            c.addParam("hangup_by", hangup_by);
                            c.addParam("version", version);
                            break;
                    }
                    c.addParam("partner_id", "2");
                    c.sendGet(receivedTimestamp0);

                    //Cap nhat call status
                    String update_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ev.getDateReceived());
                    DBSelectionSet.getInstance().updateCall(ev.getUniqueId(), "CdrEvent", update_date, customer_code);

                    //Xoa SDT Incoming
                    Object data_cache_transfer = MemcacheConnection2.getInstance().get("cache_blindtransfer_" + ev.getUniqueId());
                    if (data_cache_transfer == null) {
                        DBSelectionSet.getInstance().deleteCallLog(ev.getCallerId());
                        if (ev.getCallto() != null && !ev.getCallto().isEmpty()) {
                            DBSelectionSet.getInstance().deleteCallLog(ev.getCallto());
                        }
                        if (ev.getUserField() != null && !ev.getUserField().isEmpty()) {
                            String[] usfs = ev.getUserField().split("-");
                            if (usfs.length > 0 && usfs[0].length() > 8) {
                                DBSelectionSet.getInstance().deleteCallLog(usfs[0]);
                            }
                        }
                        //Xoá cache
                        MemcacheConnection2.getInstance().delete("cache_blindtransfer_" + ev.getUniqueId());
                    }
                } else if (ev.getCallback() != null && ev.getCallbacktype() != null && !"".equals(ev.getCallbacktype()) && !"".equals(ev.getCallback())) {
                    String channel = ev.getChannel();
                    if (channel.startsWith("SIP")) {
                        String[] arr_channel = channel.split("/");
                        customer_code = arr_channel[1].split("_")[0];
                        site = DBSelectionSet.getInstance().getSiteByCustomer(customer_code);
                        if (site != null) {
                            String callbackto = ev.getCallback();
                            //String callbacktype = ev.getCallbacktype();
                            String callto = ev.getCallto();
                            String dialid = ev.getDialid();
                            String calluuid = ev.getUniqueId();
                            String starttime = new SimpleDateFormat("yyyyMMdd'T'HHmmss").format(ev.getStartTimeAsDate());
                            String answertime;
                            try {
                                answertime = new SimpleDateFormat("yyyyMMdd'T'HHmmss").format(ev.getAnswerTimeAsDate());
                            } catch (Exception ex) {
                                answertime = "";
                                System.out.println(ex.getMessage());
                            }
                            String endtime = new SimpleDateFormat("yyyyMMdd'T'HHmmss").format(ev.getEndTimeAsDate());
                            String version = DBSelectionSet.getInstance().getVersion(site.get("customer_code"));
                            String url = site.get("method") + site.get("callback_url");
                            Callback c = new Callback(url, this.queuehost, this.log2console);
                            c.addParam("pbx_customer_code", site.get("customer_code"));
                            c.addParam("secret", site.get("secret"));
                            c.addParam("callstatus", "PutCDR");
                            c.addParam("calluuid", calluuid);
                            c.addParam("direction", "outbound");
                            c.addParam("callernumber", callbackto);
                            c.addParam("destinationnumber", callto);
                            c.addParam("starttime", starttime);
                            c.addParam("answertime", answertime);
                            c.addParam("endtime", endtime);
                            c.addParam("billduration", "0");
                            c.addParam("totalduration", ev.getDuration().toString());
                            c.addParam("dialid", dialid);
                            c.addParam("dnis", ev.getCallerId());
                            
                            // add header to 4x
                            String param_phonenumber = null;
                            String param_name = null;
                            String param_email = null;
                            HashMap<String, String> params_to_4x = DBSelectionSet.getInstance().getParamsTo4x(ev.getUniqueId(), site.get("customer_code"));
                            if(params_to_4x != null){
                                param_phonenumber = params_to_4x.get("param_phonenumber");
                                param_name = params_to_4x.get("param_name");
                                param_email = params_to_4x.get("param_email");
                                c.addParam("param_phonenumber", param_phonenumber);
                                c.addParam("param_name", param_name);
                                c.addParam("param_email", param_email);
                            }
                            
                            if (ev.getDiallistid() != null) {
                                c.addParam("diallist_id", ev.getDiallistid());
                            }
                            if (ev.getTrycount() != null) {
                                c.addParam("try_count", ev.getTrycount());
                            }
                            if (ev.getDialtype() != null) {
                                c.addParam("dial_type", ev.getDialtype());
                            }
                            String sipcause = " ";
                            if ("ANSWERED".equalsIgnoreCase(ev.getDisposition())) {
                                c.addParam("disposition", "NO ANSWER");
                            } else {
                                try {
                                    if (ev.getSipcause() != null) {
                                        sipcause = ev.getSipcause();
                                    }
                                } catch (Exception e) {
                                    System.out.println(e);
                                }
                                if (sipcause.contains("503")) {
                                    c.addParam("disposition", "FAILED");
                                } else {
                                    c.addParam("disposition", ev.getDisposition());
                                }
                            }
                            c.addParam("hangup_by", hangup_by);
                            c.addParam("partner_id", "2");
                            c.addParam("version", version);
                            c.sendGet(receivedTimestamp0);
                        }
                    }
                } else {
                    //xử lý click2call, trạng thái click gọi có thành công hay không
                    String channel = ev.getChannel();
                    if (ev.getDialid() != null && ev.getChannel().endsWith(";2") && !"".equals(ev.getDialid()) && channel.contains("Local")) {
                        String starttime = new SimpleDateFormat("yyyyMMdd'T'HHmmss").format(ev.getStartTimeAsDate());
                        String answertime;
                        String extension = ev.getDestination();
                        try {
                            answertime = new SimpleDateFormat("yyyyMMdd'T'HHmmss").format(ev.getAnswerTimeAsDate());
                        } catch (Exception ex) {
                            answertime = "";
                            logger.warn("AnswerTime: " + ex.getClass() + " - " + ex.getMessage());
                        }
                        String endtime = new SimpleDateFormat("yyyyMMdd'T'HHmmss").format(ev.getEndTimeAsDate());

                        String[] channel2;
                        channel2 = channel.split("@");
                        String[] channel3;
                        channel3 = channel2[1].split("-");
                        customer_code = channel3[0];
                        site = DBSelectionSet.getInstance().getSiteByExtension(extension, customer_code);
                        String url = site.get("method") + site.get("callback_url");
                        Callback c = new Callback(url, this.queuehost, this.log2console);
                        c.addParam("pbx_customer_code", site.get("customer_code"));
                        c.addParam("secret", site.get("secret"));
                        c.addParam("callstatus", "CDRExtension");
                        c.addParam("calluuid", ev.getUniqueId());
                        c.addParam("callernumber", ev.getCallerId());
                        c.addParam("destination", ev.getDestination());
                        c.addParam("dialid", ev.getDialid());
                        c.addParam("starttime", starttime);
                        c.addParam("answertime", answertime);
                        c.addParam("endtime", endtime);
                        c.addParam("billduration", ev.getBillableSeconds().toString());
                        c.addParam("totalduration", ev.getDuration().toString());
                        String sipcause = " ";
                        try {
                            if (ev.getSipcause() != null) {
                                sipcause = ev.getSipcause();
                            }
                        } catch (Exception e) {
                            System.out.println(e);
                        }
                        if (sipcause.contains("503")) {
                            c.addParam("disposition", "FAILED");
                        } else {
                            c.addParam("disposition", ev.getDisposition());
                        }
                        c.addParam("partner_id", "2");
                        c.sendGet(receivedTimestamp0);
                    }
                }
            }
        } catch (URISyntaxException | JSONException ex) {
            logger.error(ex);
        }

        /* Gửi event cho ACS */
        try {
            if (ev.getCalltype() != null && "worldfoneacs".equals(ev.getCalltype())) {
                JobInternal j_cdr = new JobInternal(this.log2console);
                j_cdr.addParam("event", "CDR");
                j_cdr.addParam("uniqueid", ev.getUniqueId());
                j_cdr.addParam("AccountCode", ev.getAccountCode());
                j_cdr.addParam("AmaFlags", ev.getAmaFlags());
                j_cdr.addParam("AnswerTime", ev.getAnswerTime());
                j_cdr.addParam("Callback", ev.getCallback());
                j_cdr.addParam("Callbacktype", ev.getCallbacktype());
                j_cdr.addParam("CallerId", ev.getCallerId());
                j_cdr.addParam("CallerIdName", ev.getCallerIdName());
                j_cdr.addParam("CallerIdNum", ev.getCallerIdNum());
                j_cdr.addParam("Callto", ev.getCallto());
                j_cdr.addParam("Channel", ev.getChannel());
                j_cdr.addParam("ChannelStateDesc", ev.getChannelStateDesc());
                j_cdr.addParam("ConnectedLineName", ev.getConnectedLineName());
                j_cdr.addParam("ConnectedLineNum", ev.getConnectedLineNum());
                j_cdr.addParam("Context", ev.getContext());
                j_cdr.addParam("Destination", ev.getDestination());
                j_cdr.addParam("DestinationChannel", ev.getDestinationChannel());
                j_cdr.addParam("DestinationContext", ev.getDestinationContext());
                j_cdr.addParam("Dialid", ev.getDialid());
                j_cdr.addParam("Diallistid", ev.getDiallistid());
                j_cdr.addParam("Dialtype", ev.getDialtype());
                j_cdr.addParam("Disposition", ev.getDisposition());
                j_cdr.addParam("EndTime", ev.getEndTime());
                j_cdr.addParam("LastData", ev.getLastData());
                j_cdr.addParam("Monitorfilename", ev.getMonitorfilename());
                j_cdr.addParam("Src", ev.getSrc());
                j_cdr.addParam("StartTime", ev.getStartTime());
                j_cdr.addParam("Trycount", ev.getTrycount());
                j_cdr.addParam("Sipcause", ev.getSipcause());
                j_cdr.addParam("UserField", ev.getUserField());
                j_cdr.addParam("BillableSeconds", ev.getBillableSeconds().toString());
                j_cdr.addParam("Duration", ev.getDuration().toString());
                j_cdr.addParam("Customer_code", ev.getCustomercode());
                j_cdr.addParam("Calltype", ev.getCalltype());
                j_cdr.addParam("Acs_disposition", ev.getAcsdisposition());
                j_cdr.addParam("Carrier", ev.getCarrier());
                j_cdr.QueuePut("worldfoneacs_cdr_event");
            } else if (ev.getCalltype() != null && (ev.getAcsdisposition() != null && "ANSWERED".equals(ev.getAcsdisposition()))) {
                JobInternal j_cdr = new JobInternal(this.log2console);
                j_cdr.addParam("event", "CDR");
                j_cdr.addParam("uniqueid", ev.getUniqueId());
                j_cdr.addParam("AccountCode", ev.getAccountCode());
                j_cdr.addParam("AmaFlags", ev.getAmaFlags());
                j_cdr.addParam("AnswerTime", ev.getAnswerTime());
                j_cdr.addParam("Callback", ev.getCallback());
                j_cdr.addParam("Callbacktype", ev.getCallbacktype());
                j_cdr.addParam("CallerId", ev.getCallerId());
                j_cdr.addParam("CallerIdName", ev.getCallerIdName());
                j_cdr.addParam("CallerIdNum", ev.getCallerIdNum());
                j_cdr.addParam("Callto", ev.getCallto());
                j_cdr.addParam("Channel", ev.getChannel());
                j_cdr.addParam("ChannelStateDesc", ev.getChannelStateDesc());
                j_cdr.addParam("ConnectedLineName", ev.getConnectedLineName());
                j_cdr.addParam("ConnectedLineNum", ev.getConnectedLineNum());
                j_cdr.addParam("Context", ev.getContext());
                j_cdr.addParam("Destination", ev.getDestination());
                j_cdr.addParam("DestinationChannel", ev.getDestinationChannel());
                j_cdr.addParam("DestinationContext", ev.getDestinationContext());
                j_cdr.addParam("Dialid", ev.getDialid());
                j_cdr.addParam("Diallistid", ev.getDiallistid());
                j_cdr.addParam("Dialtype", ev.getDialtype());
                j_cdr.addParam("Disposition", ev.getDisposition());
                j_cdr.addParam("EndTime", ev.getEndTime());
                j_cdr.addParam("LastData", ev.getLastData());
                j_cdr.addParam("Monitorfilename", ev.getMonitorfilename());
                j_cdr.addParam("Src", ev.getSrc());
                j_cdr.addParam("StartTime", ev.getStartTime());
                j_cdr.addParam("Trycount", ev.getTrycount());
                j_cdr.addParam("Sipcause", ev.getSipcause());
                j_cdr.addParam("UserField", ev.getUserField());
                j_cdr.addParam("BillableSeconds", ev.getBillableSeconds().toString());
                j_cdr.addParam("Duration", ev.getDuration().toString());
                j_cdr.addParam("Customer_code", ev.getCustomercode());
                j_cdr.addParam("Calltype", "worldfoneacs");
                j_cdr.addParam("Acs_disposition", "ANSWERED");
                j_cdr.addParam("Carrier", ev.getCarrier());
                j_cdr.QueuePut("worldfoneacs_cdr_event");
            }
        } catch (Exception ex) {
            logger.error(ex);
        }

        /* Gửi event cho Job CDR
        try {
            if (!ev.getDestinationContext().contains("queue")) {
                JobInternal j = new JobInternal(this.log2console);
                j.addParam("event", "CDR");
                j.addParam("uniqueid", ev.getUniqueId());
                j.QueuePut("cdr_sync_event", 12);
            }
        } catch (Exception ex) {
            logger.error(ex);
        }  */
    }

    private void handleOriginateResponseEvent(OriginateResponseEvent ev) {
        if (ev.getActionId() != null && !"".equals(ev.getActionId())) {
            String calltype = "";
            String calltime = "";
            try {
                String internalActionID = ev.getInternalActionId();
                String[] internalAction = internalActionID.split("@");
                calltype = internalAction[0];
                calltime = internalAction[1];
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
            if ("ACS".equalsIgnoreCase(calltype)) {
                Integer[] reasonCode = new Integer[]{1, 2, 6, 7, 8};
                // Convert String Array to List
                List<Integer> list = Arrays.asList(reasonCode);
                if (list.contains(ev.getReason())) {
                    JobInternal j_cdr = new JobInternal(this.log2console);
                    j_cdr.addParam("event", "OriginateResponse");
                    j_cdr.addParam("OriginateTime", calltime);
                    j_cdr.addParam("ActionID", ev.getActionId());
                    j_cdr.addParam("Response", ev.getResponse());
                    j_cdr.addParam("Channel", ev.getChannel());
                    j_cdr.addParam("Context", ev.getContext());
                    j_cdr.addParam("Exten", ev.getExten());
                    j_cdr.addParam("Reason", ev.getReason().toString());
                    j_cdr.addParam("uniqueid", ev.getUniqueId());
                    j_cdr.addParam("CallerIDNum", ev.getCallerIdNum());
                    j_cdr.addParam("CallerIDName", ev.getCallerIdName());
                    j_cdr.addParam("Context", ev.getContext());
                    j_cdr.addParam("Application", ev.getApplication());
                    j_cdr.QueuePut("worldfoneacs_cdr_event");
                }
            }
        }
    }

    private void handlePeerStatusEvent(PeerStatusEvent ev, long receivedTimestamp0) {
        try {
            List<String> items = Arrays.asList(ev.getPeer().split("/"));
            String customer_code = items.get(1).substring(0, 5);
            String extension = items.get(1).substring(5, items.get(1).length());
            if ("SIP".equalsIgnoreCase(ev.getChannelType())) {
                if ("Registered".equalsIgnoreCase(ev.getPeerStatus())) {
                    HashMap site = DBSelectionSet.getInstance().getSiteByCustomer(customer_code);
                    if (site != null && site.get("version") != null && "5".equals(site.get("version").toString())) {
                        try {
                            String url = site.get("method") + site.get("callback_url").toString();
                            Callback c = new Callback(url, this.queuehost, this.log2console);
                            c.addParam("pbx_customer_code", site.get("customer_code").toString());
                            c.addParam("secret", site.get("secret").toString());
                            c.addParam("callstatus", "PeerStatus");
                            c.addParam("extension", extension);
                            c.addParam("status", "Registered");
                            c.sendGet(receivedTimestamp0);
                        } catch (URISyntaxException ex) {
                            logger.error(ex);
                        }
                    }
                } else if ("Unregistered".equalsIgnoreCase(ev.getPeerStatus())) {
                    HashMap site = DBSelectionSet.getInstance().getSiteByCustomer(customer_code);
                    if (site != null && site.get("version") != null && "5".equals(site.get("version").toString())) {
                        try {
                            String url = site.get("method") + site.get("callback_url").toString();
                            Callback c = new Callback(url, this.queuehost, this.log2console);
                            c.addParam("pbx_customer_code", site.get("customer_code").toString());
                            c.addParam("secret", site.get("secret").toString());
                            c.addParam("callstatus", "PeerStatus");
                            c.addParam("extension", extension);
                            c.addParam("status", "Unregistered");
                            c.sendGet(receivedTimestamp0);
                        } catch (URISyntaxException ex) {
                            logger.error(ex);
                        }
                    }
                }
            }
        } catch (StringIndexOutOfBoundsException | NullPointerException ex) {
            System.out.println("---------------------------" + ex.getMessage() + "---------------------------");
            System.out.println(ev);
            System.out.println("-----------------------------------------------------------------------------");
        }

        String _channeltype = ev.getChannelType();
        String _peer = ev.getPeer();
        String _peerstatus = ev.getPeerStatus();
        String __port = "0";
        try {
            int _port = ev.getPort();
            __port = String.valueOf(_port);
        } catch (NumberFormatException | NullPointerException ex) {
            System.out.println("Exception PeerStatus Port: " + ex.getMessage());
        }

        String _address = ev.getAddress();
        try {
            String[] arrAddr = _address.split(":");
            _address = arrAddr[0];
            if ("0".equals(__port) && arrAddr.length > 1) {
                __port = arrAddr[1];
            }
        } catch (ArrayIndexOutOfBoundsException | NullPointerException ex) {
            System.out.println("Exception PeerStatus Address: " + ex.getMessage());
        }

        String peername = _peer.toLowerCase();
        String status = _peerstatus.toLowerCase();

        String _username = "";
        String[] arrPeer = _peer.split("/");
        if (arrPeer.length > 1) {
            _username = arrPeer[1];
        }
        try {
            JobInternal jobPeer = new JobInternal();
            jobPeer.addParam("timestamp", "" + receivedTimestamp0);
            jobPeer.addParam("event", "PeerStatus");
            jobPeer.addParam("peername", peername);
            jobPeer.addParam("username", _username);
            jobPeer.addParam("host", _address);
            jobPeer.addParam("dyn", "d");
            jobPeer.addParam("nat", "n");
            jobPeer.addParam("port", __port);
            jobPeer.addParam("status", status);
            jobPeer.addParam("freshtime", new SimpleDateFormat("yyyyMMdd'T'HHmmss").format(Calendar.getInstance().getTime()));
            jobPeer.addParam("protocol", _channeltype);
            jobPeer.addParam("curCDRtable", curCDRtable);
            jobPeer.QueuePut();
        } catch (Exception ex) {
            System.out.println(ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    private void handleTransferEvent(TransferEvent ev) {
        String type = ev.getTransferType();
        String channel = ev.getChannel();
        String uniqueid = ev.getUniqueId();
        String targetuniqueid = ev.getTargetUniqueId();
        String targetchannel = ev.getTargetChannel();
        String exten = ev.getTransferExten();
        String context = ev.getTransferContext();
        DBSelectionSet.getInstance().insertTransferEvent(type, channel, uniqueid, targetuniqueid, targetchannel, exten, context);
    }

    private void handleMusicOnHoldEvent(MusicOnHoldEvent ev) {
        try {
            String currentDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ev.getDateReceived());
            JobInternal j_moh = new JobInternal(this.log2console);
            j_moh.addParam("event", "MusicOnHold");
            j_moh.addParam("channel", ev.getChannel());
            j_moh.addParam("state", ev.getState());
            j_moh.addParam("uniqueid", ev.getUniqueId());
            j_moh.addParam("currentdatetime", currentDateTime);
            j_moh.addParam("accountcode", ev.getAccountCode());
            j_moh.addParam("calleridname", ev.getCallerIdName());
            j_moh.addParam("calleridnum", ev.getCallerIdNum());
            j_moh.addParam("channelstatedesc", ev.getChannelStateDesc());
            j_moh.addParam("connectedlinename", ev.getConnectedLineName());
            j_moh.addParam("context", ev.getContext());
            j_moh.addParam("exten", ev.getExten());
            j_moh.addParam("file", ev.getFile());
            j_moh.addParam("func", ev.getFunc());
            j_moh.addParam("linkedid", ev.getLinkedId());
            j_moh.addParam("channelstate", ev.getChannelState().toString());
            j_moh.addParam("sequencenumber", ev.getSequenceNumber().toString());
            j_moh.addParam("curCDRtable", curCDRtable);
            j_moh.QueuePut();
        } catch (Exception ex) {
            System.out.println(ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    private void handleQueueMemberStatus(QueueMemberStatusEvent ev, long receivedTimestamp0) {
        try {
            String key_cache = "PBX_QueueMemberStatusEvent_" + ev.getInterface() + ev.getQueue();
            Object oCache = MemcacheConnection.getInstance().get(key_cache);
            Boolean update = false;
            if (oCache == null) {
                MemcacheConnection.getInstance().set(key_cache, ev.getPaused() + "" + ev.getStatus() + "" + ev.getIncall(), new Date(System.currentTimeMillis() + 300000));
                update = true;
            } else if (!oCache.toString().equalsIgnoreCase(ev.getPaused() + "" + ev.getStatus())) {
                MemcacheConnection.getInstance().delete(key_cache);
                MemcacheConnection.getInstance().set(key_cache, ev.getPaused() + "" + ev.getStatus() + "" + ev.getIncall(), new Date(System.currentTimeMillis() + 3600000));
                update = true;
            }
            if (update) {
                // JobInternal event sync queue member status
                JobInternal j = new JobInternal();
                j.addParam("event", "QueueMemberStatus");
                j.addParam("timestamp", receivedTimestamp0 + "");
                j.addParam("queue", ev.getQueue());
                j.addParam("location", ev.getInterface());
                j.addParam("callstaken", ev.getCallsTaken().toString());
                j.addParam("lastcall", ev.getLastCall().toString());
                j.addParam("status", ev.getStatus().toString());
                j.addParam("paused", ev.getPaused().toString());
                j.addParam("membership", ev.getMembership());
                j.addParam("curCDRtable", curCDRtable);
                j.QueuePut();
            }
        } catch (Exception ex) {
            System.out.println(ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    private void handleQueueMemberPauseEvent(QueueMemberPauseEvent ev) {
        try {
            String currentDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ev.getDateReceived());
            JobInternal j = new JobInternal(this.log2console);
            j.addParam("event", "QueueMemberPause");
            j.addParam("queue", ev.getQueue());
            j.addParam("privilege", ev.getPrivilege());
            j.addParam("location", ev.getInterface());
            j.addParam("membername", ev.getMemberName());
            j.addParam("paused", ev.getPaused().toString());
            j.addParam("datetime", currentDateTime);
            j.addParam("curCDRtable", curCDRtable);
            j.QueuePut();
        } catch (Exception ex) {
            System.out.println(ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    private void handleExtensionStatusEvent(ExtensionStatusEvent ev) {
        try {
            String currentDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ev.getDateReceived());
            //JobExtensionStatus ext_status = new JobExtensionStatus();
            JobInternal j = new JobInternal(this.log2console);
            j.addParam("event", "ExtensionStatus");
            j.addParam("Hint", ev.getHint());
            j.addParam("Exten", ev.getExten());
            j.addParam("Status", String.valueOf(ev.getStatus()));
            j.addParam("Status_ext", ev.getStatustext());
            j.addParam("datetime", currentDateTime);
            j.QueuePut();
        } catch (Exception ex) {
            System.out.println(ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

}
