/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.worldfonecc.access;

import com.worldfonecc.helpers.Common;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 *
 * @author MitsuyoRai
 */
public class DBSelectionSet {

    private final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("CrmEvent");

    private static volatile DBSelectionSet instance;

    public static DBSelectionSet getInstance() {
        DBSelectionSet _instance = DBSelectionSet.instance;
        if (_instance == null) {
            synchronized (DBSelectionSet.class) {
                _instance = DBSelectionSet.instance;
                if (_instance == null) {
                    DBSelectionSet.instance = _instance = new DBSelectionSet();
                }
            }
        }
        return _instance;
    }

    private DBSelectionSet() {
    }

    private boolean log2console = false;
    private final String mc_key_prefix = "dbsip_";

    public List<JSONObject> getCurCDR(String customer_code, String collection) {
        MongoUtils mongodb = new MongoUtils();
        List<JSONObject> curcdr = mongodb.where("customer_code", customer_code).get(collection);
        mongodb.close();
        return curcdr;
    }

    public String getVersion(String customer_code) {
        String crm_version = "";
        try {
            String pbx_customers = RedisSlaveConnection.getInstance().get("pbx_" + customer_code + "_pbx_customers");
            if (pbx_customers != null) {
                JSONArray arr = new JSONArray(pbx_customers);
                for (int i = 0; i < arr.length(); i++) {
                    if (arr.getJSONObject(i).has("crm_external_version")) {
                        crm_version = arr.getJSONObject(i).get("crm_external_version").toString();
                        break;
                    }
                }
            }
        } catch (JedisConnectionException | JSONException ex) {
            logger.error("DBSelectionSet getVersion", ex);
        }
        if ("".equals(crm_version)) {
            String mckey = mc_key_prefix + "version:" + customer_code;
            if (MemcacheConnection.getInstance().get(mckey) != null) {
                if (this.log2console) {
                    System.out.println(mckey);
                }
                crm_version = MemcacheConnection.getInstance().get(mckey).toString();
            }
            if ("".equals(crm_version)) {
                String sql = "SELECT `crm_external_version` FROM `pbx_customers` WHERE `customer_code`=?";
                crm_version = MysqlConnection.getInstance().getColumnByQuery(sql, "crm_external_version", customer_code);

                if (!"".equals(crm_version)) {
                    MemcacheConnection.getInstance().set(mckey, crm_version, new Date(System.currentTimeMillis() + 3000000));
                }
            }
        }
        return crm_version;
    }

    public HashMap<String, String> getSiteByCustomer(String customer_code) {
        HashMap<String, String> sipuser = null;
        try {
            String pbx_sites = RedisSlaveConnection.getInstance().get("pbx_" + customer_code + "_con_crm_sites");
            if (pbx_sites != null) {
                JSONArray arr = new JSONArray(pbx_sites);
                for (int i = 0; i < arr.length(); i++) {
                    if (arr.getJSONObject(i).has("callback_url")) {
                        sipuser = new HashMap<>();
                        Iterator<String> keysItr = arr.getJSONObject(i).keys();
                        while (keysItr.hasNext()) {
                            String key = keysItr.next();
                            String value = arr.getJSONObject(i).get(key).toString();
                            sipuser.put(key, value);
                        }
                        break;
                    }
                }
            }
        } catch (JedisConnectionException | JSONException ex) {
            logger.error("DBSelectionSet getVersion", ex);
        }
        if (sipuser == null || sipuser.isEmpty()) {
            String mckey = mc_key_prefix + "site:" + customer_code;
            if (MemcacheConnection.getInstance().get(mckey) != null) {
                if (this.log2console) {
                    System.out.println(mckey);
                }
                sipuser = Common.JSONToHashMap(MemcacheConnection.getInstance().get(mckey).toString());
            }

            if (sipuser == null || sipuser.isEmpty()) {
                String sql = "SELECT * FROM `con_crm_sites` WHERE `customer_code`=?";
                sipuser = MysqlConnection.getInstance().getOneByQuery(sql, customer_code);
                if (!(sipuser == null || sipuser.isEmpty())) {
                    try {
                        String json = JSONObject.valueToString(sipuser);
                        MemcacheConnection.getInstance().set(mckey, json, new Date(System.currentTimeMillis() + 300000));
                    } catch (JSONException ex) {
                        ex.printStackTrace(System.out);
                    }
                }
            }
        }

        return sipuser;
    }

    public ArrayList<String> getDestBySite(String siteid, String customer_code) {
        ArrayList<String> sipuser = null;
        try {
            String crm_dest = RedisSlaveConnection.getInstance().get("pbx_" + customer_code + "_pbx_crm_destination");
            if (crm_dest != null) {
                JSONArray arr = new JSONArray(crm_dest);
                sipuser = new ArrayList<>();
                for (int i = 0; i < arr.length(); i++) {
                    if (arr.getJSONObject(i).has("dest_id")) {
                        sipuser.add(arr.getJSONObject(i).get("dest_id").toString());
                    }
                }
            }
        } catch (JedisConnectionException | JSONException ex) {
            logger.error("DBSelectionSet getVersion", ex);
        }
        if (sipuser == null || sipuser.isEmpty()) {
            String mckey = mc_key_prefix + "dest:" + siteid;
            if (MemcacheConnection.getInstance().get(mckey) != null) {
                if (this.log2console) {
                    System.out.println(mckey);
                }
                sipuser = Common.JSONToArrayList(MemcacheConnection.getInstance().get(mckey).toString());
            }
            if (sipuser == null || sipuser.isEmpty()) {
                String sql = "SELECT `dest_id` FROM `pbx_crm_destination` WHERE `site_id`=?";
                sipuser = MysqlConnection.getInstance().getListColumnByQuery(sql, "dest_id", siteid);
                if (!((sipuser == null) || sipuser.isEmpty())) {
                    JSONArray jsonArray = new JSONArray(sipuser);
                    MemcacheConnection.getInstance().set(mckey, jsonArray.toString(), new Date(System.currentTimeMillis() + 30000));
                }
            }
        }

        return sipuser;
    }

    public HashMap<String, String> getSiteInfo(String calluuid, String customer_code) {
        HashMap<String, String> site = null;
        String mckey = mc_key_prefix + "site_info:" + calluuid;
        if (MemcacheConnection.getInstance().get(mckey) != null) {
            if (this.log2console) {
                System.out.println(mckey);
            }
            site = Common.JSONToHashMap(MemcacheConnection.getInstance().get(mckey).toString());
        }
        if (site == null || site.isEmpty()) {
            List<String> curevent = RedisMonitorRealtimeConnection.getInstance().HMGet("crm_curevents_" + customer_code + "_" + calluuid, "call_uuid", "call_status", "direction", "site_id", "calltype", "params");
            if (curevent.get(0) != null) {
                site = getSiteByCustomer(customer_code);
                site.put("call_uuid", curevent.get(0));
                site.put("call_status", curevent.get(1));
                site.put("direction", curevent.get(2));
                site.put("site_id", curevent.get(3));
                site.put("calltype", curevent.get(4));
                site.put("params", curevent.get(5));
            }
            if (site != null && !site.isEmpty()) {
                try {
                    String json = JSONObject.valueToString(site);
                    MemcacheConnection.getInstance().set(mckey, json, new Date(System.currentTimeMillis() + 1800000));
                } catch (JSONException ex) {
                    ex.printStackTrace(System.out);
                }
            }
        }

        return site;
    }

    public HashMap<String, String> getSiteByExtension(String extension, String customer_code) {
        HashMap<String, String> site = null;
        String mckey = mc_key_prefix + "site_extension:" + extension + "@" + customer_code;
        if (MemcacheConnection.getInstance().get(mckey) != null) {
            if (this.log2console) {
                System.out.println(mckey);
            }
            site = Common.JSONToHashMap(MemcacheConnection.getInstance().get(mckey).toString());
        }

        if (site == null || site.isEmpty()) {
            try {
                String pbx_sites = RedisSlaveConnection.getInstance().get("pbx_" + customer_code + "_con_crm_sites");
                String crm_dest = RedisSlaveConnection.getInstance().get("pbx_" + customer_code + "_pbx_crm_destination");
                if (pbx_sites != null && crm_dest != null) {
                    JSONArray arr_sites = new JSONArray(pbx_sites);
                    JSONArray arr_crm = new JSONArray(crm_dest);
                    for (int i = 0; i < arr_crm.length(); i++) {
                        JSONObject obj_crm = arr_crm.getJSONObject(i);
                        if (obj_crm.has("dest_id") && obj_crm.has("dest_type") && "extension".equalsIgnoreCase(obj_crm.get("dest_type").toString()) && extension.equalsIgnoreCase(obj_crm.get("dest_id").toString())) {
                            String site_id = obj_crm.get("site_id").toString();
                            for (int j = 0; j < arr_sites.length(); j++) {
                                JSONObject obj_sites = arr_sites.getJSONObject(j);
                                if (obj_sites.has("site_id") && site_id.equalsIgnoreCase(obj_sites.get("site_id").toString())) {
                                    site = new HashMap<>();
                                    site.put("customer_code", customer_code);
                                    site.put("site_id", site_id);
                                    site.put("method", obj_sites.get("method").toString());
                                    site.put("secret", obj_sites.get("secret").toString());
                                    site.put("callback_url", obj_sites.get("callback_url").toString());
                                    site.put("crm_type", obj_sites.get("crm_type").toString());
                                    site.put("connect_info", obj_sites.get("connect_info").toString());
                                    site.put("allcall", obj_sites.get("allcall").toString());
                                    site.put("version", obj_sites.get("version").toString());
                                    site.put("report_voicemail", obj_sites.get("report_voicemail").toString());
                                    site.put("push_cdr", obj_sites.get("push_cdr").toString());
                                    break;
                                }
                            }
                            break;
                        }
                    }
                }
            } catch (JedisConnectionException | JSONException ex) {
                logger.error("DBSelectionSet getVersion", ex);
                String sql = "SELECT `con_crm_sites`.* FROM `con_crm_sites`, `pbx_crm_destination` WHERE `pbx_crm_destination`.`site_id`=`con_crm_sites`.`site_id` AND `pbx_crm_destination`.`dest_id`=? AND `dest_type`='extension' AND `con_crm_sites`.`customer_code`=?";
                site = MysqlConnection.getInstance().getOneByQuery(sql, extension, customer_code);
            }

            if (site != null && !site.isEmpty()) {
                try {
                    String json = JSONObject.valueToString(site);
                    MemcacheConnection.getInstance().set(mckey, json, new Date(System.currentTimeMillis() + 30000));
                } catch (JSONException ex) {
                    ex.printStackTrace(System.out);
                }
            }
        }
        return site;
    }

    public HashMap<String, String> getCustomerList() {
        String mckey = mc_key_prefix + "customerlist";
        HashMap<String, String> customerlist = new HashMap<>();
        if (MemcacheConnection.getInstance().get(mckey) != null) {
            if (this.log2console) {
                System.out.println(mckey);
            }
            customerlist = Common.JSONToHashMap(MemcacheConnection.getInstance().get(mckey).toString());
        }
        return customerlist;
    }

    public boolean checkUuid(String uuid, String customer_code) {
        String parent_calluuid = RedisMonitorRealtimeConnection.getInstance().HGet("crm_curevents_" + customer_code + "_" + uuid, "parent_calluuid");
        return parent_calluuid != null && !"".equals(parent_calluuid);
    }

    public boolean checkNumber(String number) {
        return RedisMonitorRealtimeConnection.getInstance().exists("crm_cur_external_incoming_" + number);
    }

    public String getCustomercodeNumber(String number) {
        return RedisMonitorRealtimeConnection.getInstance().HGet("crm_cur_external_incoming_" + number, "customer_code");
    }

    public void setCustomercodeByCallid(String calluuid, String customer_code) {
        String pbx_sites = RedisSlaveConnection.getInstance().get("pbx_" + customer_code + "_con_crm_sites");
        if (pbx_sites != null && !"[]".equals(pbx_sites)) {
            RedisLocalConnection.getInstance().set("cache_code_" + calluuid, customer_code, 1800);
        }
    }

    public String getCustomercodeByCallid(String calluuid) {
        return RedisLocalConnection.getInstance().get("cache_code_" + calluuid);
    }

    public void delCustomercodeByCallid(String calluuid) {
        RedisLocalConnection.getInstance().expire("cache_code_" + calluuid, 10);
    }

    public boolean checkExtension(String extension, String customer_code) {
        boolean result = false;
        String mckey = mc_key_prefix + "check_extension:" + extension + "@" + customer_code;
        if (MemcacheConnection.getInstance().get(mckey) != null) {
            if (this.log2console) {
                System.out.println(mckey);
            }
            result = Boolean.valueOf(MemcacheConnection.getInstance().get(mckey).toString());
        } else {
            try {
                String crm_dest = RedisSlaveConnection.getInstance().get("pbx_" + customer_code + "_pbx_crm_destination");
                if (crm_dest != null) {
                    JSONArray arr = new JSONArray(crm_dest);
                    for (int i = 0; i < arr.length(); i++) {
                        JSONObject sipuser = arr.getJSONObject(i);
                        if (sipuser.has("dest_id")
                                && "extension".equalsIgnoreCase(sipuser.get("dest_type").toString())
                                && extension.equals(sipuser.get("dest_id").toString())) {
                            result = true;
                            this.putCustomerCodeToCache(customer_code);
                            break;
                        }
                    }
                } else {
                    result = false;
                }
            } catch (JedisConnectionException | JSONException ex) {
                Logger.getLogger(DBSelectionSet.class.getName()).log(Level.SEVERE, null, ex);
                String sql = "SELECT * FROM `pbx_crm_destination` WHERE `dest_id`=? AND `dest_type`='extension' AND `customer_code`=?";
                result = (MysqlConnection.getInstance().getOneByQuery(sql, extension, customer_code) != null);
                if (result) {
                    this.putCustomerCodeToCache(customer_code);
                }
            }
            MemcacheConnection.getInstance().set(mckey, Boolean.toString(result), new Date(System.currentTimeMillis() + 5000));
        }
        return result;
    }

    private void putCustomerCodeToCache(String customer_code) {
        String mckey = mc_key_prefix + "customerlist";
        HashMap<String, String> customerlist = new HashMap<>();
        if (MemcacheConnection.getInstance().get(mckey) != null) {
            if (this.log2console) {
                System.out.println(mckey);
            }
            customerlist = Common.JSONToHashMap(MemcacheConnection.getInstance().get(mckey).toString());
        }
        customerlist.put(customer_code, customer_code);
        try {
            String json = JSONObject.valueToString(customerlist);
            MemcacheConnection.getInstance().set(mckey, json, new Date(System.currentTimeMillis() + 600000));
        } catch (JSONException ex) {
            ex.printStackTrace(System.out);
        }
    }

    public String getCalluuid(String number) {
        return RedisMonitorRealtimeConnection.getInstance().HGet("crm_cur_external_incoming_" + number, "calluuid");
    }

    public boolean checkCallNotAnswer(String calluuid, String customer_code) {
        String status = RedisMonitorRealtimeConnection.getInstance().HGet("crm_curevents_" + customer_code + "_" + calluuid, "call_status");
        return status != null && !status.equals("DialAnswer");
    }

    public String checkCallHangup(String calluuid, String customer_code) {
        List<String> curevent = RedisMonitorRealtimeConnection.getInstance().HMGet("crm_curevents_" + customer_code + "_" + calluuid, "transfer", "childcalluuid");
        String childcalluuid = null;
        if (curevent.get(0) != null) {
            childcalluuid = curevent.get(1);
        }
        if (childcalluuid == null || childcalluuid.isEmpty()) {
            childcalluuid = "0";
        }
        return childcalluuid;
    }

    public boolean checkCall(String calluuid, String customer_code) {
        return RedisMonitorRealtimeConnection.getInstance().exists("crm_curevents_" + customer_code + "_" + calluuid);
    }

    public HashMap<String, String> getUCurrentTime(String calluuid, String customer_code) {
        List<String> curevent = RedisMonitorRealtimeConnection.getInstance().HMGet("crm_curevents_" + customer_code + "_" + calluuid, "call_uuid", "starttime", "answertime", "dialing_start_time", "startholdtime", "stopHold", "holdtime");
        HashMap<String, String> uctime = null;
        if (curevent.get(0) != null) {
            uctime = new HashMap<>();
            uctime.put("starttime", curevent.get(1));
            uctime.put("answertime", curevent.get(2));
            uctime.put("dialing_start_time", curevent.get(3));
            if (curevent.get(4) != null) {
                uctime.put("startholdtime", curevent.get(4));
            }
            if (curevent.get(5) != null) {
                uctime.put("stopHold", curevent.get(5));
            }
            if (curevent.get(6) != null) {
                uctime.put("holdtime", curevent.get(6));
            }
        }
        return uctime;
    }

    public void updateCall(String calluuid, String event, String update_date, String customer_code) {
        HashMap<String, String> hash = new HashMap<>();
        hash.put("call_status", event);
        hash.put("updated_datetime", update_date);
        if (RedisMonitorRealtimeConnection.getInstance().HMSet("crm_curevents_" + customer_code + "_" + calluuid, hash) != null) {
            if ("HangUp".equals(event) || "CdrEvent".equals(event)) {
                long move = RedisMonitorRealtimeConnection.getInstance().sMove("crm_curevents_in_progress", "crm_curevents_completed", "crm_curevents_" + customer_code + "_" + calluuid);
                if (move == 0) {
                    RedisMonitorRealtimeConnection.getInstance().sAdd("crm_curevents_completed", "crm_curevents_" + customer_code + "_" + calluuid);
                }
            }
        }
    }

    public void updateCallDialAnswer(String calluuid, String event, String childcalluuid, String update_date, String customer_code) {
        HashMap<String, String> hash = new HashMap<>();
        hash.put("call_status", event);
        hash.put("childcalluuid", childcalluuid);
        hash.put("dialing_start_time", "1");
        hash.put("answertime", update_date);
        hash.put("updated_datetime", update_date);
        RedisMonitorRealtimeConnection.getInstance().HMSet("crm_curevents_" + customer_code + "_" + calluuid, hash);
    }

    public void updateCallDialAnswer(String calluuid, String event, String childcalluuid, String params, String update_date, String customer_code) {
        HashMap<String, String> hash = new HashMap<>();
        hash.put("call_status", event);
        hash.put("childcalluuid", childcalluuid);
        hash.put("dialing_start_time", "1");
        hash.put("params", params);
        hash.put("answertime", update_date);
        hash.put("updated_datetime", update_date);
        RedisMonitorRealtimeConnection.getInstance().HMSet("crm_curevents_" + customer_code + "_" + calluuid, hash);
    }

    public boolean checkTransfer(String uuid, String customer_code) {
        String transfer = RedisMonitorRealtimeConnection.getInstance().HGet("crm_curevents_" + customer_code + "_" + uuid, "transfer");
        return transfer != null && !transfer.equals("0");
    }

    public void logExtension(String calluuid, String direction, String site_id, String parentUid, String update_date, String customer_code) {
        HashMap<String, String> hash = new HashMap<>();
        hash.put("call_uuid", calluuid);
        hash.put("call_status", "Start");
        hash.put("direction", direction);
        hash.put("site_id", site_id);
        hash.put("updated_datetime", update_date);
        hash.put("parent_calluuid", parentUid);
        hash.put("calltype", "");
        hash.put("customercode", customer_code);
        hash.put("childcalluuid", "");
        hash.put("transfer", "0");
        hash.put("dialing_start_time", "0");
        hash.put("con_crm_cureventscol", "");
        hash.put("monitor_filename", "");
        hash.put("send_status", "1");
        hash.put("starttime", "");
        hash.put("answertime", "");
        hash.put("params", "");

        RedisMonitorRealtimeConnection.getInstance().HMSet("crm_curevents_" + customer_code + "_" + calluuid, hash);
        RedisMonitorRealtimeConnection.getInstance().sAdd("crm_curevents_in_progress", "crm_curevents_" + customer_code + "_" + calluuid);
    }

    public String getCusomercodeOnhold(String channel) {
        String sql = "SELECT `customer_code` FROM `con_crm_hold_channel` WHERE `channel`=? OR `agentchan`=? LIMIT 1";
        return MysqlConnection.getInstance().getColumnByQuery(sql, "customer_code", channel, channel);
    }

    public boolean deleteCallLog(String number) {
        return RedisMonitorRealtimeConnection.getInstance().del("crm_cur_external_incoming_" + number) > 0;
    }

    public void insertBlindTransfer(String channel, String value, String uniqueid) {
        //Memcached
        MemcacheConnection.getInstance().set("cache_blindtransfer_" + uniqueid, channel, new Date(System.currentTimeMillis() + 5000));
        MemcacheConnection.getInstance().set("blindtransfer_" + uniqueid, channel.trim() + "," + value.trim(), new Date(System.currentTimeMillis() + 7200000));
        //Logging
        String sql = "INSERT INTO `blindtransfer`(`channel`,`value`,`uniqueid`) VALUES (?,?,?)";
        MysqlCDRConnection.getInstance().executeNonQuery(sql, channel, value, uniqueid);
    }

    public void insertTransferEvent(String type, String channel, String uniqueid, String targetuniqueid, String targetchannel, String exten, String context) {
        try {
            //Memcached
            int iTTL = 600000;
            JSONObject obj = new JSONObject();
            obj.put("type", type);
            obj.put("channel", channel);
            obj.put("uniqueid", uniqueid);
            obj.put("targetuniqueid", targetuniqueid);
            obj.put("targetchannel", targetchannel);
            obj.put("exten", exten);
            obj.put("context", context);
            String key1 = "transfer_" + channel.replaceAll("/|-|@|;", "");
            MemcacheConnection.getInstance().set(key1, obj.toString(), new Date(System.currentTimeMillis() + iTTL));
            switch (type) {
                case "Attended":
                    String key2 = "transfer_" + targetchannel.replaceAll("/|-|@|;", "");
                    MemcacheConnection.getInstance().set(key2, obj.toString(), new Date(System.currentTimeMillis() + iTTL));
                    break;
                case "Blind":
                    if (Pattern.matches("(.*)(SIP/(C([0-9]+))_trunk_([0-9]+)-(.*))<(.*)>$", targetchannel)) {
                        targetchannel = targetchannel.replaceAll("(.*)(SIP/(C([0-9]+))_trunk_([0-9]+)-(.*))<(.*)>$", "$2");
                    }
                    String key3 = "transfer_target_" + targetchannel.replaceAll("/|-|@|;", "");
                    MemcacheConnection.getInstance().set(key3, channel, new Date(System.currentTimeMillis() + iTTL));
                    break;
            }
            //Logging
            String sql = "INSERT INTO `transfer_event`(`type`,`channel`,`uniqueid`,`targetuniqueid`,`targetchannel`,`exten`,`context`) VALUES (?,?,?,?,?,?,?)";
            MysqlCDRConnection.getInstance().executeNonQuery(sql, type, channel, uniqueid, targetuniqueid, targetchannel, exten, context);
        } catch (JSONException ex) {
            Logger.getLogger(DBSelectionSet.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void updateLastChannel(String calluuid, String channel) {
        RedisLocalConnection.getInstance().set("channel_start_" + calluuid, channel, 3600);
    }

    public String getLastChannel(String calluuid) {
        return RedisLocalConnection.getInstance().get("channel_start_" + calluuid);
    }

    public void deleteLastChannel(String calluuid) {
        RedisLocalConnection.getInstance().expire("channel_start_" + calluuid, 5);
    }

    /**
     * startHold -> start hold
     *
     * @param calluuid
     * @param receivedTime
     * @param customer_code
     */
    public void startHold(String calluuid, String receivedTime, String customer_code) {
        if (RedisMonitorRealtimeConnection.getInstance().exists("crm_curevents_" + customer_code + "_" + calluuid)) {
            HashMap<String, String> hash = new HashMap<>();
            hash.put("startholdtime", receivedTime);
            hash.put("stopHold", "2");
            RedisMonitorRealtimeConnection.getInstance().HMSet("crm_curevents_" + customer_code + "_" + calluuid, hash);
        }
    }

    /**
     * stopHold -> stop hold
     *
     * @param calluuid
     * @param receivedTime
     * @param customer_code
     */
    public void stopHold(String calluuid, String receivedTime, String customer_code) {
        if (RedisMonitorRealtimeConnection.getInstance().exists("crm_curevents_" + customer_code + "_" + calluuid)) {
            String startholdtime = "";
            int holdtime = 0;
            List<String> curevent = RedisMonitorRealtimeConnection.getInstance().HMGet("crm_curevents_" + customer_code + "_" + calluuid, "startholdtime", "holdtime");
            if (curevent.get(0) != null) {
                startholdtime = curevent.get(0);
            }
            if (curevent.get(1) != null) {
                holdtime = Integer.parseInt(curevent.get(1));
            }

            try {
                Date dtime1, dtime2;
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                dtime1 = sdf.parse(receivedTime);
                dtime2 = sdf.parse(startholdtime);
                holdtime = (int) (holdtime + (dtime1.getTime() - dtime2.getTime()) / 1000);
            } catch (ParseException | NullPointerException ex) {
            }

            HashMap<String, String> hash = new HashMap<>();
            hash.put("holdtime", holdtime + "");
            hash.put("stopHold", "1");
            RedisMonitorRealtimeConnection.getInstance().HMSet("crm_curevents_" + customer_code + "_" + calluuid, hash);
        }
    }
    
    public HashMap<String, String> getParamsTo4x(String calluuid, String customer_code) {
        HashMap<String, String> params_return = new HashMap<>();
        List<String> params = RedisSlaveConnection.getInstance().HMGet("params_to_4x_" + customer_code + "_" + calluuid, "param_phonenumber", "param_name", "param_email");
        if (params.get(0) != null) {
            params_return.put("param_phonenumber", params.get(0));
            params_return.put("param_name", params.get(1));
            params_return.put("param_email", params.get(2));
            return params_return;
        }
        return null;
    }

}
