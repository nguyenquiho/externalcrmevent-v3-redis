/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.worldfonecc.helpers;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author MitsuyoRai
 */
public class Common {

    public static HashMap<String, String> JSONToHashMap(String jsonString) {
        try {
            JSONObject jdata = new JSONObject(jsonString);
            Iterator<String> nameItr = jdata.keys();
            HashMap<String, String> outMap = new HashMap<>();
            while (nameItr.hasNext()) {
                String name = nameItr.next();
                outMap.put(name, jdata.getString(name));
            }
            return outMap;
        } catch (JSONException ex) {
            ex.printStackTrace(System.out);
        }
        return null;
    }

    public static ArrayList<String> JSONToArrayList(String jsonString) {
        try {
            ArrayList<String> outList = new ArrayList<>();
            JSONArray jArray = new JSONArray(jsonString);
            for (int i = 0; i < jArray.length(); i++) {
                outList.add(jArray.getString(i));
            }
            return outList;
        } catch (JSONException ex) {
            Logger.getLogger(Common.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static ArrayList<HashMap> JSONToArrayListHashMap(String jsonString) {
        try {
            ArrayList<HashMap> outList = new ArrayList<>();
            JSONArray jArray = new JSONArray(jsonString);
            for (int i = 0; i < jArray.length(); i++) {
                outList.add(JSONToHashMap(jArray.getJSONObject(i).toString()));
            }
            return outList;
        } catch (JSONException ex) {
            Logger.getLogger(Common.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
    
    public static String diffTime(SimpleDateFormat sdf, String datetime1, String datetime2) {
        Date dtime1, dtime2;
        try {
            dtime1 = sdf.parse(datetime1);
            dtime2 = sdf.parse(datetime2);
            return String.valueOf((dtime1.getTime() - dtime2.getTime()) / 1000);
        } catch (ParseException | NullPointerException ex) {
            System.out.println(ex.getMessage());
        }
        return "0";
    }
}
