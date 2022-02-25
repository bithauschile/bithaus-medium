/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.bithaus.medium.utils;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

/**
 *
 * @author jmakuc
 */
public class MapUtils {
    

    public static String getString(Map map, String key) {
        
        Object val = map.get(key);
        
        if(val == null)
            return null;
        
        return val.toString();
    }
    
    /**
     * Gets the value of the given key trying to parse it as yyyy-MM-dd HH:mm:ss
     * @param map
     * @param key
     * @return
     * @throws ParseException 
     */
    public static Date getDate(Map map, String key) throws ParseException {
                
        return getDate(map, key, "yyyy-MM-dd HH:mm:ss");
        
    }
    
    /**
     * Get the value of the given key trying to parse it as ISO 8601 ("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
     * @param map
     * @param key
     * @return
     * @throws ParseException 
     */
    public static Date getDateFromInstant(Map map, String key) throws ParseException {
        
        Object val = map.get(key);
        
        if(val == null)
            return null;                        
        
        return Date.from(Instant.parse(val.toString()));
        
    }
    
    public static Date getDate(Map map, String key, String pattern) throws ParseException {
        
        Object val = map.get(key);
        
        if(val == null)
            return null;
        
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        
        DateFormat formatter = new SimpleDateFormat(pattern);
        formatter.setCalendar(cal);
        
        return formatter.parse(val.toString());        
        
    }
    
    public static String[] getStringArray(Map map, String key) {
        
        Object val = map.get(key);
        
        if(val == null)
            return null;
        
        if(val instanceof ArrayList) {
            
            ArrayList arrayList = (ArrayList) val;
            
            String[] vals = new String[arrayList.size()];
            for(int i = 0; i < arrayList.size(); i++) {
                
                if(arrayList.get(i) == null)
                    vals[i] = null;
                else
                    vals[i] = arrayList.get(i).toString();
            }
            
            return vals;
            
        }            
        
        return null;
    }
    
    public static Integer getInteger(Map map, String key) {
        
        Object val = map.get(key);
        
        if(val == null)
            return null;
        
        return Integer.parseInt(val.toString());
    }
    
    public static Long getLong(Map map, String key) {
        
        Object val = map.get(key);
        
        if(val == null)
            return null;
        
        return Long.parseLong(val.toString());
    }    
    
    public static BigDecimal getBigDecimal(Map map, String key) {
        
        Object val = map.get(key);
        
        if(val == null)
            return null;

        return new BigDecimal(val.toString());
    }    
}
