/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.bithaus.medium.message.converters;

import cl.bithaus.medium.message.MediumMessage;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

/**
 *
 * @author jmakuc
 * @param <O> Output message type for the converter
 */
public abstract class MessageConverter <O extends MediumMessage> {
    
    public abstract O[] toMedium(String rawdata) throws MessageConverterException;
    
    public String getString(Map map, String key) {
        
        Object val = map.get(key);
        
        if(val == null)
            return null;
        
        return val.toString();
    }
    
    public Date getDate(Map map, String key) throws ParseException {
        
        Object val = map.get(key);
        
        if(val == null)
            return null;
        
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        formatter.setCalendar(cal);
        
        return (Date) formatter.parse(val.toString());        
    }
    
    public String[] getStringArray(Map map, String key) {
        
        Object val = map.get(key);
        
        if(val == null)
            return null;
        
        if(val instanceof String[])
            return (String[]) val;
        
        return null;
    }
    
    public Integer getInteger(Map map, String key) {
        
        Object val = map.get(key);
        
        if(val == null)
            return null;
        
        return Integer.parseInt(val.toString());
    }
    
    public BigDecimal getBigDecimal(Map map, String key) {
        
        Object val = map.get(key);
        
        if(val == null)
            return null;

        return new BigDecimal(val.toString());
    }
}
