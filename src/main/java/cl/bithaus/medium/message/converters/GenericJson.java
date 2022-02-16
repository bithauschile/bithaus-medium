/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.bithaus.medium.message.converters;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.Map;

/**
 *
 * @author jmakuc
 */
public class GenericJson {
    
    private Map map;
    
    private static Gson gson;
    
    {
        GsonBuilder builder = new GsonBuilder();                
        this.gson = builder.create();
    }
    
    private GenericJson(Map map) {
        
        this.map = map;
    }
    
    public static GenericJson create(String raw) {
    
        Map map = gson.fromJson(raw, Map.class);
        GenericJson gj = new GenericJson(map);
        
        return gj;
    }
    
    
    
    
}
