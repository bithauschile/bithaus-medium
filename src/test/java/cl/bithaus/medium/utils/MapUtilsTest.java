/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.bithaus.medium.utils;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author jmakuc
 */
public class MapUtilsTest {
    
    public MapUtilsTest() {
    }
    
    @BeforeAll
    public static void setUpClass() {
    }
    
    @AfterAll
    public static void tearDownClass() {
    }
    
    @BeforeEach
    public void setUp() {
    }
    
    @AfterEach
    public void tearDown() {
    } 
    
    /**
     * Test of getDate method, of class MapUtils.
     */
    @Test
    public void testGetInstant() throws Exception {
        System.out.println("testGetInstant");
        Map map = new HashMap();
        map.put("test", "2022-02-22T21:17:31.000Z");
        
        Date result = MapUtils.getDateFromInstant(map, "test");
        
    }
 
    
}
