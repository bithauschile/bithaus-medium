/*
 * Copyright (c) BitHaus Software Chile
 * All rights reserved. www.bithaus.cl.
 * 
 * All rights to this product are owned by Bithaus Chile and may only by used 
 * under the terms of its associated license document. 
 * You may NOT copy, modify, sublicense or distribute this source file or 
 * portions of it unless previously authorized by writing by Bithaus Software Chile.
 * In any event, this notice must always be included verbatim with this file.
 */
package cl.bithaus.medium.utils.test;

import java.io.FileInputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 *
 * @author jmakuc
 */
public class TestMessageGeneratorTest {
    
    public TestMessageGeneratorTest() throws Exception {
        java.util.logging.LogManager.getLogManager().readConfiguration(new FileInputStream("conf-example/logger_unitTests.properties"));
        
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

    
    /*<
    /**
     * Test of init method, of class TestMessageGenerator.
     */
    @Test
    public void testInit() throws Exception {
        
        String configFile = "conf-example/generator.properties";
        TestMessageGenerator.main(new String[] {configFile});
        
    }
    
}
