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

import cl.bithaus.medium.utils.MediumMessageGenerator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple message generator for simple tests
 * @author jmakuc
 */
public class TestMessageGenerator extends MediumMessageGenerator<TestMessage>{

    private static final AtomicLong serializer = new AtomicLong();
    
    @Override
    protected TestMessage getNextMessage() {
        
        TestMessage m = new TestMessage("message + " + serializer.incrementAndGet());
        return m;
    }

    @Override
    public void init(Map config) {
        
    }
        
}
