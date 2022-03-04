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

import cl.bithaus.medium.message.MediumMessage;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple test message
 * @author jmakuc
 */
public class TestMessage extends MediumMessage {
    
    private static final AtomicLong serializer = new AtomicLong();
    
    private String data;
    
    public TestMessage() {
               
        super("" + serializer.incrementAndGet());
        initMetadata();
    }
    
    public TestMessage(String data) {
        
        this();
        this.data = data;
        initMetadata();
    }
    
    private void initMetadata() {
        
        this.getMetadata().setKey("key-" + getUuid());
        this.getMetadata().setSource("source-system");
        this.getMetadata().setTarget("target-system");
        this.getMetadata().setTimestamp(new Date().getTime());
        
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TestMessage other = (TestMessage) obj;
        if (!Objects.equals(this.data, other.data)) {
            return false;
        }
        
        return Objects.equals(this.getMetadata(), other.getMetadata());
    }
 
    
    
    
}
