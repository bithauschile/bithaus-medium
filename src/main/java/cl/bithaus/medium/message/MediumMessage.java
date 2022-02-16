/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.bithaus.medium.message;

import java.util.Map;

/**
 * Medium Generic Message
 * @author jmakuc
 */
public abstract class MediumMessage {
    
    private String uuid;
    private Envelope envelope;
    
    private final String messageType;
    
    
    public MediumMessage(String messageType, String uuid) {
    
        this.messageType = messageType;
        this.uuid = uuid;
    }
    
    public MediumMessage(String messageType, String uuid, Envelope envelope) {
        
        this.messageType = messageType;
        this.uuid = uuid;
        this.envelope = envelope;
    }

    public Envelope getEnvelope() {
        return envelope;
    }

    public void setEnvelope(Envelope envelope) {
        this.envelope = envelope;
    }

    public String getMessageType() {
        return messageType;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }
    
    
 
    
    
    /**
     * Message metadata
     */    
    public class Envelope {
        
        private Map<String,String> keys;
        private Long timestamp;

        public Map<String, String> getKeys() {
            return keys;
        }

        public void setKeys(Map<String, String> keys) {
            this.keys = keys;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }
                        
    }
}
