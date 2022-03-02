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
    
    public static final String HEADER_MESSAGE_CLASS = "cl.bithaus.medium.message.className";
    public static final String HEADER_MESSAGE_SOURCE = "cl.bithaus.medium.message.source";
    public static final String HEADER_MESSAGE_TARGET = "cl.bithaus.medium.message.target";
        
    private String uuid;
    private Metadata metadata;
    
     
    public MediumMessage(String uuid) {
    
        this.uuid = uuid;
        this.metadata = new Metadata();
        this.metadata.getHeaders().put(HEADER_MESSAGE_CLASS, this.getClass().getCanonicalName());
    }
    
    public MediumMessage(String uuid, Metadata metadata) {
        
        this.uuid = uuid;
        this.metadata = metadata;
        this.metadata.getHeaders().put(HEADER_MESSAGE_CLASS, this.getClass().getCanonicalName());
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
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
    public static class Metadata {
        
        private String key;
        private Map<String,String> headers;
        private Long timestamp;
        private String source;
        private String target;
        private String rxTopic;
        private String txTopic;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }
        
        public Map<String, String> getHeaders() {
            return headers;
        }

        public void setHeaders(Map<String, String> headers) {
            this.headers = headers;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getTarget() {
            return target;
        }

        public void setTarget(String target) {
            this.target = target;
        }

        public String getRxTopic() {
            return rxTopic;
        }

        public void setRxTopic(String rxTopic) {
            this.rxTopic = rxTopic;
        }

        public String getTxTopic() {
            return txTopic;
        }

        public void setTxTopic(String txTopic) {
            this.txTopic = txTopic;
        }
        
        
        
        
                        
    }
}
