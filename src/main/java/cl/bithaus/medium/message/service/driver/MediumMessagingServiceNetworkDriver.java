/*
 * Copyright (c) BitHaus Software Factory & Boutique (Chile)
 * All rights reserved. www.bithaus.cl.
 * 
 * All rights to this product are owned by BitHaus Chile and may only by used 
 * under the terms of its associated license document. 
 * You may NOT copy, modify, sublicense or distribute this source file or 
 * portions of it unless previously authorized by writing by BitHaus Chile.
 * In any event, this notice must always be included verbatim with this file.
 * 
 */
package cl.bithaus.medium.message.service.driver;

import cl.bithaus.medium.record.MediumProducerRecord;
import cl.bithaus.medium.message.exception.MediumMessagingServiceException;
import java.util.Map;

/**
 * Connector class for MediumMessagingService and underlying Kafka
 * @author jmakuc
 */
public interface MediumMessagingServiceNetworkDriver {
    
    /**
     * Initialization rutine of the network driver
     * @param driverProperties driver configuration
     * @param callback receptor of the raw message from the underlying message system
     * @throws MediumMessagingServiceException 
     */
    public void init(Map driverProperties, MediumMessagingServiceNetworkDriverCallback callback) throws MediumMessagingServiceException;
    
    /**
     * Sends a messge to the underlying messaging system synchronously
     * @param record record to be sent
     * @throws MediumMessagingServiceException 
     */
    public void send(MediumProducerRecord record) throws MediumMessagingServiceException;
    
    /**
     * Sends a messge to the underlying messaging system asynchronously
     * @param record record to be sent
     * @throws MediumMessagingServiceException 
     */
    public void sendAsync(MediumProducerRecord record) throws MediumMessagingServiceException;

    /**
     * Flushes the underlying producer. This method should block until all 
     * the messages are acknowledged by the underlying messaging system.
     * @throws MediumMessagingServiceException
     */
    public void flush() throws MediumMessagingServiceException;

    /**
     * Subscribe the underlying consume to read messages from a limited group 
     * of topics.
     * @param topics 
     * @throws cl.bithaus.medium.message.exception.MediumMessagingServiceException 
     */
    public void subscribe(String[] topics) throws MediumMessagingServiceException;
        
    /**
     * Topics configured on this driver
     * @return 
     */
    public String[] getAvailableTopics();
    
    /**
     * Is the driver ready for work
     * @return 
     */
    public boolean isReady();
        
    public void start() throws MediumMessagingServiceException;
    
    public void stop() throws MediumMessagingServiceException;
    
         
    
    
}
