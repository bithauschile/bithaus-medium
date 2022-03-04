/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
     * Sends a messge to the underlying Kafka
     * @param record record to be sent
     * @throws MediumMessagingServiceException 
     */
    public void send(MediumProducerRecord record) throws MediumMessagingServiceException;
        
    public String[] getAvailableTopic();
    
    public boolean isReady();
    
    public void start() throws MediumMessagingServiceException;
    
    public void stop() throws MediumMessagingServiceException;
    
         
    
    
}
