package cl.bithaus.medium.message.service.driver.amqp;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.qpid.jms.message.JmsBytesMessage;
import org.apache.qpid.jms.message.JmsTextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cl.bithaus.medium.message.exception.MediumMessagingServiceException;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriver;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriverCallback;
import cl.bithaus.medium.record.MediumConsumerRecord;
import cl.bithaus.medium.record.MediumProducerRecord;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.Session;

public class MediumMessagingServiceAMQPDriver implements MediumMessagingServiceNetworkDriver, MessageListener {

    private static final Logger logger = LoggerFactory.getLogger(MediumMessagingServiceAMQPDriver.class);
    private MediumMessagingServiceNetworkDriverCallback callback;    

    private ConnectionFactory cf;
    private Connection connection;
    private Context context;
    private MediumMessagingServiceAMQPDriverConfig config;
    private Session session;

    private MessageConsumer consumer;

    /* (non-Javadoc)
     * @see cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriver#init(java.util.Map, cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriverCallback)
     */
    @Override
    public void init(Map driverProperties, MediumMessagingServiceNetworkDriverCallback callback)
            throws MediumMessagingServiceException {
    
        if(driverProperties == null)
            throw new RuntimeException("Driver properties is null");

        if(callback == null)
            throw new RuntimeException("Callback is null");

        try {

            /*
             * References:
             * - https://docs.oracle.com/javaee/7/tutorial/jms-concepts.htm#GJFJG
             * - https://access.redhat.com/documentation/es-es/red_hat_enterprise_mrg/3/html/messaging_programming_reference/apache_qpid_jndi_properties_for_amqp_messaging
             */

            this.config = new MediumMessagingServiceAMQPDriverConfig(driverProperties);
            this.callback = callback;

            Hashtable<String,String> hashtable = new Hashtable();
            hashtable.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
            hashtable.put("connectionfactory.default", config.getServerURI());
            if(config.isConsumerEnabled()) {
                
                if(config.getConsumerQueue() == null)
                    throw new RuntimeException("Consumer queue is null");

                hashtable.put("queue.CONSUMER", config.getConsumerQueue());
            }
            
            org.apache.qpid.jms.jndi.JmsInitialContextFactory a;

            // org.apache.qpid.jms.

            if(config.isProducerEnabled()) {
                
                if(config.getProducerQueue() == null)
                    throw new RuntimeException("Producer queue is null");

                hashtable.put("queue.PRODUCER", config.getProducerQueue());
            }
            
            this.context = new InitialContext(hashtable);

            cf = (ConnectionFactory) context.lookup("default");

            logger.info("Init OK");

        }
        catch(Exception e) {
            throw new MediumMessagingServiceException("Error initializing AMQP driver", e);
        }
                        
        
    }





    @Override
    public void send(MediumProducerRecord record) throws MediumMessagingServiceException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void sendAsync(MediumProducerRecord record) throws MediumMessagingServiceException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void flush() throws MediumMessagingServiceException {
        // TODO Auto-generated method stub           
    }

    @Override
    public void subscribe(String[] topics) throws MediumMessagingServiceException {
        
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String[] getAvailableTopics() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isReady() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void start() throws MediumMessagingServiceException {

        try {

            this.connection = cf.createConnection(config.getServerUsername(), config.getServerPassword()); 
            connection.start();
        }
        catch(Exception e) {

            throw new MediumMessagingServiceException("Error starting connection", e);
        }

        try {
            this.session = this.connection.createSession(false,Session.CLIENT_ACKNOWLEDGE);
        }
        catch(Exception e) {                
            throw new MediumMessagingServiceException("Error creating session", e);
        }

        try {
            if(this.config.isConsumerEnabled())
                startConsumer();
        }
        catch(Exception e) {
            throw new MediumMessagingServiceException("Error starting consumer", e);
        }

        // if(this.config.isProducerEnabled())
        //     startProducer();
    }

    private void startConsumer() throws NamingException, JMSException {
        
        logger.info("Starting consumer");

        Destination queue = (Destination) context.lookup("CONSUMER");

        logger.info("Creating consumer for queue: " + queue.toString());

        this.consumer = session.createConsumer(queue);

        consumer.setMessageListener(this);

        logger.info("Consumer started");
        
    }



    @Override
    public void stop() throws MediumMessagingServiceException {
        
        try {
            if(this.consumer != null) {
                logger.info("Stopping consumer");
                this.consumer.close();
            }
        }
        catch (Exception e){
            throw new MediumMessagingServiceException("Error stopping consumer", e);
        }

        try {
            if(this.session != null) {
                logger.info("Stopping session");
                this.session.close();
            }
        }
        catch (Exception e){
            throw new MediumMessagingServiceException("Error stopping session", e);
        }

        try {
            if(this.connection != null) {
                logger.info("Stopping connection");
                this.connection.close();
            }
        }
        catch (Exception e){
            throw new MediumMessagingServiceException("Error stopping connection", e);
        }
        
    }





    @Override
    public void onMessage(Message rawMsg) {

        if(logger.isTraceEnabled())
            logger.trace("IN: " + rawMsg.toString());

        if(rawMsg instanceof JmsBytesMessage)
            onJmsBytesMessage((JmsBytesMessage) rawMsg);

        else if(rawMsg instanceof JmsTextMessage)
            onJmsTextMessage((JmsTextMessage) rawMsg);

        // else if(rawMsg instanceof JmsMapMessage)
        //     onJmsMapMessage((JmsMapMessage) rawMsg);

        // else if(rawMsg instanceof JmsObjectMessage)
        //     onJmsObjectMessage((JmsObjectMessage) rawMsg);

        // else if(rawMsg instanceof JmsStreamMessage)
        //     onJmsStreamMessage((JmsStreamMessage) rawMsg);

        // else if(rawMsg instanceof JmsMessage)
        //     onJmsMessage((JmsMessage) rawMsg);

        else
            logger.warn("Unsupported message type: " + rawMsg.getClass().getName() + "discarding: " + rawMsg.toString());

    }

    private void onJmsBytesMessage(JmsBytesMessage message) {
        
        try {

            Map<String,String> headers = new HashMap<>();

            message.getPropertyNames().asIterator().forEachRemaining(key -> {
                try {

                    headers.put(key.toString(), message.getStringProperty((String) key));

                } catch (JMSException e) {
                    
                    logger.warn("Error copying message properties to record headers", e);
                }
            });

            
            headers.put("JMSMessageID", message.getJMSMessageID());
            headers.put("JMSRedelivered", String.valueOf(message.getJMSRedelivered()));
            headers.put("JMSExpiration", String.valueOf(message.getJMSExpiration()));


            byte[] body = new byte[(int) message.getBodyLength()];
            message.readBytes(body);

  
            String key = null;
            String topic = message.getJMSDestination().toString();
            Long timestampStr = message.getJMSDeliveryTime();
            Long timestamp = timestampStr!=null?Long.parseLong(timestampStr.toString()):null;
            
            
            MediumConsumerRecord cr = new MediumConsumerRecord(key, new String(body), topic, headers, timestamp);        
            this.callback.onMessage(cr);

            message.acknowledge();
        }
        catch(Exception e) {

            logger.error("Error processing message", e);
            // algo mas?
            
        }
        
    }

    private void onJmsTextMessage(JmsTextMessage message) {
        
        try {

            Map<String,String> headers = new HashMap<>();

            message.getPropertyNames().asIterator().forEachRemaining(key -> {
                try {

                    headers.put(key.toString(), message.getStringProperty((String) key));

                } catch (JMSException e) {
                    
                    logger.warn("Error copying message properties to record headers", e);
                }
            });

            
            headers.put("JMSMessageID", message.getJMSMessageID());
            headers.put("JMSRedelivered", String.valueOf(message.getJMSRedelivered()));
            headers.put("JMSExpiration", String.valueOf(message.getJMSExpiration()));

            String body = message.getText();

            String key = null;
            String topic = message.getJMSDestination().toString();
            Long timestampStr = message.getJMSDeliveryTime();
            Long timestamp = timestampStr!=null?Long.parseLong(timestampStr.toString()):null;

            MediumConsumerRecord cr = new MediumConsumerRecord(key, body, topic, headers, timestamp);
            this.callback.onMessage(cr);

            message.acknowledge();

        }
        catch(Exception e) {

            logger.error("Error processing message", e);
            // algo mas?
            
        }   
    } 
    
}
