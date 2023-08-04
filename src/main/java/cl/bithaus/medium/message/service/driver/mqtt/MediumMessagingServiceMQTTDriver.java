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
package cl.bithaus.medium.message.service.driver.mqtt;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cl.bithaus.medium.message.exception.MediumMessagingServiceException;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriver;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriverCallback;
import cl.bithaus.medium.record.MediumConsumerRecord;
import cl.bithaus.medium.record.MediumProducerRecord;
import cl.bithaus.medium.utils.CryptoTools;

/**
 * MQTT Driver for MediumMessagingService
 * @author Roberto Morales <rmorales@bithaus.cl>
 * @author Jonathan Makuc <jmakuc@bithaus.cl>
 */
public class MediumMessagingServiceMQTTDriver implements MediumMessagingServiceNetworkDriver {
    
    public static final Long RECONNECT_DELAY_MILLIS = 10000L;

    public static final Logger log = LoggerFactory.getLogger(MediumMessagingServiceMQTTDriver.class);

    private MediumMessagingServiceMQTTDriverConfig config;

    private MediumMessagingServiceNetworkDriverCallback callback;

    private MqttClient mqttClient;

    private MqttConnectOptions connectionOptions;

    private boolean isReady = false;

    private List<String> subscribedTopics = new ArrayList<>();

    @Override
    public void init(Map driverProperties, MediumMessagingServiceNetworkDriverCallback callback)
            throws MediumMessagingServiceException {

        try {   
            this.config = new MediumMessagingServiceMQTTDriverConfig(driverProperties);
            this.callback = callback;

            String clientId = this.config.getUsername();

            if(clientId == null || clientId.length() == 0)
                clientId = MqttClient.generateClientId();


            this.mqttClient = new MqttClient(config.getBrokerUrl(), clientId, new MemoryPersistence());
            this.mqttClient.setCallback(getMqttCallback());

            this.connectionOptions = new MqttConnectOptions();
            this.connectionOptions.setAutomaticReconnect(true);
            this.connectionOptions.setCleanSession(true);
            if(this.config.getUsername() != null && this.config.getUsername().length() > 0) {

                log.info("Using username: " + config.getUsername()); 
                this.connectionOptions.setUserName(config.getUsername());
            }
            if(this.config.getPassword() != null && this.config.getPassword().value() != null && this.config.getPassword().value().length() > 0) {

                log.info("Using password"); 
                this.connectionOptions.setPassword(config.getPassword().value().toCharArray());
            }
            this.connectionOptions.setHttpsHostnameVerificationEnabled(false);

            if(this.config.getCaCertificateFilename() != null && this.config.getClientCertificateFilename() != null && this.config.getPrivateKeyFilename() != null)
                this.connectionOptions.setSocketFactory(this.getSocketFactory());

            this.connectionOptions.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1);

                       

            log.info("Init OK");
        }
        catch(Exception e) {
            throw new MediumMessagingServiceException("Error initializing driver", e);
        }
    }
   

    @Override
    public void start() throws MediumMessagingServiceException {
        
        try {

            try { 

                if(this.mqttClient.isConnected()) {
                    
                    this.stop();
                }
            }
            catch(Exception e) {
                log.warn("Error disconnecting", e);
            }
            
            log.info("Connecting..."); 

            IMqttToken token = this.mqttClient.connectWithResult(connectionOptions);

            token.waitForCompletion();

            this.isReady = token.isComplete();
            
            if(this.isReady())
                log.info("Connection OK");
            else
                throw new RuntimeException("Connection Not ready"); 

            if(this.config.getTopics() != null && this.config.getTopics().length > 0)
                this.subscribe(this.config.getTopics()); 

        }
        catch(Exception e) {

            e.printStackTrace();
            throw new MediumMessagingServiceException("Error starting driver", e);
        }
    }

    @Override
    public void stop() throws MediumMessagingServiceException {
        
        try {

            log.info("Disconnecting...");
            this.mqttClient.close(true);
            subscribedTopics.clear();
        }
        catch(Exception e) {

            throw new MediumMessagingServiceException("Error stopping driver", e);
        }


    }

    private MqttCallback getMqttCallback() {

        return new MqttCallback() {
                
                @Override
                public void connectionLost(Throwable cause) {

                    log.warn("Connection lost");
                    isReady = false;
                    subscribedTopics.clear();

                    try {

                        log.info("Waiting to reconnect ({} millis)", RECONNECT_DELAY_MILLIS);

                        Thread.sleep(RECONNECT_DELAY_MILLIS);
                    }
                    catch(InterruptedException e) {
                        log.warn("Interrupted while waiting to reconnect", e);
                    }

                    try {
                        start();
                    }
                    catch(MediumMessagingServiceException e) {
                        log.warn("Error reconnecting", e);
                        System.exit(1);
                    }
                }
    
                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    
                    if(log.isTraceEnabled())
                        log.trace("Message arrived on topic {}: {}", topic, message);

                    MediumConsumerRecord mediumRecord = MediumMQTTUtils.fromMQTTMessage(message, topic);
                    
                    try {

                        callback.onMessage(mediumRecord);
                    }
                    catch(Exception e) {

                        log.error("Error dispatching message: " + mediumRecord, e);
                    }

                }
    
                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
 
                    if(log.isTraceEnabled())
                        log.trace("Delivery complete: " + token.getMessageId());
                }
        };
    }    


    @Override
    public void send(MediumProducerRecord record) throws MediumMessagingServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'send'");
    }

    @Override
    public void sendAsync(MediumProducerRecord record) throws MediumMessagingServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'sendAsync'");
    }

    @Override
    public void flush() throws MediumMessagingServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'flush'");
    }

    @Override
    public void subscribe(String[] topics) throws MediumMessagingServiceException {
        
        // create an int array with the same length as topics with content this.config.getMessageQos()
        int[] qos = new int[topics.length];
        for(int i = 0; i < qos.length; i++)
            qos[i] = this.config.getMessageQos();

        log.info(null, "Subscribing to topics: {} with QOS: {}", topics, this.config.getMessageQos());

        try {
            this.mqttClient.subscribe(topics, qos); 
            this.subscribedTopics.addAll(java.util.Arrays.asList(topics));
        }
        catch(MqttException e) {

            String msg = "";

            switch(e.getReasonCode()) {
                case MqttException.REASON_CODE_CLIENT_EXCEPTION:  
                    msg = "REASON_CODE_CLIENT_EXCEPTION";
                    break;
                case MqttException.REASON_CODE_INVALID_PROTOCOL_VERSION:  
                    msg = "REASON_CODE_INVALID_PROTOCOL_VERSION";
                    break;
                case MqttException.REASON_CODE_INVALID_CLIENT_ID:  
                    msg = "REASON_CODE_INVALID_CLIENT_ID";
                    break;
                case MqttException.REASON_CODE_BROKER_UNAVAILABLE:  
                    msg = "REASON_CODE_BROKER_UNAVAILABLE";
                    break;
                case MqttException.REASON_CODE_FAILED_AUTHENTICATION:  
                    msg = "REASON_CODE_FAILED_AUTHENTICATION";
                    break;
                case MqttException.REASON_CODE_NOT_AUTHORIZED:  
                    msg = "REASON_CODE_NOT_AUTHORIZED";
                    break;
                case MqttException.REASON_CODE_UNEXPECTED_ERROR:  
                    msg = "REASON_CODE_UNEXPECTED_ERROR";
                    break;
                case MqttException.REASON_CODE_SUBSCRIBE_FAILED:  
                    msg = "REASON_CODE_SUBSCRIBE_FAILED";
                    break;
                case MqttException.REASON_CODE_CLIENT_TIMEOUT:  
                    msg = "REASON_CODE_CLIENT_TIMEOUT";
                    break;
                case MqttException.REASON_CODE_NO_MESSAGE_IDS_AVAILABLE:  
                    msg = "REASON_CODE_NO_MESSAGE_IDS_AVAILABLE";
                    break;
                case MqttException.REASON_CODE_WRITE_TIMEOUT:  
                    msg = "REASON_CODE_WRITE_TIMEOUT";
                    break;
                case MqttException.REASON_CODE_CLIENT_CONNECTED:  
                    msg = "REASON_CODE_CLIENT_CONNECTED";
                    break;
                case MqttException.REASON_CODE_CLIENT_ALREADY_DISCONNECTED:  
                    msg = "REASON_CODE_CLIENT_ALREADY_DISCONNECTED";
                    break;
                case MqttException.REASON_CODE_CLIENT_DISCONNECTING:  
                    msg = "REASON_CODE_CLIENT_DISCONNECTING";
                    break;
                case MqttException.REASON_CODE_SERVER_CONNECT_ERROR:  
                    msg = "REASON_CODE_SERVER_CONNECT_ERROR";
                    break;
                case MqttException.REASON_CODE_CLIENT_NOT_CONNECTED:  
                    msg = "REASON_CODE_CLIENT_NOT_CONNECTED";
                    break;
                case MqttException.REASON_CODE_SOCKET_FACTORY_MISMATCH:  
                    msg = "REASON_CODE_SOCKET_FACTORY_MISMATCH";
                    break;
                case MqttException.REASON_CODE_SSL_CONFIG_ERROR:  
                    msg = "REASON_CODE_SSL_CONFIG_ERROR";
                    break;
                case MqttException.REASON_CODE_CLIENT_DISCONNECT_PROHIBITED:  
                    msg = "REASON_CODE_CLIENT_DISCONNECT_PROHIBITED";
                    break;
                case MqttException.REASON_CODE_INVALID_MESSAGE:  
                    msg = "REASON_CODE_INVALID_MESSAGE";
                    break;
                case MqttException.REASON_CODE_CONNECTION_LOST:  
                    msg = "REASON_CODE_CONNECTION_LOST";
                    break;
                case MqttException.REASON_CODE_CONNECT_IN_PROGRESS:  
                    msg = "REASON_CODE_CONNECT_IN_PROGRESS";
                    break;
                case MqttException.REASON_CODE_CLIENT_CLOSED:  
                    msg = "REASON_CODE_CLIENT_CLOSED";
                    break;
                case MqttException.REASON_CODE_TOKEN_INUSE:  
                    msg = "REASON_CODE_TOKEN_INUSE";
                    break;
                case MqttException.REASON_CODE_MAX_INFLIGHT:  
                    msg = "REASON_CODE_MAX_INFLIGHT";
                    break;
            }

            throw new MediumMessagingServiceException("Error subscribing to topics: " + msg, e);
        }
    }

    @Override
    public String[] getAvailableTopics() {
        return this.config.getTopics();
    }

    @Override
    public boolean isReady() {
        return isReady;
    }
   


    private SSLSocketFactory getSocketFactory () 
        throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, KeyStoreException, CertificateException, UnrecoverableKeyException, KeyManagementException {
        
        log.info("Initializing SSL socket factory"); 

        X509Certificate caCert = CryptoTools.loadX509Certificate(this.config.getCaCertificateFilename());
        X509Certificate clientCert = CryptoTools.loadX509Certificate(this.config.getClientCertificateFilename());
        RSAPrivateKey privateKey = CryptoTools.loadRSAPrivateKey(this.config.getPrivateKeyFilename()); 

 
		// CA certificate is used to authenticate server
		KeyStore caKs = KeyStore.getInstance(KeyStore.getDefaultType());
		caKs.load(null, null);
		caKs.setCertificateEntry("ca-certificate", caCert);
		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init(caKs);

		// client key and certificates are sent to server so it can authenticate us
		KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
		ks.load(null, null);
		ks.setCertificateEntry("certificate", clientCert);
		ks.setKeyEntry("private-key", privateKey, this.config.getPassword().value().toCharArray(), new java.security.cert.Certificate[]{ clientCert });
		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(ks, this.config.getPassword().value().toCharArray());

		// finally, create SSL socket factory
		SSLContext context = SSLContext.getInstance("TLSv1.2");
		context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

		return context.getSocketFactory();
	}    

}
