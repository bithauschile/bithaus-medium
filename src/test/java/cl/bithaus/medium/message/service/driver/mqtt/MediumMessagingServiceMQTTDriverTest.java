package cl.bithaus.medium.message.service.driver.mqtt;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cl.bithaus.medium.message.exception.MediumMessagingServiceException;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriverCallback;
import cl.bithaus.medium.record.MediumConsumerRecord;

public class MediumMessagingServiceMQTTDriverTest {

    private static final Logger logger = LoggerFactory.getLogger(MediumMessagingServiceMQTTDriverTest.class);

    private String caCertFilename = "/tmp/TEST_ca.crt";
    private String clientCertFilename = "/tmp/TEST_client.crt";
    private String privateKeyFilename = "/tmp/TEST_client.key";

    private String topic = "sgi/dcc/#";

    public MediumMessagingServiceMQTTDriverTest() throws Exception {

            java.util.logging.LogManager.getLogManager().readConfiguration(new FileInputStream("conf-example/logger.properties"));

    }


    @Test
    void testInit() throws Exception {

        Map<String,String> props = new HashMap<>();
        props.put(MediumMessagingServiceMQTTDriverConfig.BROKER_URL_CONFIG, "ssl://mqtt.emd.io:8883");
        // props.put(MediumMessagingServiceMQTTDriverConfig.BROKER_URL_CONFIG, "tcp://test.mosquitto.org:1883");
        props.put(MediumMessagingServiceMQTTDriverConfig.USERNAME_CONFIG, "dcc");
        props.put(MediumMessagingServiceMQTTDriverConfig.PASSWORD_CONFIG, "CumminsPower");
        props.put(MediumMessagingServiceMQTTDriverConfig.MESSAGE_QOS, "1");
        // props.put(MediumMessagingServiceMQTTDriverConfig.TOPICS_CONFIG, topic);
        props.put(MediumMessagingServiceMQTTDriverConfig.CACERTIFICATE_FILENAME_COONFIG, caCertFilename);
        props.put(MediumMessagingServiceMQTTDriverConfig.CLIENT_CERTIFICATE_FILENAME, clientCertFilename);
        props.put(MediumMessagingServiceMQTTDriverConfig.PRIVATE_KEY_FILENAME, privateKeyFilename);

        try {

            MediumMessagingServiceMQTTDriver driver = new MediumMessagingServiceMQTTDriver();


            SynchronousQueue queue = new SynchronousQueue();

            MediumMessagingServiceNetworkDriverCallback callback = new MediumMessagingServiceNetworkDriverCallback() {

                @Override
                public void onMessage(MediumConsumerRecord record) throws MediumMessagingServiceException {

                    logger.info("onMessage: {}", record);
                    queue.offer(record);

                }                                
            };

            this.createCertsKeyFiles();

            driver.init(props, callback);

            driver.start();

            driver.subscribe(Arrays.asList(topic).toArray(new String[0]));

            Object o = queue.poll(50, TimeUnit.SECONDS); 

            Assertions.assertNotNull(o);

            MediumConsumerRecord record = (MediumConsumerRecord) o;

            logger.info("record: {}", record);
        }
        finally {

            this.deleteCertsKeyFiles();
        }


    }
 

    private void deleteCertsKeyFiles() throws Exception {

        Files.deleteIfExists(java.nio.file.Paths.get(this.caCertFilename));
        Files.deleteIfExists(java.nio.file.Paths.get(this.clientCertFilename));
        Files.deleteIfExists(java.nio.file.Paths.get(this.privateKeyFilename));
    }

    private void createCertsKeyFiles() throws Exception {

        String caCert = "-----BEGIN CERTIFICATE-----\n" +

			"-----END CERTIFICATE-----";

        Files.write(java.nio.file.Paths.get(this.caCertFilename), caCert.getBytes(), StandardOpenOption.CREATE);


        String clientCert = "-----BEGIN CERTIFICATE-----\n" +
            "-----END CERTIFICATE-----\n";

        Files.write(java.nio.file.Paths.get(this.clientCertFilename), clientCert.getBytes(), StandardOpenOption.CREATE);



        String keyStr = "-----BEGIN RSA PRIVATE KEY-----\n" +

            "-----END RSA PRIVATE KEY-----";
        
        Files.write(java.nio.file.Paths.get(this.privateKeyFilename), keyStr.getBytes(), StandardOpenOption.CREATE);
         

    }
}
