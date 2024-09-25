package cl.bithaus.medium.message.service.driver.mqtt;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

public class MediumMessagingServiceMQTTDriverConfig extends AbstractConfig {


    public static final String BROKER_URL_CONFIG = "mqtt.broker.url";
    public static final String BROKER_URL_DOC = "MQTT Broker URL";

    public static final String CLIENTID_CONFIG = "mqtt.clientid";
    public static final String CLIENTID_DOC = "MQTT Client ID";

    public static final String USERNAME_CONFIG = "mqtt.username";
    public static final String USERNAME_DOC = "MQTT Username";

    public static final String PASSWORD_CONFIG = "mqtt.password";
    public static final String PASSWORD_DOC = "MQTT Password";

    public static final String MESSAGE_QOS = "mqtt.message.qos";
    public static final String MESSAGE_QOS_DOC = "MQTT Message QOS";

    public static final String TOPICS_CONFIG = "mqtt.topics";
    public static final String TOPICS_DOC = "MQTT Topics, comma separated";

    public static final String CACERTIFICATE_FILENAME_COONFIG = "mqtt.cacertificate.filename";
    public static final String CACERTIFICATE_FILENAME_DOC = "MQTT CA Certificate Filename";

    public static final String CLIENT_CERTIFICATE_FILENAME = "mqtt.client.certificate.filename";
    public static final String CLIENT_CERTIFICATE_FILENAME_DOC = "MQTT Client Certificate Filename";

    public static final String PRIVATE_KEY_FILENAME = "mqtt.private.key.filename";
    public static final String PRIVATE_KEY_FILENAME_DOC = "MQTT Private Key Filename";

    public MediumMessagingServiceMQTTDriverConfig(Map<String,String> originals) {
        super(conf(), originals, true);
    }

    public static ConfigDef conf() {

        return new ConfigDef() 

            .define(BROKER_URL_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BROKER_URL_DOC)
            .define(USERNAME_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, USERNAME_DOC)
            .define(PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, PASSWORD_DOC)
            .define(MESSAGE_QOS, ConfigDef.Type.INT, 0, ConfigDef.Importance.HIGH, MESSAGE_QOS_DOC)
            .define(TOPICS_CONFIG, ConfigDef.Type.LIST, null, ConfigDef.Importance.LOW, TOPICS_DOC)
            .define(CACERTIFICATE_FILENAME_COONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, CACERTIFICATE_FILENAME_DOC)
            .define(CLIENT_CERTIFICATE_FILENAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, CLIENT_CERTIFICATE_FILENAME_DOC)
            .define(PRIVATE_KEY_FILENAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, PRIVATE_KEY_FILENAME_DOC)
            .define(CLIENTID_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, CLIENTID_DOC)
            ;
    }

    public String getBrokerUrl() {
        return this.getString(BROKER_URL_CONFIG);
    }

    public String getUsername() {
        return this.getString(USERNAME_CONFIG);
    }

    public String getClientID() {
        return this.getString(CLIENTID_CONFIG);
    }

    public Password getPassword() {
        return this.getPassword(PASSWORD_CONFIG);
    }

    public int getMessageQos() {
        return this.getInt(MESSAGE_QOS);
    }

    public String getPrivateKeyFilename() {
        return this.getString(PRIVATE_KEY_FILENAME);
    }

    public String[] getTopics() {

        if(this.getList(TOPICS_CONFIG) == null)
            return null;
            
        return this.getList(TOPICS_CONFIG).toArray(new String[0]);
    }

    public String getCaCertificateFilename() {
        return this.getString(CACERTIFICATE_FILENAME_COONFIG);
    }

    public String getClientCertificateFilename() {
        return this.getString(CLIENT_CERTIFICATE_FILENAME);
    }



}
