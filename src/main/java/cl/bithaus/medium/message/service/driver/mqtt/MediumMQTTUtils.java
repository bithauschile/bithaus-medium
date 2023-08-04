package cl.bithaus.medium.message.service.driver.mqtt;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.google.gson.Gson;

import cl.bithaus.medium.record.MediumConsumerRecord;
import cl.bithaus.medium.utils.MessageUtils;

public class MediumMQTTUtils {
    
    public static String HEADER_QOS = "qos";
    public static String MESSAGE_ID = "messageId";

    private static final Gson gson = MessageUtils.getMediumGson();

    public static MediumConsumerRecord fromMQTTMessage(MqttMessage message, String topic) {

        Map<String,String> headers = new HashMap<>();
        headers.put(HEADER_QOS, message.getQos() + "");
        headers.put(MESSAGE_ID, message.getId() + "");

        String payload = new String(message.getPayload()); 

        return new MediumConsumerRecord(null, payload, topic);
    }
}
