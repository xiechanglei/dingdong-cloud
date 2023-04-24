package com.xiechanglei.code.dingdongcloud.client;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@PropertySource("classpath:client.properties")
public class MqttClientInfo {
    @Value("${mqtt.client.id}")
    private String clientId;
    private MqttClient client;

    @PostConstruct
    public void init() {
        try {
            MqttClient client = new MqttClient("tcp://127.0.0.1:" + 60013, "test_002", new MemoryPersistence());
            client.connect();
            this.client = client;
        } catch (MqttException e) {
            e.printStackTrace();
        }
//        MqttMessage message = new MqttMessage(("message i:" + i).getBytes());
//        message.setQos(0);
//        client.publish("dingdong-messge", message);
    }

    public void publish(String message) {

    }

    public static void main(String[] args) throws MqttException {
        MqttClient client = new MqttClient("tcp://broker-cn.emqx.io:1883", "test_002", new MemoryPersistence());
        client.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable throwable) {

            }

            @Override
            public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                System.out.println(mqttMessage);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

            }
        });
        client.subscribe("dingdong-pub");
    }

}
