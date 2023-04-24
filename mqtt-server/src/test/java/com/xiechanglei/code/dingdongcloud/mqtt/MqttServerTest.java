package com.xiechanglei.code.dingdongcloud.mqtt;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(classes = {MqttServer.class, MqttChannelInitializer.class, MqttMessageHandler.class, MqttMessageProcessor.class})
@TestPropertySource("classpath:test.properties")
public class MqttServerTest {
    @Test
    public void subscribe() throws InterruptedException {
        Thread.sleep(1000 * 120);
    }

}
