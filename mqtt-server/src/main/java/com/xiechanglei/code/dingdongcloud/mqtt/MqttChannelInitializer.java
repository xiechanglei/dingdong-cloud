package com.xiechanglei.code.dingdongcloud.mqtt;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MqttChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final MqttMessageHandler mqttMessageHandler;
    public static final int IDLE_READ_TIME = 60;
    public static final int IDLE_WRITE_TIME = 60;

    @Override
    protected void initChannel(SocketChannel socketChannel) {
        ChannelPipeline p = socketChannel.pipeline();
        p.addLast("idleStateHandler", new IdleStateHandler(IDLE_READ_TIME, IDLE_WRITE_TIME, 0));
        p.addLast(new MqttDecoder());
        p.addLast(MqttEncoder.INSTANCE);
        p.addLast(mqttMessageHandler);
    }

}
