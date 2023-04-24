package com.xiechanglei.code.dingdongcloud.mqtt;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

/**
 * https://blog.csdn.net/myyhtw/article/details/114041042
 */
@Log4j2
@Component
@RequiredArgsConstructor
@ChannelHandler.Sharable
public class MqttMessageHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private final MqttMessageProcessor processor;

    @Override
    protected void channelRead0(ChannelHandlerContext context, MqttMessage mqttMessage) {
        log.info("received message:{}", mqttMessage);
        MqttFixedHeader header = mqttMessage.fixedHeader();
        MqttMessage response = null;
        switch (header.messageType()) {
            case CONNECT:
                response = processor.handleConnect((MqttConnectMessage) mqttMessage, context);
                break;
            case DISCONNECT:
                response = processor.handleDisconnect(context);
                break;
            case PINGREQ:
                response = processor.handlePing();
                break;
            case PUBLISH:
                response = processor.handlePublish((MqttPublishMessage) mqttMessage);
                break;
            case SUBSCRIBE:
                response = processor.handleSubscribe((MqttSubscribeMessage) mqttMessage, context);
                break;
            case UNSUBSCRIBE:
                response = processor.handleUnSubscribe((MqttUnsubscribeMessage) mqttMessage, context);
                break;
            case PUBREL:
                response = processor.handPubRel(mqttMessage);
        }
        log.info("response message:{}", response);
        if (response != null) {
            context.writeAndFlush(response);
        }
    }


    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        processor.handleDisconnect(ctx);
        super.channelInactive(ctx);
    }
}
