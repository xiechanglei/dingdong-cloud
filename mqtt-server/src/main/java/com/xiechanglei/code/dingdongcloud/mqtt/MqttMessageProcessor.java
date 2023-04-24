package com.xiechanglei.code.dingdongcloud.mqtt;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.*;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
public class MqttMessageProcessor {

    private final Map<String, ChannelHandlerContext> clientMap = new ConcurrentHashMap<>();
    private final Map<String, Set<ChannelHandlerContext>> subscribeMap = new ConcurrentHashMap<>();

    /**
     * handle subscribe
     */
    protected MqttMessage handleSubscribe(MqttSubscribeMessage mqttSubscribeMessage, ChannelHandlerContext context) {
        List<MqttTopicSubscription> mqttTopicSubscriptions = mqttSubscribeMessage.payload().topicSubscriptions();
        mqttTopicSubscriptions.forEach(subscription -> subscribeMap.computeIfAbsent(subscription.topicName(), k -> new HashSet<>()).add(context));
        List<Integer> grantedQoSLevels = mqttTopicSubscriptions.stream().map(MqttTopicSubscription::qualityOfService).map(MqttQoS::value).collect(Collectors.toList());
        MqttMessageIdVariableHeader variableHeaderBack = MqttMessageIdVariableHeader.from(mqttSubscribeMessage.variableHeader().messageId());
        MqttSubAckPayload payloadBack = new MqttSubAckPayload(grantedQoSLevels);
        MqttFixedHeader mqttFixedHeaderBack = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2 + grantedQoSLevels.size());
        return new MqttSubAckMessage(mqttFixedHeaderBack, variableHeaderBack, payloadBack);
    }


    /**
     * handle unsubscribe
     */
    public MqttMessage handleUnSubscribe(MqttUnsubscribeMessage mqttMessage, ChannelHandlerContext context) {
        List<String> topics = mqttMessage.payload().topics();
        topics.forEach(topicName -> subscribeMap.computeIfAbsent(topicName, k -> new HashSet<>()).remove(context));
        MqttFixedHeader mqttFixedHeaderBack = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2);
        return new MqttUnsubAckMessage(mqttFixedHeaderBack, MqttMessageIdVariableHeader.from(mqttMessage.variableHeader().messageId()));
    }

    /**
     * handle publish message
     */
    protected MqttMessage handlePublish(MqttPublishMessage msg) {
        MqttFixedHeader fixedHeader = msg.fixedHeader();
        MqttPublishVariableHeader variableHeader = msg.variableHeader();

        byte[] payloadBytes = new byte[msg.payload().readableBytes()];
        msg.payload().readBytes(payloadBytes);
        subscribeMap.computeIfAbsent(variableHeader.topicName(), k -> new HashSet<>()).forEach(context -> {
            MqttFixedHeader requestFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, fixedHeader.isRetain(), 0);
            MqttPublishVariableHeader requestVariableHeader = new MqttPublishVariableHeader(variableHeader.topicName(), 0);
            context.writeAndFlush(new MqttPublishMessage(requestFixedHeader, requestVariableHeader, Unpooled.buffer().writeBytes(payloadBytes)));
        });

        MqttMessage ackMessage = null;
        switch (fixedHeader.qosLevel()) {
            case AT_LEAST_ONCE:        //	至少一次
                MqttFixedHeader ackFixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, fixedHeader.isDup(), MqttQoS.AT_MOST_ONCE, fixedHeader.isRetain(), 0x02);
                ackMessage = new MqttPubAckMessage(ackFixedHeader, MqttMessageIdVariableHeader.from(variableHeader.packetId()));
                break;
            case EXACTLY_ONCE:        //	刚好一次
                MqttFixedHeader ackFixedHeader2 = new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_LEAST_ONCE, false, 0x02);
                ackMessage = new MqttMessage(ackFixedHeader2, MqttMessageIdVariableHeader.from(variableHeader.packetId()));
                break;
        }
        return ackMessage;
    }

    /**
     * handle ping message
     */
    protected MqttMessage handlePing() {
        return new MqttMessage(new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0));
    }

    /**
     * handle connect message
     */
    protected MqttMessage handleConnect(MqttConnectMessage mqttConnectMessage, ChannelHandlerContext ctx) {
        String clientIdentifier = mqttConnectMessage.payload().clientIdentifier();
        MqttConnectVariableHeader variableHeaderInfo = mqttConnectMessage.variableHeader();
        MqttFixedHeader fixedHeader = mqttConnectMessage.fixedHeader();
        MqttConnAckVariableHeader ackVariableHeader = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, variableHeaderInfo.isCleanSession());
        MqttFixedHeader ackFixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, fixedHeader.isDup(), MqttQoS.AT_MOST_ONCE, fixedHeader.isRetain(), 0x02);
        clientMap.put(clientIdentifier, ctx);
        return new MqttConnAckMessage(ackFixedHeader, ackVariableHeader);
    }

    /**
     * handle disconnect message
     */
    protected MqttMessage handleDisconnect(ChannelHandlerContext ctx) {
        clientMap.forEach((clientId, context) -> {
            if (context.channel().id().equals(ctx.channel().id())) {
                clientMap.remove(clientId);
            }
        });
        subscribeMap.forEach((sub, ctxList) -> {
            if (ctxList != null) {
                ctxList.remove(ctx);
            }
        });
        return null;
    }

    /**
     * handle subscribe rel
     */
    public MqttMessage handPubRel(MqttMessage mqttMessage) {
        MqttMessageIdVariableHeader messageIdVariableHeader = (MqttMessageIdVariableHeader) mqttMessage.variableHeader();
        MqttFixedHeader mqttFixedHeaderBack = new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0x02);
        MqttMessageIdVariableHeader mqttMessageIdVariableHeaderBack = MqttMessageIdVariableHeader.from(messageIdVariableHeader.messageId());
        return new MqttMessage(mqttFixedHeaderBack, mqttMessageIdVariableHeaderBack);
    }
}
