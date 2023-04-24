package com.xiechanglei.code.dingdongcloud.mqtt;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Service
@RequiredArgsConstructor
@Log4j2
@PropertySource("classpath:mqtt.properties")
public class MqttServer {
    @Value("${mqtt.server.port}")
    private int serverPort;
    private final MqttChannelInitializer mqttChannelInitializer;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    @PostConstruct
    public void start() {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup);
            b.channel(NioServerSocketChannel.class);
            b.childHandler(mqttChannelInitializer);
            b.option(ChannelOption.SO_BACKLOG, 128);
            b.childOption(ChannelOption.SO_KEEPALIVE, true);
            ChannelFuture channelFuture = b.bind(serverPort).sync();
            if (channelFuture.isSuccess()) {
                log.info("mqtt server listening on port {} and ready for connections...", serverPort);
            } else {
                log.warn("start mqtt sever failed on port {}", serverPort);
                this.shutDown();
            }
        } catch (Exception e) {
            shutDown();
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void shutDown() {
        log.info("shutdown mqtt server...");
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            bossGroup = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
            workerGroup = null;
        }

    }
}
