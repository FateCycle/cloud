package com.imooc.food.orderservice.config;


import com.imooc.food.orderservice.service.OrderMessageService;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.concurrent.TimeoutException;


@Configuration
public class RabbitConfig {

    @Autowired
    private OrderMessageService orderMessageService;

    @Autowired
    public void startListenMessage() throws IOException, InterruptedException {
        orderMessageService.handleMessage();
    }


}
