package com.imooc.food.orderservice.config;


import com.imooc.food.orderservice.dto.OrderMessageDTO;
import com.imooc.food.orderservice.service.OrderMessageService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.support.converter.ClassMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;


@Configuration
@Slf4j
public class RabbitConfig {

    @Autowired
    private OrderMessageService orderMessageService;


    // 声明式配置rabbitMQ
    @Bean
    public Queue queue1(){
        return new Queue("queue.order");
    }

    @Bean
    public Exchange exchange1() {
        return new DirectExchange("exchange.order.restaurant");
    }

    @Bean
    public Binding binding1() {
        return new Binding("queue.order",
                Binding.DestinationType.QUEUE,"exchange.order.restaurant",
                "key.order",null);
    }


    @Bean
    public Exchange exchange2() {
        return new DirectExchange("exchange.order.deliveryman");
    }


    @Bean
    public Binding binding2() {
        return new Binding("queue.order",
                Binding.DestinationType.QUEUE,"exchange.order.deliveryman",
                "key.order",null);
    }

    @Bean
    public Exchange exchange3() {
        return new FanoutExchange("exchange.order.settlement");
    }

    @Bean
    public Exchange exchange4() {
        return new FanoutExchange("exchange.settlement.order");
    }

    @Bean
    public Binding binding3() {
        return new Binding("queue.order",
                Binding.DestinationType.QUEUE,"exchange.order.settlement",
                "key.order",null);
    }

    @Bean
    public Exchange exchange5() {
        return new TopicExchange("exchange.order.reward");
    }


    @Bean
    public Binding binding4() {
        return new Binding("queue.order",
                Binding.DestinationType.QUEUE,"exchange.order.reward",
                "key.order",null);
    }


    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(5672);
        connectionFactory.setPassword("guest");
        connectionFactory.setUsername("guest");
        // 设置回调有效
        connectionFactory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.SIMPLE);
        connectionFactory.setPublisherReturns(true);
        connectionFactory.createConnection();
        return connectionFactory;
    }



    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
        rabbitAdmin.setAutoStartup(true);
        return rabbitAdmin;
    }


    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMandatory(true);
        // 消息返回的回调
        rabbitTemplate.setReturnsCallback(returned -> {
            log.info("{}", returned);
        });
        // 消息确认回调
        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            log.info("correlationData:{},ack:{},cause:{}", correlationData,ack,cause);
        });
        return rabbitTemplate;
    }


    @Bean
    public RabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        return factory;
    }

    @Bean
    public SimpleMessageListenerContainer messageListenerContainer(ConnectionFactory connectionFactory) {
        SimpleMessageListenerContainer simpleMessageListenerContainer =
                new SimpleMessageListenerContainer(connectionFactory);
        simpleMessageListenerContainer.setQueueNames("queue.order");
        // 并发消费的数量(相当于异步线程数)
        simpleMessageListenerContainer.setConcurrentConsumers(3);
        simpleMessageListenerContainer.setMaxConcurrentConsumers(5);
        // 确认方式  自动
        simpleMessageListenerContainer.setAcknowledgeMode(AcknowledgeMode.AUTO);
        // 消费的回调
//        simpleMessageListenerContainer.setMessageListener(message -> {
//            log.info("{}",message);
//        });

        // 确认方式  手动
//        simpleMessageListenerContainer.setAcknowledgeMode(AcknowledgeMode.MANUAL);
//        // 消费的回调
//        simpleMessageListenerContainer.setMessageListener((ChannelAwareMessageListener) (message, channel) -> {
//            log.info("{}",message);
//            assert channel != null;
//            channel.basicAck(message.getMessageProperties().getDeliveryTag(),false);
//        });


        // 消费端限流
//        simpleMessageListenerContainer.setPrefetchCount(1);

        // 消费queue.order的回调方法为默认为orderMessageService的handleMessage
        // 当然也可以通过map进行制定
        MessageListenerAdapter messageListenerAdapter = new MessageListenerAdapter(orderMessageService);
        Map<String,String> methodMap = new HashMap<>(8);
        methodMap.put("queue.order","handleMessage");
        messageListenerAdapter.setQueueOrTagToMethodName(methodMap);
        // 将消息的传参转化成对应的对象，不设置classMapper默认转化成LinkedHashMap
        Jackson2JsonMessageConverter jackson2JsonMessageConverter = new Jackson2JsonMessageConverter();
        jackson2JsonMessageConverter.setClassMapper(new ClassMapper() {
            @Override
            public void fromClass(Class<?> clazz, MessageProperties properties) {

            }

            @Override
            public Class<?> toClass(MessageProperties properties) {
                return OrderMessageDTO.class;
            }
        });
        messageListenerAdapter.setMessageConverter(jackson2JsonMessageConverter);
        simpleMessageListenerContainer.setMessageListener(messageListenerAdapter);


        return simpleMessageListenerContainer;

    }







    // 不使用声明式消息队列
//    @Autowired
//    public void initRabbit() {
//        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
//        connectionFactory.setHost("127.0.0.1");
//        connectionFactory.setPort(5672);
//        connectionFactory.setUsername("guest");
//        connectionFactory.setPassword("guest");
//
//        RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
//
//        Exchange exchange = new DirectExchange("exchange.order.restaurant");
//        rabbitAdmin.declareExchange(exchange);
//
//        Queue queue = new Queue("queue.order");
//        rabbitAdmin.declareQueue(queue);
//
//        Binding binding = new Binding("queue.order",
//                Binding.DestinationType.QUEUE,"exchange.order.restaurant",
//                "key.order",null);
//        rabbitAdmin.declareBinding(binding);
//
//
//        exchange = new DirectExchange("exchange.order.deliveryman");
//        rabbitAdmin.declareExchange(exchange);
//
//        binding = new Binding("queue.order",
//                Binding.DestinationType.QUEUE,"exchange.order.deliveryman",
//                "key.order",null);
//        rabbitAdmin.declareBinding(binding);
//
//        exchange = new FanoutExchange("exchange.order.settlement");
//        rabbitAdmin.declareExchange(exchange);
//
//        binding = new Binding("queue.order",
//                Binding.DestinationType.QUEUE,"exchange.order.settlement",
//                "key.order",null);
//        rabbitAdmin.declareBinding(binding);
//
//
//        exchange = new TopicExchange("exchange.order.reward");
//        rabbitAdmin.declareExchange(exchange);
//
//        binding = new Binding("queue.order",
//                Binding.DestinationType.QUEUE,"exchange.order.reward",
//                "key.order",null);
//        rabbitAdmin.declareBinding(binding);
//
//    }


}
