package com.imooc.food.deliverymanservicemanager.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.imooc.food.deliverymanservicemanager.dao.DeliverymanDao;
import com.imooc.food.deliverymanservicemanager.dto.OrderMessageDTO;
import com.imooc.food.deliverymanservicemanager.enummeration.DeliverymanStatus;
import com.imooc.food.deliverymanservicemanager.po.DeliverymanPO;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
public class OrderMessageService {

    @Autowired
    DeliverymanDao deliverymanDao;
    @Autowired
    RabbitTemplate rabbitTemplate;
    @Autowired
    ObjectMapper objectMapper;



    @RabbitListener(
            containerFactory = "rabbitListenerContainerFactory",
            bindings = {
                    @QueueBinding(
                            value = @Queue(name = "queue.deliveryman",arguments = {}),
                            exchange = @Exchange(name="exchange.order.deliveryman",type = ExchangeTypes.DIRECT),
                            key="key.deliveryman"
                    )
            }
    )
    public void handleMessage(@Payload Message message)  {

        String messageBody = new String(message.getBody());
        log.info("deliverCallback:messageBody:{}", messageBody);
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        try {
            // 查询空闲的骑手
            OrderMessageDTO orderMessageDTO = objectMapper.readValue(messageBody,
                    OrderMessageDTO.class);
            // 默认分配第一个骑手
            List<DeliverymanPO> deliverymanPOS = deliverymanDao.selectAvaliableDeliveryman(DeliverymanStatus.AVALIABLE);
            orderMessageDTO.setDeliverymanId(deliverymanPOS.get(0).getId());
            log.info("onMessage:restaurantOrderMessageDTO:{}", orderMessageDTO);

            String messageToSend = objectMapper.writeValueAsString(orderMessageDTO);
            CorrelationData correlationData = new CorrelationData();
            correlationData.setId(orderMessageDTO.getOrderId().toString());

            // 直接可以傳對象
            rabbitTemplate.convertAndSend("exchange.order.deliveryman",
                    "key.order",messageToSend,correlationData);


        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}

