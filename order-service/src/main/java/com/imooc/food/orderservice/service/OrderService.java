package com.imooc.food.orderservice.service;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.imooc.food.orderservice.dao.OrderDetailDao;
import com.imooc.food.orderservice.dto.OrderMessageDTO;
import com.imooc.food.orderservice.enummeration.OrderStatus;
import com.imooc.food.orderservice.po.OrderDetailPO;
import com.imooc.food.orderservice.vo.OrderCreateVO;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.TimeoutException;

@Service
public class OrderService {

    @Autowired
    private OrderDetailDao orderDetailDao;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RabbitTemplate rabbitTemplate;


    /**
     * 订单请求
     */
    public void createOrder(OrderCreateVO orderCreateVO) throws JsonProcessingException {
        // 新建订单
        OrderDetailPO orderDetailPO = new OrderDetailPO();
        BeanUtils.copyProperties(orderCreateVO,orderDetailPO);
        orderDetailPO.setStatus(OrderStatus.ORDER_CREATING);
        orderDetailPO.setDate(new Date());
        orderDetailDao.insert(orderDetailPO);


        OrderMessageDTO orderMessageDTO = new OrderMessageDTO();
        orderMessageDTO.setOrderId(orderDetailPO.getId());
        orderMessageDTO.setProductId(orderDetailPO.getProductId());
        orderMessageDTO.setAccountId(orderDetailPO.getAccountId());

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");

        String messageToSend = objectMapper.writeValueAsString(orderMessageDTO);
        // 设置ID为消息的标识符，在异步回调的过程中根据此字段进行区分
        CorrelationData correlationData = new CorrelationData();
        correlationData.setId(orderDetailPO.getId().toString());
//        Message message = new Message(messageToSend.getBytes());
//        rabbitTemplate.send("exchange.order.restaurant",
//                "key.restaurant",message);

        // 直接可以傳對象
        rabbitTemplate.convertAndSend("exchange.order.restaurant",
                "key.restaurant",messageToSend,correlationData);



    }

}
