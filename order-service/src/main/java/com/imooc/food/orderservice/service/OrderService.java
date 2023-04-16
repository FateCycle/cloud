package com.imooc.food.orderservice.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.imooc.food.orderservice.dao.OrderDetailDao;
import com.imooc.food.orderservice.dto.OrderMessageDTO;
import com.imooc.food.orderservice.enummeration.OrderStatus;
import com.imooc.food.orderservice.po.OrderDetailPO;
import com.imooc.food.orderservice.vo.OrderCreateVO;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
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


    /**
     * 订单请求
     */
    public void createOrder(OrderCreateVO orderCreateVO) {
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

        try (Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel()) {
            String messageToSend = objectMapper.writeValueAsString(orderMessageDTO);
            channel.basicPublish("exchange.order.restaurant",
                    "key.restaurant",
                    null,
                    messageToSend.getBytes(StandardCharsets.UTF_8));

        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

    }

}
