package com.imooc.food.orderservice.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.imooc.food.orderservice.dao.OrderDetailDao;
import com.imooc.food.orderservice.dto.OrderMessageDTO;
import com.imooc.food.orderservice.enummeration.OrderStatus;
import com.imooc.food.orderservice.po.OrderDetailPO;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
public class OrderMessageService {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OrderDetailDao orderDetailDao;



    public void handleMessage(OrderMessageDTO orderMessageDTO) {

        log.info("handleMessage:messageBody:{}",orderMessageDTO);
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");

        try {

            // 数据库中读取订单
            OrderDetailPO orderDetailPO = orderDetailDao.selectOrder(orderMessageDTO.getOrderId());

            switch (orderDetailPO.getStatus()) {
                //商家微服务的消息
                case ORDER_CREATING -> {
                    // 商家是否已经确认商品信息
                    // 商家已经填入订单的价格
                    if(orderMessageDTO.getConfirmed() && null != orderMessageDTO.getPrice()) {
                        // 设置商家已确认消息
                        orderDetailPO.setStatus(OrderStatus.RESTAURANT_CONFIRMED);
                        // 填入商家确认的价格
                        orderDetailPO.setPrice(orderMessageDTO.getPrice());
                        //更新数据
                        orderDetailDao.update(orderDetailPO);

                        // 进行下一个业务流程 向骑手微服务发送信息
                        try (Connection connection = connectionFactory.newConnection();
                             Channel channel = connection.createChannel()){
                            String messageToSend = objectMapper.writeValueAsString(orderMessageDTO);
                            channel.basicPublish("exchange.order.deliveryman",
                                    "key.deliveryman",null,messageToSend.getBytes(StandardCharsets.UTF_8));
                        }
                    } else {
                        // 商家确认失败，状态改为失败
                        orderDetailPO.setStatus(OrderStatus.FAILED);
                        orderDetailDao.update(orderDetailPO);
                    }
                }
                case RESTAURANT_CONFIRMED -> {
                    // 已经分派骑手
                    if(null!=orderMessageDTO.getDeliverymanId()) {
                        orderDetailPO.setStatus(OrderStatus.DELIVERYMAN_CONFIRMED);
                        orderDetailPO.setDeliverymanId(orderMessageDTO.getDeliverymanId());
                        orderDetailDao.update(orderDetailPO);

                        //发送给结算服务
                        try (Connection connection = connectionFactory.newConnection();
                             Channel channel = connection.createChannel()){
                             String messageToSend = objectMapper.writeValueAsString(orderMessageDTO);
                             channel.basicPublish("exchange.order.settlement",
                                     "key.settlement",
                                     null,
                                     messageToSend.getBytes());
                        }
                    } else {
                        // 订单失败处理
                        orderDetailPO.setStatus(OrderStatus.FAILED);
                        orderDetailDao.update(orderDetailPO);
                    }
                }
                case DELIVERYMAN_CONFIRMED -> {

                    if(null != orderMessageDTO.getSettlementId()) {

                        orderDetailPO.setStatus(OrderStatus.SETTLEMENT_CONFIRMED);
                        orderDetailPO.setSettlementId(orderMessageDTO.getSettlementId());
                        orderDetailDao.update(orderDetailPO);

                        // 发送给积分微服务
                        try(Connection connection = connectionFactory.newConnection()) {
                            Channel channel = connection.createChannel();
                            String messageToSend = objectMapper.writeValueAsString(orderMessageDTO);
                            channel.basicPublish("exchange.order.reward",
                                    "key.reward",
                                    null,
                                    messageToSend.getBytes());
                        }

                    } else {
                        orderDetailPO.setStatus(OrderStatus.FAILED);
                        orderDetailDao.update(orderDetailPO);
                    }

                }
                case SETTLEMENT_CONFIRMED -> {
                    if(null != orderMessageDTO.getRewardId()) {
                        orderDetailPO.setStatus(OrderStatus.ORDER_CREATED);
                        orderDetailPO.setRewardId(orderMessageDTO.getRewardId());
                        orderDetailDao.update(orderDetailPO);
                    } else {
                        orderDetailPO.setStatus(OrderStatus.FAILED);
                        orderDetailDao.update(orderDetailPO);
                    }
                }
                case ORDER_CREATED -> {
                }
                case FAILED -> {
                }
            }

        } catch(Exception e) {

        }

    };







}
