package com.imooc.food.settlementservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.imooc.food.settlementservice.dao.SettlementDao;
import com.imooc.food.settlementservice.dto.OrderMessageDTO;
import com.imooc.food.settlementservice.enummeration.SettlementStatus;
import com.imooc.food.settlementservice.po.SettlementPO;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
public class OrderMessageService {

    @Autowired
    private SettlementDao settlementDao;

    @Autowired
    private SettlementService settlementService;

    @Autowired
    private ObjectMapper objectMapper;

    DeliverCallback deliverCallback = (consumerTag, message) -> {
        String messageBody = new String(message.getBody());
        log.info("deliverCallback:messageBody:{}", messageBody);
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        try {
            OrderMessageDTO orderMessageDTO = objectMapper.readValue(messageBody,
                    OrderMessageDTO.class);

            // 新增结算表
            SettlementPO settlementPo = new SettlementPO();
            settlementPo.setOrderId(orderMessageDTO.getOrderId());
            settlementPo.setAmount(orderMessageDTO.getPrice());
            settlementPo.setDate(new Date());
            // 进行结算并返回结算ID
            Integer transactionId = settlementService
                    .settlement(orderMessageDTO.getAccountId(),orderMessageDTO.getPrice());
            settlementPo.setTransactionId(transactionId);
            settlementPo.setStatus(SettlementStatus.SUCCESS);

            // 新增结算
            settlementDao.insert(settlementPo);

            orderMessageDTO.setSettlementId(transactionId);
            try (Connection connection = connectionFactory.newConnection();
                 Channel channel = connection.createChannel()) {
                String messageToSend = objectMapper.writeValueAsString(orderMessageDTO);
                channel.basicPublish("exchange.order.settlement", "key.order", null, messageToSend.getBytes());
            }
        } catch (JsonProcessingException | TimeoutException e) {
            e.printStackTrace();
        }
    };

    @Async
    public void handleMessage() throws IOException, TimeoutException, InterruptedException {
        log.info("start linstening message");
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setHost("localhost");
        try (Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(
                    "exchange.order.settlement",
                    BuiltinExchangeType.DIRECT,
                    true,
                    false,
                    null);

            channel.queueDeclare(
                    "queue.settlement",
                    true,
                    false,
                    false,
                    null);

            channel.queueBind(
                    "queue.settlement",
                    "exchange.order.settlement",
                    "key.settlement");


            channel.basicConsume("queue.settlement", true, deliverCallback, consumerTag -> {
            });
            while (true) {
                Thread.sleep(100000);
            }
        }
    }
}

