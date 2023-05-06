package com.imooc.food.orderservice.service;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.imooc.food.orderservice.dao.OrderDetailDao;
import com.imooc.food.orderservice.dto.OrderMessageDTO;
import com.imooc.food.orderservice.enummeration.OrderStatus;
import com.imooc.food.orderservice.po.OrderDetailPO;
import com.imooc.food.orderservice.vo.OrderCreateVO;
import com.rabbitmq.client.*;
import com.rabbitmq.tools.json.JSONUtil;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
public class OrderService {

    @Autowired
    private OrderDetailDao orderDetailDao;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private Channel channel;


    /**
     * 订单请求
     */
    public void createOrder(OrderCreateVO orderCreateVO) throws IOException {
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
            //方案1:发送端异步确认消息
            channel.confirmSelect();
            channel.addConfirmListener(new ConfirmListener() {
                // 确认成功的回调
                @Override
                public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                    log.info("deliveryTag:{},multiple:{}",deliveryTag,multiple);
                }
                // 确认失败的回调
                @Override
                public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                    log.info("deliveryTag:{},multiple:{}",deliveryTag,multiple);
                }
            });

            //发出去的消息没有被路由的回调方法，方法1
//            channel.addReturnListener(new ReturnListener() {
//                @Override
//                public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
//                    log.info("");
//                }
//            });
        //发出去的消息没有被路由的回调方法，方法2
        channel.addReturnListener(new ReturnCallback() {
            @Override
            public void handle(Return returnMessage) {
                log.info("");
            }
        });

        // 设置消息的过期时间为15s
        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder()
                .expiration("15000").build();

            // 发送消息
            // mandatory为TRUE，消息如果没被路由会调用returnListener的回调
            channel.basicPublish("exchange.order.restaurant",
                    "key.restaurant",true,
                    properties,
                    messageToSend.getBytes(StandardCharsets.UTF_8));

            // 方案2:发送端确认消息（单条或者多条都可以使用）
//            channel.confirmSelect();
//            if(channel.waitForConfirms()) {
//                log.info("RabbitMQ confirm success");
//            } else {
//                log.info("RabbitMQ confirm fail");
//            }






    }

}
