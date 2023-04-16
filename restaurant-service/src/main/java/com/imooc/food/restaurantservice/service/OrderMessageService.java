package com.imooc.food.restaurantservice.service;



import com.fasterxml.jackson.databind.ObjectMapper;
import com.imooc.food.restaurantservice.dao.ProductDao;
import com.imooc.food.restaurantservice.dao.RestaurantDao;
import com.imooc.food.restaurantservice.dto.OrderMessageDTO;
import com.imooc.food.restaurantservice.enummeration.ProductStatus;
import com.imooc.food.restaurantservice.enummeration.RestaurantStatus;
import com.imooc.food.restaurantservice.po.ProductPO;
import com.imooc.food.restaurantservice.po.RestaurantPO;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Slf4j
@Service
public class OrderMessageService {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ProductDao productDao;

    @Autowired
    private RestaurantDao restaurantDao;

    @Async
    public void handleMessage() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");

        try(Connection connection = connectionFactory.newConnection();
            Channel channel = connection.createChannel())  {

            // 声明exchange
            channel.exchangeDeclare("exchange.order.restaurant",
                    BuiltinExchangeType.DIRECT,true,false,null);


            // 声明队列
            channel.queueDeclare("queue.restaurant",
                    true,false,false,null);

            // 声明routeKey
            channel.queueBind("queue.restaurant",
                    "exchange.order.restaurant",
                    "key.restaurant",null);

            // 绑定回调
            channel.basicConsume("queue.restaurant",true,deliverCallback,(consumerTag)->{});

            while(true) {
                Thread.sleep(100000);
            }

        } catch(Exception e) {

        }
    }

    DeliverCallback deliverCallback = (consumerTag, message)->{
        String messageBody = new String(message.getBody());
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");

        try {
            OrderMessageDTO orderMessageDTO = objectMapper
                    .readValue(messageBody,OrderMessageDTO.class);

            // 查询订单的商品
            ProductPO productPO = productDao.selsctProduct(orderMessageDTO.getProductId());
            // 查询商品所在的商户
            RestaurantPO restaurantPO = restaurantDao.selsctRestaurant(productPO.getRestaurantId());

            // 订单有效就填入price无效则设置无效标记
            if(productPO.getStatus() == ProductStatus.AVALIABLE
            && restaurantPO.getStatus() == RestaurantStatus.OPEN) {
                orderMessageDTO.setPrice(productPO.getPrice());
                orderMessageDTO.setConfirmed(true);
            } else {
                orderMessageDTO.setConfirmed(false);
            }

            try(Connection connection = connectionFactory.newConnection();
                Channel channel = connection.createChannel()) {
                String messageToSend = objectMapper.writeValueAsString(orderMessageDTO);
                channel.basicPublish("exchange.order.restaurant",
                        "key.order",null,messageToSend.getBytes(StandardCharsets.UTF_8));
            } catch(Exception e) {
                e.printStackTrace();
            }



        } catch(Exception e) {
            log.error(e.getMessage(),e);
        }

    };


}
