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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class OrderMessageService {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ProductDao productDao;

    @Autowired
    private RestaurantDao restaurantDao;

    @Autowired
    private Channel channel;

    @Async
    public void handleMessage() throws IOException, InterruptedException {

            // 声明exchange
            channel.exchangeDeclare("exchange.order.restaurant",
                    BuiltinExchangeType.DIRECT,true,false,null);



            // 声明死信队列中过期或者拒绝的消息所存放的exchange和queue
            channel.exchangeDeclare("exchange.dlx",
                    BuiltinExchangeType.TOPIC,true,false,null);

            channel.queueDeclare("queue.dlx",
                    true,false,false,null);

            channel.queueBind("queue.dlx","exchange.dlx","#");



            //队列参数，150s清理未被消费的消息
            //将queue.restaurant设置为死信队列
            //超出5条消息或者超出150s进入死信
            Map<String,Object> args = new HashMap<>(16);
            args.put("x-message-ttl",150000);
            args.put("x-dead-letter-exchange","exchange.dlx");
            args.put("x-max-length",5);

            // 声明队列
            channel.queueDeclare("queue.restaurant",
                    true,false,false,args);

            // 声明routeKey
            channel.queueBind("queue.restaurant",
                    "exchange.order.restaurant",
                    "key.restaurant",null);

            // 限流，只能有2条未确认消息，其他都是ready状态
            // 只能用于手动签收的情况
            channel.basicQos(2);

            // 绑定回调
            // 设置手动签收
            channel.basicConsume("queue.restaurant",false,deliverCallback,(consumerTag)->{});

            while(true) {
                Thread.sleep(100000);
            }

    }

    DeliverCallback deliverCallback = (consumerTag, message)->{
        String messageBody = new String(message.getBody());

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

            //单条确认
//            channel.basicAck(message.getEnvelope().getDeliveryTag(), false);

            //重回队列,拒收消息重新扔到队列中
//            channel.basicNack(message.getEnvelope().getDeliveryTag(), false,true);
            //拒收消息重新扔到死信队列中
            channel.basicNack(message.getEnvelope().getDeliveryTag(), false,false);
            //批量签收
//            if(message.getEnvelope().getDeliveryTag() % 5 == 0) {
//                channel.basicAck(message.getEnvelope().getDeliveryTag(),true);
//            }

            String messageToSend = objectMapper.writeValueAsString(orderMessageDTO);
            channel.basicPublish("exchange.order.restaurant",
                    "key.orders",null,messageToSend.getBytes(StandardCharsets.UTF_8));

        } catch(Exception e) {
            log.error(e.getMessage(),e);
        }

    };


}
