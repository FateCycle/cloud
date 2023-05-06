package com.imooc.food.orderservice.controller;



import com.imooc.food.orderservice.service.OrderService;
import com.imooc.food.orderservice.vo.OrderCreateVO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;


@Slf4j
@RestController
public class OrderController {

    @Autowired
    private OrderService orderService;

    @PostMapping("/orders")
    public void createOrder(@RequestBody OrderCreateVO orderCreateVO) throws IOException {
        log.info("createOrder:orderCreateVO:{}" , orderCreateVO);
        orderService.createOrder(orderCreateVO);
    }

    @GetMapping("/hh")
    public String index() {

        return "12324";
    }

}

