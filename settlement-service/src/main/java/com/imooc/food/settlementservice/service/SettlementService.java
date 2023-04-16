package com.imooc.food.settlementservice.service;


import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Random;

@Service
public class SettlementService {

    Random random = new Random(25);

    // 模拟结算业务
    public Integer settlement(Integer accountId, BigDecimal amount) {
        return random.nextInt(1000000);
    }

}
