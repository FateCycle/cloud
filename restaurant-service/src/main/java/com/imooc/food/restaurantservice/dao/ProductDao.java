package com.imooc.food.restaurantservice.dao;


import com.imooc.food.restaurantservice.po.ProductPO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Repository;

@Mapper
@Repository
public interface ProductDao {

    @Select("SELECT id,name,price,restaurant_id restaurantId,status,date FROM product WHERE id = #{id}")
    ProductPO selsctProduct(Integer id);
}