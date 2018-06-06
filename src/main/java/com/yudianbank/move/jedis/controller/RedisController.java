package com.yudianbank.move.jedis.controller;

import com.yudianbank.move.jedis.service.RedisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Tuple;

import java.util.*;

@Controller
@RequestMapping
public class RedisController {

    private static final Logger logger = LoggerFactory.getLogger(RedisController.class);

    @Autowired
    RedisService redisService;

    @RequestMapping("move")
    @ResponseBody
    public String move(){
        redisService.move();
        logger.info("==================end=====================");
        return "succ";
    }

}
