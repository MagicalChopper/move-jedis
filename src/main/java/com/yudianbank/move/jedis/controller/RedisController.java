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

    @Autowired
    private JedisPool jedisPool;

    @RequestMapping("move")
    @ResponseBody
    public String move(){
        Set<String> keys = (Set<String>) redisService.keys();
        Iterator it = keys.iterator();
        while(it.hasNext()){
            String key = it.next().toString();
            if(!key.startsWith("/dubbo/")){
                redisService.move(key);
            }
        }
        logger.info("==================end=====================");
        return "succ";
    }

    @RequestMapping("test")
    @ResponseBody
    public String test(){
        Jedis jedis = redisService.getResource();

        /**
         * 获取当前连接的哪个库
         */
        long dbNum = jedis.getDB();

        /**
         * 换库
         */
        String status = null;
        status  = jedis.select(0);


        /**
         * 测试namespace
         */
        jedis.select(14);
        String type = jedis.type( "test:lh:namespace" );
        String s = jedis.get( "test:lh:namespace" );
        /**
         * String读写  proY0150170718
         */
//        String strValue = jedis.get("proY0150170718");
//        status = jedis.select(15);
//        if(jedis.getDB()==15L){
//            jedis.set("proY0150170718",strValue);
//        }

        /**
         * list读写  testBatch , TRANS_LOGOS_ACCEPT , net.engining.lbt.business.ext.acct.AdjustAccountService
         */
//        List<String> list = jedis.lrange("testBatch",0,-1);
//        status = jedis.select(15);
//        if(status.equals("OK")){
//            for(int i=0;i<list.size();i++){
//                jedis.lpush("testBatch",list.get(list.size()-i-1));
//            }
//        }
        /**
         * zset读写  redisson__idle__set__{ADJUST#22#187086#net.engining.lbt.param.model.PlProduct}
         */
//        Set<Tuple> zsetTuple = jedis.zrangeWithScores("redisson__idle__set__{ADJUST#22#187086#net.engining.lbt.param.model.PlProduct}",0,-1);
//        Map<String,Double> map = new HashMap<>();
//        Iterator it = zsetTuple.iterator();
//        while (it.hasNext()){
//            Tuple tuple = (Tuple) it.next();
//            map.put(tuple.getElement(),tuple.getScore());
//        }
//
//        //换库
//        status = jedis.select(15);
//        if(status.equals("OK")){
//        jedis.zadd("redisson__idle__set__{ADJUST#22#187086#net.engining.lbt.param.model.PlProduct}",map);
//        }

//        /**
//         * hash读写  riskRepulse
//         */
//        Map<String,String> map = jedis.hgetAll("riskRepulse");
//        //换库
//        status = jedis.select(15);
//        if(status.equals("OK")){
//            jedis.hmset("riskRepulse",map);
//        }




        /**
         * set读写  escape_check_url_fadada
         */
//        Set<String > set = jedis.smembers("escape_check_url_fadada");
//        //换库
//        status = jedis.select(15);
//        if(status.equals("OK")){
//            Iterator it = set.iterator();
//            while (it.hasNext()){
//                String member = (String) it.next();
//                jedis.sadd("escape_check_url_fadada",member);
//            }
//        }

//        list  testBatch
//        zset  redisson__idle__set__{ADJUST#22#187086#net.engining.lbt.param.model.PlProduct}
//        hash  riskRepulse
//        set  escape_check_url_fadada
        return "succ";
    }
}
