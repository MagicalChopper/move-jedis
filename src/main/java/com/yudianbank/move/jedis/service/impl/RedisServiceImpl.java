package com.yudianbank.move.jedis.service.impl;

import com.yudianbank.move.jedis.service.RedisService;
import com.yudianbank.move.jedis.utils.JedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;



@Service
public class RedisServiceImpl implements RedisService{

    private static final Logger logger = LoggerFactory.getLogger(RedisServiceImpl.class);

    private static final JedisUtil jedisUtil = new JedisUtil();

    @Autowired
    private JedisPool jedisPool;

    @Override
    public Jedis getResource() {
        return jedisPool.getResource();
    }

    @SuppressWarnings("deprecation")
    @Override
    public void returnResource(Jedis jedis) {
        if(jedis !=null ) {
            jedisPool.returnResourceObject( jedis );
        }
    }

    public Object keys(){
        Jedis jedis = getResource();
        return jedis.keys("*");
    }


    @Override
    public void move(String key) {
        Jedis jedis = null;
        try{
            jedis = getResource();
            String dataType = jedis.type(key);
            executeMove(dataType, jedis,key);
        } catch (Exception e) {
            logger.error("出错的key:{},异常信息:{},堆栈信息:{}",key,e.getMessage(),e.getStackTrace());
        }finally{
            returnResource(jedis);
        }
    }

    public  void executeMove(String dataType,Jedis jedis,String key){
        switch (dataType){
            case "string":
                logger.info("正在迁移数据============key:{}",key);
                jedisUtil.strMove(jedis,key);
                break;
            case "list":
                logger.info("正在迁移数据============key:{}",key);
                jedisUtil.listMove(jedis,key);
                break;
            case "set":
                logger.info("正在迁移数据============key:{}",key);
                jedisUtil.setMove(jedis,key);
                break;
            case "zset":
                logger.info("正在迁移数据============key:{}",key);
                jedisUtil.zsetMove(jedis,key);
                break;
            case "hash":
                logger.info("正在迁移数据============key:{}",key);
                jedisUtil.hashMove(jedis,key);
                break;
            default:
                logger.error("============数据类型出现异常=============key:{}",key);
        }
    }

}