package com.yudianbank.move.jedis.service.impl;

import com.yudianbank.move.jedis.service.RedisService;
import com.yudianbank.move.jedis.utils.JedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;


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
    public void move() {
        Jedis jedis = null;
        try{
            jedis = getResource();
            List<Map<String,Object>> list = doRead(jedis);
            doWrite(jedis,list);
        } catch (Exception e) {
            logger.error(",异常信息:{},堆栈信息:{}",e.getMessage(),e.getStackTrace());
        }finally{
            returnResource(jedis);
        }
    }


    /**
     * 读操作(读完所有返回)
     * @param jedis
     * @return
     */
    public List<Map<String,Object>> doRead(Jedis jedis){
        List<Map<String,Object>> list = new ArrayList<>();
        Set<String> keys = (Set<String>) keys();
        Iterator it = keys.iterator();
        while (it.hasNext()){
            String key = (String) it.next();
            if(!key.startsWith("/dubbo/")){
                String dataType = jedis.type(key);
                Map<String,Object> map = readUnit(dataType,jedis,key);
                list.add(map);
            }
        }
        return list;
    }

    /**
     * 写操作(根据读操作返回结果遍历写入)
     * @param jedis
     * @param list
     */
    private void doWrite(Jedis jedis,List<Map<String,Object>> list) {
        for (Map<String, Object> map : list) {
            writeUnit(jedis,map);
        }
    }

    /**
     * 判断类型读一条
     * @param dataType
     * @param jedis
     * @param key
     * @return
     */
    public Map<String,Object> readUnit(String dataType,Jedis jedis,String key){
        Map<String,Object> map = new HashMap<>();
        switch (dataType){
            case "string":
                logger.info("正在读取string，key:{}",key);
                map.put(key,jedisUtil.strRead(jedis,key));
                break;
            case "list":
                logger.info("正在读取list，key:{}",key);
                map.put(key,jedisUtil.listRead(jedis,key));
                break;
            case "set":
                logger.info("正在读取set，key:{}",key);
                map.put(key,jedisUtil.setRead(jedis,key));
                break;
            case "zset":
                logger.info("正在读取zset，key:{}",key);
                map.put(key,jedisUtil.zsetRead(jedis,key));
                break;
            case "hash":
                logger.info("正在读取hash，key:{}",key);
                map.put(key,jedisUtil.hashRead(jedis,key));
                break;
            default:
                logger.error("============数据类型读取出现异常=============key:{}",key);
        }
        return map;
    }

    /**
     * 判断类型写一条
     * @param jedis
     * @param map
     */
    public void writeUnit(Jedis jedis,Map<String,Object> map){
        for(Map.Entry entry:map.entrySet()){
            String dataType = jedis.type( (String) entry.getKey() );
            switch (dataType){
                case "string":
                    logger.info("正在写入string，key:{}",entry.getKey());
                    jedisUtil.strWrite(jedis,(String) entry.getKey(), (String) entry.getValue());
                    break;
                case "list":
                    logger.info("正在写入list，key:{}",entry.getKey());
                    jedisUtil.listWrite(jedis,(String) entry.getKey(),(List<String>) entry.getValue());
                    break;
                case "set":
                    logger.info("正在写入set，key:{}",entry.getKey());
                    jedisUtil.setWrite(jedis,(String) entry.getKey(),(Set<String>) entry.getValue());
                    break;
                case "zset":
                    logger.info("正在写入zset，key:{}",entry.getKey());
                    jedisUtil.zsetWrite(jedis,(String) entry.getKey(),(Map<String,Double>) entry.getValue());
                    break;
                case "hash":
                    logger.info("正在写入hash，key:{}",entry.getKey());
                    jedisUtil.hashWrite(jedis,(String)entry.getKey(),(Map<String, String>) entry.getValue());
                    break;
                default:
                    logger.error("============数据类型写入出现异常=============key:{}",entry.getKey());
            }
        }
    }

}