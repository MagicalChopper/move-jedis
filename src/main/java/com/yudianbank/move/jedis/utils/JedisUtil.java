package com.yudianbank.move.jedis.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.*;

public class JedisUtil {

    /**
     * 换库成功状态标识
     */
    public static final String OK = "OK";
    /**
     * 读取数据库
     */
    public static final int DB_FROM = 0;
    /**
     * 写入数据库
     */
    public static final int DB_TO = 15;



    /**
     * jedis换库
     * @param jedis
     * @param index
     * @return
     */
    public static String changeDB(Jedis jedis,int index){
        return jedis.select(index);
    }

    /**
     * str读
     * @param jedis
     * @param key
     * @return
     */
    public Object strRead(Jedis jedis, String key){
        return jedis.get(key);
    }

    /**
     * str写
     * @param jedis
     * @param key
     * @param value
     */
    public void strWrite(Jedis jedis, String key,String value){
        jedis.set(key,value);
    }

    /**
     * list读
     * @param jedis
     * @param key
     * @return
     */
    public Object listRead(Jedis jedis, String key){
        return jedis.lrange(key,0,-1);
    }

    /**
     * list写
     * @param jedis
     * @param key
     * @param listValue
     */
    public void listWrite(Jedis jedis, String key,List<String> listValue){
        int i = listValue.size();
        while(i>0){
            i--;
            jedis.lpush(key, listValue.get(i));
        }
    }

    /**
     * set读
     * @param jedis
     * @param key
     * @return
     */
    public Object setRead(Jedis jedis, String key){
        return jedis.smembers(key);
    }

    /**
     * set写
     * @param jedis
     * @param key
     * @param setValue
     */
    public void setWrite(Jedis jedis, String key,Set<String> setValue){
        Iterator it = setValue.iterator();
        while (it.hasNext()){
            String member = (String) it.next();
            jedis.sadd(key,member);
        }
    }

    /**
     * zset读
     * @param jedis
     * @param key
     * @return
     */
    public Object zsetRead(Jedis jedis, String key){
        Map<String,Double> scoureMembers = new HashMap<>();
        Set<Tuple> zsetTuple = jedis.zrangeWithScores(key,0,-1);
        Iterator it = zsetTuple.iterator();
        while (it.hasNext()){
            Tuple tuple = (Tuple) it.next();
            scoureMembers.put(tuple.getElement(),tuple.getScore());
        }
        return scoureMembers;
    }

    /**
     * zset写
     * @param jedis
     * @param key
     * @param scoureMembers
     */
    public void zsetWrite(Jedis jedis, String key, Map<String,Double> scoureMembers){
        jedis.zadd(key,scoureMembers);
    }

    /**
     * hash读
     * @param jedis
     * @param key
     * @return
     */
    public Object hashRead(Jedis jedis, String key){
        return jedis.hgetAll(key);
    }

    /**
     * hash写
     * @param jedis
     * @param key
     * @param hash
     */
    public void hashWrite(Jedis jedis, String key, Map<String, String> hash){
        jedis.hmset(key,hash);
    }
}
