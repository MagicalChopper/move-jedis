package com.yudianbank.move.jedis.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.*;

public class JedisUtil {

    /**
     * 换库成功状态标识
     */
    private static final String OK = "OK";
    /**
     * 读取数据库
     */
    private static final int DB_FROM = 0;
    /**
     * 写入数据库
     */
    private static final int DB_TO = 15;

    /**
     * 切换数据库异常提示信息
     */
    private static final String CHANGE_DB_ERR_MSG = "切换数据库异常！";
    /**
     * 字符串类型的迁移
     * @param jedis
     * @param key
     */
    public void strMove(Jedis jedis, String key){
        Object obj = strRead(jedis,key);
        strWrite(jedis,key, (String) obj);
    }

    /**
     * 列表list类型的迁移
     * @param jedis
     * @param key
     */
    public void listMove(Jedis jedis, String key) {
        Object obj = listRead(jedis,key);
        listWrite(jedis,key, (List<String>) obj );
    }

    /**
     * 集合set类型的迁移
     * @param jedis
     * @param key
     */
    public void setMove(Jedis jedis,String key){
       Object obj = setRead(jedis,key);
       setWrite(jedis,key, (Set<String>) obj);
    }

    /**
     * 集合zset类型的迁移
     * @param jedis
     * @param key
     */
    public void zsetMove(Jedis jedis,String key){
       Object obj = zsetRead(jedis,key);
       zsetWrite(jedis,key, (Map<String, Double>) obj );
    }

    /**
     * hash类型的迁移
     * @param jedis
     * @param key
     */
    public void hashMove(Jedis jedis,String key){
        Object obj = hashRead(jedis,key);
        hashWrite(jedis,key, (Map<String, String>) obj );
    }

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
        if(!changeDB(jedis, DB_FROM ).equals(OK)){
            throw new RuntimeException(CHANGE_DB_ERR_MSG);
        }
        return jedis.get(key);
    }

    /**
     * str写
     * @param jedis
     * @param key
     * @param value
     */
    public void strWrite(Jedis jedis, String key,String value){
        if(!changeDB(jedis, DB_TO ).equals(OK)){
            throw new RuntimeException(CHANGE_DB_ERR_MSG);
        }
        jedis.set(key,value);
    }

    /**
     * list读
     * @param jedis
     * @param key
     * @return
     */
    public Object listRead(Jedis jedis, String key){
        if(!changeDB(jedis, DB_FROM ).equals(OK)){
            throw new RuntimeException(CHANGE_DB_ERR_MSG);
        }
        return jedis.lrange(key,0,-1);
    }

    /**
     * list写
     * @param jedis
     * @param key
     * @param listValue
     */
    public void listWrite(Jedis jedis, String key,List<String> listValue){
        if(!changeDB(jedis, DB_TO ).equals(OK)){
            throw new RuntimeException(CHANGE_DB_ERR_MSG);
        }
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
        if(!changeDB(jedis, DB_FROM ).equals(OK)){
            throw new RuntimeException(CHANGE_DB_ERR_MSG);
        }
        return jedis.smembers(key);
    }

    /**
     * set写
     * @param jedis
     * @param key
     * @param setValue
     */
    public void setWrite(Jedis jedis, String key,Set<String> setValue){
        if(!changeDB(jedis, DB_TO ).equals(OK)){
            throw new RuntimeException(CHANGE_DB_ERR_MSG);
        }
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
        if(!changeDB(jedis, DB_FROM ).equals(OK)){
            throw new RuntimeException(CHANGE_DB_ERR_MSG);
        }
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
        if(!changeDB(jedis, DB_TO ).equals(OK)){
            throw new RuntimeException(CHANGE_DB_ERR_MSG);
        }
        jedis.zadd(key,scoureMembers);
    }

    /**
     * hash读
     * @param jedis
     * @param key
     * @return
     */
    public Object hashRead(Jedis jedis, String key){
        if(!changeDB(jedis, DB_FROM ).equals(OK)){
            throw new RuntimeException(CHANGE_DB_ERR_MSG);
        }
        return jedis.hgetAll(key);
    }

    /**
     * hash写
     * @param jedis
     * @param key
     * @param hash
     */
    public void hashWrite(Jedis jedis, String key, Map<String, String> hash){
        if(!changeDB(jedis, DB_TO ).equals(OK)){
            throw new RuntimeException(CHANGE_DB_ERR_MSG);
        }
        jedis.hmset(key,hash);
    }
}
