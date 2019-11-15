package com.spring.redis;

import com.spring.redis.util.RedisUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {RedisApplication.class})
public class RedisTest {

    Logger logger = LoggerFactory.getLogger(RedisTest.class);

    @Autowired
    RedisUtil redisUtil;

    @Test
    public void testSet(){
        logger.info("插入redis>>[{}-{}]", "phone", "17688556401");
        redisUtil.set("phone","17688556401");
    }

    @Test
    public void testGet(){
        Object param = redisUtil.get("phone");
        logger.info((param==null) ? "" : param.toString());
    }

}
