package com.spring.redis.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.support.atomic.RedisAtomicLong;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Component
public class RedisUtil {

    @Resource
    protected RedisTemplate<String, Object> redisTemplate;

    @Resource
    protected StringRedisTemplate stringRedisTemplate;

    private static final long DEFAULT_EXPIRE = 30;// 30s 超时时间
    private static final Logger logger = LoggerFactory.getLogger(RedisUtil.class);

    /**
     * 将value对象写入缓存
     *
     * @param key
     * @param value void
     */
    public void set(String key, Object value) {
        if (null != key) {
            redisTemplate.opsForValue().set(getKey(key), value);
        }
    }

    /**
     * 将value对象写入缓存
     *
     * @param key
     * @param value
     * @param time  void
     */
    public void set(String key, Object value, long time) {
        if (null != key) {
            set(key, value);
            expire(key, time);
        }

    }

    /**
     * get对象
     *
     * @param key
     * @return Object
     */
    public Object get(String key) {
        if (null != key) {
            return redisTemplate.opsForValue().get(getKey(key));
        }
        return null;
    }

    /**
     * 删除缓存 根据key精确匹配删除
     *
     * @param key void
     */
    public void del(String... key) {
        if (key != null && key.length > 0) {
            List<String> delKeys = new ArrayList<String>();
            for (String k : key) {
                if (StringUtils.isNotBlank(k)) {
                    delKeys.add(getKey(k));
                    // redisTemplate.delete(getKey(k));
                }
            }
            redisTemplate.delete(delKeys);
        }
    }

    /**
     * 查找所有符合给定模式( pattern)的 key
     *
     * @param pattern void
     * @return
     */
    public Set<String> keys(String pattern) {
        String innerPattern = getKey(pattern);
        return redisTemplate.keys(innerPattern);
    }

    /**
     * 指定缓存的失效时间（单位：秒）
     *
     * @param key
     * @param seconds 多少秒后失效
     */
    public void expire(String key, long seconds) {
        expireOrigin(getKey(key), seconds);
    }

    public void expireOrigin(String originKey, long time) {
        if (time > 0) {
            redisTemplate.expire(originKey, time, TimeUnit.SECONDS);
        }
    }

    public long getExpire(String key) {
        return getExpireOrigin(getKey(key));
    }

    public long getExpireOrigin(String originKey) {
        return redisTemplate.getExpire(originKey, TimeUnit.SECONDS);
    }

    /**
     * 更新值，但不改变原来的过期时间
     *
     * @Methods Name setOriginWithPreExpire
     * @Create In 2018年6月19日 By wangzhijie
     * @param originKey
     * @param value     void
     */
    public void setOriginWithPreExpire(String originKey, Object value) {
        long expireTime = this.getExpireOrigin(originKey);
        logger.info("expireTime>>[{}]", expireTime);
        this.setOrigin(originKey, value, expireTime);
    }

    public void setWithPreExpire(String key, Object value) {
        long expireTime = this.getExpire(key);
        this.set(key, value, expireTime);
    }

    /**
     * 生成seq
     *
     * @param key
     * @return long
     */
    public long generate(String key) {
        RedisAtomicLong counter = new RedisAtomicLong(getKey(key), redisTemplate.getConnectionFactory());
        return counter.incrementAndGet();
    }

    /**
     * 生成seq，设置失效时间
     *
     * @param key
     * @param expireTime
     * @return long
     */
    public long generate(String key, Date expireTime) {
        RedisAtomicLong counter = new RedisAtomicLong(getKey(key), redisTemplate.getConnectionFactory());
        counter.expireAt(expireTime);
        return counter.incrementAndGet();
    }

    /**
     * 按给定增长额生成seq
     *
     * @param key
     * @param increment
     * @return long
     */
    public long generate(String key, int increment) {
        RedisAtomicLong counter = new RedisAtomicLong(getKey(key), redisTemplate.getConnectionFactory());
        return counter.addAndGet(increment);
    }

    /**
     * 按给定增长额生成seq，设置失效时间
     *
     * @param key
     * @param increment
     * @param expireTime
     * @return long
     */
    public long generate(String key, int increment, Date expireTime) {
        RedisAtomicLong counter = new RedisAtomicLong(getKey(key), redisTemplate.getConnectionFactory());
        counter.expireAt(expireTime);
        return counter.addAndGet(increment);
    }

    /**
     * 获取key的前缀-应用简称:
     *
     * @return StringBuffer
     */
    private StringBuilder getPreKey() {
        StringBuilder buffer = new StringBuilder("");
        String shortname = "";
        if (StringUtils.isNotBlank(shortname)) {
            buffer.append(shortname).append(":");
        }
        return buffer;
    }

    /**
     * 获取加上前缀的key
     *
     * @param key
     * @return String
     */
    private String getKey(String key) {
        if (StringUtils.isNotBlank(key)) {
            return getPreKey().append(key).toString();
        }
        return key;
    }

    /**
     * 生成lock key
     *
     * @param key
     * @return String
     */
    public String generateLockKey(String key) {
        return String.format("LOCK:%s", getKey(key));
    }

    /**
     * 查找所有符合给定模式( pattern)的 key
     *
     * @param pattern void
     * @return
     */
    public Set<String> keysOrigin(String pattern) {
        return redisTemplate.keys(pattern);
    }

    /**
     * 将value对象写入缓存
     *
     * @param key
     * @param value void
     */
    public void setOrigin(String key, Object value) {
        if (null != key) {
            redisTemplate.opsForValue().set(key, value);
        }
    }

    /**
     * 将value对象写入缓存
     *
     * @param key
     * @param value
     * @param time  void
     */
    public void setOrigin(String key, Object value, long time) {
        if (null != key) {
            setOrigin(key, value);
            if (time > 0) {
                redisTemplate.expire(key, time, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * get对象
     *
     * @param key
     * @return Object
     */
    public Object getOrigin(String key) {
        if (null != key) {
            return redisTemplate.opsForValue().get(key);
        }
        return null;
    }

    /**
     * 删除缓存 根据key精确匹配删除
     *
     * @param key void
     */
    public void delOrigin(String... key) {
        if (key != null && key.length > 0) {
            List<String> delKeys = new ArrayList<String>();
            for (String k : key) {
                if (StringUtils.isNotBlank(k)) {
                    delKeys.add(k);
                    redisTemplate.delete(k);
                }
            }
            redisTemplate.delete(delKeys);
        }
    }

    /**
     * 加锁（key带应用简称） 建议使用RedisLockUtil内部方法
     *
     * @param key
     * @return boolean
     */
    @Deprecated
    public boolean lock(String key) {
        return lock(key, DEFAULT_EXPIRE);
    }

    /**
     * 加锁（key带应用简称） 建议使用RedisLockUtil内部方法
     *
     * @param key
     * @param time 超时时间（秒）
     * @return boolean 成功 true，失败 fasle
     */
    @Deprecated
    public boolean lock(String key, long time) {
        return lockOrigin(generateLockKey(key), time);
    }

    /**
     * 解锁（key带应用简称） 建议使用RedisLockUtil内部方法
     *
     * @param key void
     */
    @Deprecated
    public void unlock(String key) {
        unlockOrigin(generateLockKey(key));
    }

    /**
     * 加锁 建议使用RedisLockUtil内部方法
     *
     * @param key  加锁key
     * @param time 超时时间（秒）
     * @return boolean 成功 true，失败 fasle
     */
    @Deprecated
    public boolean lockOrigin(String key, long time) {
        if (StringUtils.isBlank(key)) {
            return false;
        }
        try {
            RedisSerializer<String> serializer = redisTemplate.getStringSerializer();
            if (redisTemplate.getConnectionFactory().getConnection().setNX(serializer.serialize(key), new byte[0])) {
                redisTemplate.expire(key, time, TimeUnit.SECONDS);// 暂设置过期，防止异常中断锁未释放
                logger.debug("add RedisLock[" + key + "].");
                return true;
            }
        } catch (Exception e) {
            unlockOrigin(key);
        }
        return false;
    }

    /**
     * 解锁 建议使用RedisLockUtil内部方法
     *
     * @param key void
     */
    @Deprecated
    public void unlockOrigin(String key) {
        if (StringUtils.isBlank(key)) {
            return;
        }
        logger.debug("release RedisLock[" + key + "].");
        RedisSerializer<String> serializer = redisTemplate.getStringSerializer();
        redisTemplate.getConnectionFactory().getConnection().del(serializer.serialize(key));
    }

    /**
     * 加锁 建议使用RedisLockUtil内部方法
     *
     * @param key
     * @param time 有效期（秒）
     * @return boolean
     */
    @Deprecated
    public boolean lockBusiness(String key, long time) {
        String lockKey = generateLockKey(key);
        if (lockOrigin(lockKey, time)) {
            set(lockKey, DateUtils.getDate("yyyy-MM-dd HH:mm:ss"));
            return true;
        }
        return false;
    }

    /**
     * 解锁 建议使用RedisLockUtil内部方法
     *
     * @param key void
     */
    @Deprecated
    public void unlockBusiness(String key) {
        String lockKey = generateLockKey(key);
        unlockOrigin(lockKey);
        del(lockKey);
    }

    public Long leftPushAllForList(String key, Object... values) {
        return leftPushAllForListOrigin(getKey(key), values);
    }

    public Long leftPushAllForListOrigin(String originKey, Object... values) {
        if (originKey != null) {
            return this.redisTemplate.opsForList().leftPushAll(originKey, values);
        }
        return 0L;
    }

    public Object leftPopForList(String key) {
        return leftPopForListOrigin(getKey(key));
    }

    public Object leftPopForListOrigin(String originKey) {
        if (originKey != null) {
            return this.redisTemplate.opsForList().leftPop(originKey);
        }
        return null;
    }

    public Long rightPushAllForList(String key, Object... values) {
        return rightPushAllForListOrigin(getKey(key));
    }

    public Long rightPushAllForListOrigin(String originKey, Object... values) {
        if (originKey != null) {
            return this.redisTemplate.opsForList().rightPushAll(originKey, values);
        }
        return 0L;
    }

    public Object rightPopForList(String key) {
        return rightPopForListOrigin(getKey(key));
    }

    public Object rightPopForListOrigin(String originKey) {
        if (originKey != null) {
            return this.redisTemplate.opsForList().rightPop(originKey);
        }
        return null;
    }

    public Long leftPushIfPresentForList(String key, Object value) {
        return leftPushIfPresentForListOrigin(getKey(key), value);
    }

    public Long leftPushIfPresentForListOrigin(String originKey, Object value) {
        if (originKey != null) {
            return this.redisTemplate.opsForList().leftPushIfPresent(originKey, value);
        }
        return 0L;
    }

    public Long sizeForList(String key) {
        return sizeForListOrigin(getKey(key));
    }

    public Long sizeForListOrigin(String originKey) {
        if (originKey != null) {
            return this.redisTemplate.opsForList().size(originKey);
        }
        return 0L;
    }

    public Long rightPushIfPresentForList(String key, Object value) {
        return rightPushIfPresentForListOrigin(getKey(key), value);
    }

    public Long rightPushIfPresentForListOrigin(String originKey, Object value) {
        if (originKey != null) {
            return this.redisTemplate.opsForList().rightPushIfPresent(originKey, value);
        }
        return 0L;
    }

    public void putForHash(String key, Object hashKey, Object value) {
        putForHashOrigin(getKey(key), hashKey, value);
    }

    public void putForHashOrigin(String originKey, Object hashKey, Object value) {
        if (originKey != null) {
            this.redisTemplate.opsForHash().put(originKey, hashKey, value);
        }
    }

    public Object getForHash(String key, Object hashKey) {
        return getForHashOrigin(getKey(key), hashKey);
    }

    public Object getForHashOrigin(String originKey, Object hashKey) {
        if (originKey != null) {
            return this.redisTemplate.opsForHash().get(originKey, hashKey);
        }
        return null;
    }

    public Long deleteForHash(String key, Object... hashKeys) {
        return deleteForHashOrigin(getKey(key), hashKeys);
    }

    public Long deleteForHashOrigin(String originKey, Object... hashKeys) {
        if (originKey != null) {
            return this.redisTemplate.opsForHash().delete(originKey, hashKeys);
        }
        return 0L;
    }

    public Boolean putIfAbsentForHash(String key, Object hashKey, Object value) {
        return putIfAbsentForHashOrigin(getKey(key), hashKey, value);
    }

    public Boolean putIfAbsentForHashOrigin(String originKey, Object hashKey, Object value) {
        if (originKey != null) {
            return this.redisTemplate.opsForHash().putIfAbsent(originKey, hashKey, value);
        }
        return false;
    }

    public Boolean hasKeyForHash(String key, Object hashKey) {
        return hasKeyForHashOrigin(getKey(key), hashKey);
    }

    public Boolean hasKeyForHashOrigin(String originKey, Object hashKey) {
        if (originKey != null) {
            return this.redisTemplate.opsForHash().hasKey(originKey, hashKey);
        }
        return false;
    }

    public Map<Object, Object> entriesForHash(String key) {
        return entriesForHashOrigin(getKey(key));
    }

    public Map<Object, Object> entriesForHashOrigin(String originKey) {
        if (originKey != null) {
            return this.redisTemplate.opsForHash().entries(originKey);
        }
        return Collections.emptyMap();
    }

    /**
     * 往redis的set中加values
     *
     * @Methods Name setAdd
     * @Create In 2018年6月14日 By wangzhijie
     * @param key
     * @param values
     * @return Long
     */
    public Long addForSet(String key, Object... values) {
        return addForSetOrigin(getKey(key), values);
    }

    public Long addForSetOrigin(String originKey, Object... values) {
        if (originKey != null) {
            return this.redisTemplate.opsForSet().add(originKey, values);
        }
        return 0L;
    }

    public Boolean isMemberForSet(String key, Object o) {
        return isMemberForSetOrigin(getKey(key), o);
    }

    public Boolean isMemberForSetOrigin(String originKey, Object o) {
        if (originKey != null) {
            return this.redisTemplate.opsForSet().isMember(originKey, o);
        }
        return false;
    }

    /**
     * 查询redis的key的set的大小
     *
     * @Methods Name setSize
     * @Create In 2018年6月14日 By wangzhijie
     * @param key
     * @return Long
     */
    public Long sizeForSet(String key) {
        return sizeForSetOrigin(getKey(key));
    }

    public Long sizeForSetOrigin(String originKey) {
        if (originKey != null) {
            return this.redisTemplate.opsForSet().size(originKey);
        }
        return 0L;
    }

    public Long removeForSet(String key, Object... values) {
        return removeForSetOrigin(getKey(key), values);
    }

    public Long removeForSetOrigin(String originKey, Object... values) {
        if (originKey != null) {
            return this.redisTemplate.opsForSet().remove(originKey, values);
        }
        return 0L;
    }

    public Set<Object> membersForSet(String key) {
        return membersForSetOrigin(getKey(key));
    }

    public Set<Object> membersForSetOrigin(String originKey) {
        if (originKey != null) {
            return this.redisTemplate.opsForSet().members(originKey);
        }
        return Collections.emptySet();
    }

    // -----------------------------------------------------------------------------
    /**
     * 向有序集合中增加 成员-分值
     *
     * @Methods Name hgetall
     * @param key
     * @return Map<Object,Object>
     */
    public Boolean addForZSet(String key, Object member, double score) {
        return addForZSetOrigin(getKey(key), member, score);
    }

    public Boolean addForZSetOrigin(String originKey, Object member, double score) {
        return redisTemplate.opsForZSet().add(originKey, member, score);
    }

    public Set<Object> membersForZSetByScore(String key, double minScore, double maxScore) {
        return membersForZSetByScoreOrigin(getKey(key), minScore, maxScore);
    }

    public Set<Object> membersForZSetByScoreOrigin(String originKey, double minScore, double maxScore) {
        return this.redisTemplate.opsForZSet().rangeByScore(originKey, minScore, maxScore);
    }

    public Long removeForZSet(String key, Object... values) {
        return removeForZSetOrigin(getKey(key), values);
    }

    public Long removeForZSetOrigin(String originKey, Object... values) {
        return this.redisTemplate.opsForZSet().remove(originKey, values);
    }

    public Long removeForZSetByScore(String key, double minScore, double maxScore) {
        return removeForZSetByScoreOrigin(getKey(key), minScore, maxScore);
    }

    public Long removeForZSetByScoreOrigin(String originKey, double minScore, double maxScore) {
        return this.redisTemplate.opsForZSet().removeRangeByScore(originKey, minScore, maxScore);
    }

    public Long sizeForZSet(String key) {
        return sizeForZSetOrigin(getKey(key));
    }

    public Long sizeForZSetOrigin(String originKey) {
        return this.redisTemplate.opsForZSet().size(originKey);
    }

    public Long countForZSetByScore(String key, double minScore, double maxScore) {
        return countForZSetByScoreOrigin(getKey(key), minScore, maxScore);
    }

    public Long countForZSetByScoreOrigin(String originKey, double minScore, double maxScore) {
        return this.redisTemplate.opsForZSet().count(originKey, minScore, maxScore);
    }

    public Double scoreForZSet(String key, Object member) {
        return scoreForZSetOrigin(getKey(key), member);
    }

    public Double scoreForZSetOrigin(String originKey, Object member) {
        return this.redisTemplate.opsForZSet().score(originKey, member);
    }

    // -----------------------------------------------------------------------------
    /**
     * 向散列中增加 成员-值
     *
     * @Methods Name sadd
     * @param key
     * @param field 成员
     * @param value 值
     */
    public void hset(String key, Object field, Object value) {
        redisTemplate.opsForHash().put(getKey(key), field, value);
    }

    /**
     * 向散列中增加多个 成员-值
     *
     * @Methods Name sadd
     * @param key
     * @param map
     */
    public void hmset(String key, Map<? extends Object, ? extends Object> map) {
        redisTemplate.opsForHash().putAll(getKey(key), map);
        ;
    }

    /**
     * 获取散列中某成员的值
     *
     * @Methods Name hget
     * @param key
     * @param field
     * @return Object
     */
    public Object hget(String key, Object field) {
        return redisTemplate.opsForHash().get(getKey(key), field);
    }

    /**
     * 获取散列中所有成员和值
     *
     * @Methods Name hgetall
     * @param key
     * @return Map<Object,Object>
     */
    public Map<Object, Object> hgetall(String key) {
        return redisTemplate.opsForHash().entries(getKey(key));
    }

    /**
     * 获取散列中多个成员的值
     *
     * @Methods Name hmget
     * @param key
     * @param fileds
     * @return List<Object>
     */
    public List<Object> hmget(String key, List<Object> fileds) {
        return redisTemplate.opsForHash().multiGet(getKey(key), fileds);
    }

    /**
     * 删除散列中某成员
     *
     * @Methods Name hdel
     * @param key
     * @param field
     * @return Long
     */
    public Long hdel(String key, Object field) {
        return redisTemplate.opsForHash().delete(getKey(key), field);
    }

    public Long increment(String key, long delta) {
        return redisTemplate.opsForValue().increment(getKey(key), delta);
    }

    // -----------------使用StringRedisTempalte-------------------------
    public void setWithStringReidsTemplate(String key, String value) {
        if (null != key) {
            stringRedisTemplate.opsForValue().set(getKey(key), value);
        }
    }

    public void setWithStringReidsTemplate(String key, String value, long time) {
        if (null != key) {
            setWithStringReidsTemplate(key, value);
            expireWithStringReidsTemplate(key, time);
        }
    }

    public String getWithStringReidsTemplate(String key) {
        if (null != key) {
            return stringRedisTemplate.opsForValue().get(getKey(key));
        }
        return null;
    }

    public void expireWithStringReidsTemplate(String key, long seconds) {
        expireOriginWithStringReidsTemplate(getKey(key), seconds);
    }

    public void expireOriginWithStringReidsTemplate(String originKey, long time) {
        if (time > 0) {
            stringRedisTemplate.expire(originKey, time, TimeUnit.SECONDS);
        }
    }

    public void delWithStringReidsTemplate(String... key) {
        if (key != null && key.length > 0) {
            List<String> delKeys = new ArrayList<>();
            for (String k : key) {
                if (StringUtils.isNotBlank(k)) {
                    delKeys.add(getKey(k));
                }
            }
            stringRedisTemplate.delete(delKeys);
        }
    }

}
