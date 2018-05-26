package it.uniroma2.sabd.mjolnir.helpers.persistence;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import static it.uniroma2.sabd.mjolnir.MjolnirConstants.REDIS_HOST;
import static it.uniroma2.sabd.mjolnir.MjolnirConstants.REDIS_PORT;

public class RedisHelper {

    private JedisPool jedisPool = null;

    public Jedis getRedisInstance() {
        if (jedisPool == null) {
            jedisPool = new JedisPool(new JedisPoolConfig(), REDIS_HOST, REDIS_PORT);
        }
        return jedisPool.getResource();
    }


}
