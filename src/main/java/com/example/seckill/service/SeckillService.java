package com.example.seckill.service;

import com.example.seckill.common.SeckillMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Service
public class SeckillService {

    private static final Logger log = LoggerFactory.getLogger(SeckillService.class);
    private static final String TOPIC = "seckill-order";

    private final StringRedisTemplate redis;
    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final DefaultRedisScript<Long> seckillScript;
    private final DefaultRedisScript<Long> rollbackScript;
    private final ObjectMapper mapper = new ObjectMapper();

    public SeckillService(StringRedisTemplate redis,
                          JdbcTemplate jdbc,
                          KafkaTemplate<String, String> kafka,
                          @Qualifier("seckillScript") DefaultRedisScript<Long> seckillScript,
                          @Qualifier("rollbackScript") DefaultRedisScript<Long> rollbackScript) {
        this.redis = redis;
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.seckillScript = seckillScript;
        this.rollbackScript = rollbackScript;
    }

    /** 库存预热：活动开始前必须调用，否则所有请求都返回"未预热" */
    public Map<String, Object> warmup(long activityId) {
        Map<String, Object> row = jdbc.queryForMap(
                "SELECT id, available_stock, end_time FROM coupon_activity WHERE id = ?", activityId);

        int stock = ((Number) row.get("available_stock")).intValue();
        String stockKey = stockKey(activityId);
        String boughtKey = boughtKey(activityId);

        redis.opsForValue().set(stockKey, String.valueOf(stock), Duration.ofHours(24));
        redis.delete(boughtKey);


        log.info("预热完成 activityId={} stock={}", activityId, stock);
        return Map.of("activityId", activityId, "stock", stock);
    }

    public Long queryStock(long activityId) {
        String v = redis.opsForValue().get(stockKey(activityId));
        return v == null ? null : Long.parseLong(v);
    }

    /**
     * 秒杀入口：Redis 原子预扣 → 投递 MQ → 返回"排队中"
     * 注意：返回的是受理结果，不是下单成功
     */
    public Object seckill(long activityId, long userId) {
        String stockKey = stockKey(activityId);
        String boughtKey = boughtKey(activityId);

        Long r = redis.execute(seckillScript, List.of(stockKey, boughtKey), String.valueOf(userId));
        long code = r == null ? -3 : r;

        if (code == 0) return Map.of("status", "SOLD_OUT");
        if (code == -1) return Map.of("status", "DUPLICATE");
        if (code == -2) return Map.of("status", "NOT_WARMED_UP");
        if (code != 1) return Map.of("status", "ERROR");

        String orderNo = nextOrderNo();
        SeckillMsg msg = new SeckillMsg(activityId, userId, orderNo);

        try {
            // key=activityId，保证同一活动的消息落在同一分区 → 顺序消费
            kafka.send(TOPIC, String.valueOf(activityId), mapper.writeValueAsString(msg))
                 .get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("消息投递失败，回滚预扣 activityId={} userId={}", activityId, userId, e);
            // 必须回滚：否则这个用户永久失去资格，库存也永久少一个
            redis.execute(rollbackScript, List.of(stockKey, boughtKey), String.valueOf(userId));
            return Map.of("status", "BUSY");
        }

        return Map.of("status", "QUEUING", "orderNo", orderNo);
    }

    public Map<String, Object> queryResult(String orderNo) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT order_no, status, create_time FROM coupon_order WHERE order_no = ?", orderNo);
        if (rows.isEmpty()) {
            return Map.of("orderNo", orderNo, "status", "PENDING");
        }
        Map<String, Object> row = rows.get(0);
        return Map.of("orderNo", orderNo,
                "status", statusName(((Number) row.get("status")).intValue()),
                "createTime", String.valueOf(row.get("create_time")));
    }

    private String statusName(int s) {
        return switch (s) {
            case 0 -> "UNPAID";
            case 1 -> "PAID";
            case 2 -> "CANCELLED";
            case 3 -> "RESTOCKED";
            default -> "UNKNOWN";
        };
    }

    private String nextOrderNo() {
        return "SK" + LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
                + String.format("%04d", ThreadLocalRandom.current().nextInt(10000));
    }

    /** Redis Cluster 下必须用 hash tag，否则多 key 报 CROSSSLOT */
    private String stockKey(long activityId) {
        return "stock:{" + activityId + "}";
    }

    private String boughtKey(long activityId) {
        return "bought:{" + activityId + "}";
    }
}
