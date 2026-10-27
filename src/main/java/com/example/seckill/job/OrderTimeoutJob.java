package com.example.seckill.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class OrderTimeoutJob {

    private static final Logger log = LoggerFactory.getLogger(OrderTimeoutJob.class);

    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;
    private final int timeoutMinutes;

    public OrderTimeoutJob(JdbcTemplate jdbc, StringRedisTemplate redis,
                           @Value("${seckill.order-timeout-minutes:15}") int timeoutMinutes) {
        this.jdbc = jdbc;
        this.redis = redis;
        this.timeoutMinutes = timeoutMinutes;
    }

    @Scheduled(fixedDelay = 30_000)
    public void cancelTimeoutOrders() {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT order_no, user_id, activity_id FROM coupon_order " +
                "WHERE status = 0 AND create_time < NOW(3) - INTERVAL ? MINUTE LIMIT 200",
                timeoutMinutes);

        for (Map<String, Object> row : rows) {
            String orderNo = (String) row.get("order_no");
            long activityId = ((Number) row.get("activity_id")).longValue();
            long userId = ((Number) row.get("user_id")).longValue();

            // 状态机条件更新：只有真的把 0 改成 2 的线程，才有资格回补库存
            int n = jdbc.update(
                    "UPDATE coupon_order SET status = 2 WHERE order_no = ? AND status = 0", orderNo);
            if (n == 0) {
                continue;
            }

            jdbc.update("UPDATE coupon_activity SET available_stock = available_stock + 1 WHERE id = ?", activityId);
            redis.opsForValue().increment("stock:{" + activityId + "}");
            redis.opsForSet().remove("bought:{" + activityId + "}", String.valueOf(userId));
            jdbc.update("INSERT IGNORE INTO coupon_stock_log(activity_id, user_id, order_no, type) " +
                        "VALUES (?,?,?,2)", activityId, userId, orderNo);

            log.info("订单超时取消并回补库存 orderNo={}", orderNo);
        }
    }
}
