package com.example.seckill.consumer;

import com.example.seckill.common.SeckillMsg;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final ObjectMapper mapper = new ObjectMapper();

    public OrderConsumer(JdbcTemplate jdbc, TransactionTemplate tx) {
        this.jdbc = jdbc;
        this.tx = tx;
    }

    @KafkaListener(topics = "seckill-order")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        SeckillMsg msg = mapper.readValue(record.value(), SeckillMsg.class);

        try {
            tx.executeWithoutResult(s -> {
                // 1. 数据库扣库存，stock > 0 是防超卖的最后一道条件
                int n = jdbc.update(
                        "UPDATE coupon_activity SET available_stock = available_stock - 1 " +
                        "WHERE id = ? AND available_stock > 0", msg.activityId());

                if (n == 0) {
                    log.info("库存不足，Redis 与 DB 不一致:---------");
                    throw new IllegalStateException("库存不足，Redis 与 DB 不一致: " + msg);
                }

                // 2. 写订单（唯一索引冲突会让整个事务回滚）
                jdbc.update(
                        "INSERT INTO coupon_order(order_no, user_id, activity_id, status) VALUES (?,?,?,0)",
                        msg.orderNo(), msg.userId(), msg.activityId());

                // 3. 写流水
                jdbc.update(
                        "INSERT INTO coupon_stock_log(activity_id, user_id, order_no, type) VALUES (?,?,?,1)",
                        msg.activityId(), msg.userId(), msg.orderNo());
            });
            ack.acknowledge();

        } catch (DuplicateKeyException e) {
            // 幂等命中：消息重复投递，订单已存在，直接确认
            log.info("重复消息，已忽略 orderNo={}", msg.orderNo());
            ack.acknowledge();

        } catch (IllegalStateException e) {
            // 数据不一致：不要无限重试，否则分区被堵死，转人工/对账
            log.error("库存不一致，跳过并告警: {}", e.getMessage());
            ack.acknowledge();

        } catch (Exception e) {
            // 其他异常：不 ack，交给 retry + DLT
            log.error("下单失败，稍后重试: {}", msg, e);
            throw e;
        }
    }
}
