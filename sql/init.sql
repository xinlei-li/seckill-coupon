CREATE DATABASE IF NOT EXISTS seckill DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE seckill;

DROP TABLE IF EXISTS coupon_stock_log;
DROP TABLE IF EXISTS coupon_order;
DROP TABLE IF EXISTS coupon_activity;

CREATE TABLE coupon_activity (
                                 id              BIGINT       NOT NULL PRIMARY KEY,
                                 name            VARCHAR(64)  NOT NULL,
                                 total_stock     INT          NOT NULL,
                                 available_stock INT          NOT NULL,
                                 start_time      DATETIME(3)  NOT NULL,
                                 end_time        DATETIME(3)  NOT NULL,
                                 status          TINYINT      NOT NULL DEFAULT 1 COMMENT '0下线 1上线',
                                 version         INT          NOT NULL DEFAULT 0
) ENGINE = InnoDB COMMENT '优惠券活动';

CREATE TABLE coupon_order (
                              id          BIGINT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
                              order_no    VARCHAR(32) NOT NULL,
                              user_id     BIGINT      NOT NULL,
                              activity_id BIGINT      NOT NULL,
                              status      TINYINT     NOT NULL DEFAULT 0 COMMENT '0待支付 1已支付 2已取消 3已回补',
                              create_time DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                              UNIQUE KEY uk_order_no (order_no),
                              UNIQUE KEY uk_user_activity (user_id, activity_id),
                              KEY idx_status_create (status, create_time)
) ENGINE = InnoDB COMMENT '秒杀订单';

CREATE TABLE coupon_stock_log (
                                  id          BIGINT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                  activity_id BIGINT      NOT NULL,
                                  user_id     BIGINT      NOT NULL,
                                  order_no    VARCHAR(32) NOT NULL,
                                  type        TINYINT     NOT NULL COMMENT '1扣减 2回补',
                                  create_time DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                                  UNIQUE KEY uk_order_type (order_no, type),
                                  KEY idx_activity (activity_id)
) ENGINE = InnoDB COMMENT '库存流水';

INSERT INTO coupon_activity (id, name, total_stock, available_stock, start_time, end_time, status)
VALUES (1, '满100减50 测试券', 100, 100, NOW(3), DATE_ADD(NOW(3), INTERVAL 1 DAY), 1);