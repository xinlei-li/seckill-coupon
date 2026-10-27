package com.example.seckill.controller;

import com.example.seckill.common.Result;
import com.example.seckill.service.SeckillService;
import org.springframework.web.bind.annotation.*;

@RestController
public class SeckillController {

    private final SeckillService service;

    public SeckillController(SeckillService service) {
        this.service = service;
    }

    @PostMapping("/api/seckill/{activityId}")
    public Result seckill(@PathVariable long activityId,
                          @RequestHeader("X-User-Id") long userId) {
        return Result.ok(service.seckill(activityId, userId));
    }

    @GetMapping("/api/seckill/result/{orderNo}")
    public Result result(@PathVariable String orderNo) {
        return Result.ok(service.queryResult(orderNo));
    }

    @PostMapping("/admin/activity/{activityId}/warmup")
    public Result warmup(@PathVariable long activityId) {
        return Result.ok(service.warmup(activityId));
    }

    @GetMapping("/api/activity/{activityId}/stock")
    public Result stock(@PathVariable long activityId) {
        return Result.ok(service.queryStock(activityId));
    }
}
