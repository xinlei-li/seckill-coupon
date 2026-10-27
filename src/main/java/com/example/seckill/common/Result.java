package com.example.seckill.common;

public record Result(int code, String msg, Object data) {

    public static Result ok(Object data) {
        return new Result(0, "ok", data);
    }

    public static Result fail(int code, String msg) {
        return new Result(code, msg, null);
    }
}
