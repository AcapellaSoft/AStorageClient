package ru.acapella.kv.core;

public class CodeException extends Throwable {
    public final int code;

    public CodeException(int code) {
        this.code = code;
    }

    @Override
    public String toString() {
        return "CodeException{" +
                "code=" + code +
                '}';
    }
}
