package ru.acapella.kv.core.data;

/**
 * Коды ошибок. Никак не связаны с кодами HTTP.
 * Старшие разряды кодируют принадлежность к разным группам ошибок,
 * например, ошибки в транзакциях, общие ошибки и т. п.
 */
public class ErrorCodes {
    // common
    public static final int TIMEOUT = 100;
    public static final int UNEXPECTED_ERROR = 101;
    public static final int ILLEGAL_ARGUMENT = 102;

    // transaction
    public static final int TR_ALREADY_COMPLETED = 201;
    public static final int TR_INTERRUPTED = 202;
    public static final int TR_NOT_FOUND = 203;
}
