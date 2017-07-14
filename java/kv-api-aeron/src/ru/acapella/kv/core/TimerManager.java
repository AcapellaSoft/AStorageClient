package ru.acapella.kv.core;

import ru.acapella.common.ExceptionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.stream.Collectors;

/**
 * java 8 commons
 * 2016
 *
 * @author doker
 */
public class TimerManager {

    @FunctionalInterface
    public interface ITimerCallback {
        /**
         * @return true если текущий таймер нужно отсановить и удалить, в противном случае он продолжит работу
         */
        boolean tick(Timer timer) throws Throwable;
    }

    private int hashIdCounter = 0;

    private long currentTime = -1;
    protected HashSet<Timer> timers = new HashSet<>();
    private Timer nearestTimer;

    /** Обновление текущего времени и выполнение всех истекших таймеров. */
    public void updateTimersAndTime() {
        updateCurrentTime();
        processTimers();
    }

    private ArrayList<Throwable> exceptions = new ArrayList<>();
    private ArrayList<Timer> timersCopy = new ArrayList<>();
    /**
     * Выполнение всех истекших таймеров.
     * При исполнении обработчиков таймеров может возникнуть мноество ошибок (в каждом из них).
     * Все эти ошибки вылетят в виде одного исключения, где будут содержаться стектрейсы всех ошибок.
     */
    public void processTimers() {
        if (nearestTimer == null || nearestTimer.timeout > currentTime)
            return;

        // пересчитываем ближайший таймер
        nearestTimer = null;
        long minTimeout = Long.MAX_VALUE;
        timersCopy.clear();
        timersCopy.addAll(timers);
        for (Timer timer : timersCopy) {
            if (timer.timeout <= currentTime) {
                boolean removeTimer = true;
                try {
                    removeTimer = timer.callback.tick(timer);
                } catch (InterruptedException interrupted) {
                    // выбрасывать исключения в момент прерывания текущего потока - не самая лучшая идея
                    // в тоже время исключения нельзя просто отбрасывать, поэтому просто печатаем
                    for (Throwable e : exceptions)
                        e.printStackTrace();
                    return;
                } catch (Throwable throwable) {
                    exceptions.add(throwable);
                }

                if (removeTimer) {
                    timers.remove(timer);
                    continue;
                } else {
                    timer.timeout = currentTime + timer.period;
                }
            }

            if (timer.timeout < minTimeout) {
                minTimeout = timer.timeout;
                nearestTimer = timer;
            }
        }

        if (exceptions.size() > 0) {
            String stackTraces = exceptions.stream()
                    .map(ExceptionUtils::getStackTrace)
                    .collect(Collectors.joining("\n"));
            exceptions.clear();
            throw new RuntimeException("timer errors:\n" + stackTraces);
        }
    }

    public void updateCurrentTime() { updateCurrentTime(System.nanoTime()); }
    public void updateCurrentTime(long currentTime) { this.currentTime = currentTime; }

    public long getCurrentTime() {
        return currentTime;
    }

    /** @return время в наносекундах до следющего срабатывания любого из таймеров. */
    public int getWaitPeriod() {
        if (nearestTimer == null)
            return -1;
        else {
            final int timeout = (int) (nearestTimer.timeout - currentTime);
            return Math.max(timeout, 0);
        }
    }

    /** idempotent */
    private void addTimer(Timer timer) {
        if (timers.add(timer)) {
            if (nearestTimer == null) {
                nearestTimer = timer;
            } else {
                if (nearestTimer.timeout > timer.timeout)
                    nearestTimer = timer;
            }
        }
    }

    /** idempotent */
    private void removeTimer(Timer timer) {
        if (timers.remove(timer)) {
            if (nearestTimer == timer)
                calcNearestTimer();
        }
    }

    /** idempotent */
    private void calcNearestTimer() {
        long minTimeout = Long.MAX_VALUE;
        nearestTimer = null;

        for (Timer timer : timers)
            if (timer.timeout < minTimeout) {
                minTimeout = timer.timeout;
                nearestTimer = timer;
            }
    }

    /**
     * Создает и стартует новый таймер.
     * @param period после данного периода времени в наносекундах срабатывает таймер
     * @param tag тег для отладки
     * @param callback вызывается по истечению таймаута
     * @return созданный таймер
     */
    public Timer runTimer(long period, String tag, ITimerCallback callback) {
        if (currentTime == -1) currentTime = System.nanoTime();
        final Timer timer = new Timer(this, hashIdCounter++, callback, period, currentTime + period, tag);
        addTimer(timer);
        return timer;
    }

    /**
     * Создает и стартует новый таймер.
     * @param period после данного периода времени в наносекундах срабатывает таймер
     * @param callback вызывается по истечению таймаута
     * @return созданный таймер
     */
    public Timer runTimer(long period, ITimerCallback callback) {
        return runTimer(period, null, callback);
    }

    public final static class Timer {
        private final TimerManager manager;
        private final int id;

        private ITimerCallback callback;
        private long timeout;
        private long period;
        private final String tag;

        private Timer(TimerManager manager, int id, ITimerCallback callback, long period, long timeout, String tag) {
            this.manager = manager;
            this.id = id;
            this.callback = callback;
            this.period = period;
            this.timeout = timeout;
            this.tag = tag;
        }

        /** Можно менять в любой момент времени */
        public void setCallback(ITimerCallback callback) {
            this.callback = callback;
        }

        /** При старте уже запущенного таймера ничего не происходит. */
        public void start() {
            manager.addTimer(this);
        }

        /** При остановке уже остановленного таймера ничего не происходит. */
        public void stop() {
            manager.removeTimer(this);
        }

        /** Если таймер уже сработал не делает ничего */
        public void restart() {
            this.timeout = manager.currentTime + period;
            manager.calcNearestTimer();
        }

        /**
         * При устновке периода, при котором действие уже должно было выполнится,
         * оно выполнится только при следущем вызове {@link TimerManager#processTimers()}
         * или {@link TimerManager#updateTimersAndTime()}.
         */
        public void setPeriod(int period) {
            if (period != this.period) {
                long startTime = timeout - this.period;
                this.period = period;
                this.timeout = startTime + period;
                if (manager.nearestTimer == this)
                    manager.calcNearestTimer();
            }
        }

        public long getPeriod() {
            return period;
        }

        @Override
        public int hashCode() {
            return id;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Timer))
                return false;
            Timer other = (Timer) obj;
            return (this.id == other.id) && (this.manager == other.manager);
        }

        @Override
        public String toString() {
            return String.format("timer(%s, %d)", tag, period);
        }
    }
}
