package com.dinstone.kafka.assistant.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.Future;

public class ConsumerTask<K, V> {

    private ConsumerRecord<K, V> record;

    private ConsumerPromise<Void> promise;

    private long submitTime;

    private long scheduleTime;

    private long completeTime;

    public ConsumerTask(ConsumerRecord<K, V> record) {
        this.record = record;
        this.promise = new ConsumerPromise<Void>();
        this.submitTime = System.currentTimeMillis();
    }

    public boolean isFinish() {
        return promise.isDone();
    }

    public void complete() {
        promise.set(null);
        this.completeTime = System.currentTimeMillis();
    }

    public void complete(Throwable e) {
        promise.exception(e);
        this.completeTime = System.currentTimeMillis();
    }

    public ConsumerRecord<K, V> schedule() {
        if (scheduleTime == 0) {
            this.scheduleTime = System.currentTimeMillis();
        }
        return record;
    }

    public ConsumerRecord<K, V> record() {
        return record;
    }

    public Future<Void> future() {
        return promise;
    }

    public long waitTime() {
        return scheduleTime - submitTime;
    }

    public long workTime() {
        return completeTime - scheduleTime;
    }

    public long finishTime() {
        return completeTime - submitTime;
    }

    @Override
    public String toString() {
        return "ConsumerTask [record=" + record + ", promise=" + promise + "]";
    }

}
