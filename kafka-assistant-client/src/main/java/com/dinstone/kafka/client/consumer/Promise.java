package com.dinstone.kafka.client.consumer;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Promise<T> implements Future<T> {

	private Lock lock = new ReentrantLock();

	private Condition ready = lock.newCondition();

	private boolean done;

	private Object result;

	/**
	 * 0:init; 1:nomal; 2:error; 3:cancel
	 */
	private int state;

	public boolean cancel(boolean mayInterruptIfRunning) {
		lock.lock();
		try {
			if (done) {
				return false;
			}

			state = 3;
			done = true;

			return true;
		} finally {
			lock.unlock();
		}
	}

	public boolean isCancelled() {
		lock.lock();
		try {
			return state == 3;
		} finally {
			lock.unlock();
		}
	}

	public boolean isDone() {
		lock.lock();
		try {
			return done;
		} finally {
			lock.unlock();
		}
	}

	public T get() throws InterruptedException, ExecutionException {
		lock.lock();
		try {
			while (!done) {
				ready.await();
			}
			return getValue();
		} finally {
			lock.unlock();
		}
	}

	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		lock.lock();
		try {
			if (!done) {
				boolean success = ready.await(timeout, unit);
				if (!success) {
					throw new TimeoutException("operation timeout (" + timeout + " " + unit + ")");
				}
			}
			return getValue();
		} finally {
			lock.unlock();
		}
	}

	@SuppressWarnings("unchecked")
	private T getValue() throws ExecutionException {
		if (state == 1) {
			return (T) result;
		}
		if (state == 2) {
			throw new ExecutionException((Throwable) result);
		}

		throw new CancellationException();
	}

	public void set(T r) {
		lock.lock();
		try {
			if (done) {
				throw new IllegalStateException("promise is set");
			}

			result = r;
			done = true;
			state = 1;

			this.ready.signalAll();
		} finally {
			lock.unlock();
		}
	}

	public void exception(Throwable e) {
		lock.lock();
		try {
			if (done) {
				throw new IllegalStateException("promise is set");
			}

			result = e;
			done = true;
			state = 2;

			this.ready.signalAll();
		} finally {
			lock.unlock();
		}
	}

}
