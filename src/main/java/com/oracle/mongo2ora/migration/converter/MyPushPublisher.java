package com.oracle.mongo2ora.migration.converter;

import oracle.rsi.RSIException;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;

public class MyPushPublisher<T> implements oracle.rsi.PushPublisher<T> {

	private boolean isClosed = false;
	private Flow.Subscriber<? super T> rsiSubscriber;
	private final AtomicLong request = new AtomicLong(0L);

	public MyPushPublisher() {
	}

	public void subscribe(Flow.Subscriber<? super T> subscriber) {
		if (this.rsiSubscriber == null) {
			(this.rsiSubscriber = subscriber).onSubscribe(new FlowSubscription());
		}
		else {
			if (!this.rsiSubscriber.equals(subscriber)) {
				subscriber.onError(new RSIException("A Publisher can be subscribed with only one subscriber."));
			}

		}
	}

	public /*synchronized*/ void accept(T object) {
		if (this.isClosed) {
			throw new RSIException("Cannot accept. Publisher is closed.");
		}
		/*else if (this.request.get() == 0L) {
			throw new RSIException("Notifying memory pressure.");
		}*/
		else {
			this.rsiSubscriber.onNext(object);
			this.request.decrementAndGet();
		}
	}

	public void close() throws Exception {
		this.isClosed = true;
	}

	private class FlowSubscription implements Flow.Subscription {
		private boolean isCancelled = false;

		private FlowSubscription() {
		}

		public void request(long var1) {
			if (!this.isCancelled) {
				if (var1 <= 0L) {
					MyPushPublisher.this.rsiSubscriber.onError(new IllegalArgumentException("Non-positive request signals are illegal."));
				}

				MyPushPublisher.this.request.addAndGet(var1);
			}
		}

		public void cancel() {
			if (!this.isCancelled) {
				this.isCancelled = true;
			}
		}
	}
}
