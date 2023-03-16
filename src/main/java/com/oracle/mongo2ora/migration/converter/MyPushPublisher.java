package com.oracle.mongo2ora.migration.converter;

import com.oracle.mongo2ora.util.Tools;
import oracle.rsi.RSIException;

import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;

public class MyPushPublisher<T> implements oracle.rsi.PushPublisher<T> {

	private boolean isClosed = false;
	private Flow.Subscriber<Object[]> rsiSubscriber;
//	private final AtomicLong request = new AtomicLong(0L);

	public MyPushPublisher() {
	}

	public void subscribe(Flow.Subscriber<? super T> subscriber) {
		if (this.rsiSubscriber == null) {
			(this.rsiSubscriber = (Flow.Subscriber<Object[]>)subscriber).onSubscribe(new FlowSubscription());
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
/*		int times = 0;
		long value;
		do {
			value = this.request.get();
			times++;
			if (value == 0L) {
				switch (times) {
					case 1:
						Thread.yield();
						break;
					case 2:
					case 3:
					case 4:
						Tools.sleep(1L);
						break;
					case 5:
					case 6:
					case 7:
					case 8:
						Tools.sleep(2L);
						break;
					default:
						Tools.sleep(4L);
				}
			}
		}
		while (value == 0L);
*/
		final List<Object[]> rows = (List<Object[]>)object;

		for(Object[] row : rows)
		this.rsiSubscriber.onNext(row);


//		this.request.decrementAndGet();
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

//				MyPushPublisher.this.request.addAndGet(var1);
			}
		}

		public void cancel() {
			if (!this.isCancelled) {
				this.isCancelled = true;
			}
		}
	}
}
