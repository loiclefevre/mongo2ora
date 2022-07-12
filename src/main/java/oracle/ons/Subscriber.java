package oracle.ons;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Subscriber implements Closable {
	public static final int CBMODE_NOCALLBACK = 0;
	public static final int CBMODE_ONETHREAD = 1;
	public static final int CBMODE_THREADPERCB = 2;
	/** @deprecated */
	@Deprecated
	public static final int ErrorSubscriberInvalid = 1;
	/** @deprecated */
	@Deprecated
	public static final int ErrorNotificationReceive = 2;
	/** @deprecated */
	@Deprecated
	public static final int ErrorSubscriberRegister = 3;
	/** @deprecated */
	@Deprecated
	public static final int ErrorServerSupport = 4;
	public static final String RegisterType = "ONSregister";
	public static final String UnregisterType = "ONSunregister";
	public static final String SubscriberId = "SubscriberID";
	public static final String Registerd = "ONSregisterID";
	public static final String DisconnectId = "ONSdisconnectID";
	public static final String DirectRoute = "DirectRoute";
	public static final String StatusResult = "Result";
	public static final String ResultSuccess = "success";
	public static final String StatusMessage = "Message";
	private String componentName;
	private ONSConfiguration config;
	private static AtomicInteger globalId = new AtomicInteger(1);
	private static final int STATE_NOT_INITIALIZED = 0;
	private static final int STATE_NOT_REGISTERED = 1;
	private static final int STATE_REGISTERED = 2;
	private static final int STATE_CLOSED = 3;
	private AtomicInteger state;
	protected int id;
	private String subscription;
	private final BlockingQueue<Notification> notificationQueue;
	protected volatile CallBack callback;
	private volatile int callbackMode;
	private volatile ONSException subscriptionError;
	private volatile boolean registered;
	private final Semaphore subscriptionStatusLock;
	private NotificationNetwork network;
	static final Notification loopbackCloseEvent = new Notification((Throwable)null, "~InternalSubscriberCloseNotification", (Notification)null);
	private boolean wantSystemNotifications;
	private final Subscriber.SingletonCallbackAction callbackSingleton;

	public void close() {
		if (this.state.compareAndSet(2, 3)) {
			this.network.unregisterSubscriber(this);
			this.network.release();
			this.registered = false;
			this.notificationQueue.add(loopbackCloseEvent);
		}

	}

	public String getSubscriptionKey() {
		return this.subscription;
	}

	public boolean isOpen() {
		return this.registered;
	}

	protected boolean handleInternalNotification(Notification n) {
		if (n.verb.equals("status")) {
			if (n.getResult() == 1) {
				if (this.state.compareAndSet(1, 2)) {
					this.registered = true;
				}
			} else {
				this.subscriptionError = new SubscriptionRefused(n.getMessage("Unknown error"));
				this.network.unregisterSubscriber(this);
			}

			this.subscriptionStatusLock.release();
			return true;
		} else {
			return false;
		}
	}

	private void deliverNotification(Notification n) throws InterruptedException {
		if (this.callback != null && this.callbackMode == 2) {
			if (!n.isSystemNotification() || this.wantSystemNotifications || this.callback instanceof NotificationCallback) {
				this.network.master.getWorkloadManager().schedule(new Subscriber.CallCallbackAction(this.callback, n));
			}
		} else {
			this.notificationQueue.put(n);
			if (this.callback != null && this.callbackMode == 1 && !this.callbackSingleton.hasCarrier() && !this.notificationQueue.isEmpty()) {
				this.network.master.getWorkloadManager().schedule(this.callbackSingleton);
			}
		}

	}

	protected void put(Notification n) throws InterruptedException {
		if (this.handleInternalNotification(n)) {
			if (this.wantSystemNotifications) {
				this.deliverNotification(new Notification(this.subscriptionError, "~InternalNotification", n));
			}

		} else {
			if (n.verb.equals("event")) {
				this.deliverNotification(n);
			}

		}
	}

	public Subscriber(ONSConfiguration config, String subscription, String component, CallBack cb) throws ONSException {
		this.componentName = null;
		this.state = new AtomicInteger(0);
		this.notificationQueue = new LinkedBlockingDeque();
		this.callback = null;
		this.callbackMode = 0;
		this.subscriptionError = null;
		this.registered = false;
		this.subscriptionStatusLock = new Semaphore(0, false);
		this.wantSystemNotifications = false;
		this.callbackSingleton = new Subscriber.SingletonCallbackAction();
		this.id = globalId.getAndIncrement();
		this.subscription = subscription;
		this.config = config;
		this.callback = cb;
		this.callbackMode = cb == null ? 0 : 2;
		this.componentName = component;
		this.network = config.getNetwork();
	}

	protected Subscriber(ONSConfiguration config, String subscription, String component, long timeout) throws ONSException {
		this(config, subscription, component, (CallBack)null);
		if (timeout == 0L) {
			timeout = config.getSocketTimeout();
		}

		this.register(timeout);

		try {
			this.waitUntilRegistered(timeout);
		} catch (ONSException var7) {
			this.close();
			throw var7;
		} catch (InterruptedException var8) {
			Thread.currentThread().interrupt();
		}

		this.network.logger.fine("Subscriber " + this.id + " registration status : " + this.registered);
	}

	public void register() throws ONSException {
		this.register(this.config.getSocketTimeout());
	}

	public void register(long timeout) throws ONSException {
		if (timeout == 0L) {
			timeout = this.config.getSocketTimeout();
		}

		if (this.state.compareAndSet(0, 1)) {
			this.network.demand();

			try {
				this.network.waitUntilOnline(timeout, true);
			} catch (InterruptedException var4) {
				Thread.currentThread().interrupt();
			}

			this.network.logger.fine("ONS Registering " + this.toString());
			this.network.registerSubscriber(this);
		}

	}

	public void lazyRegister() {
		if (this.state.compareAndSet(0, 1)) {
			this.network.demand();
			this.network.logger.fine("ONS Registering " + this.toString());
			this.network.registerSubscriber(this);
		}

	}

	public static Subscriber backgroundSubscriber(ONSConfiguration config, String sstr, CallBack callback) {
		Subscriber s = new Subscriber(config, sstr, "", callback);
		s.setWantSystemNotifications(true);
		s.lazyRegister();
		return s;
	}

	public static Subscriber backgroundSubscriber(String onsConnectString, String sstr, CallBack callback) {
		return backgroundSubscriber(new ONSConfiguration(onsConnectString), sstr, callback);
	}

	public void setWantSystemNotifications(boolean value) {
		this.wantSystemNotifications = value;
	}

	public Subscriber(ONS proxy, long timeout) {
		this(proxy, "!", proxy.getConfiguration().getComponent(), proxy.getConfiguration().getSocketTimeout());
	}

	/** @deprecated */
	@Deprecated
	public int register(String registerId, String disconnectId, long timeout) throws ONSException {
		return 0;
	}

	public Subscriber(ONS proxy, String subscription) throws ONSException {
		this(proxy, subscription, proxy.getConfiguration().getComponent());
	}

	public Subscriber(ONS proxy, String subscription, String component) throws ONSException {
		this(proxy, subscription, component, proxy.getConfiguration().getSocketTimeout());
	}

	private static ONS getDefaultONS() {
		ONS ons = ONS.getRunningONS();
		if (ons == null) {
			ons = ONS.getONS();
		}

		return ons;
	}

	/** @deprecated */
	@Deprecated
	public Subscriber(String subscription, String component) throws ONSException {
		this(getDefaultONS(), subscription, component);
	}

	/** @deprecated */
	@Deprecated
	public Subscriber(String subscription, String component, long timeout) throws ONSException {
		this(getDefaultONS(), subscription, component, timeout);
	}

	public Subscriber(ONS proxy, String subscription, String component, long timeout) throws ONSException {
		this(proxy.getConfiguration(), subscription, component, timeout);
		proxy.addChildObject(this);
	}

	public Subscriber(ONSConfiguration config, String subscription) {
		this(config, subscription, config.getComponent(), (CallBack)null);
		this.register();
	}

	public Subscriber(ONSConfiguration config, String subscription, CallBack cb) {
		this(config, subscription, config.getComponent(), cb);
		this.register();
	}

	public boolean waitUntilRegistered() throws ONSException, InterruptedException {
		return this.waitUntilRegistered(this.config.getSocketTimeout(), true);
	}

	public boolean waitUntilRegistered(long timeout) throws ONSException, InterruptedException {
		return this.waitUntilRegistered(timeout, false);
	}

	public boolean waitUntilRegistered(long timeout, boolean throwOnTimeout) throws ONSException, InterruptedException {
		if (this.subscriptionStatusLock.availablePermits() == 0) {
			if (this.subscriptionStatusLock.tryAcquire(timeout, TimeUnit.MILLISECONDS)) {
				this.subscriptionStatusLock.release();
			} else if (throwOnTimeout) {
				throw new SubscriptionException("Subscription time out");
			}
		}

		if (this.subscriptionError != null) {
			ONSException x = this.subscriptionError;
			this.subscriptionError = null;
			throw x;
		} else {
			return this.registered;
		}
	}

	private Notification internalReceive(boolean blocking, long timeout) {
		if (this.state.get() == 3) {
			return null;
		} else {
			try {
				Notification x;
				do {
					x = blocking ? this.take() : this.poll(timeout);
					if (x == loopbackCloseEvent) {
						return null;
					}
				} while(x != null && x.isSystemNotification() && !this.wantSystemNotifications);

				return x;
			} catch (InterruptedException var5) {
				Thread.currentThread().interrupt();
				return null;
			}
		}
	}

	public Notification take() throws InterruptedException {
		if (this.callback != null) {
			throw new SubscriptionException("Trying to poll a callback subscriber");
		} else {
			return (Notification)this.notificationQueue.take();
		}
	}

	public Notification poll(long timeout) throws InterruptedException {
		if (this.callback != null) {
			throw new SubscriptionException("Trying to poll a callback subscriber");
		} else {
			return timeout == 0L ? (Notification)this.notificationQueue.poll() : (Notification)this.notificationQueue.poll(timeout, TimeUnit.MILLISECONDS);
		}
	}

	public String getSubscription() {
		return this.subscription;
	}

	public synchronized void register_callback(CallBack cb, int mode) {
		if (this.callback != null) {
			throw new SubscriptionException("Callback already registered");
		} else {
			this.callback = cb;
			this.callbackMode = mode;
			if (this.callbackMode == 2) {
				ArrayList nl = new ArrayList();

				while(true) {
					Notification n;
					do {
						if ((n = (Notification)this.notificationQueue.poll()) == null) {
							if (!nl.isEmpty()) {
								this.network.master.getWorkloadManager().schedule(new Subscriber.CallCallbackAction(this.callback, (Notification[])nl.toArray(new Notification[nl.size()])));
							}

							return;
						}
					} while(n.isSystemNotification() && !this.wantSystemNotifications && !(this.callback instanceof NotificationCallback));

					nl.add(n);
				}
			} else if (this.callbackMode == 1 && !this.notificationQueue.isEmpty()) {
				this.network.master.getWorkloadManager().schedule(this.callbackSingleton);
			}

		}
	}

	protected Message getSubscriptionMessage() {
		return (new Message("subscribe")).put("Subscription", this.subscription);
	}

	public String toString() {
		return String.format("ONSSubscription : { Subscription : %s; Id : %d }", this.subscription, this.id);
	}

	protected void finalize() throws Throwable {
		this.close();
		super.finalize();
	}

	public boolean isClosed() {
		return this.state.get() == 3;
	}

	public String subscription() {
		return this.getSubscription();
	}

	public String component() {
		return this.componentName;
	}

	public synchronized void cancel_callback() throws SubscriptionException {
		this.callbackMode = 0;
		this.callback = null;
	}

	/** @deprecated */
	@Deprecated
	public Publisher getPublisher() {
		return new Publisher(this.network, this.componentName);
	}

	/** @deprecated */
	@Deprecated
	public int id() {
		return this.id;
	}

	public Notification receive(boolean blocking) {
		return this.internalReceive(blocking, 0L);
	}

	public Notification receive(long timeout) {
		return this.internalReceive(false, timeout);
	}

	/** @deprecated */
	@Deprecated
	public int unregister(long timeout) {
		this.close();
		return 0;
	}

	/** @deprecated */
	@Deprecated
	public void relinquish(Notification e) {
	}

	protected void setServerSubscriberInfo(Node node, String sid) {
	}

	private class SingletonCallbackAction implements Runnable {
		private volatile boolean running;

		private SingletonCallbackAction() {
			this.running = false;
		}

		public boolean hasCarrier() {
			return this.running;
		}

		public void run() {
			synchronized(this) {
				Notification n;
				for(; (n = (Notification)Subscriber.this.notificationQueue.poll()) != null; this.running = false) {
					this.running = true;
					if (Subscriber.this.callback != null) {
						try {
							if (!n.isSystemNotification() || Subscriber.this.wantSystemNotifications || Subscriber.this.callback instanceof NotificationCallback) {
								Subscriber.this.callback.notification_callback(n);
							}
						} catch (Exception var5) {
							Subscriber.this.network.logger.warning(var5.getLocalizedMessage());
						}
					} else {
						Subscriber.this.network.logger.fine("no callback");
					}
				}

			}
		}
	}

	private static class CallCallbackAction implements Runnable {
		CallBack callback_local;
		Notification[] n;

		private CallCallbackAction(CallBack callback, Notification n) {
			this.callback_local = callback;
			this.n = new Notification[1];
			this.n[0] = n;
		}

		private CallCallbackAction(CallBack callback, Notification[] n) {
			this.callback_local = callback;
			this.n = n;
		}

		public void run() {
			if (this.callback_local != null) {
				Notification[] var1 = this.n;
				int var2 = var1.length;

				for(int var3 = 0; var3 < var2; ++var3) {
					Notification i = var1[var3];
					this.callback_local.notification_callback(i);
				}
			}

		}
	}
}
