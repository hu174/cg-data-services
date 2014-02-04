package org.cg.representation;

import static org.cg.representation.RouteConstants._capacity;

import java.util.Comparator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

public class PuckThreadPoolExecutor extends ThreadPoolExecutor {
	private static final int CORE_POOL_SIZE = 10;
	private static final int MAXIMUM_POOL_SIZE = 20;
	private static final long KEEP_ALIVE_TIME = 10;
	private static final int QUEUE_SIZE = 20;
	private static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;
	
	private static int corePoolSize;
	private static int maximumPoolSize;
	private static long keepAliveTime;
	private static int queueSize;
	private static TimeUnit unit;
	
	private static final Logger log = Logger.getLogger(PuckThreadPoolExecutor.class);
	/**
	private static void setDefault(){
		queueSize = QUEUE_SIZE;
		unit = TIME_UNIT;
		corePoolSize = CORE_POOL_SIZE;
		maximumPoolSize = MAXIMUM_POOL_SIZE;
		keepAliveTime = KEEP_ALIVE_TIME;
	}
	
	private static void validate(){
		Preconditions.checkArgument(corePoolSize > 0, "corePoolSize must be positive: %s" , corePoolSize);
		Preconditions.checkArgument(maximumPoolSize >= corePoolSize, "maximumPoolSize must greater or equals to corePoolSize.");
		Preconditions.checkArgument(queueSize > 0, "queueSize must be positive: %s", queueSize);
		Preconditions.checkArgument(keepAliveTime > 0, "keepAliveTime must be positive: %s", keepAliveTime);
		Preconditions.checkNotNull(unit, "TimeUnit may not be null. ");
	}
	*/
	public PuckThreadPoolExecutor build(){
		PuckThreadPoolExecutor executor = new PuckThreadPoolExecutor( corePoolSize,  maximumPoolSize, keepAliveTime,  unit,  new ArrayBlockingQueue<Runnable>(queueSize));
		return executor;
	}
	
	
	// Creates a new ThreadPoolExecutor with the given initial parameters and
	// default thread factory and rejected execution handler.
	public PuckThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
	}

	// Creates a new ThreadPoolExecutor with the given initial parameters and
	// default thread factory
	public PuckThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
				handler);
	}

	// Creates a new ThreadPoolExecutor with the given initial parameters and
	// default rejected execution handler
	public PuckThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
				threadFactory);
	}

	// Creates a new ThreadPoolExecutor with the given initial parameters
	public PuckThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
			RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
				threadFactory, handler);
	}

}
