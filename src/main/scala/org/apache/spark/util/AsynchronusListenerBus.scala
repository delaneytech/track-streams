package org.apache.spark.util

import java.util.concurrent.{LinkedBlockingQueue, Semaphore, TimeoutException}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import org.apache.spark.SparkContext

import scala.util.DynamicVariable

/**
  * Asynchronously passes events to registered listeners.
  *
  * Until `start()` is called, all posted events are only buffered. Only after this listener bus
  * has started will events be actually propagated to all attached listeners. This listener bus
  * is stopped when `stop()` is called, and it will drop further events after stopping.
  *
  * @param name name of the listener bus, will be the name of the listener thread.
  * @tparam L type of listener
  * @tparam E type of event
  */
private[spark] abstract class AsynchronousListenerBus[L <: AnyRef, E](name: String)
  extends ListenerBus[L, E] {

  self =>

  private var sparkContext: SparkContext = null

  /* Cap the capacity of the event queue so we get an explicit error (rather than
   * an OOM exception) if it's perpetually being added to more quickly than it's being drained. */
  private val EVENT_QUEUE_CAPACITY = 10000
  private val eventQueue = new LinkedBlockingQueue[E](EVENT_QUEUE_CAPACITY)

  // Indicate if `start()` is called
  private val started = new AtomicBoolean(false)
  // Indicate if `stop()` is called
  private val stopped = new AtomicBoolean(false)

  /** A counter for dropped events. It will be reset every time we log it. */
  private val droppedEventsCounter = new AtomicLong(0L)

  /** When `droppedEventsCounter` was logged last time in milliseconds. */
  @volatile private var lastReportTimestamp = 0L

  // Indicate if we are processing some event
  // Guarded by `self`
  private var processingEvent = false

  // A counter that represents the number of events produced and consumed in the queue
  private val eventLock = new Semaphore(0)

  private val listenerThread = new Thread(name) {
    setDaemon(true)
    override def run(): Unit = Utils.tryOrStopSparkContext(sparkContext) {
      AsynchronousListenerBus.withinListenerThread.withValue(true) {
        while (true) {
          eventLock.acquire()
          self.synchronized {
            processingEvent = true
          }
          try {
            val event = eventQueue.poll
            if (event == null) {
              // Get out of the while loop and shutdown the daemon thread
              if (!stopped.get) {
                throw new IllegalStateException("Polling `null` from eventQueue means" +
                  " the listener bus has been stopped. So `stopped` must be true")
              }
              return
            }
            postToAll(event)
          } finally {
            self.synchronized {
              processingEvent = false
            }
          }
        }
      }
    }
  }

  /**
    * Start sending events to attached listeners.
    *
    * This first sends out all buffered events posted before this listener bus has started, then
    * listens for any additional events asynchronously while the listener bus is still running.
    * This should only be called once.
    *
    * @param sc Used to stop the SparkContext in case the listener thread dies.
    */
  def start(sc: SparkContext) {
    if (started.compareAndSet(false, true)) {
      sparkContext = sc
      listenerThread.start()
    } else {
      throw new IllegalStateException(s"$name already started!")
    }
  }

  def post(event: E) {
    if (stopped.get) {
      // Drop further events to make `listenerThread` exit ASAP
      logError(s"$name has already stopped! Dropping event $event")
      return
    }
    val eventAdded = eventQueue.offer(event)
    if (eventAdded) {
      eventLock.release()
    } else {
      onDropEvent(event)
      droppedEventsCounter.incrementAndGet()
    }

    val droppedEvents = droppedEventsCounter.get
    if (droppedEvents > 0) {
      // Don't log too frequently
      if (System.currentTimeMillis() - lastReportTimestamp >= 60 * 1000) {
        // There may be multiple threads trying to decrease droppedEventsCounter.
        // Use "compareAndSet" to make sure only one thread can win.
        // And if another thread is increasing droppedEventsCounter, "compareAndSet" will fail and
        // then that thread will update it.
        if (droppedEventsCounter.compareAndSet(droppedEvents, 0)) {
          val prevLastReportTimestamp = lastReportTimestamp
          lastReportTimestamp = System.currentTimeMillis()
          logWarning(s"Dropped $droppedEvents SparkListenerEvents since " +
            new java.util.Date(prevLastReportTimestamp))
        }
      }
    }
  }

  /**
    * For testing only. Wait until there are no more events in the queue, or until the specified
    * time has elapsed. Throw `TimeoutException` if the specified time elapsed before the queue
    * emptied.
    * Exposed for testing.
    */
  @throws(classOf[TimeoutException])
  def waitUntilEmpty(timeoutMillis: Long): Unit = {
    val finishTime = System.currentTimeMillis + timeoutMillis
    while (!queueIsEmpty) {
      if (System.currentTimeMillis > finishTime) {
        throw new TimeoutException(
          s"The event queue is not empty after $timeoutMillis milliseconds")
      }
      /* Sleep rather than using wait/notify, because this is used only for testing and
       * wait/notify add overhead in the general case. */
      Thread.sleep(10)
    }
  }

  /**
    * For testing only. Return whether the listener daemon thread is still alive.
    * Exposed for testing.
    */
  def listenerThreadIsAlive: Boolean = listenerThread.isAlive

  /**
    * Return whether the event queue is empty.
    *
    * The use of synchronized here guarantees that all events that once belonged to this queue
    * have already been processed by all attached listeners, if this returns true.
    */
  private def queueIsEmpty: Boolean = synchronized { eventQueue.isEmpty && !processingEvent }

  /**
    * Stop the listener bus. It will wait until the queued events have been processed, but drop the
    * new events after stopping.
    */
  def stop() {
    if (!started.get()) {
      throw new IllegalStateException(s"Attempted to stop $name that has not yet started!")
    }
    if (stopped.compareAndSet(false, true)) {
      // Call eventLock.release() so that listenerThread will poll `null` from `eventQueue` and know
      // `stop` is called.
      eventLock.release()
      listenerThread.join()
    } else {
      // Keep quiet
    }
  }

  /**
    * If the event queue exceeds its capacity, the new events will be dropped. The subclasses will be
    * notified with the dropped events.
    *
    * Note: `onDropEvent` can be called in any thread.
    */
  def onDropEvent(event: E): Unit
}

private[spark] object AsynchronousListenerBus {
  /* Allows for Context to check whether stop() call is made within listener thread
  */
  val withinListenerThread: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)
}

