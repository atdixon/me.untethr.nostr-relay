(ns me.untethr.nostr.write-thread
  (:require [clojure.tools.logging :as log]
            [me.untethr.nostr.store :as store]
            [me.untethr.nostr.util :as util])
  (:import (com.google.common.util.concurrent FutureCallback Futures ListenableFuture ListeningExecutorService MoreExecutors ThreadFactoryBuilder)
           (java.util.concurrent ExecutorService Executors Future ThreadFactory)))

(defn create-single-thread-executor
  ^ExecutorService []
  (Executors/newSingleThreadExecutor
    (.build
      (doto (ThreadFactoryBuilder.)
        (.setDaemon true)
        (.setNameFormat "write-thread-%d")
        (.setUncaughtExceptionHandler
          (reify Thread$UncaughtExceptionHandler
            (^void uncaughtException [_this ^Thread _th ^Throwable t]
              (log/error t "uncaught exeception in write thread"))))))))

(defonce ^ListeningExecutorService single-event-thread
  (MoreExecutors/listeningDecorator (create-single-thread-executor)))

(defn add-callback!
  [^ListenableFuture fut success-fn failure-fn]
  {:pre [(fn? success-fn) (fn? failure-fn)]}
  (Futures/addCallback fut
    (reify FutureCallback
      (^void onSuccess [_this result]
        (success-fn result))
      (^void onFailure [_this ^Throwable t]
        (failure-fn t)))
    ;; for now, run listeners on our single thread executor
    single-event-thread))

(defn- enq-checkpoint!
  [singleton-db-conn _]
  (.submit single-event-thread
    ^Runnable
    (fn []
      (let [start-ns (System/nanoTime)]
        (store/checkpoint! singleton-db-conn)
        (log/debugf "checkpointed db (%d ms)"
          (util/nanos-to-millis (- (System/nanoTime) start-ns)))))))

(defn run-async!
  ^ListenableFuture [singleton-db-conn task-fn success-fn failure-fn]
  {:pre [(fn? task-fn) (fn? success-fn) (fn? failure-fn)]}
  (doto
    (.submit single-event-thread
      (reify Callable
        (call [_this]
          (task-fn singleton-db-conn))))
    ;; for now, after every write we'll *enqueue* a full checkpoint; if
    ;; there's other write work behind us our checkpoint won't delay any
    ;; of that. (consider if work patterns + this behavior have any impact
    ;; on readers and if we should have a diff. checkpoint strategy)
    ;;  disabled for now -- using autocheckpointing:
    #_(add-callback!
      (partial enq-checkpoint! singleton-db-conn)
      (fn []))
    (add-callback! success-fn failure-fn)))
