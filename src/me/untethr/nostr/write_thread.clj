(ns me.untethr.nostr.write-thread
  (:require [clojure.tools.logging :as log])
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

(defn run-async!
  (^ListenableFuture [task-fn]
   {:pre [(fn? task-fn)]}
   (.submit single-event-thread
     (reify Callable
       (call [_this]
         (task-fn)))))
  (^ListenableFuture [task-fn success-fn failure-fn]
   (doto
     (run-async! task-fn)
     (add-callback! success-fn failure-fn))))
