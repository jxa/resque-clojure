(ns resque-clojure.core
  (:use [resque-clojure.util :only [filter-map]])
  (:require [resque-clojure.resque :as resque]
            [resque-clojure.redis :as redis]
            [resque-clojure.supervisor :as supervisor]))


(defn configure [c]
  "set configuration parameters. Available keys:
     :host
     :port
     :password
     :uri
     :database
     :timeout
     :namespace
     :max-shutdown-wait
     :poll-interval
     :max-workers
     :error-handler"
  (let [redis-keys (keys @redis/config)
        resque-keys (keys @resque/config)
        super-keys (keys @supervisor/config)
        redis-map (filter-map c redis-keys)
        resque-map (filter-map c resque-keys)
        super-map (filter-map c super-keys)]
    (redis/configure redis-map)
    (resque/configure resque-map)
    (supervisor/configure super-map)))


(defn enqueue [queue worker-name & args]
  "create a new resque job
     queue: name of the queue (does not start with resque:queue)
     worker-name: fully-qualified function name (ex. clojure.core/str in clojure, MyWorker in ruby)
     args: data to be sent as args to the worker. must be able to be serialized to json"
  (apply resque/enqueue queue worker-name args))

(defn start [queues]
  "start listening for jobs on queues (vector)."
  (supervisor/start queues))

(defn stop []
  "stops polling queues. waits for all workers to complete current job"
  (supervisor/stop))
