(ns resque-clojure.core
  (:require [clojure.contrib.json :as json]
            [resque-clojure.redis :as redis]
            [resque-clojure.worker :as worker]))

(declare namespace-key)
(declare full-queue-name)
(declare enqueue!)
(declare enqueue)
(declare dequeue)
(declare work-on)
(declare incoming-listener)

(def redis-agent (agent {}))

(defn namespace-key [key]
  (str "resque:" key))

(defn full-queue-name [name]
  (namespace-key (str "queue:" name)))

(defn enqueue [connection queue-name worker-name & args]
  (redis/rpush connection
               (full-queue-name queue-name)
               (json/json-str {:class worker-name :args args})))

(defn dequeue [connection queue-name]
  (let [data (redis/lpop connection (full-queue-name queue-name))]
    (if (nil? data)
      {:empty queue-name}
      {:received (json/read-json data)})))

(defn worker-complete [key ref old new]
  (println "worker is done. result: " new))

(defn listen-to [queue-name connection-args]
  (redis/with-connection c connection-args
    (let [worker-agent (agent {})]
      (add-watch worker-agent 'worker-complete worker-complete)
      (loop []
        (let [msg (dequeue c queue-name)]
          (if (:received msg)
            (send worker-agent worker/work-on (:received msg))))
        (Thread/sleep 5.0)
        (recur)))))