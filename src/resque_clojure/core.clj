(ns resque-clojure.core
  (:import [java.util Date])
  (:require [clojure.contrib.json :as json]
            [resque-clojure.redis :as redis]
            [resque-clojure.worker :as worker]))

(declare namespace-key
         full-queue-name
         enqueue!
         enqueue
         dequeue
         work-on
         incoming-listener
         report-error
         format-error)

(def redis-agent (agent {}))

(defn namespace-key [key]
  (str "resque:" key))

(defn full-queue-name [name]
  (namespace-key (str "queue:" name)))

(defn enqueue [queue worker-name & args]
  (redis/sadd (namespace-key "queues") queue)
  (redis/rpush (full-queue-name queue)
               (json/json-str {:class worker-name :args args})))

(defn dequeue [queue]
  (let [data (redis/lpop (full-queue-name queue))]
    (if (nil? data)
      {:empty queue}
      {:received (json/read-json data)})))

(defn worker-complete [key ref old-state new-state]
  (if (= :error (:result new-state))
    (report-error new-state)))

(defn listen-to [queue]
  (let [worker-agent (agent {} :error-handler (fn [a e] (throw e)))]
    (add-watch worker-agent 'worker-complete worker-complete)
    (loop []
      (let [msg (dequeue queue)]
        (if (:received msg)
          (send worker-agent worker/work-on (:received msg) queue)))
      (Thread/sleep 5.0)
      (recur))))

(defn format-error [result]
  (let [exception (:exception result)
        stacktrace (map #(.toString %) (.getStackTrace exception))
        exception-class (-> exception (.getClass) (.getName))]
    {:failed_at (format "%1$tY/%1$tm/%1$td %1$tk:%1$tM:%1$tS" (Date.))
     :payload (:job result)
     :exception exception-class
     :error (or (.getMessage exception) "(null)")
     :backtrace stacktrace
     :worker "hostname:pid:queue"
     :queue (:queue result)}))

(defn report-error [result]
  (redis/rpush (namespace-key "failed") (json/json-str (format-error result))))

;; TODO:
;; - register worker with redis
;; - set worker status in redis
;; - solicit feedback
;;   - carin
;;   - mailing list / irc
;; - publish to clojars
;; - add resque-status as an option