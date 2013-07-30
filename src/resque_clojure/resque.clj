(ns resque-clojure.resque
  (:import [java.util Date])
  (:require [resque-clojure.redis :as redis]
            [clojure.data.json :as json]
            [resque-clojure.worker :as worker]))

;; private api
(declare -namespace-key
         -full-queue-name
         -format-error
         -dequeue-randomized)

;;
;; public api
;;

(def config (atom {:namespace "resque"
                   :error-handler nil}))

(defn configure [c]
  (swap! config merge c))

(defn enqueue [queue worker-name & args]
  (redis/sadd (-namespace-key "queues") queue)
  (redis/rpush (-full-queue-name queue)
               (json/json-str {:class worker-name :args args})))

(defn dequeue [queues]
  "Randomizes the list of queues. Then returns the first queue that contains a job.
   Returns a hash of: {:queue \"queue-name\" :data {...}} or nil"
  (let [msg (-dequeue-randomized queues)]
    (if msg
      (let [{:keys [class args]} (json/read-json (:data msg))]
        (assoc msg :func class :args args)))))

(defn report-error [result]
  (let [error (-format-error result)
        handle (:error-handler @config)]
    (redis/rpush (-namespace-key "failed") (json/json-str error))
    (when handle
      (handle error))))

(defn register [queues]
  (let [worker-name (worker/name queues)
        worker-started-key (str "worker:" worker-name ":started")
        time (format "%1$ta %1$tb %1$td %1$tk:%1$tM:%1$tS %1$tz %1$tY" (Date.))]
    (redis/sadd (-namespace-key "workers") worker-name)
    (redis/set (-namespace-key worker-started-key) time)))

(defn unregister [queues]
  (let [worker-name (worker/name queues)
        keys (redis/keys (str "*" worker-name "*"))
        workers-set (-namespace-key "workers")]
    (redis/del worker-name)
    (redis/srem workers-set worker-name)
    (if (empty? (redis/smembers workers-set))
      (redis/del workers-set))
    (doseq [key keys]
      (redis/del key))))

;;
;; private
;;

(defn -namespace-key [key]
  (str (:namespace @config) ":" key))

(defn -full-queue-name [name]
  (-namespace-key (str "queue:" name)))

(defn -format-error [result]
  (let [exception (:exception result)
        stacktrace (map #(.toString %) (.getStackTrace exception))
        exception-class (-> exception (.getClass) (.getName))]
    {:failed_at (format "%1$tY/%1$tm/%1$td %1$tk:%1$tM:%1$tS" (Date.))
     :payload (select-keys result [:job :class :args])
     :exception exception-class
     :error (or (.getMessage exception) "(null)")
     :backtrace stacktrace
     :worker (apply str (interpose ":" (reverse (.split (.getName (java.lang.management.ManagementFactory/getRuntimeMXBean)) "@"))))
     :queue (:queue result)}))

(defn -dequeue-randomized [queues]
  "Randomizes the list of queues. Then returns the first queue that contains a job"
  (loop [qs (shuffle queues)]
    (let [q (first qs)
          nsq (-full-queue-name q)
          job (redis/lpop nsq)]
      (cond
       (empty? qs) nil
       job {:queue q :data job}
       :else (recur (rest qs))))))
