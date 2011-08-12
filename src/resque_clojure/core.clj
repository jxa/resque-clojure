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
         format-error
         register)

(def run-loop? (ref true))
(def working-agents (ref {}))
(def idle-agents (ref {}))
(def queues (ref []))
(def max-wait (10*1000)) ;; milliseconds

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
  (let [worker-agent (make-agent)]
    (register [queue])
    (loop []
      (let [msg (dequeue queue)]
        (if (:received msg)
          (send worker-agent worker/work-on (:received msg) queue)))
      (Thread/sleep 5.0)
      (recur))))

;; aaaaaaaaaaaaaaaaaaaaaaaaaaarrrrrrrrrrrrrrrrrrrrgh
;; (defn listen []
;;   (if @run-loop
;;     (let [worker-agent (reserve-worker)]
;;       (if worker-agent
;;         (let [msg (dequeue (first queues))]
;;           (if (:received msg)
;;             (send worker-agent worker/work-on (:received msg) (first queues)))
;;           (release-worker worker-agent)))))
;;   )

(defn make-agent []
  (let [worker-agent (agent {} :error-handler (fn [a e] (throw e)))]
    (add-watch worker-agent 'worker-complete worker-complete)
    (dosync (commute idle-agents conj worker-agent))))

(defn shutdown []
  (dosync (ref-set run-loop? false))
  (await-for max-wait worker-agents))

(defn reserve-worker []
  "either returns an idle worker or nil.
   marks the returned worker as working."
  
  (dosync (let [selected (first @idle-agents)
                idle (rest @idle-agents)]
            (if selected
              (alter working-agents conj selected)
              (ref-set idle-agents idle))
            selected)))

(defn release-worker [worker]
  )

;; Runtime.getRuntime().addShutdownHook(new Thread() {
;;     public void run() { /*
;;        my shutdown code here
;;     */ }
;;  });


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


(defmulti register class)

(defmethod register java.util.Collection [queues]
  (let [worker-name (worker/name queues)
        worker-started-key (str "worker:" worker-name ":started")
        time (format "%1$ta %1$tb %1$td %1$tk:%1$tM:%1$tS %1$tz %1$tY" (Date.))]
    (redis/sadd (namespace-key "workers") worker-name)
    (redis/set (namespace-key worker-started-key) time)))

(defmethod register String [queue] (register (vector queue)))

(defn unregister [queues]
  (let [worker-name (worker/name queues)
        keys (redis/keys (str "*" worker-name "*"))
        workers-set (namespace-key "workers")]
    (redis/del worker-name)
    (redis/srem workers-set worker-name)
    (if (empty? (redis/smembers workers-set))
      (redis/del workers-set))
    (doseq [key keys]
      (redis/del key))))
