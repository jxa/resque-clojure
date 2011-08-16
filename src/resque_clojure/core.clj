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
         register
         make-agent
         delete-worker
         listen-loop
         listen-to
         reserve-worker
         release-worker)

(def run-loop? (ref true))
(def working-agents (ref #{}))
(def idle-agents (ref #{}))
(def watched-queues (atom []))

(def max-wait (* 10 1000)) ;; milliseconds
(def sleep-interval (* 5 1000))

(defn namespace-key [key]
  (str "resque:" key))

(defn full-queue-name [name]
  (namespace-key (str "queue:" name)))

(defn enqueue [queue worker-name & args]
  (redis/sadd (namespace-key "queues") queue)
  (redis/rpush (full-queue-name queue)
               (json/json-str {:class worker-name :args args})))

(defn dequeue-randomized [queues]
  "Randomizes the list of queues. Then returns the first queue that contains a job"
  (first
   (filter #(:data %)
           (map (fn [q] {:queue q :data (redis/lpop (full-queue-name q))})
                (shuffle queues)))))

(defn dequeue [queues]
  "Randomizes the list of queues. Then returns the first queue that contains a job.
Returns a hash of: {:queue \"queue-name\" :data {...}} or nil"
  (let [msg (dequeue-randomized queues)]
    (if (not (nil? msg))
      (assoc msg :data (json/read-json (:data msg))))))

(defn worker-complete [key ref old-state new-state]
  (release-worker ref)
  (if (= :error (:result new-state))
    (report-error new-state)))

(defn dispatch-jobs []
  (let [worker-agent (reserve-worker)]
    (if worker-agent
      (let [msg (dequeue @watched-queues)]
        (if (msg)
          (send-off worker-agent worker/work-on (:data msg) (:queue msg))
          (release-worker worker-agent))))))

(defn start
  "start listening for jobs on queues (vector)."
  ([queues] (start queues 1))
  ([queues num-workers]
     (dotimes [n num-workers] (make-agent))
     (listen-to queues)
     (dosync (ref-set run-loop? true))
     (.start (Thread. listen-loop))))

(defn listen-loop []
  (if @run-loop?
    (do
      (dispatch-jobs)
      (Thread/sleep sleep-interval)
      (recur))))

(defn make-agent []
  (let [worker-agent (agent {} :error-handler (fn [a e] (throw e)))]
    (add-watch worker-agent 'worker-complete worker-complete)
    (add-watch worker-agent 'bs (fn [k r o n] (println "got something: " n)))
    (dosync (commute idle-agents conj worker-agent))
    worker-agent))

(defn stop []
  "stops polling queues. waits for all workers to complete current job"
  (dosync (ref-set run-loop? false))
  (await-for max-wait @working-agents))

(defn reserve-worker []
  "either returns an idle worker or nil.
   marks the returned worker as working."
  
  (dosync
   (let [selected (first @idle-agents)]
     (if selected
       (do
         (alter idle-agents disj selected)
         (alter working-agents conj selected)))
     selected)))

(defn release-worker [w]
  (dosync (alter working-agents disj w)
          (alter idle-agents conj w)))

(defn listen-to [queues]
  (register queues)
  (swap! watched-queues into queues))

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

(defn register [queues]
  (let [worker-name (worker/name queues)
        worker-started-key (str "worker:" worker-name ":started")
        time (format "%1$ta %1$tb %1$td %1$tk:%1$tM:%1$tS %1$tz %1$tY" (Date.))]
    (redis/sadd (namespace-key "workers") worker-name)
    (redis/set (namespace-key worker-started-key) time)))

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
