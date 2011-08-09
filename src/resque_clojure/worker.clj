(ns resque-clojure.worker
  (:use [clojure.string :only [split]]))

(defn lookup-fn [namespaced-fn]
  (let [[namespace fun] (split namespaced-fn #"/")]
    (ns-resolve (symbol namespace) (symbol fun))))

(defn work-on [state job queue]
  (let [{namespaced-fn :class args :args} job]
    (try
      (apply (lookup-fn namespaced-fn) args)
      {:result :pass :job job :queue queue}
      (catch Exception e
        {:result :error :exception e :job job :queue queue}))))

;; worker key
;; pair-one-4.edgecase:51309:scripsafe
;; redis pair-one-4.edgecase:6379>type resque:workers

;; set
;; redis pair-one-4.edgecase:6379>smembers resque:workers
;; 1) "pair-one-4.edgecase:51309:scripsafe"
;; redis pair-one-4.edgecase:6379>type resque:stat:processed
;; string
;; redis pair-one-4.edgecase:6379>get resque:stat:processed
;; "1"
;; redis pair-one-4.edgecase:6379>type resque:worker:pair-one-4.edgecase:51309:scripsafe:started
;; string
;; redis pair-one-4.edgecase:6379>get resque:worker:pair-one-4.edgecase:51309:scripsafe:started
;; "Tue Aug 09 16:27:27 -0400 2011"
;; redis pair-one-4.edgecase:6379>type resque:stat:failed:pair-one-4.edgecase:51309:scripsafe
;; string
;; redis pair-one-4.edgecase:6379>get resque:stat:failed:pair-one-4.edgecase:51309:scripsafe
;; "1"
;; redis pair-one-4.edgecase:6379>get resque:stat:processed:pair-one-4.edgecase:51309:scripsafe
;; "1"
;; redis pair-one-4.edgecase:6379>


;; redis pair-one-4.edgecase:6379>keys *
;;  1) "resque:workers"
;;  redis> smembers resque:workers
;;  1) "pair-one-4.edgecase:51309:scripsafe"

(defn worker-name [queues]
  (let [pid-host (.getName (java.lang.management.ManagementFactory/getRuntimeMXBean))
        [pid hostname] (split pid-host #"@")
        qs (apply str (interpose "," queues))]
    (str hostname ":" pid ":" qs)))



;;  2) "resque:stat:processed"
;;  4) "resque:worker:pair-one-4.edgecase:51309:scripsafe:started"
;;  5) "resque:stat:failed:pair-one-4.edgecase:51309:scripsafe"
;;  8) "resque:stat:processed:pair-one-4.edgecase:51309:scripsafe"
;; 10) "resque:stat:failed"

;;  7) "resque:status:da488090a4f3012e107e549a20f1acda"
;;  9) "resque:_statuses"
