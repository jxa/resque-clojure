(ns resque-clojure.supervisor
  (:require [resque-clojure.worker :as worker]
            [resque-clojure.resque :as resque]))

(def run-loop? (ref true))
(def working-agents (ref #{}))
(def idle-agents (ref #{}))
(def watched-queues (atom []))

(def config (atom {:max-shutdown-wait (* 10 1000) ;; milliseconds
                   :poll-interval (* 5 1000)
                   :max-workers 1}))

(declare release-worker reserve-worker make-agent listen-to listen-loop dispatch-jobs)

(defn configure [c]
  (swap! config merge c))

(defn stop []
  "stops polling queues. waits for all workers to complete current job"
  (dosync (ref-set run-loop? false))
  (apply await-for (:max-shutdown-wait @config) @working-agents)
  (resque/unregister @watched-queues))

(defn start [queues]
  "start listening for jobs on queues (vector)."
  (dotimes [n (:max-workers @config)] (make-agent))
  (listen-to queues)
  (dosync (ref-set run-loop? true))
  (.start (Thread. listen-loop))
  (.addShutdownHook (Runtime/getRuntime)
                    (Thread. stop)))

(defn worker-complete [key ref old-state new-state]
  (release-worker ref)
  (dispatch-jobs)
  (if (= :error (:result new-state))
    (resque/report-error new-state)))

(defn dispatch-jobs []
  (when-let [worker-agent (reserve-worker)]
    (let [msg (resque/dequeue @watched-queues)]
      (if msg
        (send-off worker-agent worker/work-on msg)
        (release-worker worker-agent)))))

(defn listen-loop []
  (if @run-loop?
    (do
      (dispatch-jobs)
      (Thread/sleep (:poll-interval @config))
      (recur))))

(defn make-agent []
  (let [worker-agent (agent {} :error-handler (fn [a e] (throw e)))]
    (add-watch worker-agent 'worker-complete worker-complete)
    (dosync (commute idle-agents conj worker-agent))
    worker-agent))

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
  (resque/register queues)
  (swap! watched-queues into queues))
