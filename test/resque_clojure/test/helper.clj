(ns resque-clojure.test.helper
  (:use [clojure.java.shell :only [sh]])
  (:require [clojure.string :as string]
            [resque-clojure.redis :as redis]
            [resque-clojure.core :as core]))

(def ^:dynamic *config*
  {'pidfile "/tmp/resque-clojure-redis.pid"
   'daemonize "yes"
   'port "6380"
   'logfile "/tmp/resque-clojure-redis.log"})

(defn start-redis []
  (let [conf (map (fn [[k v]] (str "--" k " " v)) *config*)]
    (apply sh "redis-server" conf)
    ; wait for redis to start
    (Thread/sleep 200)))

(defn stop-redis []
  (sh "redis-cli" "-p" (*config* 'port) "shutdown"))

(defn config-redis []
  (core/configure {:port (Integer/parseInt (*config* 'port))
                   :host "localhost"
                   :max-workers 1
                   :password nil}))

(defn redis-test-instance [tests]
  (start-redis)
  (config-redis)
  (try
    (tests)
    (finally (stop-redis))))

(defn redis-test-instance-with-config [config]
  (binding [*config* (merge *config* config)]
    redis-test-instance))

(defn cleanup-redis-keys [tests]
  (redis/flushdb)
  (tests))
