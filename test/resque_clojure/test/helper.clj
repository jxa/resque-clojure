(ns resque-clojure.test.helper
  (:use [clojure.contrib.shell-out :only [sh]])
  (:require [clojure.string :as string]
            [resque-clojure.redis :as redis]))

(def config {'pidfile "/tmp/resque-clojure-redis.pid"
             'daemonize "yes"
             'port "6380"
             'logfile "stdout"})

(defn start-redis []
  (let [conf (string/join "\n" (map #(str (first %) " " (last %)) config))]
    (sh "redis-server" "-" :in conf)
    ; wait for redis to start
    (Thread/sleep 200)))

(defn stop-redis []
  (sh "redis-cli" "-p" (config 'port) "shutdown"))

(defn config-redis []
  (redis/configure {:port (Integer/parseInt (config 'port))}))

(defn redis-test-instance [tests]
  (start-redis)
  (config-redis)
  (try
    (tests)
    (finally (stop-redis))))

(defn cleanup-redis-keys [tests]
  (doseq [key (redis/keys "*")]
    (redis/del key))
  (tests))