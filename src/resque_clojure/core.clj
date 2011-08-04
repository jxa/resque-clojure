(ns resque-clojure.core
  (:require [clojure.contrib.json :as json]
            [resque-clojure.redis :as redis]))

(defn init []
  (redis/init {:host "localhost" :port 6379}))

(defn full-queue-name [name]
  (str "queue:" name))

(defn enqueue [queue-name worker-name & args]
  (redis/rpush (full-queue-name queue-name)
               (json/json-str {:class worker-name :args args})))
