(ns resque-clojure.core
  (:require [clojure.contrib.json :as json]
            [resque-clojure.redis :as redis]))

(defn init []
  (redis/init {:host "localhost" :port 6379}))

(defn full-queue-name [name]
  (str "queue:" name))

(defn enqueue [connection queue-name worker-name & args]
  (send-off *redis-agent* enqueue!
            connection
            (full-queue-name queue-name)
            (json/json-str {:class worker-name :args args})))

(defn enqueue! [status connection queue-name data]
  {:sent (redis/rpush connection queue-name data)})

(defn dequeue [connection queue-name]
  (let [data (redis/lpop connection queue-name)]
    (if (nil? data)
      {:empty queue-name}
      {:received data})))

(defn incoming-listener [key reference old-state new-state]
  (if (some #{:received} (keys new-state)) ; if received is a key in the map
    (work-on (:received new-state))))

(defn work-on [job]
  )
