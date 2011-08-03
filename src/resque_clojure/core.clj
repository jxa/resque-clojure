(ns resque-clojure.core
  :import [redis.clients.jedis Jedis JedisPool])

(def *jedis-pool* (ref nil))

(defn init-pool [config]
  (dosync (ref-set *jedis-pool* (JedisPool. (:host config) (:port config)))))

(defn finalize-pool []
  (.destroy @*jedis-pool*))

