(ns resque-clojure.redis
  (:import [redis.clients.jedis Jedis]))

;; TODO: rescue from 
;; redis.clients.jedis.exceptions.JedisConnectionException

(defn connect [host port]
  (Jedis. host port))

(defn disconnect [redis]
  (.disconnect redis))

(defn rpush [redis key value]
  (.rpush redis key value))

(defn lpop [redis key]
  (.lpop redis key))

(defn llen [redis key]
  (.llen redis key))

(defn lindex [redis key index]
  (.lindex redis key (long index)))

(defn lrange [redis key start end]
  (seq (.lrange redis key (long start) (long end))))

(defn smembers [redis key]
  (seq (.smembers redis key)))

(defn sadd [redis key value]
  (.sadd redis key value))

(defn srem [redis key value]
  (.srem redis key value))

;; we could extend this to take multiple keys
(defn del [redis key]
  (let [args (make-array java.lang.String 1)]
    (aset args 0 key)
    (.del redis args)))
