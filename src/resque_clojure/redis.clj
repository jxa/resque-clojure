(ns resque-clojure.redis
  (:refer-clojure :exclude [set get keys])
  (:import [redis.clients.jedis JedisPool]
           [redis.clients.jedis.exceptions JedisException]
           [org.apache.commons.pool.impl GenericObjectPool$Config]))

(def config (atom {:host "localhost" :port 6379 :timeout 2000 :password nil}))
(def pool (ref nil))
(def ^:dynamic redis)

(declare release-pool)

(defn configure [c]
  (swap! config merge c))

(defn init-pool []
  (dosync
   (release-pool)
   (println "init-pool being called")
   (let [{:keys [host port timeout password]} @config
         port (if (string? port) (Integer/parseInt port) port)
         timeout (if (string? timeout) (Integer/parseInt timeout) timeout)
         generic-object-pool-config (GenericObjectPool$Config.)]
     (if (nil? timeout)
       (ref-set pool (JedisPool. host port))
       (if (nil? password)
         (ref-set pool (JedisPool. generic-object-pool-config host port timeout))
         (ref-set pool (JedisPool. generic-object-pool-config host port timeout password)))))))

(defn- get-connection []
  (.getResource @pool))

(defn release-pool []
  (if (not (nil? @pool))
    (.destroy @pool)))

(defmacro with-connection [& body]
  `(binding [redis (get-connection)]
     (let [result# ~@body]
       (.returnResource @pool redis)
       result#)))

(defmacro defcommand [cmd args & body]
  `(defn ~cmd ~args
     (let [fun# (fn [] (with-connection ~@body))]
       (try (fun#)
            (catch Exception e#
              (init-pool) (fun#))))))

(defcommand set [key value]
  (.set redis key value))

(defcommand get [key]
  (.get redis key))

(defcommand rpush [key value]
  (.rpush redis key value))

(defcommand lpop [key]
  (.lpop redis key))

(defcommand llen [key]
  (.llen redis key))

(defcommand lindex [key index]
  (.lindex redis key (long index)))

(defcommand lrange [key start end]
  (seq (.lrange redis key (long start) (long end))))

(defcommand smembers [key]
  (seq (.smembers redis key)))

(defcommand sadd [key value]
  (.sadd redis key value))

(defcommand srem [key value]
  (.srem redis key value))

(defcommand keys [pattern]
  (seq (.keys redis pattern)))

;; we could extend this to take multiple keys
(defcommand del [key]
  (let [args (make-array java.lang.String 1)]
    (aset args 0 key)
    (.del redis args)))

(defcommand flushdb []
  (.flushDB redis))

(defcommand auth [password]
  (.auth redis password))
