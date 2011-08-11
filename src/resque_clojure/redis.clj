(ns resque-clojure.redis
  (:import [redis.clients.jedis JedisPool]
           [redis.clients.jedis.exceptions JedisException]))

(def config (atom {:host "localhost" :port 6379}))
(def pool (ref nil))
(def redis)

(defn configure [c]
  (swap! config merge c))

(defn init-pool []
  (dosync (ref-set pool (JedisPool. (:host @config) (:port @config)))))

(defn- get-connection []
  (.getResource @pool))

(defn release-pool []
  (.destroy @pool))

(defmacro with-connection [& body]
  `(binding [redis (get-connection)]
     (let [result# ~@body]
       (.returnResource @pool redis)
       result#)))

(defmacro defcommand [cmd args & body]
  (let [inner-args (vec (map gensym args))]
    `(defn ~cmd ~args
       (let [fun# (fn ~inner-args (with-connection ~@body))]
         (try (fun# ~@args)
              (catch Exception e#
                (init-pool) (fun# ~@args)))))))

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

;; we could extend this to take multiple keys
(defcommand del [key]
  (let [args (make-array java.lang.String 1)]
    (aset args 0 key)
    (.del redis args)))
