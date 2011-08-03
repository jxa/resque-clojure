(ns resque-clojure.redis
  (:import [redis.clients.jedis Jedis JedisPool]))

(def ^{:private true} *pool* (ref nil))

(defn init [config]
  (dosync (ref-set *pool* (JedisPool. (:host config) (:port config)))))

(defn finalize []
  (.destroy @*pool*))

(defn- get-connection []
  (let [connection (.getResource @*pool*)]
    (.select connection 0)
    connection))

(def redis nil)

(defmacro with-connection [& body]
  `(binding [redis (get-connection)]
     (let [result# ~@body]
       (.returnResource @*pool* redis)
       result#)))

(defn rpush [key value]
  (with-connection
    (.rpush redis key value)))

(defn lpop [key]
  (with-connection
    (.lpop redis key)))

(defn llen [key]
  (with-connection
    (.llen redis key)))

(defn lindex [key index]
  (with-connection
    (.lindex redis key index)))

(defn lrange [key i-start i-end]
  (with-connection
    (.lrange redis key i-start i-end)))

(defn smembers [key]
  (with-connection
    (.smembers redis key)))

(defn sadd [key value]
  (with-connection
    (.sadd redis key value)))

(defn srem [key value]
  (with-connection
    (.srem redis key value)))

(defn del [key]
  (with-connection
    (.del redis key)))