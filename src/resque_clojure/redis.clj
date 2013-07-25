(ns resque-clojure.redis
  (:refer-clojure :exclude [set get keys])
  (:import [redis.clients.jedis JedisPool]
           [redis.clients.jedis.exceptions JedisException]
           [org.apache.commons.pool.impl GenericObjectPool$Config]))

(def config (atom
             {:uri nil
              :host "localhost"
              :port 6379
              :timeout 2000
              :password nil
              :database 0 }))

(def pool (ref nil))
(def ^:dynamic redis)

(declare release-pool)

(defn configure [c]
  (swap! config merge c))

(defn- make-int [i]
  (if (string? i) (Integer/parseInt i) i))

(defn init-pool []
  (dosync
   (release-pool)
   (let [{:keys [host port timeout password uri database]} @config]
     (ref-set pool
              (if uri
                (let [uri (java.net.URI. uri)
                      host (.getHost uri)
                      port (.getPort uri)
                      user (.getUserInfo uri)
                      password (when user (-> user (.split ":") (aget 1)))]
                  (JedisPool. (GenericObjectPool$Config.) host port timeout password database))
                (JedisPool. (GenericObjectPool$Config.) host (make-int port)
                            (make-int timeout) password database))))))

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

(defcommand rpush [key & values]
  (.rpush redis key (into-array String values)))

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

(defcommand sadd [key & values]
  (.sadd redis key (into-array String values)))

(defcommand srem [key & values]
  (.srem redis key (into-array String values)))

(defcommand keys [pattern]
  (seq (.keys redis pattern)))

(defcommand del [& keys]
  (.del redis (into-array String keys)))

(defcommand flushdb []
  (.flushDB redis))
