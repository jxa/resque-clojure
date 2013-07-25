(ns resque-clojure.test.redis
  (:require [resque-clojure.redis :as redis]
            [resque-clojure.test.helper :as helper])
  (:use [clojure.test]
        [resque-clojure.util :only [includes?]]))

(def test-key "resque-clojure-test")
(use-fixtures :once (helper/redis-test-instance-with-config {}))
(use-fixtures :each helper/cleanup-redis-keys)

(deftest set-and-get
  (redis/set test-key "setvalue")
  (is (= "setvalue" (redis/get test-key))))

(deftest list-ops-rpush-and-lpop
  (redis/rpush test-key "value")
  (is (= "value" (redis/lpop test-key)))
  (is (= nil (redis/lpop test-key))))

(deftest list-ops-llen
  (is (= 0 (redis/llen test-key)))
  (redis/rpush test-key "value")
  (is (= 1 (redis/llen test-key))))

(deftest list-ops-lindex
  (is (nil? (redis/lindex test-key 0)))
  (redis/rpush test-key "value0")
  (redis/rpush test-key "value1")
  (is (= "value0" (redis/lindex test-key 0)))
  (is (= "value1" (redis/lindex test-key 1))))

(deftest list-ops-lrange
  (is (empty? (redis/lrange test-key 0 3)))
  (redis/rpush test-key "value0")
  (redis/rpush test-key "value1")
  (is (= '("value0") (redis/lrange test-key 0 0)))
  (is (= '("value0" "value1") (redis/lrange test-key 0 1))))

(deftest set-ops-sadd
  (is (empty? (redis/smembers test-key)))
  (redis/sadd test-key "element0")
  (is (not (empty? (redis/smembers test-key)))))

(deftest set-ops-rem
  (redis/sadd test-key "element0")
  (is (not (empty? (redis/smembers test-key))))
  (redis/srem test-key "element0")
  (is (empty? (redis/smembers test-key))))

(deftest set-ops-smembers
  (redis/sadd test-key "element0")
  (redis/sadd test-key "element1")
  (is (includes? (redis/smembers test-key) "element0"))
  (is (includes? (redis/smembers test-key) "element1"))
  (is (not (includes? (redis/smembers test-key) "element2"))))

(deftest test-keys
  (redis/set test-key "asdf")
  (is (includes? (redis/keys test-key) test-key)))

(deftest test-flush
  (redis/set test-key "asdf")
  (redis/flushdb)
  (is (empty? (redis/keys "*"))))

;;(use-fixtures :once (helper/redis-test-instance-with-config {'requirepass "sekrit"}))


;; (deftest connecting-with-password
;;   (redis/configure {:password "sekrit"})
;;   (redis/init-pool)
;;   (redis/flushdb)
;;   (is (empty? (redis/keys "*")) "running any redis command here without blowing up validates password connection"))
