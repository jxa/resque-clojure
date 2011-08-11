(ns resque-clojure.test.redis
  (:use [resque-clojure.redis]
        [clojure.test]))

(def test-key "resque-clojure-test")

(use-fixtures :each
              (fn [do-tests]
                nil
                (do-tests)
                (del test-key)))

(deftest list-ops-rpush-and-lpop
  (rpush test-key "value")
  (is (= "value" (lpop test-key)))
  (is (= nil (lpop test-key))))
  
(deftest list-ops-llen
  (is (= 0 (llen test-key)))
  (rpush test-key "value")
  (is (= 1 (llen test-key))))

(deftest list-ops-lindex
  (is (nil? (lindex test-key 0)))
  (rpush test-key "value0")
  (rpush test-key "value1")
  (is (= "value0" (lindex test-key 0)))
  (is (= "value1" (lindex test-key 1))))

(deftest list-ops-lrange
  (is (empty? (lrange test-key 0 3)))
  (rpush test-key "value0")
  (rpush test-key "value1")
  (is (= '("value0") (lrange test-key 0 0)))
  (is (= '("value0" "value1") (lrange test-key 0 1))))

(deftest set-ops-sadd
  (is (empty? (smembers test-key)))
  (sadd test-key "element0")
  (is ( not (empty? (smembers test-key)))))

(deftest set-ops-rem
  (sadd test-key "element0")
  (is ( not (empty? (smembers test-key))))
  (srem test-key "element0")
  (is (empty? (smembers test-key))))

(deftest set-ops-smembers
  (sadd test-key "element0")
  (sadd test-key "element1")
  (is (some #{"element0"} (smembers test-key)))
  (is (some #{"element1"} (smembers test-key)))
  (is (nil? (some #{"element2"} (smembers test-key)))))
