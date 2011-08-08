(ns resque-clojure.test.redis
  (:use [resque-clojure.redis]
        [clojure.test]))

(def test-key "resque-clojure-test")
(def connection (connect "localhost" 6379))

(use-fixtures :once
              (fn [do-tests]
                nil
                (do-tests)
                (disconnect connection)))
(use-fixtures :each
              (fn [do-tests]
                nil
                (do-tests)
                (del connection test-key)))

(deftest it-can-connect
  (is (not (nil? connection))))

(deftest list-ops-rpush-and-lpop
  (rpush connection test-key "value")
  (is (= "value" (lpop connection test-key)))
  (is (= nil (lpop connection test-key))))
  
(deftest list-ops-llen
  (is (= 0 (llen connection test-key)))
  (rpush connection test-key "value")
  (is (= 1 (llen connection test-key))))

(deftest list-ops-lindex
  (is (nil? (lindex connection test-key 0)))
  (rpush connection test-key "value0")
  (rpush connection test-key "value1")
  (is (= "value0" (lindex connection test-key 0)))
  (is (= "value1" (lindex connection test-key 1))))

(deftest list-ops-lrange
  (is (empty? (lrange connection test-key 0 3)))
  (rpush connection test-key "value0")
  (rpush connection test-key "value1")
  (is (= '("value0") (lrange connection test-key 0 0)))
  (is (= '("value0" "value1") (lrange connection test-key 0 1))))

(deftest set-ops-sadd
  (is (empty? (smembers connection test-key)))
  (sadd connection test-key "element0")
  (is ( not (empty? (smembers connection test-key)))))

(deftest set-ops-rem
  (sadd connection test-key "element0")
  (is ( not (empty? (smembers connection test-key))))
  (srem connection test-key "element0")
  (is (empty? (smembers connection test-key))))

(deftest set-ops-smembers
  (sadd connection test-key "element0")
  (sadd connection test-key "element1")
  (is (some #{"element0"} (smembers connection test-key)))
  (is (some #{"element1"} (smembers connection test-key)))
  (is (nil? (some #{"element2"} (smembers connection test-key)))))
