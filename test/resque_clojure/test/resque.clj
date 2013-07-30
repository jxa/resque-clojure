(ns resque-clojure.test.resque
  (:use [clojure.test]
        [resque-clojure.resque])
  (:require [resque-clojure.test.helper :as helper]
            [resque-clojure.redis :as redis]))

(def test-key "resque-clojure-test")

(use-fixtures :once helper/redis-test-instance)
(use-fixtures :each helper/cleanup-redis-keys)

(deftest test-namespace-key
  (is (= "resque:key" (-namespace-key "key"))))

(deftest test-full-queue-name
  (is (= "resque:queue:test" (-full-queue-name "test"))))

(deftest test-enqueue-dequeue
  (is (nil? (dequeue [test-key])))
  (enqueue test-key "data")
  (is (some #{test-key} (redis/smembers "resque:queues")))
  (is (= {:queue test-key :data "{\"class\":\"data\",\"args\":null}" :func "data" :args nil} (dequeue [test-key]))))

(deftest test-multiple-queues-pop-only-one
  (let [test-key-2 "test-key-2"]
    (enqueue test-key "data")
    (enqueue test-key-2 "data2")
    (is (not (nil? (dequeue [test-key test-key-2]))))
    (is (or (and (= 1 (redis/llen (-full-queue-name test-key))) (= 0 (redis/llen (-full-queue-name test-key-2))))
            (and (= 0 (redis/llen (-full-queue-name test-key))) (= 1 (redis/llen (-full-queue-name test-key-2))))))))

(deftest test-format-error
  (let [e (try (/ 1 0) (catch Exception e e))
        job {:class "clojure.core/slurp" :args ["/etc/passwd"]}
        result {:exception e :job job :queue "test-queue"}
        formatted (-format-error result)]
    (is (re-find #"^\d{4}/\d\d/\d\d \d{1,2}:\d\d:\d\d" (:failed_at formatted)))
    (is (= (job (:payload formatted))))
    (is (= "java.lang.ArithmeticException" (:exception formatted)))
    (is (= "Divide by zero" (:error formatted)))
    (is (re-find #"^clojure.lang.Numbers.divide" (first (:backtrace formatted))))
    (is (= "test-queue" (:queue formatted)))))

(deftest namespace-affects-keys
  (configure {:namespace "staging"})
  (is (= "staging" (:namespace @config)))
  (is (= "staging:key" (-namespace-key "key")))
  (is (= "staging:queue:test" (-full-queue-name "test")))
  (configure {:namespace "resque"}))
