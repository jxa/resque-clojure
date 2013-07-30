(ns resque-clojure.test.integration
  (:use [clojure.test])
  (:require [resque-clojure.core :as resque]
            [resque-clojure.redis :as redis]
            [resque-clojure.test.helper :as helper]))

(use-fixtures :once helper/redis-test-instance)

(def our-list (atom []))
(defn add-to-list [& args]
  (swap! our-list into args))

(defn divide-by-zero [n]
  (/ n 0))

(deftest test-single-queue-integration
  (reset! our-list [])
  (resque/enqueue "test-queue" "resque-clojure.test.integration/add-to-list" "one" 2 3)
  (resque/start ["test-queue"])
  (Thread/sleep 50)
  (resque/stop)
  (is (= ["one" 2 3] @our-list)))

(deftest test-multiple-queue-integration
  (reset! our-list [])
  (resque/enqueue "test-queue" "resque-clojure.test.integration/add-to-list" "one" 2 3)
  (resque/enqueue "test-queu2" "resque-clojure.test.integration/add-to-list" "four")
  (resque/start ["test-queue" "test-queu2"])
  (Thread/sleep 50)
  (resque/stop)
  (is (or (= ["one" 2 3 "four"] @our-list)
          (= ["four" "one" 2 3] @our-list))))

(def custom-errors (atom []))

(deftest test-custom-error-handling
  (let [our-errors-handled (atom [])]
    (resque/configure {:error-handler (fn [error]
                                        (swap! our-errors-handled conj error))})
    (redis/del "resque:failed")
    (resque/enqueue "test-queue" "resque-clojure.test.integration/divide-by-zero" 123)
    (resque/start ["test-queue"])
    (Thread/sleep 50)
    (resque/stop)
    (testing "standard error handling"
      (is (= 1 (redis/llen "resque:failed")))
      (is (re-find #"java.lang.ArithmeticException" (redis/lpop "resque:failed"))))
    (testing "our custom error handling function"
      (is (= 1 (count @our-errors-handled)))
      (is (= "java.lang.ArithmeticException"
             (:exception (first @our-errors-handled)))))))
