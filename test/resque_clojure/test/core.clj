(ns resque-clojure.test.core
  (:use [resque-clojure.core])
  (:use [clojure.test])
  (:use [resque-clojure.redis]))

(def test-key "resque-clojure-test")

(use-fixtures :each
              (fn [do-tests]
                nil
                (do-tests)
                (with-connection c {}
                  (del c test-key))))

(deftest test-namespace-key
  (is (= "resque:key" (namespace-key "key"))))

(deftest test-full-queue-name 
  (is (= "resque:queue:test" (full-queue-name "test"))))

(deftest test-enqueue-dequeue
  (with-connection c {}
    (del c (full-queue-name test-key))
    (is (= {:empty test-key} (dequeue c test-key)))
    (enqueue c test-key "data")
    (is (= {:received {:class "data" :args nil}} (dequeue c test-key)))))