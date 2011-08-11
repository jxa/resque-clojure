(ns resque-clojure.test.core
  (:use [resque-clojure.core])
  (:use [clojure.test])
  (:use [resque-clojure.redis]))

(def test-key "resque-clojure-test")

(use-fixtures :each
              (fn [do-tests]
                nil
                (do-tests)
                (do
                  (del test-key)
                  (del "resque:queues"))))

(deftest test-namespace-key
  (is (= "resque:key" (namespace-key "key"))))

(deftest test-full-queue-name 
  (is (= "resque:queue:test" (full-queue-name "test"))))

(deftest test-enqueue-dequeue
  (del (full-queue-name test-key))
  (is (= {:empty test-key} (dequeue test-key)))
  (enqueue test-key "data")
  (is (some #{test-key} (smembers "resque:queues")))
  (is (= {:received {:class "data" :args nil}} (dequeue test-key))))

(deftest test-format-error
  (let [e (try (/ 1 0) (catch Exception e e))
        job {:class "clojure.core/slurp" :args ["/etc/passwd"]}
        result {:exception e :job job :queue "test-queue"}
        formatted (format-error result)]
    (is (re-find #"^\d{4}/\d\d/\d\d \d{1,2}:\d\d:\d\d" (:failed_at formatted)))
    (is (= (job (:payload formatted))))
    (is (= "java.lang.ArithmeticException" (:exception formatted)))
    (is (= "Divide by zero" (:error formatted)))
    (is (re-find #"^clojure.lang.Numbers.divide" (first (:backtrace formatted))))
    (is (= "hostname:pid:queue" (:worker formatted)))
    (is (= "test-queue" (:queue formatted)))))