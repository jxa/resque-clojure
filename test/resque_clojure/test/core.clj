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
                  (del c test-key)
                  (del c "resque:queues"))))

(deftest test-namespace-key
  (is (= "resque:key" (namespace-key "key"))))

(deftest test-full-queue-name 
  (is (= "resque:queue:test" (full-queue-name "test"))))

(deftest test-enqueue-dequeue
  (with-connection c {}
    (del c (full-queue-name test-key))
    (is (= {:empty test-key} (dequeue c test-key)))
    (enqueue c test-key "data")
    (is (some #{test-key} (smembers c "resque:queues")))
    (is (= {:received {:class "data" :args nil}} (dequeue c test-key)))))

(deftest test-format-error
  (let [e (try (/ 1 0) (catch Exception e e))
        job {:class "clojure.core/slurp" :args ["/etc/passwd"]}
        result {:exception e :job job :queue "test-queue"}
        formatted (format-error result)]
    (is (re-find #"^\d{4}/\d\d/\d\d \d\d:\d\d:\d\d" (:failed_at formatted)))
    (is (= (job (:payload formatted))))
    (is (= "java.lang.ArithmeticException" (:exception formatted)))
    (is (= "Divide by zero" (:error formatted)))
    (is (re-find #"^clojure.lang.Numbers.divide" (first (:backtrace formatted))))
    (is (= "hostname:pid:queue" (:worker formatted)))
    (is (= "test-queue" (:queue formatted)))))