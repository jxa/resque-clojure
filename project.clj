(defproject resque-clojure "0.2.1"
  :description "Redis based library for asynchronous processing"
  :url "https://github.com/jxa/resque-clojure"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/data.json "0.1.2"]
                 [redis.clients/jedis "1.5.2"]]

  :repositories {"nfr-releases" "s3p://newfound-mvn-repo/releases/"
                 "nfr-snapshots" "s3p://newfound-mvn-repo/snapshots/"})
