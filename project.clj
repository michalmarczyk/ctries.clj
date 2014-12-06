(defproject ctries.clj "0.0.1"
  :description "Clojure implementation of Ctries"
  :url "https://github.com/michalmarczyk/ctries.clj"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[org.clojure/clojure "1.7.0-alpha4"]]
  :jvm-opts ^:replace []
  :profiles {:dev {:dependencies [[collection-check "0.1.4"]]}})
