(ns ctries.clj-test
  (:use clojure.test)
  (:require [ctries.clj :as ct]
            [collection-check :refer [assert-map-like]]))

(def igen clojure.test.check.generators/int)

(deftest collection-check
  (is (assert-map-like (persistent! (ct/concurrent-map)) igen igen)))
