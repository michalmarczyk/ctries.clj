(ns ctries.clj-test
  (:use clojure.test)
  (:require [ctries.clj :as ct]
            [collection-check :refer [assert-map-like assert-equivalent-maps]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]])
  (:import (java.util.concurrent
             ConcurrentMap ConcurrentHashMap Executors TimeUnit)))

(deftest collection-check
  (is (assert-map-like (persistent! (ct/concurrent-map)) gen/int gen/int)))

(defn put! [^ConcurrentMap m k v]
  (.put m k v))

(defn put-all! [^ConcurrentMap m m']
  (.putAll m m'))

(defn put-if-absent! [^ConcurrentMap m k v]
  (.putIfAbsent m k v))

(defn remove!
  ([^ConcurrentMap m k]
     (.remove m k))
  ([^ConcurrentMap m k v]
     (.remove m k v)))

(defn replace!
  ([^ConcurrentMap m k v]
     (.replace m k v))
  ([^ConcurrentMap m k o n]
     (.replace m k o n)))

(defn generate-action
  [[op kgen vgen vcnt]]
  (apply gen/tuple (gen/return op) kgen (repeat vcnt vgen)))

(defn generate-actions [kgen vgen]
  (gen/list
    (gen/fmap seq
      (gen/one-of
        [(gen/tuple (gen/return 'ctries.clj-test/put!) kgen vgen)
         (gen/tuple (gen/return 'ctries.clj-test/put-if-absent!) kgen vgen)
         (gen/tuple (gen/return 'ctries.clj-test/remove!) kgen)
         (gen/tuple (gen/return 'ctries.clj-test/remove!) kgen vgen)
         (gen/tuple (gen/return 'ctries.clj-test/replace!) kgen vgen)
         (gen/tuple (gen/return 'ctries.clj-test/replace!) kgen vgen vgen)]))))

(defn compile-actions [actions]
  (let [seed (gensym "seed__")]
    (eval `(fn [~seed]
             ~(mapv (fn [action]
                      `(~(first action) ~seed ~@(next action)))
                actions)))))

(defspec check-concurrent-map-equivalence 1000
  (prop/for-all [actions (generate-actions gen/int gen/int)]
    (let [m1 (ConcurrentHashMap.)
          m2 (ct/concurrent-map)
          f  (compile-actions actions)
          r1 (f m1)
          r2 (f m2)]
      (and (.equals m1 m2)
           (.equals m2 m1)
           (== (.hashCode m1) (.hashCode m2))
           (== (.size m1) (.size m2))
           (= (into {} m1) (into {} m2))
           (= r1 r2)
           (= r2 r1)))))

(defspec check-map-hasheq 1000
  (prop/for-all [ks (gen/list gen/int)
                 vs (gen/list gen/int)]
    (let [m1 (zipmap ks vs)
          m2 (apply ct/concurrent-map (interleave ks vs))
          m3 @m2
          m4 (persistent! m2)]
      (and (== (.hashCode m1) (.hashCode m2) (.hashCode m3) (.hashCode m4))
           (== (.hasheq m1) (.hasheq m2) (.hasheq m3) (.hasheq m4))
           (== (.hasheq m2) (hash m2))
           (== (.hasheq m3) (hash m3))
           (== (.hasheq m4) (hash m4))))))

(deftest concurrent-puts
  (let [e (Executors/newFixedThreadPool 8)
        m (ct/concurrent-map)]
    (dotimes [_ 10000]
      (.execute e #(.put m (Object.) (Object.))))
    (.shutdown e)
    (is (.awaitTermination e 1 TimeUnit/MINUTES))
    (is (== 10000 (count m)))))

(deftest concurrent-fire-and-forgets
  (let [e (Executors/newFixedThreadPool 8)
        m (ct/concurrent-map)]
    (dotimes [_ 10000]
      (.execute e #(let [k (Object.)] (.insertHash m k (Object.) (hash k)))))
    (.shutdown e)
    (is (.awaitTermination e 1 TimeUnit/MINUTES))
    (is (== 10000 (count m)))))

(deftest concurrent-removes
  (let [e (Executors/newFixedThreadPool 8)
        m (ct/concurrent-map)
        a (atom [])]
    (dotimes [_ 10000]
      (let [k (Object.)]
        (.put m k k)
        (swap! a conj k)))
    (reduce (fn [_ k] (.execute e #(.remove m k))) nil @a)
    (.shutdown e)
    (is (.awaitTermination e 1 TimeUnit/MINUTES))
    (is (zero? (count m)))))

(deftest concurrent-put-if-absent
  (let [e (Executors/newFixedThreadPool 8)
        m (ct/concurrent-map)]
    (dotimes [i 10000]
      (.execute e #(.putIfAbsent m (mod i 100) i)))
    (.shutdown e)
    (is (.awaitTermination e 1 TimeUnit/MINUTES))
    (is (== 100 (count m)))))

(deftest concurrent-binary-replace
  (let [e (Executors/newFixedThreadPool 8)
        m (ct/concurrent-map)]
    (dotimes [i 10000]
      (.put m i (Object.)))
    (doseq [i (interleave (range 10000) (range 10000 20000))]
      (.execute e #(.replace m i i)))
    (.shutdown e)
    (is (.awaitTermination e 1 TimeUnit/MINUTES))
    (is (== 10000 (count m)))
    (is (every? #(= (key %) (val %)) m))))

(deftest concurrent-ternary-replace
  (let [e (Executors/newFixedThreadPool 8)
        m (ct/concurrent-map)]
    (dotimes [i 10000]
      (.put m i i))
    (doseq [i (interleave (range 10000) (range 10000 20000))]
      (.execute e
        (if (odd? i)
          #(.replace m i i (dec i))
          #(.replace m i i (inc i)))))
    (.shutdown e)
    (is (.awaitTermination e 1 TimeUnit/MINUTES))
    (is (== 10000 (count m)))
    (is (every? #(let [i (key %)]
                   (if (odd? i)
                     (= (dec i) (val %))
                     (= (inc i) (val %))))
          m))))
