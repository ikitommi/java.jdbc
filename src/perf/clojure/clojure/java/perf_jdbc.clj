;;  This namespace contains performance tests
;;
;; start repl with `lein perf repl`
;; perf measured with the following setup:
;;
;; Model Name:            MacBook Pro
;; Model Identifier:      MacBookPro11,3
;; Processor Name:        Intel Core i7
;; Processor Speed:       2,5 GHz
;; Number of Processors:  1
;; Total Number of Cores: 4
;; L2 Cache (per Core):   256 KB
;; L3 Cache:              6 MB
;; Memory:                16 GB
;;
;; (calibrate) execution time mean: 840ms
;;

(ns clojure.java.perf-jdbc
  (:require [criterium.core :as cc]
            [clojure.java.jdbc :as sql])
  (:import (java.sql Connection PreparedStatement ResultSet Statement ResultSetMetaData)))

(defn calibrate []
  ;; 840ms
  (cc/quick-bench (reduce + (take 10e6 (range)))))

(def db
  {:connection (sql/get-connection "jdbc:h2:mem:test_mem")})

(defn create-table! [db]
  (sql/db-do-commands
    db (sql/create-table-ddl
         :fruit
         [[:id :int "DEFAULT 0"]
          [:name "VARCHAR(32)" "PRIMARY KEY"]
          [:appearance "VARCHAR(32)"]
          [:cost :int]
          [:grade :real]]
         {:table-spec ""})))

(defn- drop-table! [db]
  (doseq [table [:fruit :fruit2 :veggies :veggies2]]
    (try
      (sql/db-do-commands db (sql/drop-table-ddl table))
      (catch java.sql.SQLException _))))

(defn add-stuff! [db]
  (sql/insert-multi! db
                     :fruit
                     nil
                     [[1 "Apple" "red" 59 87]
                      [2 "Banana" "yellow" 29 92.2]
                      [3 "Peach" "fuzzy" 139 90.0]
                      [4 "Orange" "juicy" 89 88.6]])
  ;(sql/update! db :fruit {:appearance "bruised" :cost 14} ["name=?" "Banana"])
  )

(def dummy-con
  (reify
    Connection
    (createStatement [_]
      (reify
        Statement
        (addBatch [_ _])))
    (prepareStatement [_ _]
      (reify
        PreparedStatement
        (setObject [_ _ _])
        (setString [_ _ _])
        (close [_])
        (executeQuery [_]
          (reify
            ResultSet
            (getMetaData [_]
              (reify
                ResultSetMetaData
                (getColumnCount [_] 1)
                (getColumnLabel [_ _] "name")))
            (next [_] true)
            (close [_])
            (^Object getObject [_ ^int s]
              "Apple")
            (^String getString [_ ^String s]
              "Apple")))))))

(defn select [db]
  (sql/query db ["SELECT * FROM fruit WHERE appearance = ?" "red"]
             {:row-fn :name :result-set-fn first}))

(defn select* [^Connection con]
  (let [ps (doto (.prepareStatement con "SELECT * FROM fruit WHERE appearance = ?")
             (.setString 1 "red"))
        rs (.executeQuery ps)
        _ (.next rs)
        value (.getString rs "name")]
    (.close ps)
    value))

(defn test-dummy []
  (do
    (let [db {:connection dummy-con}]
      (assert (= "Apple" (select db)))
      ;(time (dotimes [_ 100000] (select db)))

      ; 3.029268 ms (3030 ns)
      (cc/quick-bench (dotimes [_ 1000] (select db))))

    (let [con dummy-con]
      (assert (= "Apple" (select* con)))
      ;(time (dotimes [_ 100000] (select* con)))

      ;  716.661522 ns (0.7ns) -> 4300x faster
      (cc/quick-bench (dotimes [_ 1000] (select* con))))))

(defn test-h2 []
  (do
    (drop-table! db)
    (create-table! db)
    (add-stuff! db)

    (let [db db]
      (assert (= "Apple" (select db)))
      ;(time (dotimes [_ 100000] (select db)))

      ; 6700 ns
      (cc/quick-bench (select db)))

    (let [con (:connection db)]
      (assert (= "Apple" (select* con)))
      ;(time (dotimes [_ 100000] (select* con)))

      ; 1090ns -> 6x faster
      (cc/quick-bench (select* con)))))

(comment
  (calibrate)
  (test-dummy)
  (test-h2))
