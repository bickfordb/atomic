(ns atomic.migrations
  (:require [clojure.string :as string])
  (:use atomic
        atomic.util))

(defn drop-table
  "Drop a table
  
  Example
    (drop-table @db :users)

  Arguments
  db -- a database
  table-kw -- a keyword
  "
  [db table-kw]
  (execute-sql db (format "DROP TABLE IF EXISTS %s" (name table-kw)) []))

(defn trash [x] (filter identity x))

(defn create-index 
  [db table-kw cols & opts]
  (let [opts0 (apply hash-map opts)
        ; by default make an index name like tblname_col1_col2_..._idx
        default-index-name (->>
                             [(stringify table-kw) 
                              (map stringify cols)
                              "idx"]
                             (flatten)
                             (string/join "_"))
        index-name (get opts0 :name default-index-name)
        query (->> 
                ["CREATE INDEX "
                 index-name  
                 "ON"
                 (stringify table-kw)
                 "(" 
                 (interpose "," (map stringify cols))
                 ")" ]
                (filter identity)
                (flatten)
                (string/join " "))]
    (execute-sql db query)))

(defn create-table 
  [db table-kw cols & table-opts]
  (let [table-opts0 (apply hash-map table-opts) 
        get-col-part (fn [[col-kw opts]] 
                       [(name col-kw)
                        (stringify (get opts :type "text"))
                        (when (get opts :default)
                          (format "DEFAULT %s" (stringify (:default opts))))
                        (when (:not-null? opts) "NOT NULL")
                        (when (:primary? opts) "PRIMARY KEY")
                        (when (:autoincrement? opts) "AUTOINCREMENT")])
        create-opts [(if (:if-not-exists? table-opts0) "IF NOT EXISTS" "")]
        post-opts [(if (:engine table-opts0) (format "ENGINE=%s" (:engine table-opts0)))
                   (if (:charset table-opts0) (format "CHARSET=%s" (:charset table-opts0)))]
        query (->> ["CREATE TABLE"
                    create-opts 
                    (stringify table-kw)
                    "("
                    (interpose "," (map get-col-part cols))
                    ")"
                    post-opts] 
                (flatten)
                (filter identity)
                (string/join " "))]
        (execute-sql db query [])))

(defn create-migration-table
  [db]
  (create-table db :migration {:id {:type "integer" :primary? true}} :if-not-exists? true))

(deftable :migration :id)

(defn get-migration-ids
  [db]
  (apply hash-set (map :id (many db :migration))))

(defn create-migration 
  ([id up down] {:id id :up up :down down})
  ([id up] (create-migration id up nil)))

(defn run-migration 
  [db {:keys [up id]}]
  (when up
    (up db))
  (tx db 
    (execute-sql db "insert into migration (id) values (?)" [id])))

(defn run-all-migrations 
  [db & migrations]
  (create-migration-table db)
  (let [existing-ids (get-migration-ids db)]
    (doseq [[migration-id m] (sort (for [m migrations] [(:id m) m]))]
      (when (not (contains? existing-ids migration-id))
        (run-migration db m)))))

