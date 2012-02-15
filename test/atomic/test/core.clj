(ns atomic.test.core
  (:use atomic.util)
  (:use clojure.core)
  (:use clojure.test)
  (:use atomic.migrations)
  (:use atomic))

(deftable 
  :user
  :id
  :name)

(deftable 
  :review
  :id
  :user_id
  :comment
  (has-one :reviewer :user :user_id :reviews))

(deftable 
  :business
  (column :id :primary? true)
  (column :name :initial "Something")
  (column :updated_at :initial timestamp :default timestamp)
  (column :created_at :initial timestamp))

(defn memory-db
  []
  (create-db "org.sqlite.JDBC" "jdbc:sqlite::memory:"))

(deftest 
  execute-sql-test
  (let [db (memory-db)
        result (execute-sql db "SELECT 1" [])
        rows (:rows result)]
    (is (= rows [[1]]))))

(defn empty-db 
  []
  (let [db (memory-db)]
    (execute-sql db "create table user (id integer primary key, name text, created_at integer)")
    (execute-sql db "create table business (id integer primary key, name text, updated_at integer, created_at integer)")
    (execute-sql db "create table review (id integer primary key, user_id integer, comment text)")
    db))

(deftest 
  simple-test
  (let [db (empty-db)]
    (is (= []
           (:rows (execute-sql db "select id from user"))))
    (execute-sql db "insert into user (id, name) values (?, ?)" [1 "Brandon"])
    (execute-sql db "insert into user (id, name) values (?, ?)" [2 "Alice"])
    (execute-sql db "insert into user (id, name) values (?, ?)" [3 "Bob"])
    (execute-sql db "insert into user (id, name) values (?, ?)" [4 "Charlie"])
    (is (= [[1 "Brandon"]]
           (:rows (execute-sql db "select id, name from user where id = ?" [1]))))
    (execute-sql db "insert into review (id, user_id, comment) values (?, ?, ?)" [99 1 "liked it"])
    (execute-sql db "insert into review (id, user_id, comment) values (?, ?, ?)" [100 2 "see you tomorrow"])
    (execute-sql db "insert into review (id, user_id, comment) values (?, ?, ?)" [101 3 "cheesesteak"])
    (let [rows (-> 
            select
            (from :user {:as :u})
            (join :review {:as :r}
                  (on (= :r.user_id :u.id)))
            (where (= :u.id 2))
            (execute db)
            (:rows))]
      (is (= rows
            [{:u {:id 2
                  :name "Alice"}
              :r {:id 100
                  :user_id 2
                  :comment "see you tomorrow"}}])))

    (let [a-user (one db :user :reviews (= :id 1))]
      (is (= 
            (:comment (first (:reviews a-user)))
            "liked it")))
    (let [a-review (one db :review :reviewer (= :user_id 1))]
      (is (= 
            (:name (:reviewer a-review))
            "Brandon")))
    (let [u (one db :user (= :id 5) :reviews)] 
      (is (nil? u)))
    (let [u (one db :user (= :id 4) :reviews)] 
      (is (= (:id u) 4))
      (is (not (nil? (:reviews u))))
      (is (= (:reviews u) [])))
  ))

(deftest parse-relation-paths-test
         (is (= 
               (parse-dotted-keyword :a)
               [:a]))
         (is (= 
               (parse-dotted-keyword :a.b)
               [:a :b]))
         (is (= 
               (parse-dotted-keyword :a.b.c)
               [:a :b :c])))

(deftest parse-relation-paths-test
         (is (=
               (parse-relation-paths [:a])
               #{[:a]}))
         (is (=
               (parse-relation-paths [:a :b])
               #{[:a] [:b]}))
         (is (=
               (parse-relation-paths [:a.b])
               #{[:a] [:a :b]})))

(deftest insert-test
  (let [db (empty-db)]
    (-> (insert-into :user {:name "Brandon"})
      (execute db))
    (is (= (many db :user) [{:id 1 :name "Brandon"}]))
    )
)


(deftest update-test
  (let [db (empty-db)]
    (-> (insert-into :user {:name "Brandon"}) (execute db))
    (-> 
      (update :user {:name "Sam"})
      (where (= 1 :id))
      (execute db))
    (is (= (many db :user) [{:id 1 :name "Sam"}]))
    )
)

(deftest delete-test
  (let [db (empty-db)]
    (-> 
      (insert-into :user {:name "Brandon"}) 
      (execute db))
    (-> 
      (insert-into :user {:name "Sam"}) 
      (execute db))
    (-> 
      (delete :user) 
      (where (= :name "Sam"))
      (execute db))
    (is (= (many db :user) [{:name "Brandon" :id 1}]))
))

(deftest insert-default-test
   (let [db (empty-db)]
     (-> 
       (insert-into :business {:name "Something"})
       (execute db))
     (is 
       (= (:name (one db :business))
          "Something"))))

(deftest update-default-test
   (let [db (empty-db)]
     (-> 
       (insert-into :business {:name "hey0"})
       (execute db))
     (let [b0 (one db :business)]
       (-> (update :business {:name "hey1"})
           (execute db))
       (let [b1 (one db :business)]
         (is (= (:id b0) (:id b1)))
         (is (> (:updated_at b1) (:updated_at b0)))))))

(deftest insert-id-test
   (let [db (empty-db)]
     (is (= 1
            (create db :business {:name "Hello"})))))

(deftest tx-test
  "Test transactions"
  (let [db (empty-db)]
    (try (tx db
        (create db :business {:name "B"})
        (throw Exception))
      (catch Exception e))
    (is (= 0 (count (many db :business))))
    (tx db
        (create db :business {:name "B"}))
    (is (= 1 (count (many db :business))))))

(deftest tx-read-only-test
  "Test transactions"
         (let [db (empty-db)]
    (tx-read-only db
                  (create db :business {:name "B"}))
    (is (= 0 (count (many db :business))))))

(defn create-baz-table
  [db]
  (create-table db :baz {:id {:type "integer" :primary? true}
                         :name {:type "varchar(128)"}}))

(defn create-baz-index 
  [db] (create-index db :baz [:name]))

(deftest migrations-test
  (let [db (memory-db)]
    (run-all-migrations 
      db
      {:id 1 :up create-baz-table}
      {:id 2 :up create-baz-index}
      )))

