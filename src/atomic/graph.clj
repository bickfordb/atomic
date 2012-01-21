(ns atomic.graph
  (:use atomic
        atomic.util))

(defn make-query
  "make a query given an entity and some where-conditions"
  [engine entity where-parts]
  (let [table (lookup-table (:schema engine) entity)
        key-paths (for [c (:columns table)] [(:key c)])
        cols (reduce concat (for [column (:columns table)]
                        [(keyword (format "%s.%s" (name entity) (:name column))) ; column expr
                         [(:key column)]
                         ])) ; key path
        ; for some reason -> doesn't work here:
        from-q (from select entity)
        cols-q (apply columns from-q cols)
        where-q (assoc cols-q :where where-parts)]
    where-q))

(defn get-table 
  [schema src-table [h & more]]
  (if (not h)
    src-table
    (get-table schema (:table (get @(:foreign-keys schema) [src-table h])) more)))


(defn join-to 
  "Join a set of dotted relation-paths to a result set.
  
  In other words, fill-in a list of related row objects.

  For instance if you have a schema like:

    driver (id, name)
    car (id, driver_id, garage_id, name)
    garage (id, house_id)
    house (id)

  (join-to engine :review [:user :user.emails] [{:id 1 :user_id 3}]) =>
    [{:id 1 :user_id 3 :user { :id 3 :emails [{:id 1 :address \"foo@bar.com\"}]}}]
  "
  [engine table relation-paths result-set]
  (let [relation-paths0 (sort (parse-relation-paths relation-paths))
        schema (:schema engine)
        result-set0 (atom result-set)]
    (doseq [relation-path relation-paths0]
      (let [parent-table-key (get-table schema table (all-but-last relation-path))
            relation-key (last relation-path)
            foreign-key (get @(:foreign-keys schema) [parent-table-key relation-key])
            remote-table (:table foreign-key)
            foreign-key-type (:type foreign-key)
            remote-primary-key-col (get-primary-key schema remote-table)
            remote-primary-key (:key remote-primary-key-col)
            remote-primary-key-name (:name remote-primary-key-col)
            column (:column foreign-key)]
        (when (not foreign-key)
          (throw (Exception. (format "expecting a foreign key for %s" relation-key))))
        (when (not remote-table)
          (throw (Exception. (format "expecting a remote table for %s" relation-key))))
        (cond 
          (= foreign-key-type :has-one)
          ; has-one case
          ; get all of the x.y.column values
          (let [col-vals (apply hash-set (map column (get-in2 (all-but-last relation-path) @result-set0)))
                ; Select the remote rows
                rows (map relation-key (:rows (-> select
                       (from remote-table {:as relation-key})
                       (where (in (join-keywords relation-key
                                                 remote-primary-key)
                                  col-vals))
                       (execute engine))))
                ; Get (remote primary key) -> (remote row)
                pk-to-row (zipmap (map remote-primary-key rows) rows)
                ]
            ; Store the result
            (reset! result-set0 (map-in 
                                (fn [item] 
                                  (assoc item relation-key (get pk-to-row (get item column))))
                                (all-but-last relation-path)
                                @result-set0)))
          ; handle a has-many
          (= foreign-key-type :has-many) 
          (let [local-primary-key :id ; fixme
                items (get-in2 (all-but-last relation-path) @result-set0)
                local-keys (map local-primary-key items)
                rows (map relation-key (:rows (-> select 
                       (from remote-table {:as relation-key})
                       (where (in (join-keywords relation-key column) local-keys))
                       (execute engine))))
                local-primary-key-to-rows (get-key-to-seq rows column)
                get-val (fn [item] (get local-primary-key-to-rows (get item local-primary-key [])))
                replace-item (fn [item] (assoc item relation-key (get-val item)))
                new-result-set (map-in replace-item (all-but-last relation-path) @result-set0)]
            (reset!  result-set0 new-result-set)
          ) 
          :else (throw (Exception. (format "unexpected foreign key type: %s" foreign-key-type))))))
    @result-set0))

(defmacro one 
  "Get one item

  Examples

    : Load a user with id 5
    (one engine :user (> :id 5)) 

    ; Load a user with id 3, and review and review.business joined
    (one engine :user (= :id 3) :review :review.business) 
    
    ; Load a user with id 5 and email joined
    (one engine :user (= :id 5) :email) 
    
    ; Load a user with id 5 and email joined
    (one engine :user (= :id 5) :emails)) 
  "
  [engine entity & options]
  `(let [where# (list ~@(parse-dsl (filter not-keyword? options)))
         join-key-paths# (filter keyword? (list ~@options)) 
         query# (make-query ~engine ~entity where#)
         result# (execute query# ~engine)
         row# (first (:rows result#))
         joined-rows# (join-to ~engine ~entity join-key-paths# [row#])]
       (first joined-rows#)))

(defmacro many 
  "Get many items"
  [engine entity & options]
  `(let [where# (list ~@(parse-dsl (filter not-keyword? options)))
         join-key-paths# (filter keyword? (list ~@options)) 
         query# (make-query ~engine ~entity where#)
         result# (execute query# ~engine)
         joined-rows# (join-to ~engine ~entity join-key-paths# (:rows result#))]
       joined-rows#))
