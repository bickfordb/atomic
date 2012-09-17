(ns atomic
  (:require lg)
  (:use [clojure.string :only [join]])
  (:import
    (clojure.lang RT)
    (java.sql BatchUpdateException DriverManager SQLException Statement)))

; Dynamic global representing the current connection pool
(def ^:dynamic *pool* nil)

; Dynamic global representing the current database connection
(def ^:dynamic *conn* nil)

; Dynamic global representing the SQL isolation preference
(def ^:dynamic *isolation* nil)

(def autoload-driver?
  "Automatically load drivers?"
  (atom true))

(def drivers
  "Mapping of driver string to JDBC URL pattern

  When a pattern matches a pool URL the matching class will be loaded.
  (reset! autoload-driver? (atom false)) to turn this off.
  "
  (atom {"org.sqlite.JDBC" #".*sqlite.*"
         "org.Postgresql.Driver" #".*postgresql.*"
         "com.mysql.jdbc.Driver" #".*mysql.*"}))

(defn- split-kw-opts
  "Split a list of options into keyword options and other values

  Returns a tuple of [option-mapping positional-args]
  "
  [opts]
  (loop [opts opts
         kw-opts {}
         other-opts []]
    (let [[fst snd & t] opts
          [_ & t'] opts]
      (cond
        (empty? opts) [kw-opts other-opts]
        (keyword? fst) (recur t (assoc kw-opts fst snd) other-opts)
        :else (recur t' kw-opts (conj other-opts fst))))))

(defn- modify!
  [cell f & args]
  (let [ret-cell (atom nil)
        g (fn [st]
            (let [[st' ret] (apply f st args)]
              (reset! ret-cell ret)
              st'))]
    (swap! cell g)
    @ret-cell))

(defn create-connection
  [conn-params]
  (let [{:keys [url username password]} conn-params]
    (cond
      (and username password) (DriverManager/getConnection url username password)
      :else (DriverManager/getConnection url username password))))

(defn pool-st-acquire-connection
  [st]
  (let [{:keys [connections conn-params]} st]
    (if (empty? connections)
      ; FIXME: call .isValid on the connection
      [st (create-connection conn-params)]
      [(assoc st :connections (rest connections)) (first connections)])))

(defn pool-st-release-connection
  [st conn]
  (let [{:keys [connections min-water]} st]
     (if (> (count connections) min-water)
        (do
            (.close conn)
            st)
        (assoc st :connections (cons conn connections)))))

(defprotocol PPool
  (^boolean supports-generated-keys? [_])
  (^java.sql.Connection acquire-connection [_])
  (release-connection [_ ^java.sql.Connection connection]))

(defrecord Pool [st]
  PPool
  (supports-generated-keys? [pool] (get @st :generated-keys? true))
  (acquire-connection [pool] (modify! st pool-st-acquire-connection))
  (release-connection [pool conn] (swap! st pool-st-release-connection conn)))

(defn create-pool
  [url & opts]
  (let [opts' (apply hash-map opts) {:keys [user password min-water generated-keys?]
                                     :or {min-water 1 generated-keys? true}} opts'
        conn-params {:user user
                     :password password
                     :url url}]
    (when @autoload-driver?
      (doseq [[driver-name pat] @drivers]
        (when (re-matches pat url))
          (try
            (RT/loadClassForName driver-name)
            (catch Exception exception nil))))

    (Pool. (atom {:conn-params conn-params
                  :min-water min-water
                  :generated-keys? generated-keys?
                  :connections []}))))

(defmacro with-pool
  [p & body]
  `(binding [atomic/*pool* ~p] ~@body))

(defmacro with-conn
  [& body]
  `(binding [atomic/*conn* (atomic/acquire-connection atomic/*pool*)]
     (when (nil? atomic/*conn*)
       (throw (Exception. "unexpected nil connection")))
     (try
       (do ~@body)
       (finally
         (atomic/release-connection atomic/*pool* atomic/*conn*)))))

(defn column
  [col-name & col-opts]
  (let [name (name col-name)
        key col-name
        x {:key key :name name :column? true}]
    (if (not (empty? col-opts))
      (apply assoc x col-opts)
      x)))

(defn- to
  [srcs dsts]
  (let [ret (transient {})]
    (loop [srcs srcs
           dsts dsts]
      (if (or (empty? srcs) (empty? dsts))
        ret
        (let [[src & srcs'] srcs
              [dst & dsts'] dsts]
          (assoc! ret src dst)
          (recur srcs' dsts'))))
    (persistent! ret)))

(defn- map'
  "strict version of map"
  [f a-seq]
  (doall (map f a-seq)))

(defn- flatten'
  "Classic/1-deep version of flatten"
  [seq-seq]
  (apply concat seq-seq))

(defn- intersperse
  [sep a-seq]
  (loop [i 0
         ret []
         a-seq a-seq]
    (if (empty? a-seq)
      ret
      (let [[h & t] a-seq
            ret' (if (= i 0)
                   (conj ret h)
                   (conj ret sep h))]
        (recur (inc i)
               ret'
               (rest a-seq))))))

(defn new-schema
  [& opts]
  (let [[kws objs] (split-kw-opts opts)
        {:keys [name]} kws
        tbls (filter :table? objs)
        relations (filter :relation? objs)
        name-to-table (to (map :key tbls) tbls)]
    {:tables name-to-table}))

(defn table
  [table-kw & table-opts]
  (let [table-name (name table-kw)
         [kws vals] (split-kw-opts table-opts)
         columns (filter :column? vals)
         {:keys [name]} kws]
     {:name (if name name table-name)
      :key table-kw
      :table? true
      :columns columns}))

(defn get-result-array
  [^java.sql.ResultSet result-set]
  (let [md (.getMetaData result-set)
        num-cols (.getColumnCount md)
        col-idxs (range 1 (inc num-cols))
        cols (doall (for [idx col-idxs] [idx (.getColumnType md idx)]))]
    (loop [ret []]
      (if (.next result-set)
        (let [row (doall (for [[col-idx col-type] cols]
                           (let [v (condp = col-type
                                     java.sql.Types/BIGINT (.getLong result-set col-idx)
                                     java.sql.Types/BIT (.getInt result-set col-idx)
                                     java.sql.Types/BLOB (.getBytes result-set col-idx)
                                     java.sql.Types/BOOLEAN (.getInt result-set col-idx)
                                     java.sql.Types/CLOB (.getClob result-set col-idx)
                                     java.sql.Types/DATE (.getDate result-set col-idx)
                                     java.sql.Types/DECIMAL (.getBigDecimal result-set col-idx)
                                     java.sql.Types/DOUBLE (.getDouble result-set col-idx)
                                     java.sql.Types/FLOAT (.getDouble result-set col-idx)
                                     java.sql.Types/INTEGER (.getInt result-set col-idx)
                                     java.sql.Types/NUMERIC (.getDouble result-set col-idx)
                                     java.sql.Types/SMALLINT (.getInt result-set col-idx)
                                     java.sql.Types/VARCHAR (.getString result-set col-idx))
                                 was-null? (.wasNull result-set)]
                             (if (.wasNull result-set) nil v))))]
          (recur (conj ret row)))
        ret))))

(defn prefix-expr
  [op & es]
  [:prefix op es])

(defn infix-expr
  [op & es]
  (assert (>= (count es) 1))
  (condp = (count es)
    1 (first es)
    [:infix op es]))

(defn unary-expr
  [op & es]
  [:unary op es])

(def ?and (partial infix-expr "AND"))
(def ?or (partial infix-expr "OR"))
(def ?+ (partial infix-expr "+"))
(def ?- (partial infix-expr "-"))
(def ?= (partial infix-expr "="))
(def ?!= (partial infix-expr "!="))
(def ?<= (partial infix-expr "<="))
(def ?< (partial infix-expr "<"))
(def ?>= (partial infix-expr ">="))
(def ?> (partial infix-expr ">"))
(def ?in (partial infix-expr "IN"))
(defn ?func
  "function expression
  e.g. '(FUNC \"MIN\" 1 2)'
  "
  [function-name & xs]
  (apply prefix-expr function-name xs))

(def ?min (partial ?func "MIN"))
(def ?max (partial ?func "MAX"))

(defn- where
  [expr]
  {:where? true :expr expr})

(defn WHERE
  [& forms]
  (assert (not (empty? forms)))
  (condp = (count forms)
    1 (where (first forms))
    (where (apply ?and forms))))

(defn comp-unit
  ([sql bind]
   {:sql sql :bind bind})
  ([sql]
   (comp-unit sql ())))

(declare compile-expr)

(defn compile-literal-expr
  [[h & _]]
  (if (= h nil)
    [(comp-unit "NULL")]
    [(comp-unit "?" [h])]))

(defn compile-var-expr
  [[h & _]]
  [(comp-unit h ())])

(defn compile-prefix-expr
  [[function-name sub-exprs]]
  (let [sep [(comp-unit ", ")]
        c-sub-exprs (map' compile-expr sub-exprs)
        separated (flatten' (intersperse sep c-sub-exprs))
        expr (concat [(comp-unit function-name) (comp-unit "(")]
                  :separated
                  [(comp-unit ")")])]
    expr))

(defn compile-infix-expr
  [[operator sub-exprs]]
  (let [sub (map' compile-expr sub-exprs)
        open (comp-unit "(")
        close (comp-unit ")")
        op (comp-unit (str " " operator " "))]
    (loop [i 0
           items sub
           ret []]
      (if (empty? items)
        ret
        (let [[h & items'] items
              prefix (if (= i 0)
                       [open]
                       [op open])
              item (concat prefix h [close])
              ret' (concat ret item)]
          (recur (inc i) items' ret'))))))

(defn compile-expr
  [e]
  (let [ret (cond
              (vector? e) (let [[h & xs] e]
                            (condp = h
                              :var (compile-var-expr xs)
                              :literal (compile-literal-expr xs)
                              :prefix (compile-prefix-expr xs)
                              :infix (compile-infix-expr xs)
                              (compile-literal-expr [e])))
              (keyword? e) [(comp-unit (name e))]
              :else (compile-literal-expr [e]))]
    ret))

(defmacro with-opt-conn
  "Set *conn* from an option map containing :pool or :conn or *conn* or *pool* (in that order)."
  [opts & body]
  `(let [opts# ~opts
         f# (fn [] ~@body)
         conn# (:conn opts#)
         pool# (:pool opts#)]
     (cond
       ;conn# (binding [atomic/*conn* conn#] (f#))
       ;pool# (binding [atomic/*pool* pool#] (atomic/with-conn (f#)))
       (not (nil? atomic/*conn*)) (f#)
       (not (nil? atomic/*pool*)) (atomic/with-conn (f#))
       :else (throw (Exception. ("no pool or connection defined."))))))

(defn- prepare-statement
  [conn sql bind & opts]
  (when (nil? conn)
    (throw (Exception. "expecting non-nil connection")))
  (lg/debug "prepare-statement '%s' bind: %s" sql bind)
  (let [opts' (apply hash-map opts)
        {:keys [generated-keys?]} opts'
        stmt (if generated-keys?
               (.prepareStatement conn sql java.sql.Statement/RETURN_GENERATED_KEYS)
               (.prepareStatement conn sql))]
    (loop [param-idx 1
           bind bind]
      (when (not (empty? bind))
        (let [[h & t] bind]
          (cond
            (string? h) (.setString stmt param-idx h)
            (float? h) (.setDouble stmt param-idx h)
            (integer? h) (.setInt stmt param-idx h)
            :else (throw (Error. (format "unexpected: %s" h))))
          (recur (inc param-idx) t))))
    stmt))

(defn- render-compilation-units
  [compilation-units]
  (let [sql (apply str (map :sql compilation-units))
        bind (apply concat (map :bind compilation-units))]
    [sql bind]))

(defn exec-sql
  [sql & opts]
  (let [[opts args] (split-kw-opts opts)]
    (with-opt-conn opts
                   (let [stmt (prepare-statement *conn* sql args)
                         has-result? (.execute stmt)
                         rows (if has-result? (get-result-array (.getResultSet stmt)) [])]
                         rows))))

(defn- prepare-statement'
  [conn comp-units & opts]
  (let [[sql bind] (render-compilation-units comp-units)]
    (apply prepare-statement conn sql bind opts)))

(defn SELECT
  [schema table-kw & select-opts]
  (let [[kws vals] (split-kw-opts select-opts)]
    (with-opt-conn kws
                   (let [tbl (get (:tables schema) table-kw)
                         _ (when (not tbl)
                             (throw (Exception. "expecting a table")))
                         pool (if (:pool kws) (:pool kws) *pool*)
                         {:keys [columns name]} tbl
                         wheres (filter :where? vals)
                         where (if (not (empty? wheres))
                                 (apply ?and (map :expr wheres)))
                         table-ident "T"
                         col-part [(comp-unit (join ", " (for [col columns] (format "%s.%s" table-ident (:name col)))))]
                         cmd-part [(comp-unit "SELECT ")]
                         from-part [(comp-unit (format " FROM %s AS %s" name table-ident))]
                         where-part (when where (concat [(comp-unit " WHERE ")] (compile-expr where)))
                         comp-units (concat cmd-part col-part from-part where-part)
                         [sql bind-vals] (render-compilation-units comp-units)
                         stmt (prepare-statement' *conn* comp-units)]
                     (let [result-set (.executeQuery stmt)
                           column-keys (map :key columns)
                           rows (get-result-array result-set)]
                       (for [row rows]
                         (to column-keys row)))))))

(defn DELETE
  [schema table-kw & select-opts]
  (let [[kws vals] (split-kw-opts select-opts)]
    (with-opt-conn kws
                   (let [tbl (get (:tables schema) table-kw)
                         _ (when (not tbl)
                             (throw (Exception. "expecting a table")))
                         pool (if (:pool kws) (:pool kws) *pool*)
                         {:keys [columns name]} tbl
                         wheres (filter :where? vals)
                         where (if (not (empty? wheres))
                                 (apply ?and (map :expr wheres)))
                         table-ident "T"
                         col-part [(comp-unit (join ", " (for [col columns] (format "`%s.%s`" table-ident (:name col)))))]
                         cmd-part [(comp-unit "DELETE ")]
                         from-part [(comp-unit (format " FROM %s %s" name table-ident))]
                         where-part (when where (concat [(comp-unit " WHERE ")] (compile-expr where)))
                         comp-units (concat cmd-part col-part from-part where-part)
                         [sql bind-vals] (render-compilation-units comp-units)
                         stmt (prepare-statement' *conn* comp-units)]
                     (let [result-set (.executeQuery stmt)
                           column-keys (map :key columns)
                           rows (get-result-array result-set)]
                       (for [row rows]
                         (to column-keys row)))))))


(defn- surround
  [start-tok comp-units end-tok]
  (concat [start-tok] (flatten' comp-units) [end-tok]))

(defn- generated-keys?
  [opts]
  (cond
    (contains? opts :generated-keys?) (:generated-keys? opts)
    *pool* (supports-generated-keys? *pool*)
    :else false))

(defn INSERT
  [schema table-kw value-dict & opts]
  (let [[kw-opts _] (split-kw-opts opts)]
    (with-opt-conn kw-opts
                   (let [key-values (seq value-dict)
                         table (get (:tables schema) table-kw)
                         full-table-name (if (:name schema)
                                           (str (:name schema) "." (:name table))
                                           (:name table))
                         keys (map first key-values)
                         values (map second key-values)
                         columns-part (surround (comp-unit "(")
                                                (intersperse [(comp-unit ",")] (map compile-expr keys))
                                                (comp-unit ")"))
                         values-compiled (map compile-expr values)
                         values-part (surround (comp-unit "(")
                                               (intersperse [(comp-unit ",")] (map compile-expr values))
                                               (comp-unit ")"))
                         all-parts (concat [(comp-unit (str "INSERT INTO " full-table-name " "))]
                                           columns-part
                                           [(comp-unit " VALUES ")]
                                           values-part)
                         stmt (prepare-statement' *conn* all-parts :generated-keys? (generated-keys? kw-opts))
                         row-count (.executeUpdate stmt)
                         generated-keys (.getGeneratedKeys stmt)
                         insert-id (when (.next generated-keys)
                                     (.getLong generated-keys 1))]
                     insert-id))))

(defn ROLLBACK
  []
  (when *conn*
    (.rollback #^java.sql.Connection *conn*)))

(defn COMMIT
  []
  (when *conn*
    (.commit #^java.sql.Connection *conn*)))


(defmacro serializable
  [& body]
  `(binding [atomic/*isolation* java.sql.Connection/TRANSACTION_SERIALIZABLE]
    ~@body))

(defmacro repeatable-read
  [& body]
  `(binding [atomic/*isolation* java.sql.Connection/TRANSACTION_REPEATABLE_READ]
    ~@body))

(defmacro tx
  [& body]
  `(atomic/with-opt-conn
     {}
     (let [conn# atomic/*conn*
           autocommit?# (.getAutoCommit conn#)
           isolation# (.getTransactionIsolation conn#)
           f# (fn [] ~@body)
           in-tx?# (not autocommit?#)
           preferred-isolation# (or atomic/*isolation* isolation#)]
       (if in-tx?#
         (f#) ; we're already in a transaction
         (do
           (.setAutoCommit conn# false)
           (.setTransactionIsolation conn# preferred-isolation#)
           (try
             (do
               (f#)
               (.commit conn#))
             (catch Exception error#
               (.rollback conn#)
               (throw error#))
             (finally
               ; turn autocommit back on
               (.setAutoCommit conn# true)
               ; return to the original isolation level
               (.setTransactionIsolation conn# isolation#))))))))

(defn- to-migration
  [forms]
  (let [opts (apply hash-map forms)]
    {:up (list (symbol "fn") [] (:up opts))
     :down (list (symbol "fn") [] (:down opts))}))

(def migration-schema (new-schema
                        (table :migration
                               (column :migration_id))))

(defmacro migration
  [& opts]
  (atomic/to-migration opts))

(defn create-migrations-table!
  []
  (exec-sql "CREATE TABLE IF NOT EXISTS migration (migration_id INTEGER PRIMARY KEY NOT NULL)"))

(defn run-up-migration!
  [migration-id migration]
  (let [rows (SELECT migration-schema :migration (WHERE (?= :migration_id migration-id)))
        exists? (> (count rows) 0)
        {:keys [up]} migration]
    (when (not exists?)
      (when up
        (lg/debug "running up migration: %s" migration-id)
        (up))
      (INSERT migration-schema :migration {:migration_id migration-id}))))

(defn run-down-migration!
  [migration-id migration]
  (let [rows (SELECT migration-schema :migration (WHERE (?= :migration_id migration-id)))
        exists? (> (count rows) 0)
        {:keys [down]} migration]
    (when exists?
      (when down
        (lg/debug "running down migration: %s" migration-id)
        (down))
      (DELETE migration-schema :migration (?= :migration_id migration-id)))))

(defn run-all-up-migrations!
  [ms]
  (create-migrations-table!)
  (lg/debug "migrations: %s" ms)
  (loop [i 0
         ms ms]
    (when (not (empty? ms))
      (run-up-migration! i (first ms))
      (recur (inc i)
             (rest ms)))))

(defn run-all-down-migrations!
  [ms]
  (create-migrations-table!)
  (loop [i (dec (count ms))
         ms (reverse ms)]
    (when (not (empty? ms))
      (run-up-migration! i (first ms))
      (recur (dec i)
             (rest ms)))))

