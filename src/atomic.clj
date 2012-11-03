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
  (drain-connections! [_])
  (release-connection [_ ^java.sql.Connection connection]))

(defrecord Pool [st]
  PPool
  (supports-generated-keys? [pool] (get @st :generated-keys? true))
  (acquire-connection [pool] (modify! st pool-st-acquire-connection))
  (drain-connections! [pool]
    (swap! st (fn [st']
                (doseq [conn (:connections st')]
                  (.close #^java.sql.Connection conn))
                (assoc st' :connections [])))
    nil)
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
  `(let [pool# *pool*
         conn# (atomic/acquire-connection pool#)]
       (when (nil? conn#) (throw (Exception. "unexpected nil connection")))
       (try
         (binding [atomic/*conn* conn#] ~@body)
         (catch Exception error
           (atomic/release-connection pool# conn#)
           (throw error)))
       (atomic/release-connection pool# conn#)))

(defn column
  [col-name & col-opts]
  (let [name (name col-name)
        key col-name
        x {:key key :name name :column? true}]
    (if (not (empty? col-opts))
      (apply assoc x col-opts)
      x)))

(defn- flatten'
  "Classic/1-deep version of flatten"
  [seq-seq]
  (apply concat seq-seq))

(defn- to
  [keys vals]
  (apply hash-map (flatten' (map list keys vals))))

(defn- map'
  "strict version of map"
  [f a-seq]
  (doall (map f a-seq)))

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


(defn rewrite-keyword-path
  "Rewrite a keyword

  Arguments
  kws -- a list of keywords
  suf

  Returns
  a list of keywords

  "
  [[prefix target] match-prefix new-prefix]
  (if (= prefix match-prefix)
    [new-prefix target]))

(defn- type?
  [x kw]
  (get x kw))

(defn table
  [table-kw & table-opts]
  (let [table-name (name table-kw)
         [kws vals] (split-kw-opts table-opts)
         columns (filter :column? vals)
         {:keys [name]} kws]
     {:name (if name name table-name)
      :key table-kw
      :table? true
      :relations (filter #(get % :relation?) vals)
      :columns (filter #(get % :column?) vals)}))

(def sql-to-java-type-map
  {"UUID" (.getClass (java.util.UUID/randomUUID))})

(defn fix-other
  [other]
  (cond
    (instance? java.util.UUID other) (str other)
    :else other))

(defn get-column-value
  [^java.sql.ResultSet result-set
   ^long col-idx
   ^long col-type]
  (condp = col-type
    java.sql.Types/NULL nil
    java.sql.Types/BIGINT (.getLong result-set col-idx)
    ;java.sql.Types/UUID (str (.getUUID result-set col-idx))
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
    java.sql.Types/VARCHAR (.getString result-set col-idx)
    java.sql.Types/TIMESTAMP (.getTimestamp result-set col-idx)
    java.sql.Types/OTHER (fix-other (.getObject result-set col-idx)) ;(.getObject result-set col-idx sql-to-java-type-map)
    ))

(defn get-result-array
  [^java.sql.ResultSet result-set]
  (let [md (.getMetaData result-set)
        num-cols (.getColumnCount md)
        col-idxs (range 1 (inc num-cols))
        col-types (doall (for [idx col-idxs] [idx (.getColumnType md idx)]))]
    (loop [ret []]
      (if (.next result-set)
        ; doall here for strictness
        (let [row (doall (for [[col-idx col-type] col-types]
                           (let [col-value (get-column-value result-set col-idx col-type)
                                 was-null? (.wasNull result-set)]
                             (when (not (.wasNull result-set))
                               col-value))))]
          (recur (conj ret row)))
        ret))))

(defn prefix-expr
  [op & es]
  {:type :prefix
   :op op
   :args es})

(defn infix-expr
  [op & es]
  (assert (>= (count es) 1))
  (condp = (count es)
    1 (first es)
    {:type :infix
     :op op
     :args es}))

(defn unary-expr
  [op & es]
  {:type :unary
   :op op
   :args es})

(defn tuple-expr
  [exprs]
  (assert (> (count exprs) 0))
  {:type :tuple
   :args exprs})


(def ?and (partial infix-expr "AND"))
(def ?or (partial infix-expr "OR"))
(def ?is (partial infix-expr "IS"))
(def ?+ (partial infix-expr "+"))
(def ?- (partial infix-expr "-"))
(def ?uuid (partial prefix-expr "UUID"))
(defn ?=
  [lhs rhs]
  (if (nil? rhs)
    (?is lhs rhs)
    (infix-expr "=" lhs rhs)))

(def ?!= (partial infix-expr "!="))


(defn ?!=
  [lhs rhs]
  (if (nil? rhs)
    (infix-expr "IS NOT" lhs rhs)
    (infix-expr "!=" lhs rhs)))

(def ?<= (partial infix-expr "<="))
(def ?< (partial infix-expr "<"))
(def ?>= (partial infix-expr ">="))
(def ?> (partial infix-expr ">"))
(def ?in (partial infix-expr "IN"))

(defn ?in
  [lhs & exprs]
  "(?in id [1 2 3])"
  (if
    ; bail out
    (= (count exprs) 0)
    (infix-expr "IN" lhs (tuple-expr exprs))))

(defn ?func
  "function expression
  e.g. '(FUNC \"MIN\" 1 2)'
  "
  [function-name & xs]
  (apply prefix-expr function-name xs))

(def ?min (partial ?func "MIN"))
(def ?max (partial ?func "MAX"))
(def ?avg (partial ?func "AVG"))
(def ?sum (partial ?func "SUM"))

(defn ?where
  [& forms]
  (assert (not (empty? forms)))
  {:where? true
   :expr (apply ?and forms)})

(defn ?on
  [& forms]
  {:on? true
   :expr (apply ?and forms)})

(defn sql-expr
  ([sql bind]
   {:sql sql
    :sql-expr? true
    :bind bind})
  ([sql]
   (sql-expr sql ())))

(declare compile-expr)

(defn compile-literal-expr
  [[h & _]]
  (if (= h nil)
    [(sql-expr "NULL")]
    [(sql-expr "?" [h])]))

(defn compile-prefix-expr
  [{:keys [op args]}]
  (let [sep [(sql-expr ", ")]
        c-sub-exprs (map' compile-expr args)
        separated (flatten' (intersperse sep c-sub-exprs))
        expr (concat
               [(sql-expr op) (sql-expr "(")]
               separated
               [(sql-expr ")")])]
    expr))

(defn compile-infix-expr
  [{:keys [op args]}]
  (apply vector
         (let [sub (map' compile-expr args)
               open (sql-expr "(")
               close (sql-expr ")")
               op (sql-expr (str " " op " "))]
           (reduce
             (fn [lhs rhs] (concat [open] lhs [op] rhs [close]))
             (first sub)
             (rest sub)))))

(defn compile-tuple-expr
  [{:keys [args]}]
  (assert (> (count args) 0))
  (concat
    (list (sql-expr "("))
    (intersperse (sql-expr ", ") (flatten' (map compile-expr args)))
    (list (sql-expr ")"))))

(defn compile-identifier-expr
  [e]
  (let [parts (apply vector (.split #^String (name e) "[.]"))
        path (join "." (for [p parts] (format "\"%s\"" p)))]
    [(sql-expr path)]))

(defn compile-expr
  [e]
  (let [ret (cond
              (map? e) (let [{:keys [type]} e]
                            (condp = type
                              :tuple (compile-tuple-expr e)
                              :prefix (compile-prefix-expr e)
                              :infix (compile-infix-expr e)))
              (keyword? e) (compile-identifier-expr e)
              :else (compile-literal-expr [e]))]
    ret))

(defn map-expr-keyword
  [f e]
  (cond
    (map? e) (let [{:keys [type args]} e]
               (assoc e :args (map #(map-expr-keyword f %) args)))
    (keyword? e) (f e)
    :else e))

(defmacro with-opt-conn
  "Set *conn* from an option map containing :pool or :conn or *conn* or *pool* (in that order)."
  [opts & body]
  `(let [opts# ~opts
         f# (fn [] ~@body)
         conn# (:conn opts#)
         pool# (:pool opts#)]
     (cond
       conn# (binding [atomic/*conn* conn#] (f#))
       pool# (binding [atomic/*pool* pool#] (atomic/with-conn (f#)))
       (not (nil? atomic/*conn*)) (f#)
       (not (nil? atomic/*pool*)) (atomic/with-conn (f#))
       :else (throw (Exception. "no pool or connection defined.")))))

(defn- prepare-statement
  [conn sql bind & opts]
  (when (nil? conn)
    (throw (Exception. "expecting non-nil connection")))
  (lg/debug "prepare-statement '%s' bind: %s" sql (apply vector bind))
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
            (instance? java.util.UUID h) (.setObject stmt param-idx h)
            (instance? java.sql.Timestamp h) (.setTimestamp stmt param-idx #^java.sql.Timestamp h)
            :else (throw (Error. (format "unexpected: %s %s" (type h) h))))
          (recur (inc param-idx) t))))
    stmt))

(defn- render-sql-exprs
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
  [conn sql-exprs & opts]
  (let [[sql bind] (render-sql-exprs sql-exprs)]
    (apply prepare-statement conn sql bind opts)))

(defn join-key-paths
  [& kws]
  (keyword (join "." (map name kws))))

(defn rewrite-path-prefix
  [expr src-alias dst-alias]
  (if
    (= src-alias (keyword ""))
    (let [f (fn [kw]
              (let [^String s (name kw)]
                (if (.contains s ".")
                  kw
                  (join-key-paths dst-alias kw))))]
      (map-expr-keyword f expr))
    (let [pat (re-pattern (format "^%s[.]" (java.util.regex.Pattern/quote (name src-alias))))
          dst' (.concat #^String (name dst-alias) ".")
          f (fn [kw]
              (keyword (clojure.string/replace-first (name kw) pat dst')))]
      (map-expr-keyword f expr))))

(defn rewrite-path-prefixes
  "Rewrite an expression"
  [rewrite-patterns expr]
  (reduce (fn [e [src dst]] (rewrite-path-prefix e src dst)) expr rewrite-patterns))

(defn compile-join-expr
  [rewrite-paths join-expr]
  (let [{:keys [type alias internal-alias table on]} join-expr
        join-type-name (condp = type
                         :right-join "RIGHT JOIN"
                         :left-join "LEFT JOIN"
                         "INNER JOIN")
        prefix (sql-expr (format " %s \"%s\" AS \"%s\"" join-type-name (name table) (name internal-alias)))
        on (when on (rewrite-path-prefixes rewrite-paths on))
        on-part (if on (concat [(sql-expr " ON ")] (compile-expr on)) [])]
    (concat [prefix] on-part)))

(defn compile-select-expr
  "Compile a select expression

  Arguments
  A mapping with the following keys
   :schema -- the source schema
   :table-kw -- the source table keyword
   :where -- the source table where part

  Returns
  A mapping of
    :sql the SQL string
    :bind the bind value
    :column-keys a list of column keys to create a mapping
  "
  [{:keys [schema
           table-kw
           wheres
           joins]}]
  (let [{:keys [tables]} schema
        ; table: the table mapping storing in schema
        table (get tables table-kw)
        _ (when (not table)
            (throw (Exception. (format "expecting a table to be defined for: %s" table-kw))))
        {:keys [columns]} table
        ; table-name: the sql name of the table
        table-name (:name table)
        ; where: the AND-ed where expression or nil
        where (when
                (not (empty? wheres))
                (apply ?and (map :expr wheres)))
        ; The root table alias is named _0
        root-table-alias :_0
        ; Add the internal alias
        ; Add an internal join alias key to the list of joins
        ; Each join alias is _1..._n
        joins (map-indexed
                 (fn [idx join] (assoc join
                                       :internal-alias (keyword (format "_%d" (inc idx)))))
                 joins)
        ; rewrite-paths: sequence of a [(keyword prefix, rewrite prefix)]
        rewrite-paths (concat
                        [[(keyword "") root-table-alias]]
                        (for [join joins]
                          [(:alias join) (:internal-alias join)]))
        ; columns: The list of column expressions
        columns (for [col (:columns table)]
                  {:name (:key col)
                   :key (:key col)
                   :alias root-table-alias})
        ; Rewrite the where expression with the join prefix
        where (when where
                 (reduce
                    (fn [where [old-prefix new-prefix]]
                      (rewrite-path-prefix where old-prefix new-prefix))
                    where
                    rewrite-paths))
        ; Add the columns from the join expressions
        join-columns (for [join joins]
                   (let [{:keys [table alias internal-alias]} join
                         _ (assert table)
                         join-table (get tables table)
                         _ (assert join-table)
                         {:keys [columns]} join-table]
                     (for [column columns]
                       {:alias internal-alias
                        :key (join-key-paths alias (:key column))
                        :name (:key column)})))
        columns (apply concat columns join-columns)
        col-part [(sql-expr (join ", " (for [col columns]
                                         (format "\"%s\".\"%s\""
                                                 (name (:alias col)) (name (:name col))))))]
        ; Build the query
        cmd-part [(sql-expr "SELECT ")]
        ; The from part of the expression
        from-part [(sql-expr (format " FROM \"%s\" AS \"%s\"" table-name (name root-table-alias)))]
        ; The join part of the expression
        join-part (flatten' (map #(compile-join-expr rewrite-paths %) joins))
        where-part (when where (concat [(sql-expr " WHERE ")] (compile-expr where)))
        sql-exprs (concat cmd-part col-part from-part join-part where-part)
        column-keys (map :key columns)
        [sql bind] (render-sql-exprs sql-exprs)]
      {:sql sql
       :bind bind
       :column-keys column-keys}))


(defn ?select
  [schema table-kw & select-opts]
  (let [[kws vals] (split-kw-opts select-opts)]
    (with-opt-conn kws
                   (let [mappings (filter map? vals)
                         compiled-expr (compile-select-expr {:schema schema
                                                             :table-kw table-kw
                                                             :wheres (filter :where? mappings)
                                                             :joins (filter :join? mappings)})
                         {:keys [sql bind column-keys]} compiled-expr
                         stmt (prepare-statement *conn* sql bind)]
                     (doall column-keys)
                     (let [result-set (.executeQuery stmt)
                           rows (get-result-array result-set)]
                       (doall (for [row rows]
                                (to column-keys row))))))))

(defn ?delete
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
                         col-part [(sql-expr (join ", " (for [col columns] (format "\"%s\".\"%s\"" table-ident (:name col)))))]
                         cmd-part [(sql-expr "DELETE ")]
                         from-part [(sql-expr (format " FROM \"%s\" \"%s\"" name table-ident))]
                         where-part (when where (concat [(sql-expr " WHERE ")] (compile-expr where)))
                         sql-exprs (concat cmd-part col-part from-part where-part)
                         [sql bind-vals] (render-sql-exprs sql-exprs)
                         stmt (prepare-statement' *conn* sql-exprs)]
                     (let [result-set (.executeQuery stmt)
                           column-keys (map :key columns)
                           rows (get-result-array result-set)]
                       (for [row rows]
                         (to column-keys row)))))))


(defn- surround
  [start-tok sql-exprs end-tok]
  (concat [start-tok] (flatten' sql-exprs) [end-tok]))

(defn- generated-keys?
  [opts]
  (cond
    (contains? opts :generated-keys?) (:generated-keys? opts)
    *pool* (supports-generated-keys? *pool*)
    :else false))

(defn ?insert
  [schema table-kw value-dict & opts]
  (let [[kw-opts _] (split-kw-opts opts)]
    (with-opt-conn kw-opts
                   (let [key-values (seq value-dict)
                         table (get (:tables schema) table-kw)
                         _ (if-not table
                             (throw (Exception. (format "expecting table \"%s\" to be defined" table-kw))))
                         full-table-name (if (:name schema)
                                           (format "\"%s\".\"%s\"" (:name schema) (:name table))
                                           (format "\"%s\"" (:name table)))
                         keys (map first key-values)
                         values (map second key-values)
                         columns-part (surround (sql-expr "(")
                                                (intersperse [(sql-expr ",")] (map compile-expr keys))
                                                (sql-expr ")"))
                         values-compiled (map compile-expr values)
                         values-part (surround (sql-expr "(")
                                               (intersperse [(sql-expr ",")] (map compile-expr values))
                                               (sql-expr ")"))
                         all-parts (concat [(sql-expr (str "INSERT INTO " full-table-name " "))]
                                           columns-part
                                           [(sql-expr " VALUES ")]
                                           values-part)
                         stmt (prepare-statement' *conn* all-parts :generated-keys? (generated-keys? kw-opts))
                         row-count (.executeUpdate stmt)
                         generated-keys (.getGeneratedKeys stmt)
                         generated-values (get-result-array generated-keys)
                         [[insert-id & xs] & ys] generated-values
                         ;insert-id (when (.next generated-keys)
                         ;            (.getLong generated-keys 1))
                         ]
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
             (let [ret# (f#)]
               (.commit conn#)
               ret#)
             (catch Exception error#
               (.rollback conn#)
               (throw error#))
             (finally
               ; turn autocommit back on
               (.setAutoCommit conn# true)
               ; return to the original isolation level
               (.setTransactionIsolation conn# isolation#))))))))

(def migration-schema (new-schema
                        (table :migration
                               (column :migration_id))))

(defn ?inner-join
  [table-keyword
   alias-keyword
   & exprs]
  (let [ons (filter :on? (filter map? exprs))
        on (when (not (empty? ons))
             (apply ?and (map :expr ons)))]
    {:join? true
     :type :inner-join
     :table table-keyword
     :alias alias-keyword
     :on on}))

(def ?join ?inner-join)

(defn ?left-join
  "Add a left-join clause to a SELECT"
  [& xs]
  (assoc (apply ?inner-join xs)
         :type :left-join))

(defn ?right-join
  "Add a right-join clause to a SELECT"
  [& xs]
  (assoc (apply ?inner-join xs)
         :type :right-join))

(defmacro migration
  ([up down]
  `(hash-map
     :up (fn [] ~up)
     :down (fn [] ~down)))
  ([up]
   `(hash-map
      :up (fn [] ~up))))

(defmacro def-migration
  [a-symbol & body]
  `(def ~a-symbol (migration ~@body)))

(defn create-migrations-table!
  []
  (exec-sql "CREATE TABLE IF NOT EXISTS migration (migration_id INTEGER PRIMARY KEY NOT NULL)"))

(defn parse-keyword-path
  [a-keyword]
  (let [^String name-part (name a-keyword)
        parts (.split name-part "[.]")]
    {:key (keyword (last parts))
     :path (map keyword (drop-last parts))}))

(defn run-up-migration!
  [migration-id migration]
  (let [rows (?select migration-schema :migration (?where (?= :migration_id migration-id)))
        exists? (> (count rows) 0)
        {:keys [up]} migration]
    (when (not exists?)
      (when up
        (lg/debug "running up migration: %s" migration-id)
        (up))
      (?insert migration-schema :migration {:migration_id migration-id}))))

(defn run-down-migration!
  [migration-id migration]
  (let [rows (?select migration-schema :migration (?where (?= :migration_id migration-id)))
        exists? (> (count rows) 0)
        {:keys [down]} migration]
    (when exists?
      (when down
        (lg/debug "running down migration: %s" migration-id)
        (down))
      (?delete migration-schema :migration (?= :migration_id migration-id)))))

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
  "Run all of the down migrations in a migration sequence"
  [ms]
  (create-migrations-table!)
  (loop [i (dec (count ms))
         ms (reverse ms)]
    (when (not (empty? ms))
      (run-up-migration! i (first ms))
      (recur (dec i)
             (rest ms)))))

(defn ?select-one
  [& args]
  (first (apply ?select args)))

(defn create-index
  [table
   & cols]
  ;(assert (> (count cols) 0))
  (exec-sql (format "CREATE INDEX ON \"%s\" USING BTREE(%s)"
                    (name table)
                    (clojure.string/join ", "
                                         (for [col cols]
                                           (format "\"%s\"" (name col)))))))

(defn drop-table
  [table]
  (exec-sql (format "DROP TABLE \"%s\"" (name table))))

