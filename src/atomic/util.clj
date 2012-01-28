(ns atomic.util
  (:require clojure.string))

(defn join-keywords
  [& kws]
  (keyword (clojure.string/join "." (map name (flatten kws)))))

(defn get-key-to-seq 
  [items a-key]
  (let [x (transient (hash-map))]
    (doseq [i items]
      (let [v (get i a-key)
            prev (get x v [])]
        (conj! x [v (cons i prev)])))
    (persistent! x)))

(defn zipn
  "Python / ML / Haskell style zip"
  [& seqs]
  (let [result (transient (vector))]
    (loop [seqs0 seqs]
      (when (not (some empty? seqs0))
        (conj! result (map first seqs0))
        (recur (map rest seqs0))))
    (persistent! result)))

(defn flatten-shallow
  [x]
  (apply concat x))

(defn not-keyword? [x] (not (keyword? x)))

(defn parse-dotted-keyword
  [p]  
  (map keyword (clojure.string/split (name p) #"[.]")))

(defn heads 
  "Get all of the prefixes ef a"
  [a]
  (let [a0 (apply vector a)]
    (for [end-idx (range 1 (inc (count a0)))]
      (subvec a0 0 end-idx))))

(defn parse-relation-paths 
  [paths]
  (let [result (transient (hash-set))]
    (dorun 
      (for [p paths]
        (dorun 
          (for [h (heads (parse-dotted-keyword p))]
            (conj! result h)))))
    (persistent! result)))

(defn each-in
  "Call func for each value in subj matching key-path"
  [func key-paths subj]
  (cond 
    (nil? subj) nil 
    (sequential? subj) (doseq [x subj] (each-in func key-paths x))
    (empty? key-paths) (func subj)
    :else (each-in func (rest key-paths) (get subj (first key-paths) nil))))

(defn get-in2 
  [key-paths subj]
  (let [r (transient (vector))]
    (each-in #(conj! r %) key-paths subj)
    (persistent! r)))

(defn map-in
  "Replace each value in subj with matching key-path"
  [func key-paths subj]
  (let [n (count key-paths)
        h (first key-paths)
        is-seq (sequential? subj)
        t (rest key-paths)]
    (cond 
      (sequential? subj) (doall (for [i subj]
                                  (map-in func key-paths i)))
      (= n 0) (func subj)
      :else (assoc subj h (map-in func t (get subj h {}))))))

(defn all-but-last 
  "Get all but the last element of a sequence"
  [a-seq]
  (take (dec (count a-seq)) a-seq))

(defn stringify 
  [s]
  (cond 
    (keyword? s) (name s)
    (nil? s) s
    :else (str s)))

(defn keywordify 
  [s]
  (cond 
    (keyword? s) s
    (nil? s) s
    :else (keyword s)))

(defn columnify
  [kw]
  (clojure.string/replace (name kw) "-" "_"))

(defn safe-load-class [p]
  (try (Class/forName p) (catch ClassNotFoundException e)))

(defn classify 
    [f coll]
    [(filter f coll) (filter #(not (f %1)) coll)])

(defn unflatten
  "Build a nested result set from a set of key paths"
  [rows key-paths]
  (let [key-paths0 (apply vector key-paths)
        num-key-paths (count key-paths0)]
    (for [row rows]
      (loop [i 0
             result {}]
          (if (< i num-key-paths)
             (recur 
               (inc i)
               (assoc-in result (get key-paths0 i) (get row i)))
            result)))))

(defn default
  [m defaults]
  (loop [m0 m
         kvs (seq defaults)] 
    (if 
      (empty? kvs) 
      m0
      (let [[k v] (first kvs)
            m1 (if (contains? m0 k) 
                 m0 
                 (assoc m0 
                        k 
                        ; expand functions:
                        (cond 
                          (keyword? v) v
                          (map? v) v
                          (set? v) v
                          (sequential? v) v
                          (ifn? v) (v)
                          (fn? v) (v)
                          :else v)))]
        (recur m1 (rest kvs))))))

(defn timestamp 
  "Helper function to get the number of seconds since the epoch"
  [] (/ (System/currentTimeMillis) 1000.0))
