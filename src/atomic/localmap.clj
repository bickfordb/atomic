(ns atomic.localmap
    (:refer-clojure :exclude [get set remove put]))

(defn create "Create a thread local-map" [] (java.lang.ThreadLocal.))

(defmacro setdefault 
  "Set a default value for key"
  [localmap key func]
  `(let [d# (or (.get ~localmap) {})]
     (if (contains? d# ~key)
       (clojure.core/get d# ~key)
       (let [v# (~func)]
            (.set ~localmap (assoc d# ~key v#))
            v#))))

(defn get
      "Lookup a key"
      [localmap key] 
      (clojure.core/get (.get localmap) key))

(defn to-map
      "Get a map"
      [localmap]
      (or (.get localmap) {}))

(defn set 
  "Set a value for key"
  [localmap key val]
  (let [m (or (.get localmap) {})]
    (.set localmap (m assoc {key val}))))

