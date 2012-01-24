(ns atomic.localmap
  "Mutable thread-local map data structure 

  This is useful for isolating state from multiple threads.  Values will be
  garbage collected on thread destruction.

  Usage:
    (let [my-map (localmap/create)]
      (localmap/set my-map :my-key 25)
      (localmap/to-map my-map)) ; => {:my-key 25}
  "

  (:refer-clojure :exclude [get set remove put]))

(defn create 
  "Create a thread local-map" 
  [] 
  (java.lang.ThreadLocal.))

(defmacro setdefault 
  "Set a default value for key
  
  Arguments
  localmap -- the localmap
  key -- the key
  func -- get the value from this function if the key doesn't exist

  Returns
  the defaulted value
  "
  [localmap key func]
  `(let [d# (or (.get ~localmap) {})]
     (if (contains? d# ~key)
       (clojure.core/get d# ~key)
       (let [v# (~func)]
            (.set ~localmap (assoc d# ~key v#))
            v#))))

(defn get
  "Lookup a key
  
  Arguments
  localmap -- localmap
  key -- the key to lookup
  "
  [localmap key] 
  (clojure.core/get (.get localmap) key))

(defn to-map
  "Get a map
  
  Arguments
  localmap -- the localmap

  Returns
  map
  "
  [localmap]
  (or (.get localmap) {}))

(defn set 
  "Set a value for key
  
  Arguments
  localmap -- localmap
  key -- the key 
  val -- the value
  "
  [localmap key val]
  (let [m (or (.get localmap) {})]
    (.set localmap (m assoc {key val}))))

