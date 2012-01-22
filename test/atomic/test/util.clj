(ns atomic.test.util
  (:use atomic.util)
  (:use clojure.core)
  (:use clojure.test))

(deftest 
  get-in-test 
  (is
    (= 
      (get-in2 [:x] {})
      []))
  (is
    (= 
      (get-in2 [:x] {:x 1}) 
      [1]))
  (is
    (= 
      (get-in2 [] {:x 1}) 
      [{:x 1}]))
  (is
    (= 
      (get-in2 [:x :y] {:x [{:y 2}]}) 
      [2]))
  (is
    (= 
      (get-in2 [:x :y] {:x {:y 2}}) 
      [2])))
 
(deftest 
  each-in-test 
  (let [c (atom nil)]
    (each-in #(reset! c %) [:x :a] {:x {:a 1}})
    (is (= @c 1)))

(deftest 
  unflatten-test-nested
  (is (=
        [{:a {:b "x" :c "y"} :q {:z "z"}}]
        (unflatten [["x" "y" "z"]] [[:a :b] [:a :c] [:q :z]]))))

(deftest 
  unflatten-test-simple
  (is (=
        [{:a "x" :b "y" :c "z"}]
        (unflatten [["x" "y" "z"]] [[:a] [:b] [:c]]))))




  ; sequences
  (let [c (atom [])]
    (each-in #(swap! c conj %) [:x :a] [{:x {:a 1}} {:x {:a 2}}])
    (is (= @c [1, 2]))))

(deftest 
  map-in-test 
  (is (= [{:x 1} {:y 3 :x 1}] 
         (map-in #(assoc % :x 1) [] [{} {:y 3}])))
  
  (is (= {:y {:x 2 :q 1}}
         (map-in #(assoc % :q 1) [:y] {:y {:x 2 :q 1}})))

  (is (= {:q 1}
         (map-in #(assoc % :q 1) [] {}))))

(deftest default-test
         (is (= {:x 1} (default {} {:x 1})))
         (is (= {:x 2} (default {:x 2} {:x 1}))))


