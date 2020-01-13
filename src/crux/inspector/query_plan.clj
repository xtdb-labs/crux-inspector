(ns crux.fixtures.query-plan
  (:require [crux.inspector.instrument :as instr]
            [crux.inspector.trace :as t]
            [crux.index :as i]
            [loom.graph :as g]
            [loom.io :as lio]
            [rhizome.viz :as v]))

(defprotocol Children
  (children [i]))

(extend-protocol Children
  crux.index.NAryConstrainingLayeredVirtualIndex
  (children [this]
    [(:n-ary-index this)])

  crux.index.NAryJoinLayeredVirtualIndex
  (children [this]
    (:unary-join-indexes this))

  crux.index.UnaryJoinVirtualIndex
  (children [this]
    (:indexes this))

  crux.index.BinaryJoinLayeredVirtualIndex
  (children [^crux.index.BinaryJoinLayeredVirtualIndex this]
    (let [state ^crux.index.BinaryJoinLayeredVirtualIndexState (.state this)]
      (.indexes state)))

  crux.index.RelationVirtualIndex
  (children [^crux.index.RelationVirtualIndex this]
    (let [state ^crux.index.RelationIteratorsState (.state this)]
      (.indexes state)))

  Object
  (children [this]
    nil))

(comment
  (lio/view (-> (g/graph)
                (g/add-nodes 1 2 3)
                (g/add-edges [1 2] [1 3 10])))

  (def g
    {:a [:b :c]
     :b [:c]
     :c [:a]})

  (v/view-graph (keys g) g
                :node->descriptor (fn [n] {:label n}))

  #_(v/view-tree sequential? seq g
               :node->descriptor (fn [n] {:label (when (number? n) n)})))

(defn loom-instrumenter [g visited i]
  (swap! g assoc (t/index-name i) (mapv t/index-name (children i)))
  i)

(defn instrument-layered-idx->seq [idx]
  (let [g (atom {})
        f (partial loom-instrumenter g)]
    (let [x (instr/instrumented-layered-idx->seq f idx)]
      (clojure.pprint/pprint @g)
      (v/view-graph (keys @g) @g
                    :node->descriptor (fn [n] {:label n}))
      x)))

(defmacro with-plan [& form]
  `(with-redefs [i/layered-idx->seq instrument-layered-idx->seq]
     ~@form))

(comment
  ;; From Dominic Monroe... this preserves order. TODO, compare

  (require '[dorothy.core :as dot])
  (require '[dorothy.jvm :refer (render save! show!)])

  (defn rank
    [attrs statements]
    {:type ::rank
     :statements (map #'dot/to-ast statements)
     :attrs attrs})

  (defmethod dot/dot* ::rank
    [this]
    (let [{:keys [attrs statements]} this]
      (str "{\n"
           (apply
            str
            (for [[k v] attrs]
              (str (#'dot/escape-id k) \= (#'dot/escape-id v) ";\n")))
           (apply str (interleave
                       (map dot/dot* statements)
                       (repeat ";\n")))
           "} ")))

  (show!
   (dot/dot
    (dot/digraph
     [[:node0 {:label "0"}]
      [:node1 {:label "1"}]
      [:node2 {:label "2"}]
      [:node3 {:label "3"}]
      [:nodeA {:label "A"}]
      [:nodeB {:label "B"}]
      [:nodeC {:label "C"}]
      [:node0 :> :nodeA]
      [:node1 :> :nodeB]
      [:node1 :> :nodeC]
      [:node3 :> :nodeA]
      (rank
       {:rank "same"
        :rankdir "LR"}
       [[:node0 :> :node1 :> :node2 :> :node3 {:style "invis"}]])
      ])))
  )
