(ns crux.inspector-test
  (:require [crux.inspector.query-plan]
            [crux.io :as cio]
            [crux.api :as api]
            [clojure.test :as t]
            [crux.tx :as tx])
  (:import java.util.UUID
           [crux.api Crux ICruxAPI]
           [java.util ArrayList List]))

(def ^:dynamic ^ICruxAPI *api*)

(defn maps->tx-ops
  ([maps]
   (vec (for [m maps]
          [:crux.tx/put m])))
  ([maps ts]
   (vec (for [m maps]
          [:crux.tx/put m ts]))))

(defn transact!
  "Helper fn for transacting entities"
  ([api entities]
   (transact! api entities (cio/next-monotonic-date)))
  ([^ICruxAPI api entities ts]
   (let [submitted-tx (api/submit-tx api (maps->tx-ops entities ts))]
     ;; TODO when 'sync' gets replaced by 'await-tx', this should use the higher-level protocol again
     (tx/await-tx (:indexer api) submitted-tx 10000))
   entities))

(defn- with-node [f]
  (let [db-dir (cio/create-tmpdir "kv-store")
        event-log-dir (str (cio/create-tmpdir "event-log-dir"))]
    (try
      (with-open [node (Crux/startNode {:crux.node/topology :crux.standalone/topology
                                        :crux.standalone/event-log-dir event-log-dir
                                        :crux.kv/db-dir (str db-dir)})]
        (binding [*api* node]
          (f)))
      (finally
        (cio/delete-dir db-dir)
        (cio/delete-dir event-log-dir)))))

(t/use-fixtures :each with-node)

(t/deftest test-some-issue-443
  (dotimes [n 2]
    (when (= 0 (mod n 100))
      (print "."))
    (transact! *api* [{:crux.db/id (keyword (str "ida-" n)) :fare-prefix (str "fp" n) :reference (str "ref" n)}
                        {:crux.db/id (keyword (str "idb-" n)) :fare-prefix (str "fp" n) :reference (str "ref" n)}]))

  (println "Transacted")

  (sorted-map :d :e :a :b :c :b)

  (crux.fixtures.query-plan/with-plan
    (t/is (count (api/q (api/db *api*) '{:find [discount catalog]
                                         :where [[discount :fare-prefix f]
                                                 [catalog :fare-prefix f]
                                                 [discount :reference ref]
                                                 [catalog :reference ref]]}))))

  #_(crux.fixtures.trace/with-tracing
      (t/is (= 1000
               (count (api/q (api/db *api*) '{:find [discount catalog]
                                              :where [[discount :fare-prefix f]
                                                      [catalog :fare-prefix f]
                                                      [discount :reference ref]
                                                      [catalog :reference ref]]}))))))
