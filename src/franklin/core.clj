; Copyright (c) 2019 Corey Torrisi
;
; This program and the accompanying materials are made
; available under the terms of the Eclipse Public License 2.0
; which is available at https://www.eclipse.org/legal/epl-2.0/
;
; SPDX-License-Identifier: EPL-2.0

(ns ^{:doc "The core Franklin library."
      :author "Corey Torrisi"}
  franklin.core
  (:require [clojure.string :as s]
            [clojure.data.codec.base64 :as b64]
            [cognitect.aws.client.api :as aws]
            [clojure.string :as str])
  (:import (clojure.lang BigInt)))

(defn- assert-ddb-num [x]
  (assert (<= (.precision (bigdec x)) 38)
          "DynamoDB limits its Number data type to 38 digits of precision.")
  x)

(defn- ddb-num? [x]
  (or (int? x)
      (float? x)
      (and (instance? BigInt x)     (assert-ddb-num x))
      (and (instance? BigDecimal x) (assert-ddb-num x))
      (and (instance? BigInteger x) (assert-ddb-num x))))

(defn- map-key-val-fns
  ([vf m]
   (map-key-val-fns nil vf m))
  ([kf vf m]
   (persistent!
     (reduce-kv (fn [m k v]
                  (assoc! m (if kf (kf k) k) (if vf (vf v) v)))
                (transient {})
                m))))

(defn- bytes->base64-str [x] (String. ^"[B" (b64/encode x) "UTF-8"))
(defn- base64->bytes [x] (b64/decode (.getBytes (slurp x))))

(defn- ddb-num->clj-num
  "Given a DynamoDB formatted number, converts into a Clojure number."
  [x]
  (if (s/includes? x ".")
    (bigdec x)
    (condp #(< %2 %1) (count x)
      3   (Byte/parseByte x)
      5   (Short/parseShort x)
      10  (Integer/parseInt x)
      19  (Long/parseLong x)
      (bigint x))))

(defn- clj->ddb
  "Given a Clojure data type, converts into a DynamoDB data type."
  [x]
  (condp #(%1 %2) x
    nil?          {:NULL true}
    string?       {:S x}
    boolean?      {:BOOL x}
    ddb-num?      {:N (str (assert-ddb-num x))}
    vector?       {:L (mapv clj->ddb x)}
    bytes?        {:B (bytes->base64-str x)}
    map?          {:M (map-key-val-fns clj->ddb x)}
    set?          (condp #(every? %1 %2) x
                    string? {:SS x}
                    ddb-num? {:NS (mapv str x)}
                    bytes? {:BS (mapv bytes->base64-str x)}
                    (throw (Exception. "All values in a set must be the same type and must be a supported type.")))
    (throw (Exception. (format "Unsupported value type: %s" (type x))))))

(defn- ddb->clj
  "Given a DynamoDB data type, converts into a Clojure data type."
  [x]
  (or (:S x)
      (:BOOL x)
      (some->>  (:N x)      (ddb-num->clj-num))
      (some->>  (:SS x)     (into #{}))
      (some->>  (:NS x)     (mapv ddb-num->clj-num) (into #{}))
      (some->>  (:B x)      (base64->bytes))
      (some->>  (:BS x)     (mapv base64->bytes) (into #{}))
      (some->>  (:L x)      (mapv ddb->clj))
      (when-let [m (:M x)]  (zipmap (mapv keyword (keys m))
                                    (mapv ddb->clj (vals m))))))
(defn- attr-val-key [k]
  (if (keyword? k) (str k) k))

(def ^:private ddb-item->clj-item (partial map-key-val-fns keyword ddb->clj))
(def ^:private clj-item->ddb-item (partial map-key-val-fns clj->ddb))
(def ^:private clj-expr-attr-vals->ddb-item (partial map-key-val-fns attr-val-key clj->ddb))

(defn- ddb-vec->clj-vec
  "Given a vector of DynamoDB data typed items, converts into a vector of Clojure data typed items."
  [x]
  (-> (map #(ddb-item->clj-item %) x)
      (vec)))

(defn make-client [& [opts]] (aws/client (assoc opts :api :dynamodb)))
(defn- get-client [opts] (or (:client opts) (make-client opts)))

(defn invoke
  "Wraps aws-api's invoke function accepts `client-opts` consisting of either a `cognitect.aws.client.api/client`
  config map, or a map containing:

  :client    - An existing aws-api DynamoDB client, consistent with Franklin's table context.

  Alpha. Subject to change."
  [client-opts op request]
  (aws/invoke (get-client client-opts) {:op      op
                                        :request request}))

(defn describe-table
  "DescribeTable request.

  Alpha. Subject to change."
  [{:keys [table-name] :as table-context}]
  (invoke table-context :DescribeTable {:TableName table-name}))

(defn- describe-key-schema
  "Creates a mapped key-value pair of either :partition-key-name or :sort-key-name,
   if the `KeyType` is HASH or RANGE, respectively."
  [{:keys [AttributeName KeyType]}]
  {(if (= KeyType "HASH") :partition-key-name :sort-key-name) AttributeName})

(defn make-table-context
  "Given a `table-name` and an optional set of `client-opts` in the same format as `cognitect.aws.client.api/client`
  config map. `make-table-context` constructs a map consisting of:

  :client                 - The aws-api DynamoDB client.
  :table-name             - The name of the table.
  :partition-key-name     - The name of the partition key for the table.
  :sort-key-name          - The name of the sort key for the table, if it exists.
  :key-keywords           - The partition key and sort key as a vector of keywords.

  Alpha. Subject to change."
  [table-name & [client-opts]]
  (let [table-context {:client (get-client client-opts)
                       :table-name table-name}
        key-schema (->> (describe-table table-context)
                        (:Table)
                        (:KeySchema)
                        (mapv #(describe-key-schema %))
                        (into {}))
        key-keywords (->> (vals key-schema)
                          (map #(keyword %))
                          (vec)
                          (assoc {} :key-keywords))]
    (merge table-context key-schema key-keywords)))

(defn- make-sort-key-expr
  "Given a `sort-key-name` and `comparator`, constructs a sort key expression string for the DynamoDB request."
  [sort-key-name comparator]
  (if (> (count comparator) 2)
    (case (str/lower-case comparator)
      "between" (str sort-key-name " BETWEEN :sk1 AND :sk2")
      "begins_with" (str "begins_with (" sort-key-name ", :sk1)")
      (throw (Exception. (format "Invalid sort key comparison-operator %s" comparator))))
    (s/join " " [sort-key-name comparator ":sk1"])))

(defn- make-projection-expression [projections]
  (when (seq projections) (s/join ", " (map #(name %) projections))))

(defn- make-scan-query-base-request
  "Constructs the base request map common to Query and Scan."
  [{:keys [table-name]}
   {:keys [exclusive-start-key projections index-name limit consistent-read?
           filter-expr expr-attr-vals return-cc select]}]
  {:TableName                 table-name
   :FilterExpression          filter-expr
   :ExpressionAttributeValues (when expr-attr-vals (clj-expr-attr-vals->ddb-item expr-attr-vals))
   :ExclusiveStartKey         (when exclusive-start-key (clj-item->ddb-item exclusive-start-key))
   :ProjectionExpression      (make-projection-expression projections)
   :Limit                     limit
   :IndexName                 index-name
   :ConsistentRead            consistent-read?
   :ReturnConsumedCapacity    return-cc
   :Select                    (when (empty? projections) select)})

(defn- make-query-request
  "Constructs a Query request."
  [{:keys [partition-key-name sort-key-name] :as table-context}
   {:keys [partition-key sort-key descending?] :as query-opts}]
  (let [base-request (make-scan-query-base-request table-context query-opts)
        comparator (or (:comparator sort-key) "=")
        key-condition-expr (str (str partition-key-name " = :pk")
                                (when sort-key (str " AND " (make-sort-key-expr sort-key-name comparator))))
        expr-attr-vals (merge {":pk" (clj->ddb partition-key)}
                              (when sort-key {":sk1" (clj->ddb (:key-1 sort-key))})
                              (when (= "between" (str/lower-case comparator))
                                {":sk2" (clj->ddb (:key-2 sort-key))})
                              (:ExpressionAttributeValues base-request))]
    (assoc base-request :KeyConditionExpression key-condition-expr
                        :ExpressionAttributeValues expr-attr-vals
                        :ScanIndexForward (when descending? false))))

(defn query-raw
  "Query request without converting items to Clojure data types.

  Alpha. Subject to change."
  [table-context query-opts]
  (invoke table-context :Query (make-query-request table-context query-opts)))

(defn query
  "Query request with items converted to Clojure data types.

  Alpha. Subject to change."
  [table-context query-opts]
  (-> (query-raw table-context query-opts)
      (update :Items #(mapv ddb-item->clj-item %))))

(defn scan-raw
  "Scan request without converting items to Clojure data types."
  [table-context
   {:keys [segment total-segments] :as scan-opts}]
  (invoke table-context :Scan (-> (make-scan-query-base-request table-context scan-opts)
                                  (assoc :Segment segment
                                         :TotalSegments total-segments))))

(defn scan
  "Scan request with items converted to Clojure data types.

  Alpha. Subject to change."
  ([table-context]
    (scan table-context {}))
  ([table-context scan-opts]
   (-> (scan-raw table-context scan-opts)
       (update :Items #(mapv ddb-item->clj-item %)))))

(defn query-latest
  "Convenience function to query the latest result. Assumes sort-key is a timestamp.

  Alpha. Subject to change."
  [table-context query-opts]
  (query table-context (assoc query-opts :descending? true :limit 1)))

(defn- make-item-base-request
  "Constructs the base request common to all item-based requests."
  [{:keys [table-name]}
   {:keys [condition-expr expr-attr-names expr-attr-vals
           return-cc return-coll-metrics return-vals]}]
  {:TableName table-name
   :ConditionExpression condition-expr
   :ExpressionAttributeNames expr-attr-names
   :ExpressionAttributeValues (when expr-attr-vals (clj-expr-attr-vals->ddb-item expr-attr-vals))
   :ReturnConsumedCapacity return-cc
   :ReturnItemCollectionMetrics return-coll-metrics
   :ReturnValues return-vals})

(defn- clj-item->ddb-key
  "Converts a map to a key in the expected DynamoDB format."
  [{:keys [key-keywords]}
   item]
  (-> (select-keys item key-keywords)
      (clj-item->ddb-item)))

(defn put-item
  "PutItem request.

  Alpha. Subject to change."
  [table-context
   {:keys [item key] :as item-opts}]
  (invoke table-context :PutItem (-> (make-item-base-request table-context item-opts)
                                     (assoc :Item (clj-item->ddb-item (or item key))))))

(defn update-item
  "UpdateItem request.

  Alpha. Subject to change."
  [table-context
   {:keys [item key update-expr] :as item-opts}]
  (invoke table-context :UpdateItem (-> (make-item-base-request table-context item-opts)
                                        (assoc :Key (clj-item->ddb-key table-context (or item key))
                                               :UpdateExpression update-expr))))

(defn delete-item
  "DeleteItem request.

  Alpha. Subject to change."
  [table-context
   {:keys [item key] :as item-opts}]
  (invoke table-context :DeleteItem (-> (make-item-base-request table-context item-opts)
                                        (assoc :Key (clj-item->ddb-key table-context (or item key))))))

(defn get-item-raw
  "GetItem request without items converted to Clojure data types.

  Alpha. Subject to change."
  [{:keys [table-name] :as table-context}
   {:keys [item key projections expr-attr-names return-cc]}]
  (invoke table-context :GetItem {:TableName                table-name
                                  :Key                      (clj-item->ddb-key table-context (or item key))
                                  :ProjectionExpression     (make-projection-expression projections)
                                  :ExpressionAttributeNames expr-attr-names
                                  :ReturnConsumedCapacity   return-cc}))

(defn get-item
  "GetItem request with items converted to Clojure data types.

  Alpha. Subject to change."
  [table-context item-opts]
  (-> (get-item-raw table-context item-opts)
      (update :Item #(ddb-item->clj-item %))))

(defn- make-batch-write-item-request-item
  "Constructs an individual PutRequest or DeleteRequest map based on `delete?` key in the item map.
  For convenience, `delete?` is reserved and may not be used as a partition or sort key when using `batch-write-item`."
  [table-context item]
  (if (:delete? item)
    {:DeleteRequest {:Key (clj-item->ddb-key table-context item)}}
    {:PutRequest {:Item (clj-item->ddb-item item)}}))

(defn- invoke-batch-write-item
  "Invokes the BatchWriteItem request."
  [{:keys [table-name] :as table-context}
   {:keys [items return-cc return-item-coll-metrics]}]
  (let [request-items (-> (map #(make-batch-write-item-request-item table-context %) items)
                          (vec))]
    (invoke table-context :BatchWriteItem {:RequestItems                {table-name request-items}
                                           :ReturnConsumedCapacity      return-cc
                                           :ReturnItemCollectionMetrics return-item-coll-metrics})))

(defn batch-write-item
  "BatchWriteItem request.

  Alpha. Subject to change."
  [table-context
   {:keys [items] :as item-opts}]
  (loop [partitioned (partition 25 25 nil items)]
    (let [next-items (rest partitioned)]
      (invoke-batch-write-item table-context (assoc item-opts :items (first partitioned)))
      (when (seq next-items)
        (recur next-items)))))

(defn- make-batch-get-item-request
  "Constructs a BatchGetItem request map."
  [{:keys [table-name]}
   {:keys [keys consistent? expr-attr-names projections]}]
  {:RequestItems {table-name {:Keys                     (vec (map #(clj-item->ddb-item %) keys))
                              :ConsistentRead           consistent?
                              :ExpressionAttributeNames expr-attr-names
                              :ProjectionExpression     (make-projection-expression projections)}}})

(defn batch-get-item
  "BatchGetItem request.

  Alpha. Subject to change."
  [{:keys [table-name] :as table-context}
   item-opts]
  (-> (invoke table-context :BatchGetItem (make-batch-get-item-request table-context item-opts))
      (update-in [:Responses (keyword table-name)] ddb-vec->clj-vec)))
