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
            [cognitect.aws.client.api :as api]
            [cognitect.aws.client.api.async :as aws]
            [clojure.core.async :as a])
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
(defn- expr-attr-val-key [k]
  (if (keyword? k) (str k) k))

(def ^:private ddb-item->clj-item (partial map-key-val-fns keyword ddb->clj))
(def ^:private clj-item->ddb-item (partial map-key-val-fns clj->ddb))
(def ^:private clj-expr-attr-vals->ddb-item (partial map-key-val-fns expr-attr-val-key clj->ddb))

(defn make-client [& [opts]] (api/client (assoc opts :api :dynamodb)))
(defn- get-client [opts] (or (:client opts) (make-client opts)))

(def ^:private max-retries 30)
(def ^:private base-delay 500)
(def ^:private max-delay 20000)

(defn- backoff
  "Parameters and EqualJitterBackoffStrategy based on `aws-sdk-java`'s
  `com.amazonaws.retry.PredefinedBackoffStrategies`."
  [retries]
  (when (< retries max-retries)
    (let [retries (min retries max-retries)
          delay (min (* (bit-shift-left 1 retries) base-delay) max-delay)
          half-delay (/ delay 2)]
      (+ half-delay (rand-int (+ half-delay 1))))))

(defn- default-retriable?
  "This `default-retriable?` differs from `cognitect.aws.retry/default-retriable?` to include throttled requests."
  [response]
  (or (seq (:UnprocessedItems response))
      (contains? #{:cognitect.anomalies/busy
                   :cognitect.anomalies/unavailable}
                 (:cognitect.anomalies/category response))
      (contains? #{"com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException"
                   "com.amazonaws.dynamodb.v20120810#ThrottlingException"}
                 (:__type response))))

(defn invoke
  "Wraps aws-api's async invoke function accepts `client-opts` consisting of either a `cognitect.aws.client.api/client`
  config map, or a map containing:

  :client    - An existing aws-api DynamoDB client, consistent with Franklin's table context.

  Alpha. Subject to change."
  [op client-opts request & [ch]]
  (aws/invoke (get-client client-opts) {:op         op
                                        :ch         ch
                                        :request    request
                                        :retriable? default-retriable?
                                        :backoff    backoff}))

(def ^:private invoke-describe-table  (partial invoke :DescribeTable))
(def ^:private invoke-query           (partial invoke :Query))
(def ^:private invoke-scan            (partial invoke :Scan))
(def ^:private invoke-put             (partial invoke :PutItem))
(def ^:private invoke-update          (partial invoke :UpdateItem))
(def ^:private invoke-delete          (partial invoke :DeleteItem))
(def ^:private invoke-get             (partial invoke :GetItem))
(def ^:private invoke-batch-write     (partial invoke :BatchWriteItem))
(def ^:private invoke-batch-get       (partial invoke :BatchGetItem))

(defn describe-table
  "DescribeTable request.

  Alpha. Subject to change."
  [{:keys [table-name] :as table-context}]
  (a/<!! (invoke-describe-table table-context {:TableName table-name})))

(defn- describe-key-schema
  "Creates a mapped key-value pair of either :partition-key-name or :sort-key-name,
   if the `KeyType` is HASH or RANGE, respectively."
  [{:keys [AttributeName KeyType]}]
  {(if (= KeyType "HASH") :partition-key-name :sort-key-name) AttributeName})

(defn- describe-single-index [v]
  (let [key-schema    (->> (:KeySchema v)
                           (mapv #(describe-key-schema %))
                           (into {}))
        key-keywords  (->> (vals key-schema)
                           (map keyword)
                           (vec))]
    (assoc key-schema :key-keywords key-keywords)))

(defn- describe-index-key-schema [m]
  (persistent!
    (reduce-kv (fn [m _ v]
                 (let [index-name (keyword (:IndexName v))]
                   (assoc! m index-name (describe-single-index v))))
               (transient {})
               m)))

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
        table-description (:Table (describe-table table-context))
        primary-index (describe-single-index table-description)
        global-secondary-indices (describe-index-key-schema (:GlobalSecondaryIndexes table-description))]
    (merge table-context
           {:primary-index primary-index}
           {:global-secondary-indices global-secondary-indices})))

(def ^:private equality-pattern     (re-pattern #"^(=|<|<=|>|>=)$"))
(def ^:private between-pattern      (re-pattern #"^(?i)between$"))
(def ^:private begins-with-pattern  (re-pattern #"^(?i)begins(-|_)with$"))

(defn- make-sort-key-expr
  "Given a `sort-key-name` and `comparator`, constructs a sort key expression string for the DynamoDB request."
  [sort-key-name comparator]
  (condp #(some? (re-matches %1 %2)) comparator
    equality-pattern (s/join " " [sort-key-name comparator ":sk1"])
    between-pattern (str sort-key-name " BETWEEN :sk1 AND :sk2")
    begins-with-pattern (str "begins_with (" sort-key-name ", :sk1)")
    (throw (Exception. (format "Invalid sort key comparison-operator %s" comparator)))))

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
  [{:keys [primary-index global-secondary-indices] :as table-context}
   {:keys [partition-key index-name sort-key descending?] :as query-opts}]
  (let [base-request (make-scan-query-base-request table-context query-opts)
        sort-key-kv (when (map? sort-key) (first sort-key))
        comparator (if sort-key-kv (name (key sort-key-kv)) "=")
        secondary-index (when index-name (get global-secondary-indices (keyword index-name)))
        sort-key-name (when sort-key
                        (-> (or secondary-index primary-index)
                            (:sort-key-name)))
        partition-key-name (-> (or secondary-index primary-index)
                               (:partition-key-name))
        key-condition-expr (str (str partition-key-name " = :pk")
                                (when sort-key (str " AND " (make-sort-key-expr sort-key-name comparator))))
        expr-attr-vals (merge {":pk" (clj->ddb partition-key)}
                              (when sort-key
                                (if-let [between-vals (or (:between sort-key) (get sort-key "between"))]
                                  (zipmap [":sk1" ":sk2"] (map #(clj->ddb %) between-vals))
                                  (let [sort-key-val (if sort-key-kv (val sort-key-kv) sort-key)]
                                    {":sk1" (clj->ddb sort-key-val)})))
                              (:ExpressionAttributeValues base-request))]
    (assoc base-request :KeyConditionExpression key-condition-expr
                        :ExpressionAttributeValues expr-attr-vals
                        :ScanIndexForward (when descending? false))))

(defn- make-response
  "Makes a response for data manipulation operations, which are asynchronous by default.

  If `sync?` is truthy, the caller thread is blocked until a result is returned.
  Otherwise an async channel is returned."
  [sync? response]
  (if sync? (a/<!! response) response))

(defn- xform-make-response
  "Makes a response for retrieval operations, which are synchronous by default.

  Applies a function `f` to the DynamoDB response.

  If `ch` the result is returned to the channel `ch`.
  Otherwise the result is returned to the caller thread."
  [f response ch]
  (if ch
    (do (a/go (a/>! ch (f (a/<! response)))) ch)
    (f (a/<!! response))))

(def ^:private mapv-ddb-items->clj-items  (partial mapv ddb-item->clj-item))
(def ^:private make-scan-query-response   (partial xform-make-response #(update % :Items mapv-ddb-items->clj-items)))
(def ^:private make-get-item-response     (partial xform-make-response #(:Item (update % :Item ddb-item->clj-item))))
(def ^:private make-single-item-response  (partial xform-make-response #(ddb-item->clj-item (first (:Items %)))))

(defn query
  "Query request with items converted to Clojure data types.

  Alpha. Subject to change."
  [table-context
   {:keys [ch] :as query-opts}]
  (let [request (make-query-request table-context query-opts)
        response (invoke-query table-context request)]
    (make-scan-query-response response ch)))

(defn scan
  "Scan request with items converted to Clojure data types.

  Alpha. Subject to change."
  ([table-context & [{:keys [segment total-segments ch] :as scan-opts}]]
   (let [request (-> (make-scan-query-base-request table-context (or scan-opts {}))
                     (assoc :Segment segment
                            :TotalSegments total-segments))
         response (invoke-scan table-context request)]
     (make-scan-query-response response ch))))

(defn- query-item
  "Performs a sort based on `descending?` on the sort-key for the specified `partition-key`.

  Alpha. Subject to change."
  [descending? table-context {:keys [ch] :as query-opts}]
  (let [request (make-query-request table-context (assoc query-opts :descending? descending? :limit 1))
        response (invoke-query table-context request)]
    (make-single-item-response response ch)))

(def query-first-item (partial query-item false))
(def query-last-item (partial query-item true))

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
  [{:keys [index-name primary-index global-secondary-indices]}
   item]
  (->> (if index-name (get global-secondary-indices (keyword index-name)) primary-index)
       (:key-keywords)
       (select-keys item)
       (clj-item->ddb-item)))

(defn put-item
  "PutItem request.

  Alpha. Subject to change."
  [table-context
   {:keys [item key sync? ch] :as item-opts}]
  (let [request (-> (make-item-base-request table-context item-opts)
                    (assoc :Item (clj-item->ddb-item (or item key))))
        response (invoke-put table-context request ch)]
    (make-response sync? response)))

(defn update-item
  "UpdateItem request.

  Alpha. Subject to change."
  [table-context
   {:keys [item key update-expr sync? ch] :as item-opts}]
  (let [request (-> (make-item-base-request table-context item-opts)
                    (assoc :Key (clj-item->ddb-key table-context (or item key))
                           :UpdateExpression update-expr))
        response (invoke-update table-context request ch)]
    (make-response sync? response)))

(defn delete-item
  "DeleteItem request.

  Alpha. Subject to change."
  [table-context
   {:keys [item key sync? ch] :as item-opts}]
  (let [request (-> (make-item-base-request table-context item-opts)
                    (assoc :Key (clj-item->ddb-key table-context (or item key))))
        response (invoke-delete table-context request ch)]
    (make-response sync? response)))

(defn get-item
  "GetItem request.

  Alpha. Subject to change."
  [{:keys [table-name] :as table-context}
   {:keys [item key projections expr-attr-names return-cc ch]}]
  (let [request {:TableName                table-name
                 :Key                      (clj-item->ddb-key table-context (or item key))
                 :ProjectionExpression     (make-projection-expression projections)
                 :ExpressionAttributeNames expr-attr-names
                 :ReturnConsumedCapacity   return-cc}
        response (invoke-get table-context request)]
    (make-get-item-response response ch)))

(defn- make-batch-write-item-request-item
  "Constructs an individual PutRequest or DeleteRequest map based on `delete?` key in the item map.
  For convenience, `delete?` is reserved and may not be used as a partition or sort key when using `batch-write-item`."
  [table-context item]
  (if (:delete? item)
    {:DeleteRequest {:Key (clj-item->ddb-key table-context item)}}
    {:PutRequest {:Item (clj-item->ddb-item item)}}))

(defn batch-write-item
  "BatchWriteItem request.

  Alpha. Subject to change."
  [{:keys [table-name] :as table-context}
   {:keys [items return-cc return-item-coll-metrics sync? ch]}]
  (let [request-item-partitions (->> items
                                     (map #(make-batch-write-item-request-item table-context %))
                                     (partition-all 25))
        result-chan (or ch (a/chan))]
    (doseq [request-items request-item-partitions]
      (invoke-batch-write table-context {:RequestItems                {table-name request-items}
                                         :ReturnConsumedCapacity      return-cc
                                         :ReturnItemCollectionMetrics return-item-coll-metrics} result-chan))
    (if sync?
      (reduce conj
              []
              (repeatedly (count request-item-partitions)
                          #(a/<!! result-chan)))
      result-chan)))

(defn batch-get-item
  "BatchGetItem request.

  Alpha. Subject to change."
  [{:keys [table-name] :as table-context}
   {:keys [keys consistent? expr-attr-names projections ch]}]
  (let [request {:RequestItems {table-name {:Keys                     (mapv #(clj-item->ddb-item %) keys)
                                            :ConsistentRead           consistent?
                                            :ExpressionAttributeNames expr-attr-names
                                            :ProjectionExpression     (make-projection-expression projections)}}}
        response (invoke-batch-get table-context request)]
    (xform-make-response #(update-in % [:Responses (keyword table-name)] mapv-ddb-items->clj-items) response ch)))
