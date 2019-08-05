# Franklin

[![](docs/franklin.png)](#)

> A friendly turtle to harness the power of DynamoDB.

[![Clojars Project](https://img.shields.io/clojars/v/ctorrisi/franklin.svg)](https://clojars.org/ctorrisi/franklin)
[![cljdoc badge](https://cljdoc.org/badge/ctorrisi/franklin)](https://cljdoc.org/d/ctorrisi/franklin/CURRENT)

## Usage

Leiningen and Boot
```clojure
[ctorrisi/franklin "0.0.1-alpha1"]
```

deps
```clojure
{:deps {ctorrisi/franklin {:mvn/version "0.0.1-alpha1"}}}
```

## Friendly?

* minimal dependencies (only depends on [aws-api])
* interact with clojure data types
* provide table-centric queries and persistence operations
* provide sensible defaults
* simple **and** easy

## Non-goals

* Supporting legacy operations
* Full API coverage

Use [aws-api] for legacy operations and operations not related to querying or persistence.

## Operations

All operations are centred around being executed on a single table (table-centric).

```clojure
(ns franklin.example (:require [franklin.core :as f]))
```

In order to demonstrate these operations, assume the following table named ``user_location`` exists with a ``String`` typed partition key named ``user_name`` and a ``Number`` typed sort key named ``time_stamp``.

### make-table-context

The minimum to create a table context for a table named ``user_location``.

```clojure
(def user-location-ctx (f/make-table-context "user_location"))
```

The definition above is fine if your program is using a single table with default AWS connection/credential options.

### make-client

``make-client`` is a convenience function that wraps [``cognitect.aws.client.api/client``](https://github.com/cognitect-labs/aws-api/blob/master/src/cognitect/aws/client/api.clj#L22) with ``{:api :dynamodb}``.

```clojure
(def ddb-client (f/make-client {:region "ap-southeast-2"}))
(def ctx (f/make-table-context "user-location" {:client ddb-client}))
(def other-ctx (f/make-table-context "user-details" {:client ddb-client}))
```

### put-item
```clojure
(f/put-item ctx {:item {:user_name "corey"
                        :time_stamp 1564641545000
                        :latitude -37.813629
                        :longitude 144.963058}})
```

### update-item
```clojure
(f/update-item ctx {:key {:user_name "corey"
                          :time_stamp 1564641545000}
                          :update-expr "set latitude = :lat"
                          :expr-attr-vals {":lat" -37.809010}})
```

### get-item
```clojure
(f/get-item ctx {:key {:user_name "corey"
                       :time_stamp 1564641545000}})

=> {:Item {:user_name "corey"
           :time_stamp 1564641545000
           :latitude -37.80901
           :longitude 144.963058}}
```

### scan
```clojure
(f/scan ctx)

=> {:Items [{:user_name "corey" :latitude -37.80901 :longitude 144.963058 :time_stamp 1564641545000}]
    :Count 1
    :ScannedCount 1}
```

### query
```clojure
(f/query ctx {:partition-key "corey"
              :sort-key      {:comparator "="  ; default
                              :key-1 1564641545000}})

=> {:Items [{:time_stamp 1564641545000 :user_name "corey" :latitude -37.813629 :longitude 144.963058}]
    :Count 1
    :ScannedCount 1}

(f/query ctx {:partition-key "corey"
              :projections   [:latitude "longitude"]})

=> {:Items [{:latitude -37.813629 :longitude 144.963058}]
    :Count 1
    :ScannedCount 1}
```

### batch-write-item
To delete an item, ``assoc`` the ``:delete?`` key with a truthy value in the item's map.
```clojure
(f/batch-write-item ctx {:items [{:user_name  "alice"
                                  :time_stamp 1564641565850}
                                 {:user_name  "bob"
                                  :time_stamp 1564641575140}
                                 {:user_name  "corey"
                                  :time_stamp 100}
                                 {:user_name  "corey"
                                  :time_stamp 1564641545000
                                  :delete?    true}]})

(f/scan ctx)

=> {:Items [{:time_stamp 1564641575140, :user_name "bob"}
            {:time_stamp 100, :user_name "corey"}
            {:time_stamp 1564641565850, :user_name "alice"}]
    :Count 3,
    :ScannedCount 3}
```

### batch-get-item
```clojure
(f/batch-get-item ctx {:keys [{:user_name  "alice"
                               :time_stamp 1564641565850}
                              {:user_name  "bob"
                               :time_stamp 1564641575140}]})

=> {:Responses {:user_location [{:time_stamp 1564641565850, :user_name "alice"}
                                {:time_stamp 1564641575140, :user_name "bob"}]}
    :UnprocessedKeys {}}
```

## Credits

Thanks to:
* [Peter Taoussanis](https://github.com/ptaoussanis) for [Faraday](https://github.com/ptaoussanis/faraday) and some inspiration for this library.
* [James Reeves](https://github.com/weavejester) for [Rotary](https://github.com/weavejester/rotary) that Faraday was adapted from.
* [Cognitect](https://github.com/cognitect) for [aws-api].
* [Pixabay](https://pixabay.com) for the stock graphics.

## Licence

Distributed under the [Eclipse Public License - v 2.0](https://raw.githubusercontent.com/ctorrisi/franklin/master/LICENSE)

Copyright &copy; 2019 Corey Torrisi

[aws-api]: https://github.com/cognitect-labs/aws-api/
[aws-api-client]: https://github.com/cognitect-labs/aws-api/blob/master/src/cognitect/aws/client/api.clj#L22 "``cognitect.aws.client.api/client``"
