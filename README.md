# Franklin

[![](docs/franklin.png)](#)

> A friendly turtle-y companion to harness the power of DynamoDB.

[![Clojars Project](https://img.shields.io/clojars/v/ctorrisi/franklin.svg)](https://clojars.org/ctorrisi/franklin)
[![cljdoc badge](https://cljdoc.org/badge/ctorrisi/franklin)](https://cljdoc.org/d/ctorrisi/franklin)

## Usage

Leiningen and Boot
```clojure
[ctorrisi/franklin "0.0.1-alpha12"]
```

deps
```clojure
ctorrisi/franklin {:mvn/version "0.0.1-alpha12"}
```

## Friendly?

* minimal dependencies (only depends on [aws-api])
* idiomatic clojure
* table-centric query and persistence operations
* sensible defaults
* simple and easy

## Non-goals

* Supporting legacy operations
* Coverage of all operations

Use [aws-api] for legacy operations and operations not related to queries or persistence.

## Operations

All operations are centred around being executed on a single table (table-centric).

```clojure
(ns franklin.example (:require [franklin.core :as f]))
```

In order to demonstrate these operations, assume the following table named ``user_location`` exists with a ``String`` typed partition key named ``username`` and a ``Number`` typed sort key named ``tstamp``.

## Concurrency

All operations can be either synchronous or asynchronous.

### Synchronous by default
* query
* scan
* query-first-item
* query-last-item
* get-item
* batch-get-item

#### Sync -> Async

`assoc` a `clojure.core.async` channel to the `ch` key of the `item-opts` map.

The result can be received asynchronously from the specified channel in `ch`.

```clojure
(def c (clojure.core.async/chan))

(f/query ctx {:partition-key "pk1"
              :ch c})
```

### Asynchronous by default
* put-item
* update-item
* delete-item
* batch-write-item

The default `clojure.core.async` channel that is returned from these functions can be overridden by providing the `ch` key in the `item-opts` map.

#### Async -> Sync

`assoc` a truthy value to the `async?` key of your `item-opts`.

```clojure
(def c (clojure.core.async/chan))

(f/put-item ctx {:item {:username "uname1"
                        :tstamp 100}
                 :ch c ; optional
                 :async? true})
```

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
(def ctx (f/make-table-context "user_location" {:client ddb-client}))
(def other-ctx (f/make-table-context "user_details" {:client ddb-client}))
```

### put-item
```clojure
(f/put-item ctx {:item {:username "corey"
                        :tstamp 100
                        :latitude -37.813629
                        :longitude 144.963058}})
```

### update-item
```clojure
(f/update-item ctx {:key {:username "corey"
                          :tstamp 100}
                    :update-expr "set latitude = :lat"
                    :expr-attr-vals {:lat -37.809010}})
```

### get-item
```clojure
(f/get-item ctx {:key {:username "corey"
                       :tstamp 100}})

=> {:username "corey"
    :tstamp 100
    :latitude -37.80901
    :longitude 144.963058}
    
(f/get-item ctx {:key {:username "corey"
                       :tstamp 101}})

=> {}
```

### query

```clojure
(f/query ctx {:partition-key "corey"
              :sort-key      100})

=> {:Items [{:tstamp 100 :username "corey" :latitude -37.813629 :longitude 144.963058}]
    :Count 1
    :ScannedCount 1}

(f/query ctx {:partition-key "corey"
              :sort-key      {:between [99 101]}
              :projections   [:latitude "longitude"]})

=> {:Items [{:latitude -37.813629 :longitude 144.963058}]
    :Count 1
    :ScannedCount 1}
```

#### sort-key options

Supports all of the comparison operators available in [DynamoDB's Query Key Condition Expression](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html).

```clojure
{:sort-key {:= x}}
{:sort-key x} ; equals is implicit, same as above.
{:sort-key {:< x}}
{:sort-key {:<= x}}
{:sort-key {:> x}}
{:sort-key {:>= x}}
{:sort-key {:between [x y]}}
{:sort-key {:begins-with x}}
; also supports a string variant for the comparison operator, i.e.
{:sort-key {"=" x}}
{:sort-key {"between" [x y]}}
```

### scan
```clojure
(f/scan ctx)

=> {:Items [{:username "corey" :latitude -37.80901 :longitude 144.963058 :tstamp 100}]
    :Count 1
    :ScannedCount 1}
```

### batch-write-item
To delete an item, ``assoc`` the ``:delete?`` key with a truthy value in the item's map.
```clojure
(f/batch-write-item ctx {:items [{:username "alice"
                                  :tstamp 100}
                                 {:username "bob"
                                  :tstamp 100}
                                 {:username "corey"
                                  :tstamp 200}
                                 {:username "corey"
                                  :tstamp 300}
                                 {:username "corey"
                                  :tstamp 100 
                                  :delete? true}]})

(f/scan ctx)

=> {:Items [{:tstamp 100 :username "alice"}
            {:tstamp 100 :username "bob"}
            {:tstamp 200 :username "corey"}
            {:tstamp 300 :username "corey"}]
    :Count 4
    :ScannedCount 4}
```

### batch-get-item
```clojure
(f/batch-get-item ctx {:keys [{:username  "alice"
                               :tstamp 100}
                              {:username  "bob"
                               :tstamp 100}]})

=> {:Responses {:user_location [{:tstamp 100 :username "alice"}
                                {:tstamp 100 :username "bob"}]}
    :UnprocessedKeys {}}
```

### delete-item
```clojure
(f/delete-item ctx {:key {:username "alice"
                          :tstamp 100}})

(f/scan ctx)

=> {:Items [{:tstamp 100 :username "bob"}
            {:tstamp 200 :username "corey"}
            {:tstamp 300 :username "corey"}]
    :Count 3
    :ScannedCount 3}
```

### query-first-item
```clojure
(f/query-first-item ctx {:partition-key "corey"})

=> {:tstamp 200 :username "corey"}
```

### query-last-item
```clojure
(f/query-last-item ctx {:partition-key "corey"})

=> {:tstamp 300 :username "corey"}
```

## Credits

Thanks to:
* [Peter Taoussanis](https://github.com/ptaoussanis) for [Faraday](https://github.com/ptaoussanis/faraday) and some inspiration for this library.
* [James Reeves](https://github.com/weavejester) for [Rotary](https://github.com/weavejester/rotary) which Faraday was adapted from.
* [Cognitect](https://github.com/cognitect) for [aws-api].
* [Pixabay](https://pixabay.com) for the stock graphics.

## Licence

Distributed under the [Eclipse Public License - v 2.0](https://raw.githubusercontent.com/ctorrisi/franklin/master/LICENSE)

Copyright &copy; 2019 Corey Torrisi

[aws-api]: https://github.com/cognitect-labs/aws-api/
[aws-api-client]: https://github.com/cognitect-labs/aws-api/blob/master/src/cognitect/aws/client/api.clj#L22 "``cognitect.aws.client.api/client``"
