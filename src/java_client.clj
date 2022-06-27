(ns java-client
  (:require [cognitect.http-client :as http-client]
            [clojure.core.async :refer (put!) :as a]
            [cognitect.aws.http :as aws-http]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as credentials]
            [clojure.pprint :as pprint]
            [clojure.string :as str])
  (:import (java.net.http
             HttpClient
             HttpClient$Version
             HttpRequest
             HttpRequest$BodyPublishers HttpResponse)
           (java.time Duration)
           (java.net URI)))

; Do an example where the submit puts an anamoly in the channel
(defn build-aws-request
  [{:keys [request-method headers uri body server-name]}]
  (let [request (-> (HttpRequest/newBuilder)
                    (.headers headers)
                    (.method (str/upper-case (name request-method)) (HttpRequest$BodyPublishers/ofString body))
                    (.uri (URI/create (str server-name uri)))
                    (.build))]
    (println request)
    request))

(def java-http-client (-> (HttpClient/newBuilder)
                      (.version HttpClient$Version/HTTP_1_1)
                      (.connectTimeout (Duration/ofSeconds 10))
                      (.build)))

(defn response->map [^HttpResponse resp]
  {:status (.statusCode resp)
   :body (.body resp)
   :version "HTTP_1_1"
   :headers (into {}
                  (map (fn [[k v]] [k (if (> (count v) 1) (vec v) (first v))]))
                  (.map (.headers resp)))})

(comment
  (defn- on-complete
    "Helper for submit. Builds error map if submit failed, or Ring
  response map if submit succeeded."
    [state ^Result result request]
    (merge (if (.isFailed result)
             (error->anomaly (.getFailure result))
             state)
           (select-keys request [::meta]))))

(defn submit
  [client request channel]
  (let [aws-request (build-aws-request request)
        aws-response (.send client aws-request)]
    (print aws-request)
    (println aws-response)
    (put! channel (response->map aws-response))))

(defn stop
  [client]
  (println "Do nothing"))

(def client (reify aws-http/HttpClient
              (-submit [_ request channel] (submit java-http-client request channel))
              (-stop [_] (stop java-http-client))))

(def s3 (aws/client {:api                  :s3
                     :http-client client
                     :credentials-provider (credentials/profile-credentials-provider "aws-api")}))


(aws/ops s3)
(aws/invoke s3 {:op :ListBuckets})

(def bucket-name "test-from-aws-explore")

(aws/invoke s3 {:op :CreateBucket :request {:Bucket bucket-name}})

(aws/invoke s3 {:op :ListObjectsV2 :request {:Bucket bucket-name}})

(def request (->
               (HttpRequest/newBuilder)
               (.method (str/upper-case (name "Get")) (HttpRequest$BodyPublishers/ofString "{}"))
               (.uri (URI/create "http://www.google.com"))
               (.build)))

(.send java-http-client request nil)

