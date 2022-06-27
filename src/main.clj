(ns main
  (:require [cognitect.http-client :as http-client]
            [cognitect.aws.http :as aws-http]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as credentials])
  (:use [clojure.pprint]))

(defn submit
  "In this example, additional headers are added on the request object"
  [client request channel]
  (let [request-with-custom-headers (assoc-in request [:headers :custom-header] "value")]
    (pprint request-with-custom-headers)
    (http-client/submit client request-with-custom-headers channel)))

(defn stop
  [client]
  (http-client/stop client))

(def cognitect-client (http-client/create {:trust-all true}))

(def client (reify aws-http/HttpClient
              (-submit [_ request channel] (submit cognitect-client request channel))
              (-stop [_] (stop cognitect-client))))

(def s3 (aws/client {:api         :s3
                     :http-client client
                     :credentials-provider (credentials/profile-credentials-provider "aws-api")}))

(aws/ops s3)
(aws/invoke s3 {:op :ListBuckets})

(def bucket-name "test-from-aws-explore")

(aws/invoke s3 {:op :CreateBucket :request {:Bucket bucket-name}})

(aws/invoke s3 {:op :ListObjectsV2 :request {:Bucket bucket-name}})


