(ns custom-client
  (:require [cognitect.aws.http :as aws-http]
            [clojure.core.async :refer (put!)]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as credentials])
  (:import [java.net URI]
           [java.net.http
            HttpRequest
            HttpRequest$BodyPublishers
            HttpResponse$BodyHandlers
            HttpRequest$Builder
            HttpClient$Version
            HttpClient]
           (java.time Duration)
           (java.util.concurrent CompletableFuture)
           (java.util.function Function)))


(def method-string
     {:get "GET"
      :post "POST"
      :put "PUT"
      :head "HEAD"
      :delete "DELETE"
      :patch "PATCH"})

;Macro from https://github.com/schmee/java-http-clj/blob/72550432db9f146621acabd647db262da865740e/src/java_http_clj/util.clj#L16
(defmacro clj-fn->function ^Function [f]
  `(reify Function
     (~'apply [_# x#] (~f x#))))

; TODO: Allow InputStream and String types in body
(defn body->publisher
  [body]
  (if (nil? body)
    (HttpRequest$BodyPublishers/noBody)
    (HttpRequest$BodyPublishers/ofByteArray body)))

(defn request->complete-uri
  [{:keys [scheme server-name server-port uri query-string]}]
  (str (name scheme) "://"
       server-name
       (when server-port (str ":" server-port))
       uri
       (when query-string (str "?" query-string))))

(defn request-map->java-net-http-request
  [{:keys [headers body request-method scheme timeout-msec]
    :or   {scheme "https"} :as req-map}]
  (let [body-publisher (body->publisher body)
        ^HttpRequest req  (doto (HttpRequest/newBuilder)
              (.method ^String (method-string request-method) body-publisher)
              (.uri (URI/create (request->complete-uri req-map))))
        req (reduce-kv
              (fn [^HttpRequest$Builder req k v]
                (.header req ^String (name k) ^String v))
              req
              ; TODO: Remove host from unrestricted headers list
              (dissoc headers "host"))]
    (print (type (.build ^HttpRequest$Builder req)))
    (.build ^HttpRequest$Builder req)))


(defn submit
  [client
   request
   channel]
  (let [java-request' (request-map->java-net-http-request request)
        req' (.sendAsync client java-request' (HttpResponse$BodyHandlers/ofByteArray))]
    (.thenApply req' (clj-fn->function
                       (fn [result]
                         (put! channel (.body result)))))
    channel))

(defn stop [_] nil)

(def java-http-client '(-> (HttpClient/newBuilder)
                          (.version HttpClient$Version/HTTP_2)
                          (.connectTimeout (Duration/ofSeconds 10))
                          (.build)))

(def client (reify aws-http/HttpClient
              (-submit [_ request channel]
                (println request)
                (submit java-http-client request channel))
              (-stop [_] (stop java-http-client))))

(def s3 (aws/client {:api                  :s3
                     :http-client          client
                     :credentials-provider (credentials/profile-credentials-provider "aws-api")}))

(aws/ops s3)
(aws/invoke s3 {:op :ListBuckets})
