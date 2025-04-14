package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/prometheus/prometheus/prompb"
	"io"
	"log"
	"net/http"
	"net/http/httputil"

	"github.com/caarlos0/env/v11"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto" // ← こちらのレガシーパッケージを使う
	"github.com/golang/snappy"

	// OTel 用
	protoLogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	protoMetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	protoTrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	googleprotojson "google.golang.org/protobuf/encoding/protojson"
	googleproto "google.golang.org/protobuf/proto"
)

var config Config

type Config struct {
	ListenAddr      string `env:"LISTEN_ADDR" envDefault:":4318"`
	Endpoint        string `env:"ENDPOINT"`
	LogsEndpoint    string `env:"LOGS_ENDPOINT"`
	TracesEndpoint  string `env:"TRACES_ENDPOINT"`
	MetricsEndpoint string `env:"METRICS_ENDPOINT"`
}

func (c *Config) Init() {
	if c.LogsEndpoint == "" && c.Endpoint != "" {
		c.LogsEndpoint = c.Endpoint + "/v1/logs"
	}
	if c.TracesEndpoint == "" && c.Endpoint != "" {
		c.TracesEndpoint = c.Endpoint + "/v1/traces"
	}
	if c.MetricsEndpoint == "" && c.Endpoint != "" {
		c.MetricsEndpoint = c.Endpoint + "/v1/metrics"
	}
}

type protoRequestResponse struct {
	request  googleproto.Message
	response googleproto.Message
}

func getProtoRequestResponse(tp string) protoRequestResponse {
	switch tp {
	case "logs":
		return protoRequestResponse{
			request:  &protoLogs.ExportLogsServiceRequest{},
			response: &protoLogs.ExportLogsServiceResponse{},
		}
	case "traces":
		return protoRequestResponse{
			request:  &protoTrace.ExportTraceServiceRequest{},
			response: &protoTrace.ExportTraceServiceResponse{},
		}
	case "metrics":
		return protoRequestResponse{
			request:  &protoMetrics.ExportMetricsServiceRequest{},
			response: &protoMetrics.ExportMetricsServiceResponse{},
		}
	}
	panic("invalid type")
}

func main() {
	if err := env.Parse(&config); err != nil {
		log.Fatalf("Failed to parse environment variables: %v", err)
	}
	config.Init()
	logConfiguredEndpoints(config)

	http.HandleFunc("/v1/logs", handleLogs)
	http.HandleFunc("/v1/traces", handleTraces)
	http.HandleFunc("/v1/metrics", handleMetrics)
	log.Println("Starting OTLP/HTTP spy on ", config.ListenAddr)
	log.Fatal(http.ListenAndServe(config.ListenAddr, nil))
}

func handleLogs(w http.ResponseWriter, r *http.Request) {
	handleRequest(w, r, getProtoRequestResponse("logs"), config.LogsEndpoint)
}

func handleTraces(w http.ResponseWriter, r *http.Request) {
	handleRequest(w, r, getProtoRequestResponse("traces"), config.TracesEndpoint)
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	handleRequest(w, r, getProtoRequestResponse("metrics"), config.MetricsEndpoint)
}

func handleRequest(w http.ResponseWriter, r *http.Request, protoMessage protoRequestResponse, forwardTo string) {
	buf := &bytes.Buffer{}

	logRequestReceived(buf, r.URL.Path)
	logHTTPRequest(buf, r)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// 圧縮形式に応じて解凍後のデータを取得（解凍が必要であれば）
	encoding := r.Header.Get("Content-Encoding")
	log.Println("Content-Encoding...:", encoding)
	uncompressedReqBody, err := maybeDecompress(body, encoding)
	if err != nil {
		log.Printf("解凍に失敗しました（エンコーディング: %s）: %v", encoding, err)
		uncompressedReqBody = body
	}
	remoteWriteVer := r.Header.Get("X-Prometheus-Remote-Write-Version")
	if remoteWriteVer == "0.1.0" {
		log.Println("Detected Prometheus Remote Write:", remoteWriteVer)
		handleRemoteWriteDecode(buf, uncompressedReqBody)
	} else {
		otlpProto := getProtoRequestResponse("metrics")
		if err := googleproto.Unmarshal(uncompressedReqBody, otlpProto.request); err != nil {
			log.Printf("Failed to parse OTLP data: %v", err)
			dumpBody("Request (raw, failed to parse)", uncompressedReqBody)
		} else {
			// デコード成功 -> JSON 表示
			logProtoMessage(buf, otlpProto.request, "Decoded OTLP Request")
		}
	}
	if forwardTo == "" {
		w.WriteHeader(http.StatusOK)
		log.Println(buf.String())
		return
	}

	resp, err := forwardRequest(buf, forwardTo, body, r)
	if err != nil {
		log.Printf("Forwarding failed: %v", err)
		http.Error(w, "failed to forward request", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Failed to read response body: %v", err)
		http.Error(w, "failed to read response", http.StatusInternalServerError)
		return
	}

	if err := googleproto.Unmarshal(respBytes, protoMessage.response); err != nil {
		logRawResponseBody(buf, respBytes)
	} else {
		logProtoMessage(buf, protoMessage.response, "Response")
	}

	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	w.WriteHeader(resp.StatusCode)
	if _, err := w.Write(respBytes); err != nil {
		log.Printf("Failed to write response body to client: %v", err)
	}
	log.Println(buf.String())
}

func logConfiguredEndpoints(cfg Config) {
	if cfg.LogsEndpoint != "" {
		log.Println("[Proxy] Logs will be forwarded to    =>", cfg.LogsEndpoint)
	}
	if cfg.TracesEndpoint != "" {
		log.Println("[Proxy] Traces will be forwarded to  =>", cfg.TracesEndpoint)
	}
	if cfg.MetricsEndpoint != "" {
		log.Println("[Proxy] Metrics will be forwarded to =>", cfg.MetricsEndpoint)
	}
}

func logRequestReceived(w io.Writer, path string) {
	fmt.Fprintln(w, "===> Received OTLP request: ", path)
	fmt.Fprintln(w, "")
}

func logHTTPRequest(w io.Writer, req *http.Request) {
	dump, err := httputil.DumpRequest(req, false)
	if err != nil {
		log.Println("Failed to dump request: ", err)
		return
	}

	fmt.Fprint(w, "=== HTTP Request Headers ===\n\n")
	fmt.Fprintln(w, string(dump))
}

func logHTTPResponse(w io.Writer, resp *http.Response) {
	fmt.Fprint(w, "=== Forwarded Response Headers ===\n\n")
	dump, err := httputil.DumpResponse(resp, false)
	if err != nil {
		log.Println("Failed to dump response: ", err)
		return
	}
	fmt.Fprintln(w, string(dump))
}

func logRawResponseBody(w io.Writer, respData []byte) {
	fmt.Fprint(w, "=== Raw Response ===\n\n")
	fmt.Fprintln(w, string(respData))
	fmt.Fprintln(w, "")
}

func logProtoMessage(w io.Writer, m googleproto.Message, t string) {
	message, err := marshalProtoMessage(m)
	if err != nil {
		log.Printf("Failed to marshal to JSON: %v", err)
	}
	fmt.Fprintf(w, "=== OTLP Message (%s) ===\n\n", t)
	fmt.Fprintln(w, string(message))
	fmt.Fprintln(w, "")
}

func marshalProtoMessage(m googleproto.Message) ([]byte, error) {
	return googleprotojson.MarshalOptions{
		Multiline: true,
		Indent:    "  ",
	}.Marshal(m)
}

func forwardRequest(buf io.Writer, endpoint string, body []byte, original *http.Request) (*http.Response, error) {
	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create forward request: %w", err)
	}

	for key, values := range original.Header {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	logHTTPResponse(buf, resp)
	return resp, nil
}

func maybeDecompress(data []byte, encoding string) ([]byte, error) {
	switch encoding {
	case "gzip":
		r, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	case "snappy":
		r, err := snappy.Decode(nil, data)
		if err != nil {
			return nil, err
		}
		return r, nil
	default:
		return data, nil
	}
}

func dumpBody(prefix string, data []byte) {
	log.Printf("=== %s Dump ===\n%s", prefix, string(data))
}

func handleRemoteWriteDecode(w io.Writer, data []byte) {
	// Remote Write (prompb.WriteRequest) は古い protoc 生成の場合が多いので
	// github.com/golang/protobuf/proto を使う
	var req prompb.WriteRequest
	if err := proto.Unmarshal(data, &req); err != nil {
		log.Printf("Failed to parse Prometheus remote write data: %v", err)
		dumpBody("Remote Write (raw, failed to parse)", data)
		return
	}
	var marshaler = &jsonpb.Marshaler{Indent: "  "}
	var buf bytes.Buffer
	err := marshaler.Marshal(&buf, &req)
	if err != nil {
		log.Printf("Failed to marshal WriteRequest to JSON: %v", err)
		dumpBody("Remote Write (raw, marshal error)", data)
		return
	}

	fmt.Fprintf(w, "=== Remote Write (Decoded) ===\n\n")
	fmt.Fprintln(w, buf.String())
	fmt.Fprintln(w, "")
}
