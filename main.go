package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/elastic/go-elasticsearch/v8"
)

type AddDocRequest struct {
	Index string                 `json:"index"`
	ID    string                 `json:"id"`
	Doc   map[string]interface{} `json:"doc"`
}

type DocHit struct {
	ID     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source"`
}

type DocsResponse struct {
	Hits struct {
		Hits []DocHit `json:"hits"`
	} `json:"hits"`
}

var es *elasticsearch.Client

func main() {
	var err error
	cfg := elasticsearch.Config{
		Addresses: []string{
			getEnv("ELASTICSEARCH_URL", "http://localhost:9200"),
		},
	}
	es, err = elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the Elasticsearch client: %s", err)
	}

	http.HandleFunc("/docs", handleGetDocs)
	http.HandleFunc("/doc", handleAddDoc)
	http.HandleFunc("/alldocs", handleGetAllDocs)
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pong"))
	})
	log.Println("Server running on :8080")
	log.Fatal(http.ListenAndServe(":8080", corsMiddleware(http.DefaultServeMux)))
}

func handleAddDoc(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req AddDocRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	docBytes, _ := json.Marshal(req.Doc)
	res, err := es.Index(
		req.Index,
		bytes.NewReader(docBytes),
		es.Index.WithDocumentID(req.ID),
		es.Index.WithRefresh("true"),
	)
	if err != nil || res.IsError() {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func handleGetDocs(w http.ResponseWriter, r *http.Request) {
	index := r.URL.Query().Get("index")
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex(index),
		es.Search.WithBody(&buf),
		es.Search.WithTrackTotalHits(true),
		es.Search.WithPretty(),
	)
	if err != nil || res.IsError() {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer res.Body.Close()
	var esResp map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&esResp); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var hits []DocHit
	for _, hit := range esResp["hits"].(map[string]interface{})["hits"].([]interface{}) {
		h := hit.(map[string]interface{})
		hits = append(hits, DocHit{
			ID:     h["_id"].(string),
			Source: h["_source"].(map[string]interface{}),
		})
	}
	resp := DocsResponse{}
	resp.Hits.Hits = hits
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func handleGetAllDocs(w http.ResponseWriter, r *http.Request) {
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("*"),
		es.Search.WithBody(&buf),
		es.Search.WithTrackTotalHits(true),
		es.Search.WithPretty(),
	)
	if err != nil || res.IsError() {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer res.Body.Close()
	var esResp map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&esResp); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var hits []DocHit
	for _, hit := range esResp["hits"].(map[string]interface{})["hits"].([]interface{}) {
		h := hit.(map[string]interface{})
		hits = append(hits, DocHit{
			ID:     h["_id"].(string),
			Source: h["_source"].(map[string]interface{}),
		})
	}
	resp := DocsResponse{}
	resp.Hits.Hits = hits
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
} 
