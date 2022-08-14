package main

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"sda-pipeline/internal/config"

	"github.com/gorilla/mux"

	log "github.com/sirupsen/logrus"
)

var Conf *config.Config
var err error

func main() {
	Conf, err = config.NewConfig("api")
	if err != nil {
		log.Fatal(err)
	}

	srv := setup(Conf)

	if Conf.API.ServerCert != "" && Conf.API.ServerKey != "" {
		log.Infof("Web server is ready to receive connections at https://%s:%d", Conf.API.Host, Conf.API.Port)
		if err := srv.ListenAndServeTLS(Conf.API.ServerCert, Conf.API.ServerKey); err != nil {
			log.Fatalln(err)
		}
	} else {
		log.Infof("Web server is ready to receive connections at http://%s:%d", Conf.API.Host, Conf.API.Port)
		if err := srv.ListenAndServe(); err != nil {
			log.Fatalln(err)
		}
	}
}

func setup(config *config.Config) *http.Server {
	r := mux.NewRouter().SkipClean(true)

	r.HandleFunc("/ready", readinessResponse).Methods("GET")

	cfg := &tls.Config{
		MinVersion:               tls.VersionTLS12,
		CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		},
	}

	srv := &http.Server{
		Addr:              config.API.Host + ":" + fmt.Sprint(config.API.Port),
		Handler:           r,
		TLSConfig:         cfg,
		TLSNextProto:      make(map[string]func(*http.Server, *tls.Conn, http.Handler)),
		ReadHeaderTimeout: 20 * time.Second,
		ReadTimeout:       5 * time.Minute,
		WriteTimeout:      20 * time.Second,
	}

	return srv
}

func readinessResponse(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
