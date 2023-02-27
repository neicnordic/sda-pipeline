package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/config"
	"sda-pipeline/internal/database"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt"

	log "github.com/sirupsen/logrus"
)

var Conf *config.Config
var err error

func main() {
	Conf, err = config.NewConfig("api")
	if err != nil {
		log.Fatal(err)
	}
	Conf.API.MQ, err = broker.NewMQ(Conf.Broker)
	if err != nil {
		log.Fatal(err)
	}
	Conf.API.DB, err = database.NewDB(Conf.Database)
	if err != nil {
		log.Fatal(err)
	}

	Conf.API.JtwKeys = make(map[string][]byte)
	if Conf.API.JwtPubKeyPath != "" {
		if err := config.GetJwtKey(Conf.API.JwtPubKeyPath, Conf.API.JtwKeys); err != nil {
			log.Panicf("Error while getting key %s: %v", Conf.API.JwtPubKeyPath, err)
		}
	}

	sigc := make(chan os.Signal, 5)
	signal.Notify(sigc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-sigc
		shutdown()
		os.Exit(0)
	}()

	srv := setup(Conf)

	if Conf.API.ServerCert != "" && Conf.API.ServerKey != "" {
		log.Infof("Web server is ready to receive connections at https://%s:%d", Conf.API.Host, Conf.API.Port)
		if err := srv.ListenAndServeTLS(Conf.API.ServerCert, Conf.API.ServerKey); err != nil {
			shutdown()
			log.Fatalln(err)
		}
	} else {
		log.Infof("Web server is ready to receive connections at http://%s:%d", Conf.API.Host, Conf.API.Port)
		if err := srv.ListenAndServe(); err != nil {
			shutdown()
			log.Fatalln(err)
		}
	}
}

func setup(config *config.Config) *http.Server {

	r := gin.Default()

	r.GET("/ready", readinessResponse)
	r.GET("/files", getFiles)

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

func shutdown() {
	defer Conf.API.MQ.Channel.Close()
	defer Conf.API.MQ.Connection.Close()
	defer Conf.API.DB.Close()
}

func readinessResponse(c *gin.Context) {
	statusCocde := http.StatusOK

	if Conf.API.MQ.Connection.IsClosed() {
		statusCocde = http.StatusServiceUnavailable
		newConn, err := broker.NewMQ(Conf.Broker)
		if err != nil {
			log.Errorf("failed to reconnect to MQ, reason: %v", err)
		} else {
			Conf.API.MQ = newConn
		}
	}

	if Conf.API.MQ.Channel.IsClosed() {
		statusCocde = http.StatusServiceUnavailable
		Conf.API.MQ.Connection.Close()
		newConn, err := broker.NewMQ(Conf.Broker)
		if err != nil {
			log.Errorf("failed to reconnect to MQ, reason: %v", err)
		} else {
			Conf.API.MQ = newConn
		}
	}

	if DBRes := checkDB(Conf.API.DB, 5*time.Millisecond); DBRes != nil {
		log.Debugf("DB connection error :%v", DBRes)
		Conf.API.DB.Reconnect()
		statusCocde = http.StatusServiceUnavailable
	}

	c.JSON(statusCocde, "")
}

func checkDB(database *database.SQLdb, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if database.DB == nil {
		return fmt.Errorf("database is nil")
	}

	return database.DB.PingContext(ctx)
}

// getFiles returns the files from the database for a specific user
func getFiles(c *gin.Context) {

	log.Debugf("request files in project")
	c.Writer.Header().Set("Content-Type", "application/json")
	// Get user ID to extract all files
	userID, err := getUserFromToken(c.Writer, c.Request)
	if err != nil {
		// something went wrong with user token
		c.JSON(500, err.Error())

		return
	}

	files, err := Conf.API.DB.GetUserFiles(userID)
	if err != nil {
		// something went wrong with querying or parsing rows
		c.JSON(500, err.Error())

		return
	}

	// Return response
	c.JSON(200, files)
}

// getUserFromToken parses the token, validates it against the key and returns the key
func getUserFromToken(w http.ResponseWriter, r *http.Request) (string, error) {
	// Check that a token is provided
	tokenStr, err := getToken(r.Header.Get("Authorization"))
	if err != nil {
		log.Error("authorisation header missing frm request")

		return "", fmt.Errorf("could not get token from header: %v", err)
	}

	var claims jwt.MapClaims
	var ok bool

	token, err := jwt.Parse(tokenStr, func(tokenStr *jwt.Token) (interface{}, error) { return nil, nil })
	// Return error if token is broken (without claims)
	if claims, ok = token.Claims.(jwt.MapClaims); !ok {
		log.Error("could not parse claims from token")

		return "", fmt.Errorf("broken token (claims are empty): %v\nerror: %s", claims, err)
	}

	strIss := fmt.Sprintf("%v", claims["iss"])
	// Poor string unescaper for elixir
	strIss = strings.ReplaceAll(strIss, "\\", "")

	log.Debugf("Looking for key for %s", strIss)

	iss, err := url.ParseRequestURI(strIss)
	if err != nil || iss.Hostname() == "" {
		return "", fmt.Errorf("Failed to get issuer from token (%v)", strIss)
	}

	switch token.Header["alg"] {
	case "ES256":
		key, err := jwt.ParseECPublicKeyFromPEM(Conf.API.JtwKeys[iss.Hostname()])
		if err != nil {
			return "", fmt.Errorf("failed to parse EC public key (%v)", err)
		}
		_, err = jwt.Parse(tokenStr, func(tokenStr *jwt.Token) (interface{}, error) { return key, nil })
		if err != nil {
			return "", fmt.Errorf("signed token (ES256) not valid: %v, (token was %s)", err, tokenStr)
		}
	case "RS256":
		key, err := jwt.ParseRSAPublicKeyFromPEM(Conf.API.JtwKeys[iss.Hostname()])
		if err != nil {
			return "", fmt.Errorf("failed to parse RSA256 public key (%v)", err)
		}
		_, err = jwt.Parse(tokenStr, func(tokenStr *jwt.Token) (interface{}, error) { return key, nil })
		if err != nil {
			return "", fmt.Errorf("signed token (RS256) not valid: %v, (token was %s)", err, tokenStr)
		}
	default:
		return "", fmt.Errorf("unsupported algorithm %s", token.Header["alg"])
	}

	return fmt.Sprintf("%v", claims["sub"]), nil
}

// getToken parses the token string from header
func getToken(header string) (string, error) {
	log.Debug("parsing access token from header")

	if len(header) == 0 {
		log.Error("authorization check failed, empty header")

		return "", fmt.Errorf("access token must be provided")
	}

	// Check that Bearer scheme is used
	headerParts := strings.Split(header, " ")
	if headerParts[0] != "Bearer" {
		log.Error("authorization check failed, no Bearer on header")

		return "", fmt.Errorf("authorization scheme must be bearer")
	}

	// Check that header contains a token string
	var token string
	if len(headerParts) == 2 {
		token = headerParts[1]
	} else {
		log.Error("authorization check failed, no token on header")

		return "", fmt.Errorf("token string is missing from authorization header")
	}

	if len(token) < 2 {
		log.Error("authorization check failed, too small token")

		return "", fmt.Errorf("token string is missing from authorization header")
	}

	log.Debug("access token found")

	return token, nil
}
