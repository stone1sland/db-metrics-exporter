package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"io/ioutil"
	"regexp"
	"strings"
	log "github.com/sirupsen/logrus"
	_ "github.com/lib/pq"
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.Info("Initialized")
	log.SetOutput(os.Stdout)
  }

func sanitizeQuery(query string) (string, bool) {
	re := regexp.MustCompile(`(?i)^\s*SELECT\b`)
	if re.MatchString(query) {
		return query, true
	}
	return "", false
}

func validateConfig(lines []string) bool {
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 2)
		if len(parts) < 2 {
			log.Warn("Invalid line format in config.txt at line %d: %s", i+1, line)
			return false
		}
		_, isValid := sanitizeQuery(parts[1])
		if !isValid {
			log.Warn("Invalid query in config.txt at line %d: %s", i+1, parts[1])
			return false
		}
	}
	return true
}

func main() {
	ip := flag.String("ip", "127.0.0.1", "Database IP address")
	port := flag.String("port", "5432", "Database port")
	user := flag.String("user", "USERNAME", "Database user")
	password := flag.String("password", "PASSWORD", "Database password")
	dbname := flag.String("db", "DBNAME", "Database name")
	ssl := flag.String("ssl", "disable", "SSL mode (disable, require, verify-ca, verify-full)")
	flag.Parse()
	validSSLModes := map[string]bool{
		"disable":    true,
		"require":    true,
		"verify-ca":  true,
		"verify-full": true,
	}
	if !validSSLModes[*ssl] {
		log.Fatalf("unsupported sslmode %q; use disable, require, verify-ca, or verify-full", *ssl)
	}
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		*ip, *port, *user, *password, *dbname, *ssl)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("connected to database")
	data, err := ioutil.ReadFile("config.txt")
	if err != nil {
		log.Infof("can't read config.txt or it's not exist")
		log.Fatal(err)
	}
	lines := strings.Split(string(data), "\n")
	if !validateConfig(lines) {
		log.Fatal("Config file validation failed.")
	}
	resultMap := make(map[string]interface{})
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 2)
		if len(parts) < 2 {
			log.Warnf("Invalid line format in config.txt at line %d: %s", i+1, line)
			continue
		}

		name := parts[0]
		query, isValid := sanitizeQuery(parts[1])
		if !isValid {
			log.Warnf("Invalid query in config.txt at line %d: %s", i+1, parts[1])
			continue
		}

		rows, err := db.Query(query)
		if err != nil {
			log.Warnf("Error executing query %d (%s): %v", i+1, name, err)
			continue
		}
		results := []map[string]interface{}{}
		columns, err := rows.Columns()
		if err != nil {
			log.Fatal(err)
		}
		for rows.Next() {
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range values {
				valuePtrs[i] = &values[i]
			}
			err := rows.Scan(valuePtrs...)
			if err != nil {
				log.Fatal(err)
			}
			rowMap := make(map[string]interface{})
			for i, col := range columns {
				val := values[i]
				if b, ok := val.([]byte); ok {
					val = string(b)
				}
				rowMap[col] = val
			}
			results = append(results, rowMap)
		}
		err = rows.Err()
		if err != nil {
			log.Fatal(err)
		}
		resultMap[name] = results
		err = rows.Close()
		if err != nil {
			log.Fatal(err) // This will cause a panic if there is an error closing rows
		}
	}
	//finalJSON, err := json.MarshalIndent(resultMap, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	//log.WithFields(log.Fields(string(finalJSON)).Info("output")
	log.WithFields(resultMap).Info("output")
}
