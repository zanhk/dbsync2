package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"bitbucket.org/modima/dbsync2/go-logging"
	"bitbucket.org/modima/dbsync2/koanf"
	"bitbucket.org/modima/dbsync2/koanf/providers/env"
	"bitbucket.org/modima/dbsync2/koanf/providers/posflag"
	"bitbucket.org/modima/dbsync2/pflag"
	"github.com/sony/gobreaker"
	"golang.org/x/time/rate"

	"bitbucket.org/modima/dbsync2/database"
	"bitbucket.org/modima/dbsync2/ttlcache"
)

const (
	VERSION                  = "1.15.0"
	LOGLEVEL                 = 4    // Info
	FETCH_SIZE_INBOUND_CALLS = 1000 // Number of inbound calls to fetch in one step
	FETCH_SIZE_EVENTS        = 1000 // Number of transaction events to fetch in one step
	FETCH_SIZE_CONTACT_IDS   = 1000 // Number of contact ids to fetch in one step
	FETCH_SIZE_CONTACTS      = 20   // Number of contacts to fetch in one step
	WORKER_COUNT             = 32   // Number of workers
	MAX_DB_CONNECTIONS       = 32   // Number of simultaneous database connections
	CHANNEL_SIZE             = 100  // Default channels size
	BASE_URL                 = "https://api.dialfire.com"
)

/******************************************
* RUNTIME VARS
*******************************************/
var (
	db            *database.DBConnection
	config        *AppConfig
	campaignID    string
	campaignToken string
	mode          string
	cntWorker     int
	cntDBConn     int
	debugMode     bool
	configMu      sync.RWMutex
	log           = logging.MustGetLogger("dbsync2")
	logFormat     = logging.MustStringFormatter(
		`%{color}%{time:2006/01/02 15:04:05.000} %{shortfile} %{level:.4s} ▶ %{color:reset} %{message}`,
	)
	webhookCB = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "WebhookCircuitBreaker",
		MaxRequests: 5,                // Allow up to 5 requests when closed
		Interval:    30 * time.Second, // Clear failure counts every 30 seconds
		Timeout:     10 * time.Second, // After tripping, wait 10 seconds before trying again
		OnStateChange: func(name string, from, to gobreaker.State) {
			log.Infof("Circuit breaker state changed: %v -> %v", from, to)
		},
	})
)

/******************************************
* LOGGING
*******************************************/
func initLogger(filePath string, logLevel int) error {

	var logBackend logging.Backend
	if debugMode {

		fmt.Printf("Logfile: stdout\n")
		logBackend = logging.NewLogBackend(os.Stdout, "", 0)
	} else {

		var dirPath = filePath[:strings.LastIndex(filePath, string(os.PathSeparator))]
		if err := createDirectory(dirPath); err != nil {
			return err
		}

		logFile, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return err
		}

		fmt.Printf("Logfile: %v\n", filePath)
		logBackend = logging.NewLogBackend(logFile, "", 0)
	}

	logBackendFormatter := logging.NewBackendFormatter(logBackend, logFormat)
	logBackendLeveled := logging.AddModuleLevel(logBackendFormatter)
	switch logLevel {
	case 0:
		logBackendLeveled.SetLevel(logging.CRITICAL, "")
	case 1:
		logBackendLeveled.SetLevel(logging.ERROR, "")
	case 2:
		logBackendLeveled.SetLevel(logging.WARNING, "")
	case 3:
		logBackendLeveled.SetLevel(logging.NOTICE, "")
	case 4:
		logBackendLeveled.SetLevel(logging.INFO, "")
	case 5:
		logBackendLeveled.SetLevel(logging.DEBUG, "")
	}
	logging.SetBackend(logBackendLeveled)

	return nil
}

/******************************************
* CONFIGURATION
*******************************************/

type AppConfig struct {
	Path      string `json:"-"`
	Timestamp string `json:"timestamp"`
}

func loadConfig(filePath string) (*AppConfig, error) {

	var dirPath = filePath[:strings.LastIndex(filePath, string(os.PathSeparator))]
	if err := createDirectory(dirPath); err != nil {
		return nil, err
	}

	var config AppConfig
	configFile, err := os.ReadFile(filePath)
	if err != nil {
		config = AppConfig{
			Timestamp: time.Now().UTC().Format(time.RFC3339)[:19], // default: current UTC time in format "2006-01-02T15:04:05"
		}
		//debugLog.Printf("Configuration file %v not found!", filePath)
	}

	json.Unmarshal(configFile, &config)
	config.Path = filePath

	log.Infof("Load configuration file '%v'", config.Path)

	return &config, nil
}

func (c *AppConfig) save() {
	configMu.RLock()
	jsonData, err := json.Marshal(c)
	configMu.RUnlock()
	if err != nil {
		log.Error(err)
	}
	os.WriteFile(c.Path, jsonData, 0644)
}

/*******************************************
* teardown TASKS (ON KILL)
********************************************/
func teardown() {

	// Close database connection
	if db != nil {
		db.DB.Close()
	}

	// Save configuration
	config.save()
}

/*******************************************
* * * * * * * * * * MAIN * * * * * * * * * *
********************************************/
func main() {

	// Catch signals
	c := make(chan os.Signal, 1)
	signal.Notify(c,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGQUIT,
		syscall.SIGTERM)
	go func() {
		<-c
		teardown()
		os.Exit(1)
	}()

	// Flags
	flag := pflag.NewFlagSet("", pflag.ContinueOnError)
	flag.Usage = func() {
		var description = `dbSync2 v` + VERSION +
			`
		
This tool can be used to export all transactions on contacts to either a DBMS or a webservice. The export is campaign based (flag 'c').
A valid access token for the specified campaign is required (flag 'ct'). The token will be visible in your campaign as soon as you activate the Sync Client Connector.
Further a custom start date can be specified to delimit the export (flag 's').

Example 1: Insert all transactions that occured after the 01. February 2018 in campaign "MY_CAMPAIGN" to a local running instance of SQL Server. Filter only user interactions on contacts in tasks starting with prefix 'fc_' or 'qc_':
	dbsync2 --a db_sync --fm hi_updates_only --fp 'fc_,qc_' --c MY_CAMPAIGN_ID --ct MY_CAMPAIGN_SYNC_CLIENT_TOKEN --s 2018-02-01 --url 'sqlserver://my_user:my_password@localhost:1433/my_database'
		
Example 2: Send all future transactions in campaign "MY_CAMPAIGN" to a webservice (The webservice should accept JSON data and respond with status code 200 ... 299 on success):
	dbsync2 --a webhook --c MY_CAMPAIGN_ID --ct MY_CAMPAIGN_SYNC_CLIENT_TOKEN --url 'https://example.com/api/transactions/'`

		fmt.Printf("\n%v\n\n", description)
		fmt.Printf("Flags:\n")
		flag.PrintDefaults()
		fmt.Println("\nAll flags can be replaced by environment variables prefixed by 'DBSYNC_' in all upper case")
		os.Exit(0)
	}

	flag.String("c", "", "Campaign ID (required)")
	flag.String("ct", "", "Sync Client Token for the specified campaign (required)")
	flag.Int("w", WORKER_COUNT, "Number of simultaneous workers")
	flag.Int("d", MAX_DB_CONNECTIONS, "Maximum number of simultaneous database connections")
	flag.String("a", "", `Execution mode:
webhook ... Send all transactions to a webservice
db_init ... Initialize a database with all transactions of the campaign, then stop
db_update ... Update a database with all transactions after specified start date (CLI arg 's'), then stop (default start date is one week ago)
db_sync ...  Update a database with all future transactions, optionally go back to a specified start date (CLI arg 's')`)
	flag.String("s", "", "Start date in the format '2006-01-02T15:04:05'")
	flag.String("fm", "", `Transaction filter mode:
updates_only ... only transactions of type 'update'
hi_updates_only ... only transactions of type 'update' that were triggered by a human interaction`)
	flag.String("fp", "", "Filter transactions by one or several task(-prefixes) (comma separated), e.g. 'fc_,qc_'")
	flag.String("url", "", `URL pointing to a webservice that handles the transaction data (if execution mode is 'webhook')
Database connection URL of the form '{mysql|sqlserver|postgres}://user:password@host:port/database' (if execution mode is 'db_init', 'db_update' or 'db_sync')`)
	flag.Bool("p", false, "Enable profiling")
	flag.Bool("v", false, "Print all log messages to stdout instead of using a logfile")
	flag.String("tp", "", "Prefix of the sql database table names")
	flag.Int("l", LOGLEVEL, "Log level (0 - CRITICAL .. 5 - DEBUG)")
	// New flags for webhook rate limiting (only used in mode "webhook")
	flag.Int("wr", 5, "Webhook rate limit (requests per second)")
	flag.Int("wb", 0, "Webhook burst limit (if 0, automatically set to 2 * wr)")

	flag.Parse(os.Args)

	// Config via env and flags
	envPrefix := "DBSYNC"
	k := koanf.New(".")
	err := k.Load(env.Provider(envPrefix, "_", func(s string) string {
		return strings.ToLower(strings.TrimPrefix(s, envPrefix))
	}), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading config from env: %s", err)
		os.Exit(1)
	}

	err = k.Load(posflag.Provider(flag, "_", k), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading config from flags: %s", err)
		os.Exit(1)
	}

	campaignID = k.String("c")
	if len(campaignID) == 0 {
		fmt.Fprintln(os.Stderr, "Campaign ID (--c) is required")
		os.Exit(1)
	}
	campaignToken = k.String("ct")
	if len(campaignToken) == 0 {
		fmt.Fprintln(os.Stderr, "Campaign token (--ct) is required")
		os.Exit(1)
	}

	logLevel := k.Int("l")
	url := k.String("url")
	debugMode = k.Bool("v")
	cntWorker = k.Int("w")
	cntDBConn = k.Int("d")
	mode = k.String("a")

	log.Infof("URL: %s", url)

	if mode != "webhook" && mode != "db_init" && mode != "db_update" && mode != "db_sync" {
		fmt.Fprintf(os.Stderr, "Invalid mode: %v\n", mode)
		os.Exit(1)
	}

	// Setup parameters
	tPrefix := k.String("fp")
	if len(tPrefix) > 0 {
		eventOptions["tasks"] = tPrefix
	}
	filterMode := k.String("fm")
	if len(filterMode) > 0 {
		switch filterMode {
		case "updates_only":
			eventOptions["type"] = "update"
		case "hi_updates_only":
			eventOptions["type"] = "update"
			eventOptions["hi"] = "true"
		}
	}

	if debugMode {
		logLevel = 5
	}

	var logDir = "/var/log/dbsync2/"
	if runtime.GOOS == "windows" {
		logDir, err = os.UserConfigDir()
		if err != nil {
			logDir = os.Getenv("HOME") + "\\.dbsync2\\log\\"
		} else {
			logDir += "\\.dbsync2\\log\\"
		}
	}
	err = initLogger(logDir+campaignID+"_"+time.Now().Format("20060102150405")+".log", logLevel)
	if err != nil {
		err = initLogger(os.Getenv("HOME")+"/.dbsync2/log/"+time.Now().Format("20060102150405")+".log", logLevel)
		if err != nil {
			log.Fatal(err.Error())
		}
	}

	var configDir = "/var/opt/dbsync2/"
	if runtime.GOOS == "windows" {
		configDir, err = os.UserConfigDir()
		if err != nil {
			configDir = os.Getenv("HOME") + "\\.dbsync2\\"
		} else {
			configDir += "\\.dbsync2\\"
		}
	}
	log.Infof("configDir: %s", configDir)
	config, err = loadConfig(configDir + campaignID + ".json")
	if err != nil {
		config, err = loadConfig(os.Getenv("HOME") + "/.dbsync2/" + campaignID + ".json")
		if err != nil {
			panic(err)
		}
	}

	// Periodically log channel lengths
	go func() {
		for {
			runSafely(func() {
				time.Sleep(30 * time.Second)
				log.Infof("Channel lengths: chanDataSplitter=%d, chanContactFetcher=%d, chanDatabaseUpdater=%d",
					len(chanDataSplitter), len(chanContactFetcher), len(chanDatabaseUpdater))
			})
		}
	}()

	// Periodically save config (every minute)
	go func() {
		t := time.NewTicker(time.Minute)
		for {
			<-t.C
			config.save()
		}
	}()

	// Start profiler if enabled
	if k.Bool("p") {
		go http.ListenAndServe(":8080", http.DefaultServeMux)
	}

	// Set start date from config file (if not explicitly defined)
	var startDate string
	if k.String("s") != "" {
		startDate = k.String("s")
	} else if mode != "db_init" && mode != "db_update" {
		configMu.RLock()
		startDate = config.Timestamp
		configMu.RUnlock()
	}

	// init random seed
	rand.Seed(time.Now().UnixNano())

	log.Infof("Mode: %v", mode)
	log.Infof("Campaign ID: %v", campaignID)
	log.Infof("Start date: %v", startDate)

	// Only in "webhook" mode, configure the webhook rate limiter:
	if mode == "webhook" {
		wr := k.Int("wr")
		wb := k.Int("wb")
		if wr <= 0 {
			wr = 5
		}
		if wb <= 0 {
			wb = wr * 2
		}
		// Override the global webhookLimiter with our configuration:
		webhookLimiter = rate.NewLimiter(rate.Limit(wr), wb)
		log.Infof("Configured webhook rate limiter: %d req/sec (burst %d)", wr, wb)

		modeWebhook(url, startDate)
	} else {
		var dbms = url[:strings.Index(url, ":")]
		var dbName = url[strings.LastIndex(url, "/")+1:]
		var dbValid = false
		for _, l := range []string{"mysql", "postgres", "sqlserver"} {
			if dbms == l {
				dbValid = true
				break
			}
		}
		if !dbValid {
			fmt.Fprintf(os.Stderr, "Invalid database driver '%v'\n", dbms)
			os.Exit(1)
		}
		if len(url) == 0 {
			fmt.Fprintln(os.Stderr, "Database URL (CLI arg 'dburi') is required")
			os.Exit(1)
		}
		if len(dbName) == 0 {
			fmt.Fprintln(os.Stderr, "Database name is required")
			os.Exit(1)
		}
		db, err = database.Open(dbms, url, k.String("tp"), log)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		log.Debugf("database connected")
		db.DB.SetMaxOpenConns(cntDBConn)
		db.DB.SetMaxIdleConns(cntDBConn)
		db.DB.SetConnMaxLifetime(time.Hour)
		log.Debugf("prepare database")
		prepareDatabase()
		log.Debugf("prepare done")
		switch mode {
		case "db_init":
			modeDatabaseUpdate(startDate)
			teardown()
		case "db_update":
			if k.String("s") == "" {
				startDate = time.Now().UTC().Add(-168 * time.Hour).Format("2006-01-02")
			}
			modeDatabaseUpdate(startDate)
			teardown()
		case "db_sync":
			go func() {
				t := time.NewTicker(12 * time.Hour)
				for {
					<-t.C
					startDate := time.Now().UTC().Add(-24 * time.Hour).Format("2006-01-02T15:04:05.999")
					var wg sync.WaitGroup
					wg.Add(1)
					go contactLister(&wg, startDate)
					chanInboundCallFetcher <- TimeRange{
						From: startDate,
						To:   time.Now().UTC().Format("2006-01-02T15:04:05.999"),
					}
				}
			}()
			go func() {
				go statisticAggregator(false)
				startWorker(startDate)
				chanInboundCallFetcher <- TimeRange{
					From: startDate,
					To:   time.Now().UTC().Format("2006-01-02T15:04:05.999"),
				}
			}()
			modeDatabaseSync(time.Now().UTC().Format("2006-01-02T15:04:05.999"))
		default:
			fmt.Fprintf(os.Stderr, "Invalid mode: %v\n", mode)
			os.Exit(1)
		}
	}
}

func prepareDatabase() {
	log.Debugf("load campaign field list")
	// Kampagne laden
	data, err := getCampaignFields()
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	log.Debugf("unmarshal json")
	var fieldList database.CampaignFieldList
	if err = json.Unmarshal(data, &fieldList); err != nil {
		log.Error(err)
		os.Exit(1)
	}
	log.Debugf("update tables")

	// Schema für Kontakttabelle erzeugen und ggf. DB Tabelle aktualisieren
	if err = db.UpdateTables(fieldList); err != nil {
		log.Error(err)
		os.Exit(1)
	}
	log.Debugf("update tables done")
}

/*******************************************
* MODE: WEBHOOK
********************************************/
func modeWebhook(url string, startDate string) {
	var wg1, wg2, wg3, wg4 sync.WaitGroup
	wg1.Add(cntWorker)
	wg2.Add(cntWorker)
	wg3.Add(cntWorker)
	for i := 0; i < cntWorker; i++ {
		goSafe(&wg1, func() { eventFetcher(&wg1) })
		goSafe(&wg2, func() { contactFetcher(&wg2) })
		goSafe(&wg3, func() { webhookSender(url, &wg3) })
	}
	wg4.Add(1)
	goSafe(&wg4, func() { fetcher(&wg4, false) }) // No inbound calls in webhook mode
	if startDate != "" {
		chanFetcher <- TimeRange{From: startDate}
	}
	ticker(startDate)
}

var taCacheWebhook = ttlcache.NewCache(time.Hour) // autoextend
func webhookSender(url string, wg *sync.WaitGroup) {

	defer wg.Done()

	for {

		taPointer, ok := <-chanDataSplitter
		if !ok {
			break
		}

		var contact = *taPointer.Contact
		var taskLog = contact["$task_log"].([]interface{})
		delete(contact, "$task_log")

		for _, e := range taskLog {

			var entry = e.(map[string]interface{})
			var transactions = entry["transactions"].([]interface{})

			for _, tran := range transactions {

				var transaction = tran.(map[string]interface{})
				transactionBytes, err := json.Marshal(transaction)
				if err != nil {
					log.Error(err)
					continue
				}
				var newHash = fmt.Sprintf("%x", md5.Sum(transactionBytes))

				var key = transaction["fired"].(string) + contact["$id"].(string)
				oldHash, exists := taCacheWebhook.Get(key)

				log.Debugf("send transaction %v: old hash: %v - new hash: %v", key, oldHash, newHash)

				var state = "new"
				if exists {
					if oldHash == newHash {
						continue
					} else {
						state = "updated"
					}
				}
				taCacheWebhook.Set(key, newHash)

				var data = map[string]interface{}{
					"contact":     contact,
					"transaction": transaction,
					"state":       state,
				}

				payload, err := json.Marshal(data)
				if err != nil {
					log.Error(err)
					continue
				}

				err = callWebservice(url, payload)
				if err == nil {
					// Save start date if transaction was sent successfully with proper synchronization
					configMu.Lock()
					config.Timestamp = transaction["fired"].(string)
					configMu.Unlock()
				} else {
					log.Error(err)
				}
			}
		}
	}
}

var httpClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	},
	Timeout: 30 * time.Second, // or a value appropriate for your webhook
}

var webhookLimiter = rate.NewLimiter(5, 10) // Allow up to 5 events/sec with a burst of 10.

func callWebservice(url string, data []byte) error {
	result, err := webhookCB.Execute(func() (interface{}, error) {
		// Retry loop inside the circuit breaker
		for i := 0; i < 10; i++ {
			if err := webhookLimiter.Wait(context.Background()); err != nil {
				return nil, err
			}
			req, err := http.NewRequest("POST", url, bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
			resp, err := httpClient.Do(req)
			if err == nil && resp.StatusCode < 300 {
				resp.Body.Close()
				return nil, nil
			}
			if resp != nil {
				resp.Body.Close()
			}
			timeout := time.Second * time.Duration(math.Pow(2, float64(i)))
			time.Sleep(timeout)
		}
		return nil, errors.New("webhook call failed after 10 attempts")
	})
	if err != nil {
		return err
	}
	_ = result
	return nil
}

/*******************************************
* MODE: DATABASE INITIALIZE
********************************************/
func startWorker(startDate string) []*sync.WaitGroup {
	var wg1, wg2, wg3, wg4, wg5, wg6 sync.WaitGroup

	wg1.Add(1)
	goSafe(&wg1, func() { contactLister(&wg1, startDate) })

	// Launch workers safely:
	wg2.Add(cntWorker)
	wg3.Add(cntWorker)
	wg4.Add(cntWorker)
	wg5.Add(cntWorker)
	for i := 0; i < cntWorker; i++ {
		goSafe(&wg2, func() { databaseRequester(&wg2) })
		goSafe(&wg3, func() { contactFetcher(&wg3) })
		goSafe(&wg4, func() { inboundCallFetcher(&wg4) })
		goSafe(&wg5, func() { dataSplitter(&wg5) })
	}

	wg6.Add(cntDBConn)
	for i := 0; i < cntDBConn; i++ {
		goSafe(&wg6, func() { databaseUpdater(&wg6) })
	}

	return []*sync.WaitGroup{&wg1, &wg2, &wg3, &wg4, &wg5, &wg6}
}

func waitWorkerFinished(wg []*sync.WaitGroup) {

	// Stop inbound call fetcher
	close(chanInboundCallFetcher)

	// 1. Wait until all contact ids have been listed
	wg[0].Wait()
	log.Infof("Contact listing DONE")
	//time.Sleep(time.Second)
	close(chanDatabaseRequester)

	wg[1].Wait()
	log.Infof("Contact compare DONE")
	//time.Sleep(time.Second)
	close(chanContactFetcher)

	wg[2].Wait()
	log.Infof("Contact fetch DONE")
	//time.Sleep(time.Second)
	close(chanDataSplitter)

	wg[3].Wait()
	log.Infof("Inbound Call fetch DONE")

	wg[4].Wait()
	log.Infof("Data split DONE")
	//time.Sleep(time.Second)
	close(chanDatabaseUpdater)

	wg[5].Wait()
	log.Infof("Database update DONE")
	//time.Sleep(time.Second)

}

func modeDatabaseUpdate(startDate string) {

	go statisticAggregator(true)

	// start worker
	waitGroups := startWorker(startDate)

	// fetch inbound calls
	chanInboundCallFetcher <- TimeRange{
		From:       startDate,
		To:         time.Now().UTC().Format("2006-01-02T15:04:05.999"),
		SignalDone: true,
	}

	// wait until inbound calls have been fetched
	<-chanFetchDone

	// wait until other worker have finished
	waitWorkerFinished(waitGroups)

	// Close statistics
	close(chanStatistics)
	<-chanDone // Wait until statistics have been logged
}

/*******************************************
* MODE: DATABASE SYNCHRONIZATION
********************************************/

func modeDatabaseSync(startDate string) {

	var wg1, wg2, wg3, wg4, wg5, wg6 sync.WaitGroup

	// Start worker
	wg1.Add(cntWorker)
	wg2.Add(cntWorker)
	wg3.Add(cntWorker)
	wg4.Add(cntWorker)
	for i := 0; i < cntWorker; i++ {
		goSafe(&wg1, func() { eventFetcher(&wg1) })
		goSafe(&wg2, func() { dataSplitter(&wg2) })
		goSafe(&wg3, func() { contactFetcher(&wg3) })
		goSafe(&wg4, func() { inboundCallFetcher(&wg4) })
	}

	// Start database updater
	wg5.Add(cntDBConn)
	for i := 0; i < cntDBConn; i++ {
		goSafe(&wg5, func() { databaseUpdater(&wg5) })
	}

	wg6.Add(1)
	goSafe(&wg6, func() { fetcher(&wg6, true) }) // include inbound calls

	// Events aus Vergangenheit laden
	if startDate != "" {
		chanFetcher <- TimeRange{
			From: startDate,
		}
	}

	// Runs forever
	ticker(startDate)
}

/*******************************************
* DIALFIRE API
********************************************/
func getCampaignFields() ([]byte, error) {
	url := BASE_URL + "/api/campaigns/" + campaignID + "/fields/list"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+campaignToken)

	var result []byte
	var resp *http.Response
	for i := 0; i < 10; i++ {
		resp, err = http.DefaultClient.Do(req)
		if err == nil && resp.StatusCode == 403 {
			fmt.Fprintln(os.Stderr, url+" - "+resp.Status)
			os.Exit(1)
		}

		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				resp.Body.Close()
			}
			timeout := time.Second * time.Duration(math.Pow(2, float64(i)))
			time.Sleep(timeout)
			continue
		}

		result, err = io.ReadAll(resp.Body)
		resp.Body.Close() // close immediately after reading
		if err != nil {
			continue
		}

		break
	}

	return result, nil
}

func getContactIds(startDate string, cursor string, limit int) ([]byte, error) {
	url := BASE_URL + "/api/campaigns/" + campaignID + "/contacts/list?limit=" + strconv.Itoa(limit)
	if len(startDate) > 0 {
		idx := 10
		if len(startDate) < 10 {
			idx = len(startDate)
		}
		url += "&changedSince=" + startDate[:idx]
	}
	if len(cursor) > 0 {
		url += "&cursor=" + cursor
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+campaignToken)

	var result []byte
	var resp *http.Response
	for i := 0; i < 10; i++ {
		resp, err = http.DefaultClient.Do(req)
		if err == nil && resp.StatusCode == 403 {
			fmt.Fprintln(os.Stderr, url+" - "+resp.Status)
			os.Exit(1)
		}

		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				resp.Body.Close()
			}
			timeout := time.Second * time.Duration(math.Pow(2, float64(i)))
			time.Sleep(timeout)
			continue
		}

		result, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}

		break
	}

	return result, nil
}

func Contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

func getContacts(contactIDs []string) ([]byte, error) {
	url := BASE_URL + "/api/campaigns/" + campaignID + "/contacts/?include_md5=true"

	data, err := json.Marshal(contactIDs)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+campaignToken)

	var result []byte
	var resp *http.Response
	for i := 0; i < 10; i++ {
		resp, err = http.DefaultClient.Do(req)
		if err == nil && resp.StatusCode == 403 {
			fmt.Fprintln(os.Stderr, url+" - "+resp.Status)
			os.Exit(1)
		}

		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				resp.Body.Close()
			}
			timeout := time.Second * time.Duration(math.Pow(2, float64(i)))
			time.Sleep(timeout)
			continue
		}

		result, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Errorf("Contact request failed. Response error: %v", err.Error())
			continue
		}

		break
	}

	return result, nil
}

var eventOptions = map[string]string{
	"type":  "",
	"hi":    "",
	"tasks": "",
}

// Parameters: from string, to string, cursor string
func getTransactionEvents(params map[string]string) ([]byte, error) {
	url := BASE_URL + "/api/campaigns/" + campaignID + "/contacts/transactions/?"

	// CLI Options
	for k, v := range eventOptions {
		if v != "" {
			url += k + "=" + v + "&"
		}
	}

	// Additional Parameters
	for k, v := range params {
		url += k + "=" + v + "&"
	}

	// Limit
	url += "limit=" + strconv.Itoa(FETCH_SIZE_EVENTS)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+campaignToken)

	var result []byte
	var resp *http.Response
	for i := 0; i < 10; i++ {
		resp, err = http.DefaultClient.Do(req)
		if err == nil && resp.StatusCode == 403 {
			fmt.Fprintln(os.Stderr, url+" - "+resp.Status)
			os.Exit(1)
		}

		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				resp.Body.Close()
			}
			timeout := time.Second * time.Duration(math.Pow(2, float64(i)))
			time.Sleep(timeout)
			continue
		}

		result, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}

		break
	}

	return result, nil
}

// Parameters: start string, end string
func getInboundCalls(params map[string]string) ([]byte, error) {
	url := BASE_URL + "/api/campaigns/" + campaignID + "/inbound/calls/?"

	// Additional Parameters
	for k, v := range params {
		url += k + "=" + v + "&"
	}

	// Limit
	url += "limit=" + strconv.Itoa(FETCH_SIZE_INBOUND_CALLS)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+campaignToken)

	var result []byte
	var resp *http.Response
	for i := 0; i < 10; i++ {
		resp, err = http.DefaultClient.Do(req)
		if err == nil && resp.StatusCode == 403 {
			fmt.Fprintln(os.Stderr, url+" - "+resp.Status)
			os.Exit(1)
		}

		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				resp.Body.Close()
			}
			timeout := time.Second * time.Duration(math.Pow(2, float64(i)))
			time.Sleep(timeout)
			continue
		}

		result, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}

		break
	}

	return result, nil
}

/*******************************************
* WORKER
*******************************************/
/*
type FetchResultContactList struct {
	Count   int `json:"count"`
	Results []struct {
		Id  string `json:"id"`
		MD5 string `json:"md5"`
	} `json:"results"`
	Cursor string `json:"cursor"`
}
*/
type FetchResultContactList struct {
	Count   int                 `json:"count"`
	Results []map[string]string `json:"results"`
	Cursor  string              `json:"cursor"`
}

type FetchResultInboundCalls struct {
	Count   int                      `json:"count"`
	Results []map[string]interface{} `json:"results"`
	Cursor  string                   `json:"cursor"`
}

type FetchResult struct {
	Count   int      `json:"count"`
	Results []string `json:"results"`
	Cursor  string   `json:"cursor"`
}

type TAPointerList struct {
	ContactID string
	MD5       string
	Contact   *map[string]interface{}
	//Pointer   []string
}

type TimeRange struct {
	From       string
	To         string
	SignalDone bool // Signal that all events have been fetched
}

var chanFetcher = make(chan TimeRange, CHANNEL_SIZE)

func fetcher(wg *sync.WaitGroup, includeInbound bool) {
	defer wg.Done()
	for {
		timeRange, ok := <-chanFetcher
		if !ok {
			close(chanEventFetcher)
			if includeInbound {
				close(chanInboundCallFetcher)
			}
			break
		}
		chanEventFetcher <- timeRange
		if includeInbound {
			chanInboundCallFetcher <- timeRange
		}
	}
}

var chanEventFetcher = make(chan TimeRange, CHANNEL_SIZE)
var chanFetchDone = make(chan int) // Returns number of fetched events (if TimeRange.SignalDone==true)

var eventCache = ttlcache.NewCache(2 * time.Minute) // (2 Minuten) Autoextend bei GET
func eventFetcher(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		timeRange, ok := <-chanEventFetcher
		if !ok {
			return
		}
		runSafely(func() {
			var params = map[string]string{
				"from": timeRange.From,
			}
			if timeRange.To != "" {
				params["to"] = timeRange.To
			}
			var timeout = time.Second * 10
			var newEventsCurPage, newEventsTotal int
			eventsByContactID := make(map[string]TAPointerList)

			for {
				data, err := getTransactionEvents(params)
				if err != nil {
					log.Error(err)
					break
				}
				var resp FetchResult
				if err = json.Unmarshal(data, &resp); err != nil {
					log.Error(err)
					break
				}
				for _, event := range resp.Results {
					var splits = strings.Split(event, "|")
					if len(splits) < 3 {
						log.Warning("Unexpected event format:", event)
						continue
					}
					fired, md5Str, contactID := splits[0], splits[1], splits[2]
					key := fired + contactID
					oldHash, exists := eventCache.Get(key)
					if exists && oldHash == md5Str {
						log.Debugf("Skipping event for contact %v (fired=%v) because MD5 unchanged", contactID, fired)
						continue
					}
					log.Debugf("transaction update: contact id: %v - fired: %v - old md5: %v - new md5: %v - is update: %v",
						contactID, fired, oldHash, md5Str, exists)
					newEventsCurPage++
					if eventsByContactID[contactID].ContactID == "" {
						eventsByContactID[contactID] = TAPointerList{
							ContactID: contactID,
						}
					}
					eventCache.Set(key, md5Str)
					if len(eventsByContactID) >= FETCH_SIZE_CONTACTS {
						chanContactFetcher <- eventsByContactID
						eventsByContactID = make(map[string]TAPointerList)
					}
				}
				newEventsTotal += newEventsCurPage

				if resp.Cursor != "" {
					params["cursor"] = resp.Cursor
					if newEventsCurPage > int(float64(FETCH_SIZE_EVENTS)*0.75) {
						time.Sleep(timeout)
						if timeout > time.Second {
							timeout -= timeout / 10
						} else {
							timeout = time.Second
						}
					}
				} else {
					if len(eventsByContactID) > 0 {
						chanContactFetcher <- eventsByContactID
					}
					if timeRange.SignalDone {
						chanFetchDone <- newEventsTotal
					}
					break
				}
				newEventsCurPage = 0
			}
		})
	}
}

/*******************************************
* Importstatistik
*******************************************/
type Statistic struct {
	Type  string
	Count uint
}

var chanStatistics = make(chan Statistic)
var chanDone = make(chan bool)

func statisticAggregator(active bool) {

	var start = time.Now()
	var statistics = make(map[string]uint)

	for {
		statistic, ok := <-chanStatistics
		if !ok {
			break
		}

		if active {
			statistics[statistic.Type] += statistic.Count
		}
	}

	// Print statistics
	log.Infof("------------------------------------------------------------------------------------------")
	log.Infof("Protocol:")
	for sType, sCount := range statistics {
		log.Infof("%v: %v", sType, sCount)
	}
	log.Infof("duration: %v", time.Since(start))
	chanDone <- true
}

func contactLister(wg *sync.WaitGroup, startDate string) {

	defer wg.Done()

	//var timeout = time.Second * 10 // Aktuelles timeout zwischen zwei Abfragen --> Langsam skalieren
	var limit = FETCH_SIZE_CONTACT_IDS
	var cursor string
	var contactsTotal = 0
	for {

		data, err := getContactIds(startDate, cursor, limit)
		if err != nil {
			log.Error(err)
			break
		}

		var resp FetchResultContactList
		if err = json.Unmarshal(data, &resp); err != nil {
			log.Error(err)
			break
		}

		for _, contactIdAndMD5 := range resp.Results {
			chanDatabaseRequester <- contactIdAndMD5
			contactsTotal++
		}

		if resp.Cursor != "" {

			cursor = resp.Cursor
			continue
		}

		break
	}
}

var chanContactFetcher = make(chan map[string]TAPointerList, CHANNEL_SIZE)

func contactFetcher(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		runSafely(func() {

			eventsByContactID, ok := <-chanContactFetcher
			if !ok {
				return
			}
			var contactIDs []string
			for id := range eventsByContactID {
				contactIDs = append(contactIDs, id)
			}
			data, err := getContacts(contactIDs)
			if err != nil {
				log.Error(err)
				return
			}
			dec := json.NewDecoder(bytes.NewReader(data))
			dec.UseNumber()
			if _, err = dec.Token(); err != nil {
				log.Error(err)
				log.Errorf("Failed contact IDs: %v", contactIDs)
				return
			}
			for dec.More() {
				var contact map[string]interface{}
				if err := dec.Decode(&contact); err != nil {
					log.Error(err)
					continue
				}
				var taPointer = eventsByContactID[contact["$id"].(string)]
				taPointer.MD5 = contact["$md5"].(string)
				taPointer.Contact = &contact
				chanDataSplitter <- taPointer
			}
			if _, err = dec.Token(); err != nil {
				log.Error(err)
			}
		})
	}
}

var chanInboundCallFetcher = make(chan TimeRange, CHANNEL_SIZE)

func inboundCallFetcher(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		runSafely(func() {
			timeRange, ok := <-chanInboundCallFetcher
			if !ok {
				return
			}
			var params = map[string]string{"from": timeRange.From}
			if timeRange.To != "" {
				params["to"] = timeRange.To
			}
			for {
				data, err := getInboundCalls(params)
				if err != nil {
					log.Error(err)
					break
				}
				var resp FetchResultInboundCalls
				if err = json.Unmarshal(data, &resp); err != nil {
					log.Error(err)
					break
				}
				for _, ibCall := range resp.Results {
					ibCall["remote_number"] = ibCall["calling_number"]
					ibCall["line_number"] = ibCall["called_number"]
					ibCall["started"] = ibCall["call_time"]
					ibCall["connected"] = ibCall["connect_time"]
					ibCall["disconnected"] = ibCall["hangup_time"]
					chanDatabaseUpdater <- database.Entity{
						Type: "inbound_call",
						Data: ibCall,
					}
				}
				if resp.Cursor != "" {
					params["cursor"] = resp.Cursor
				} else {
					if timeRange.SignalDone {
						chanFetchDone <- 0
					}
					break
				}
			}
		})
	}
}

var chanDataSplitter = make(chan TAPointerList, CHANNEL_SIZE)

func dataSplitter(wg *sync.WaitGroup) {

	defer wg.Done()
	for {

		runSafely(func() {
			pointerList, ok := <-chanDataSplitter
			if !ok {
				return
			}
			var contact = *pointerList.Contact
			var taskLog = contact["$task_log"].([]interface{})

			chanDatabaseUpdater <- database.Entity{
				Type: "contact",
				Data: contact,
			}

			//import all transactions that match the filter
			for _, e := range taskLog {

				// transactions
				var entry = e.(map[string]interface{})
				var transactions = entry["transactions"].([]interface{})
				for _, tran := range transactions {

					var transaction = tran.(map[string]interface{})

					// filter transactions
					if len(eventOptions["type"]) > 0 && transaction["type"] != nil && eventOptions["type"] != transaction["type"].(string) {
						continue
					}
					if eventOptions["hi"] == "true" {
						if transaction["user"] == nil || (transaction["status_detail"] != nil && transaction["status_detail"].(string) == "$assigned") {
							continue
						}
					}
					if len(eventOptions["tasks"]) > 0 && transaction["task"] != nil {
						var taTask = transaction["task"].(string)
						var taskPrefixes = strings.Split(eventOptions["tasks"], ",")
						var skip = true
						for _, tp := range taskPrefixes {
							if strings.HasPrefix(taTask, tp) {
								skip = false
								break
							}
						}
						if skip {
							continue
						}
					}

					var tid = contact["$id"].(string) + transaction["fired"].(string)
					transaction["id"] = hash(tid)
					transaction["contact_id"] = contact["$id"].(string)
					insertTransaction(transaction)
				}
			}
		})
	}
}

func insertTransaction(transaction map[string]interface{}) {

	// Connections
	var connections = transaction["connections"]
	delete(transaction, "connections")
	chanDatabaseUpdater <- database.Entity{
		Type: "transaction",
		Data: transaction,
	}

	if connections == nil {
		return
	}

	var prevConnection map[string]interface{}
	for _, con := range connections.([]interface{}) {

		if transaction["id"] == nil {
			log.Warningf("transaction id missing --> skip record")
			continue
		}

		var connection = con.(map[string]interface{})
		connection["contact_id"] = transaction["contact_id"]
		connection["transaction_id"] = transaction["id"]

		if connection["technology"] != nil && connection["technology"].(string) == "transfer" && prevConnection != nil {
			connection["parent_connection_id"] = prevConnection["id"]
		}

		insertConnection(connection)
		prevConnection = connection
	}
}

func insertConnection(connection map[string]interface{}) {
	if connection["disconnected"] == nil {
		log.Warningf("disconnected timestamp missing on connection --> skip record")
		return
	}
	if connection["id"] == nil {
		log.Warningf("id missing on connection --> skip record")
		return
	}
	if _, ok := connection["id"].(string); !ok {
		log.Warningf("id of connection not of type string --> skip record")
		return
	}

	log.Debugf("insert connection: contact id: %v - transaction id: %v - connection id: %v", connection["contact_id"], connection["transaction_id"], connection["id"])

	if connection["isThirdPartyConnection"] == nil {
		connection["isThirdPartyConnection"] = false
	}

	// Third-Party-Connections
	var thirdPartyConnections = connection["third_party_connections"]
	delete(connection, "third_party_connections")

	if thirdPartyConnections != nil {
		for _, tpc := range thirdPartyConnections.([]interface{}) {
			var tpConnection = tpc.(map[string]interface{})
			tpConnection["contact_id"] = connection["contact_id"]
			tpConnection["transaction_id"] = connection["transaction_id"]
			tpConnection["parent_connection_id"] = connection["id"]
			insertConnection(tpConnection)
		}
	}

	// Recordings
	var recordings = connection["recordings"]
	delete(connection, "recordings")
	chanDatabaseUpdater <- database.Entity{
		Type: "connection",
		Data: connection,
	}

	if recordings != nil {
		for _, rec := range recordings.([]interface{}) {
			var recording = rec.(map[string]interface{})
			log.Debugf("insert recording: contact id: %v - connection id: %v - recording location: %v", connection["contact_id"], connection["id"], recording["location"])
			if recording["location"] == nil {
				recording["id"] = hash(connection["id"].(string) + recording["started"].(string))
			} else {
				recording["id"] = hash(connection["id"].(string) + recording["location"].(string))
			}
			recording["contact_id"] = connection["contact_id"]
			recording["connection_id"] = connection["id"]

			chanDatabaseUpdater <- database.Entity{
				Type: "recording",
				Data: recording,
			}
		}
	}
}

func md5Query(eventsByContactID map[string]TAPointerList, md5sByContactIDs map[string]string) {

	ids := make([]string, 0, len(md5sByContactIDs))
	for k := range md5sByContactIDs {
		ids = append(ids, k)
	}
	idsAndMD5 := db.QueryMD5(ids)

	// Compare MD5
	for id := range md5sByContactIDs {
		newMD5 := md5sByContactIDs[id]
		oldMD5 := idsAndMD5[id]

		if len(oldMD5) > 0 && oldMD5 == newMD5 {
			continue
		}

		eventsByContactID[id] = TAPointerList{
			ContactID: id,
			MD5:       newMD5,
		}
	}
}

var chanDatabaseRequester = make(chan map[string]string, CHANNEL_SIZE)

func databaseRequester(wg *sync.WaitGroup) {

	defer wg.Done()

	var md5sByContactIDs = map[string]string{}
	var eventsByContactID = map[string]TAPointerList{}
	for {

		idMD5, ok := <-chanDatabaseRequester
		if !ok {
			break
		}

		md5sByContactIDs[idMD5["id"]] = idMD5["md5"]

		if len(md5sByContactIDs) >= FETCH_SIZE_CONTACTS {
			md5Query(eventsByContactID, md5sByContactIDs)
			md5sByContactIDs = map[string]string{}
		}

		if len(eventsByContactID) >= FETCH_SIZE_CONTACTS {
			chanContactFetcher <- eventsByContactID
			eventsByContactID = make(map[string]TAPointerList)
		}
	}

	if len(md5sByContactIDs) > 0 {
		md5Query(eventsByContactID, md5sByContactIDs)
	}

	if len(eventsByContactID) > 0 {
		chanContactFetcher <- eventsByContactID
	}
}

var chanDatabaseUpdater = make(chan database.Entity, CHANNEL_SIZE)

func databaseUpdater(wg *sync.WaitGroup) {

	//debugLog.Printf("Start database inserter")

	defer wg.Done()

	var counter = map[string]uint{}

	for {

		entity, ok := <-chanDatabaseUpdater
		if !ok {
			break
		}

		//debugLog.Printf("DB Updater: Upsert %v", entity.Data)

		err := db.Upsert(entity)
		if err == nil {
			if entity.Type == "transaction" {
				configMu.Lock()
				config.Timestamp = (entity.Data)["fired"].(string)
				configMu.Unlock()
			}
			counter[entity.Type+" success"]++
		} else {
			upsertError(entity, err)
			//debugLog.Printf("%v", entity.Data)
			counter[entity.Type+" failed"]++
		}
	}

	for eType, eCount := range counter {
		chanStatistics <- Statistic{
			Type:  eType,
			Count: eCount,
		}
	}

	//debugLog.Printf("Stop database inserter")
}

func upsertError(entity database.Entity, err error) {

	switch entity.Type {
	case "contact":
		log.Warningf("UPSERT ERROR: Contact | CONTACT ID: %v | %v\n\n", (entity.Data)["$id"], err.Error())
	case "transaction":
		log.Warningf("UPSERT ERROR: Transaction | CONTACT ID: %v | %v\n\n", (entity.Data)["contact_id"], err.Error())
	case "connection":
		log.Warningf("UPSERT ERROR: Connection | TRANSACTION ID: %v | %v\n\n", (entity.Data)["transaction_id"], err.Error())
	case "recordings":
		log.Warningf("UPSERT ERROR: Recording | CONNECTION ID: %v | %v\n\n", (entity.Data)["connection_id"], err.Error())
	case "inbound_call":
		log.Warningf("UPSERT ERROR: Inbound Call | INBOUND CALL ID: %v | %v\nDATA: %v\n\n", (entity.Data)["id"], err.Error(), entity.Data)
	}
}

/******************************************
* TICKER FÜR ZEITINTERVALLE
*******************************************/
func ticker(startDate string) {
	tMin := time.NewTicker(time.Minute)
	defer tMin.Stop()
	for now := range tMin.C {
		runSafely(func() {
			// ... your ticker code ...
			var to = now.UTC().Format("2006-01-02T15:04:05.999")
			if to < startDate {
				return // skip this cycle
			}
			var from = now.Add(-2 * time.Minute).UTC().Format("2006-01-02T15:04:05.999")
			if from < startDate {
				from = startDate
			}
			chanFetcher <- TimeRange{
				From: from,
				To:   to,
			}
		})
	}
}

/******************************************
* UTILITY FUNCTIONS
*******************************************/

func hash(text string) string {
	h := md5.New()
	io.WriteString(h, text)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func createDirectory(path string) error {

	if _, err := os.Stat(path); err != nil {

		if os.IsNotExist(err) {

			err = os.MkdirAll(path, 0755)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

func goSafe(wg *sync.WaitGroup, fn func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Warningf("Recovered in safe goroutine: %v", r)
			}
		}()
		fn()
	}()
}

func runSafely(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Warningf("Recovered in iteration: %v", r)
		}
	}()
	fn()
}
