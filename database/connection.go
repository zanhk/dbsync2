package database

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"math"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"bitbucket.org/modima/dbsync2/go-logging"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type DBConnection struct {
	DB          *sql.DB
	DBType      string
	TablePrefix string
	TableMeta   map[string]map[string]*ColumnMeta // table | column | metadata of column
}

// use []map[string]string to preserve order (golang maps are unordered)
var tableSchemas = map[string][]map[string]string{
	"contact": {
		{"$id": "short_string"},
		{"$md5": "short_string"},
		{"$ref": "short_string"},
		{"$version": "short_string"},
		{"$campaign_id": "short_string"},
		{"$task_id": "short_string"},
		{"$task": "short_string"},
		{"$status": "short_string"},
		{"$status_detail": "string"},
		{"$created_date": "short_string"},
		{"$entry_date": "short_string"},
		{"$owner": "short_string"},
		{"$follow_up_date": "short_string"},
		{"$phone": "short_string"},
		{"$timezone": "short_string"},
		{"$caller_id": "short_string"},
		{"$source": "short_string"},
		{"$comment": "text"},
		{"$error": "short_string"},
		{"$recording": "string"},
		{"$recording_url": "string"},
		{"$changed": "short_string"},
	},
	"transaction": {
		{"id": "short_string"},
		{"contact_id": "short_string"},
		{"task_id": "short_string"},
		{"task": "short_string"},
		{"status": "short_string"},
		{"status_detail": "string"},
		{"fired": "short_string"},
		{"pause_time_sec": "int"},
		{"edit_time_sec": "int"},
		{"wrapup_time_sec": "int"},
		{"user": "short_string"},
		{"user_loginName": "short_string"},
		{"user_branch": "short_string"},
		{"user_tenantAlias": "short_string"},
		{"actor": "short_string"},
		{"type": "short_string"},
		{"result": "short_string"},
		{"trigger": "short_string"},
		{"isHI": "bool"},
		{"revoked": "bool"},
		{"$changed": "short_string"},
	},
	"connection": {
		{"id": "short_string"},
		{"parent_connection_id": "short_string"},
		{"transfer_target_address": "short_string"},
		{"call_uuid": "short_string"},
		{"global_call_uuid": "string"},
		{"transaction_id": "short_string"},
		{"task_id": "short_string"},
		{"contact_id": "short_string"},
		{"phone": "short_string"},
		{"user": "short_string"},
		{"actor": "short_string"},
		{"hangup_party": "short_string"},
		{"isThirdPartyConnection": "short_string"},
		{"dialerdomain": "short_string"},
		{"fired": "short_string"},
		{"started": "short_string"},
		{"initiated": "short_string"},
		{"connected": "short_string"},
		{"disconnected": "short_string"},
		{"ended": "short_string"},
		{"duration": "int"},
		{"remote_number": "short_string"},
		{"line_number": "short_string"},
		{"technology": "short_string"},
		{"result": "short_string"},
		{"code": "int"},
		{"call_initiated": "short_string"},
		{"call_connected": "short_string"},
		{"call_disconnected": "short_string"},
		{"$changed": "short_string"},
	},
	"recording": {
		{"id": "short_string"},
		{"contact_id": "short_string"},
		{"connection_id": "short_string"},
		{"started": "short_string"},
		{"stopped": "short_string"},
		{"filename": "string"},
		{"location": "string"},
		{"$changed": "short_string"},
	},
	"inbound_call": {
		{"id": "short_string"},
		{"line_id": "short_string"},
		{"campaign_id": "short_string"},
		{"task_id": "short_string"},
		{"task_name": "short_string"},
		{"contact_id": "short_string"},
		{"remote_number": "short_string"},
		{"line_number": "short_string"},
		{"started": "short_string"},
		{"connected": "short_string"},
		{"disconnected": "short_string"},
		{"user": "short_string"},
		{"state": "short_string"},
		{"connectable_time": "short_string"},
		{"disposition": "short_string"},
		{"dispatch_error": "bool"},
		{"$changed": "short_string"},
	},
}

var dbTypes = map[string]map[string]string{
	"mysql": {
		"short_string":            "varchar(50)",
		"string":                  "varchar(100)",
		"text":                    "text",
		"int":                     "numeric",
		"float64":                 "numeric",
		"json.Number":             "numeric",
		"bool":                    "boolean",
		"map[string]interface {}": "json",
		"[]interface {}":          "json",
	},
	"postgres": {
		"short_string":            "varchar(50)",
		"string":                  "varchar",
		"text":                    "text",
		"int":                     "numeric",
		"float64":                 "numeric",
		"json.Number":             "numeric",
		"bool":                    "boolean",
		"map[string]interface {}": "json",
		"[]interface {}":          "json",
	},
	"sqlserver": {
		"short_string":            "nvarchar(50)",
		"string":                  "nvarchar",
		"text":                    "text",
		"int":                     "nvarchar(50)",
		"float64":                 "nvarchar(50)",
		"json.Number":             "nvarchar(50)",
		"bool":                    "bit",
		"map[string]interface {}": "nvarchar", // Maximum is 4000
		"[]interface {}":          "nvarchar", // Maximum is 4000
	},
}

func (con *DBConnection) toDBType(gotype string) string {

	var dbType = dbTypes[con.DBType][gotype]
	if dbType == "" {
		panic("unsupported type " + gotype)
	}

	return dbType
}

var log *logging.Logger

// URIs:
// MYSQL: user:password@/{TABLE_PREFIX}ml_camp?charset=utf8mb4&collation=utf8mb4_bin
// POSTGRES: postgres://user:password@localhost:5432/{TABLE_PREFIX}ml_camp
// MSSQL: sqlserver://user:password@localhost:1433?database={TABLE_PREFIX}ml_camp
func Open(dbType string, uri string, tablePrefix string, l *logging.Logger) (*DBConnection, error) {

	log = l

	var db *sql.DB
	var err error

	switch dbType {

	case "mysql":
		// Remove protocol prefix (not allowed with mysql)
		var idx1 = strings.Index(uri, "://")
		var idx2 = strings.Index(uri, "@")
		var idx3 = strings.LastIndex(uri, "/")
		uri = uri[idx1+3:idx2+1] + "tcp(" + uri[idx2+1:idx3] + ")" + uri[idx3:]
		uri += "?charset=utf8mb4&collation=utf8mb4_general_ci"
		db, err = sql.Open("mysql", uri)

	case "sqlserver":
		var dbIdx = strings.LastIndex(uri, "/")
		uri = uri[:dbIdx] + "?database=" + uri[dbIdx+1:]
		//log.Infof("Connect to url: %v", uri)
		db, err = sql.Open("mssql", uri)

	case "postgres":
		db, err = sql.Open("postgres", uri)
	}

	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	log.Infof("Successfully connected to %v database", dbType)

	var con = DBConnection{
		DB:          db,
		DBType:      dbType,
		TablePrefix: tablePrefix,
	}

	return &con, nil
}

type Entity struct {
	Type string
	Data map[string]interface{}
}

func (e *Entity) GetPrimaryKeyName() string {
	if e.Type == "contact" {
		return "$id"
	} else {
		return "id"
	}
}

func (e *Entity) GetID() string {
	if e.Type == "contact" {
		return e.Data["$id"].(string)
	} else {
		return e.Data["id"].(string)
	}
}

func jitter(millis int) int {
	return millis/2 + rand.Intn(millis)
}

func Contains(a []map[string]string, x string) bool {
	for i := 0; i < len(a); i++ {
		for key := range a[i] {
			if key == x {
				return true
			}
		}
	}
	return false
}

/*
func GetMapVal(a []map[string]string, x string) string {
	for i := 0; i < len(a); i++ {
		for key := range a[i] {
			if key == x {
				return a[i][key]
			}
		}
	}
	return ""
}
*/

func (con *DBConnection) Upsert(entity Entity) error {

	var tableName = con.TablePrefix + entity.Type + "s"
	var data = filter(entity)

	//log.Debugf("upsert %v", data)

	// Extract fieldNames and values
	var fieldNames []string
	var values []interface{}
	for name, value := range data {
		// Find the column type in the new tableSchemas structure
		found := false
		for _, colMap := range tableSchemas[entity.Type] {
			if _, ok := colMap[name]; ok {
				found = true
				break
			}
		}

		// Skip fields that are not defined in schema definition
		if !found {
			continue
		}

		fieldNames = append(fieldNames, name)

		// Universal handling for boolean fields, converting them to tinyint
		/*
			if valueBool, ok := value.(bool); ok {
				// Convert bool to tinyint
				if valueBool {
					values = append(values, 1)
				} else {
					values = append(values, 0)
				}
				continue
			}
		*/

		// Handle specific cases like comments
		if entity.Type == "contact" && strings.EqualFold(name, "$comment") {
			values = append(values, value)
		} else {
			// Limit string lengths as per column definitions or standards
			var maxLen = 100
			if strings.HasPrefix(name, "$") {
				maxLen = 50
			}
			if con.TableMeta[entity.Type] != nil {
				if colMeta, ok := con.TableMeta[entity.Type][name]; ok {
					maxLen = int(colMeta.Len)
				}
			}
			//log.Debugf("start convert name %v value %of of entity %v", name, value, entity.GetID())
			values = append(values, con.toDBString(value, maxLen))
		}
	}

	// Append $changed (timestamp of last update)
	fieldNames = append(fieldNames, "$changed")
	values = append(values, con.toDBString(time.Now().UTC().Format(time.RFC3339), 50))

	// Prepare statement
	var (
		stmt   *sql.Stmt
		result sql.Result
		err    error
	)
	for i := 0; i < 10; i++ {
		stmt, err = con.PrepareUpsertStatement(tableName, fieldNames)
		if err == nil {
			break
		}
		timeout := time.Second*time.Duration(math.Pow(2, float64(i))) + time.Duration(jitter(5000))*time.Millisecond
		log.Warningf("prepare upsert %v %v failed with error: %v | retry in %v", entity.Type, entity.GetID(), err.Error(), timeout)
		time.Sleep(timeout)
	}

	// Close statement
	defer stmt.Close()

	// duplicate data (1. Insert / 2. Update)
	values = append(values, values...)

	// SQLServer needs additional field $id for query
	if con.DBType == "sqlserver" {
		values = append([]interface{}{entity.GetID()}, values...)
	}

	// Execute statement
	for i := 0; i < 10; i++ {
		result, err = stmt.Exec(values...)
		if err == nil {
			_, err := result.RowsAffected()
			if err != nil {
				log.Warningf("upsert %v %v failed with error: %v", entity.Type, entity.GetID(), err.Error())
			}

			break
		}
		timeout := time.Second*time.Duration(math.Pow(2, float64(i))) + time.Duration(jitter(5000))*time.Millisecond
		log.Warningf("upsert %v %v failed with error: %v | retry in %v", entity.Type, entity.GetID(), err.Error(), timeout)
		time.Sleep(timeout)
	}
	return err
}
func (con *DBConnection) PrepareUpsertStatement(tableName string, data []string) (*sql.Stmt, error) {

	var b bytes.Buffer

	switch con.DBType {

	case "mysql":
		con.PrepareUpsertMySQL(tableName, data, &b)

	case "postgres":
		con.PrepareUpsertPostgres(tableName, data, &b)

	case "sqlserver":
		con.PrepareUpsertSQLServer(tableName, data, &b)
	}
	return con.DB.Prepare(b.String())
}

func filter(entity Entity) map[string]interface{} {
	var filteredData = make(map[string]interface{})

	// Access the slice of column definitions for the entity's type
	columns := tableSchemas[entity.Type]

	// Iterate over the slice of maps
	for _, columnMap := range columns {
		for cName := range columnMap {
			// Check if the entity data has a value for the column name
			if value, ok := entity.Data[cName]; ok {
				filteredData[cName] = value
			}
		}
	}

	return filteredData
}

type CampaignFieldList []struct {
	Type      string `json:"type"`
	FieldType string `json:"fieldType"`
	Name      string `json:"name"`
	State     string `json:"state"`
	Deleted   bool   `json:"deleted"`
}

func (con *DBConnection) UpdateTables(fieldList CampaignFieldList) error {

	var count = 0
	for _, element := range fieldList {

		if element.Type != "field" || element.Deleted {
			continue
		}

		/* Kampagnentypen:
		"text":         "string",
		"date":         "string",
		"calendar":     "string",
		"phone":        "string",
		"radiogroup":   "string",
		"dropdown":     "string",
		"autocomplete": "string",
		"checkbox":     "bool",
		"number":       "int", // TODO: Fließkomma unterstützen
		"textarea":     "text",
		*/
		// Abbildung auf datenbanktype
		var dbType string
		switch element.FieldType {

		/* Datatype of checkbox is String
		case "checkbox":
			dbType = "bool"
		*/

		case "number":
			dbType = "int"

		default:
			dbType = "text"
		}

		// Check if the column already exists in the schema
		exists := false
		for _, colMap := range tableSchemas["contact"] {
			if _, ok := colMap[element.Name]; ok {
				exists = true
				break
			}
		}

		if !exists {
			// Add the new column to the schema
			tableSchemas["contact"] = append(tableSchemas["contact"], map[string]string{element.Name: dbType})
			count++
		}
		/*
			// Maximal 150 weitere Spalten
			if count == 150 {
				break
			}
		*/
	}

	for entityType, columnMaps := range tableSchemas {
		// Convert the slice of maps to a more manageable structure if needed
		// For creating the table, you may just pass the entire slice if your createTable function is adapted to handle it
		if err := con.createTable(entityType, columnMaps); err != nil {
			return err
		}

		// For updating columns, depending on what exactly updateColumns does, you may need to adapt it as well
		if err := con.updateColumns(entityType, columnMaps); err != nil {
			return err
		}
	}

	// create index
	con.createIndex("contact", []string{"$id", "$md5"})

	// read column metadata to get length of column
	con.RefreshTableMetadata("contact")
	go func() {
		t := time.NewTicker(5 * time.Minute)
		for {
			<-t.C
			con.RefreshTableMetadata("contact")
		}
	}()

	return nil
}

func (con *DBConnection) RefreshTableMetadata(entityType string) {
	con.TableMeta = map[string]map[string]*ColumnMeta{
		entityType: con.DescribeTableColumns(entityType + "s"),
	}
}

type ColumnMeta struct {
	Name string
	Type string
	Len  int32
}

func (con *DBConnection) DescribeTableColumns(tableName string) map[string]*ColumnMeta {

	tableName = con.TablePrefix + tableName

	var b bytes.Buffer

	switch con.DBType {

	case "mysql":
		con.DescribeTableColumnsMySQL(tableName, &b)

	case "postgres":
		con.DescribeTableColumnsPostgres(tableName, &b)

	case "sqlserver":
		con.DescribeTableColumnsSQLServer(tableName, &b)
	}

	rows, err := con.DB.Query(b.String())
	if err != nil {
		if err != sql.ErrNoRows {
			log.Error(err)
		}
	}
	defer rows.Close()

	var colMetaByColName = map[string]*ColumnMeta{}
	for rows.Next() {
		var (
			colName, colType sql.NullString
			colLen           sql.NullInt32
		)
		err := rows.Scan(&colName, &colType, &colLen)
		if err != nil {
			log.Error(err)
			continue
		}
		if colName.Valid && colType.Valid {
			colMetaByColName[colName.String] = &ColumnMeta{
				Name: colName.String,
				Type: colType.String,
				Len:  colLen.Int32,
			}
		}
	}
	err = rows.Err()
	if err != nil {
		log.Error(err)
	}

	return colMetaByColName
}

func (con *DBConnection) QueryMD5(ids []string) map[string]string {

	var tableName = con.TablePrefix + "contacts"

	var b bytes.Buffer

	switch con.DBType {

	case "mysql":
		con.GetContactMD5MySQL(tableName, ids, &b)

	case "postgres":
		con.GetContactMD5Postgres(tableName, ids, &b)

	case "sqlserver":
		con.GetContactMD5SQLServer(tableName, ids, &b)
	}

	rows, err := con.DB.Query(b.String())
	if err != nil {
		if err != sql.ErrNoRows {
			log.Error(err)
		}
	}
	defer rows.Close()

	var md5sByContactID = map[string]string{}
	for rows.Next() {
		var id, md5 sql.NullString
		err := rows.Scan(&id, &md5)
		if err != nil {
			log.Error(err)
			continue
		}
		if id.Valid && md5.Valid {
			md5sByContactID[id.String] = md5.String
		}
	}
	err = rows.Err()
	if err != nil {
		log.Error(err)
	}

	return md5sByContactID
}

func (con *DBConnection) createIndex(entityType string, columns []string) {

	var tableName = con.TablePrefix + entityType + "s"

	var b bytes.Buffer

	switch con.DBType {

	case "mysql":
		con.CreateIndexMySQL(tableName, columns, &b)

	case "postgres":
		con.CreateIndexPostgres(tableName, columns, &b)

	case "sqlserver":
		con.CreateIndexSQLServer(tableName, columns, &b)
	}

	_, err := con.DB.Exec(b.String())
	if err != nil {
		log.Warning(err)
	}
}

func (con *DBConnection) createTable(entityType string, columns []map[string]string) error {

	var tableName = con.TablePrefix + entityType + "s"

	log.Debugf("create table %v using columns %v", tableName, columns)

	var b bytes.Buffer

	switch con.DBType {

	case "mysql":
		con.CreateTableMySQL(tableName, columns, &b)

	case "postgres":
		con.CreateTablePostgres(tableName, columns, &b)

	case "sqlserver":
		con.CreateTableSQLServer(tableName, columns, &b)
	}

	var sqlStmnt = b.String()
	log.Infof(sqlStmnt)

	_, err := con.DB.Exec(b.String())
	if err != nil {
		log.Error(err)
	}

	return err
}

func (con *DBConnection) updateColumns(entityType string, columnMaps []map[string]string) error {
	// Get existing columns from the database for the specified table
	existingColumns := con.getTableColumns(entityType)
	log.Infof("existing columns of table %v: %v", entityType, existingColumns)

	// Prepare to collect new columns that are not yet in the database
	var newColumns []map[string]string

	// Iterate through each column definition to check if it exists
	for _, colMap := range columnMaps {
		for cName, cType := range colMap {
			dbType := con.toDBType(cType)
			if existingColumns[cName] == "" { // Column does not exist
				newColumns = append(newColumns, map[string]string{cName: dbType})
			}
			/*
			   else if existingColumns[cName] != dbType {
			       // Uncomment and implement if you need to handle changed columns
			       changedColumns[cName] = dbType
			   }
			*/
		}
	}

	// If there are new columns to be added, pass them to the addTableColumns function
	if len(newColumns) > 0 {
		log.Infof("new columns of table %v: %v", entityType, newColumns)
		if err := con.addTableColumns(entityType, newColumns); err != nil {
			return err
		}
	}

	return nil
}

func (con *DBConnection) toDBString(value interface{}, maxLen int) interface{} {
	if value == nil {
		log.Debugf("convert nil value to db string")
		return sql.NullString{Valid: false}
	}

	log.Debugf("convert value %v to db string (type: %v)", value, reflect.TypeOf(value).String())

	var result string

	switch v := value.(type) {

	case json.Number:
		if strings.Contains(v.String(), ".") {
			vNum, _ := v.Float64()
			result = strconv.FormatFloat(vNum, 'f', 10, 64)
		} else {
			vNum, _ := v.Int64()
			result = strconv.FormatInt(vNum, 10)
		}

	case []interface{}:
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			panic(err)
		}
		result = string(jsonBytes)

	case map[string]interface{}:
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			panic(err)
		}
		result = string(jsonBytes)

	case int:
		result = strconv.Itoa(v)

	case bool:
		if v {
			result = "1"
		} else {
			result = "0"
		}

	case string:
		runes := []rune(v)
		if maxLen > 0 && len(runes) > maxLen {
			runes = runes[:maxLen]
		}
		result = string(runes)
		if !utf8.ValidString(result) {
			log.Warningf("convert invalid string: %v", result)
			// Clean up invalid UTF-8 characters
			result = strings.ToValidUTF8(result, "")
		}

	default:
		panic("unsupported type " + reflect.TypeOf(value).String())
	}

	if len(result) == 0 {
		return sql.NullString{Valid: false}
	}

	return result
}

func (con *DBConnection) getTableColumns(entityType string) map[string]string {

	var tableName = con.TablePrefix + entityType + "s"

	// create statement
	var stmt = ""
	switch con.DBType {

	case "mysql":
		stmt = "SELECT column_name, column_type FROM information_schema.columns WHERE table_name = '" + tableName + "';"

	case "sqlserver":
		//stmt = "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('" + tableName + "')"
		stmt = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "';"

	case "postgres":
		//stmt = "SELECT column_name, column_type FROM information_schema.columns WHERE table_name = '" + tableName + "';"
		stmt = "SELECT column_name, udt_name, character_maximum_length FROM information_schema.columns WHERE table_name = '" + tableName + "';"
	}

	// execute statement
	rows, err := con.DB.Query(stmt)
	if err != nil {
		log.Errorf("Statement '%v' failed with error '%v'\n", stmt, err.Error())
		os.Exit(1)
	}
	defer rows.Close()

	// process results
	var columns = map[string]string{}
	switch con.DBType {

	case "mysql":
		var colname, coltype string
		for rows.Next() {
			rows.Scan(&colname, &coltype)
			columns[colname] = coltype
		}
	case "postgres":
		var colname, coltype, collength string
		for rows.Next() {
			rows.Scan(&colname, &coltype, &collength)
			if strings.EqualFold(coltype, "varchar") {
				coltype += "(" + collength + ")"
			}
			columns[colname] = coltype
		}

	case "sqlserver":
		var colname, coltype, collength string
		for rows.Next() {
			rows.Scan(&colname, &coltype, &collength)
			if strings.EqualFold(coltype, "nvarchar") {
				coltype += "(" + collength + ")"
			}
			columns[colname] = coltype
		}
	}

	return columns
}

func (con *DBConnection) addTableColumns(entityType string, newColumns []map[string]string) error {

	var tableName = con.TablePrefix + entityType + "s"

	var b bytes.Buffer

	switch con.DBType {

	case "mysql":
		con.AddColumnsMySQL(tableName, newColumns, &b)

	case "postgres":
		con.AddColumnsPostgres(tableName, newColumns, &b)

	case "sqlserver":
		con.AddColumnsSQLServer(tableName, newColumns, &b)
	}

	var statements = strings.Split(b.String(), ";")
	for _, stmt := range statements {
		if len(stmt) > 0 {
			if err := con.execStatement(stmt); err != nil {
				return err
			}
		}
	}
	return nil
}

func (con *DBConnection) changeTableColumns(entityType string, changedColumns map[string]string) error {

	var tableName = con.TablePrefix + entityType + "s"

	var b bytes.Buffer

	switch con.DBType {

	case "mysql":
		con.ChangeColumnsMySQL(tableName, changedColumns, &b)

	case "postgres":
		con.ChangeColumnsPostgres(tableName, changedColumns, &b)

	case "sqlserver":
		con.ChangeColumnsSQLServer(tableName, changedColumns, &b)
	}

	var statements = strings.Split(b.String(), ";")
	for _, stmt := range statements {
		if len(stmt) > 0 {
			if err := con.execStatement(stmt); err != nil {
				return err
			}
		}
	}
	return nil
}

func (con *DBConnection) execStatement(sqlStmnt string) error {
	log.Infof(sqlStmnt)
	_, err := con.DB.Exec(sqlStmnt)
	if err != nil {
		me, ok := err.(*mysql.MySQLError)
		if !ok {
			return err
		}
		if me.Number == 1060 { // Duplicate column name
			log.Warningf(me.Error())
			return nil
		}
		log.Error(err)
	}
	return err
}
