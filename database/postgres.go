package database

import (
	"bytes"
	"strconv"
	"strings"
)

func (con *DBConnection) DescribeTableColumnsPostgres(tableName string, b *bytes.Buffer) {
	b.WriteString("select column_name, data_type, character_maximum_length from information_schema.columns where table_name = '" + tableName + "';")
}

func (con *DBConnection) GetContactMD5Postgres(tableName string, ids []string, b *bytes.Buffer) {
	b.WriteString("SELECT '$id', '$md5' FROM " + tableName + " WHERE '$id' IN (")
	for i, id := range ids {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("'" + id + "'")
	}
	b.WriteString(");")
}

func (con *DBConnection) CreateIndexPostgres(tableName string, columns []string, b *bytes.Buffer) {

	var indexName = ""
	for i, col := range columns {
		if i > 0 {
			indexName += "|"
		}
		indexName += col
	}

	b.WriteString("CREATE INDEX \"" + indexName + "\" ON " + tableName + "(")
	for i, col := range columns {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("\"" + col + "\"")
	}
	b.WriteString(");")
}

func (con *DBConnection) CreateTablePostgres(tableName string, columnMaps []map[string]string, b *bytes.Buffer) {
	var primaryKey = "id"
	if strings.HasSuffix(tableName, "contacts") {
		primaryKey = "$id"
	}

	var cols []string
	// Iterate over the slice of maps
	for _, colMap := range columnMaps {
		for cName, cType := range colMap {
			def := "\"" + cName + "\" " + con.toDBType(cType) // Ensure quotes for PostgreSQL identifier compatibility
			if cName == primaryKey {
				def += " NOT NULL PRIMARY KEY"
			}
			cols = append(cols, def)
		}
	}

	b.WriteString("CREATE TABLE IF NOT EXISTS ")
	b.WriteString(tableName)
	b.WriteString(" (" + strings.Join(cols, ", ") + ")")
	b.WriteString(";")
}

func (con *DBConnection) AddColumnsPostgres(tableName string, columnMaps []map[string]string, b *bytes.Buffer) {
	b.WriteString("ALTER TABLE ")
	b.WriteString(tableName)

	// Iterate over the slice of maps
	isFirst := true
	for _, colMap := range columnMaps {
		for key, value := range colMap {
			if !isFirst {
				b.WriteString(", ") // Add comma before each new ADD COLUMN clause after the first
			} else {
				isFirst = false
			}

			b.WriteString(" ADD IF NOT EXISTS ")
			b.WriteString("\"" + key + "\" ")
			b.WriteString(value)
		}
	}
	b.WriteString(";")
}

func (con *DBConnection) ChangeColumnsPostgres(tableName string, columns map[string]string, b *bytes.Buffer) {

	b.WriteString("ALTER TABLE ")
	b.WriteString(tableName)

	var idx = 0
	for key, value := range columns {

		b.WriteString(" ALTER COLUMN ")
		b.WriteString("\"" + key + "\" ")
		b.WriteString(" TYPE ")
		b.WriteString(value)

		if idx < len(columns)-1 {
			b.WriteString(",")
		}
		idx++
	}
	b.WriteString(";")
}

func (con *DBConnection) PrepareUpsertPostgres(tableName string, columns []string, b *bytes.Buffer) {

	// Convert keys and values to string array
	var primaryKey = "id"
	if strings.HasSuffix(tableName, "contacts") {
		primaryKey = "$id"
	}
	var cols []string
	var insertData []string
	var updateData []string
	for idx, col := range columns {
		cols = append(cols, "\""+col+"\"")
		insertData = append(insertData, "$"+strconv.Itoa(idx+1))
		updateData = append(updateData, "\""+col+"\"=$"+strconv.Itoa(len(columns)+idx+1))
	}

	// Insert data
	b.WriteString("INSERT INTO ")
	b.WriteString(tableName)
	b.WriteString(" (")
	b.WriteString(strings.Join(cols, ","))
	b.WriteString(") ")
	b.WriteString("VALUES")
	b.WriteString(" (")
	b.WriteString(strings.Join(insertData, ","))
	b.WriteString(")")
	b.WriteString(" ON CONFLICT ")
	b.WriteString("(\"" + primaryKey + "\")")
	b.WriteString(" DO UPDATE SET ")
	b.WriteString(strings.Join(updateData, ","))
	b.WriteString(";")
}
