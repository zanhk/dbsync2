package database

import (
	"bytes"
	"strings"
)

func (con *DBConnection) DescribeTableColumnsMySQL(tableName string, b *bytes.Buffer) {
	b.WriteString("select column_name, data_type, character_maximum_length from information_schema.columns where table_name = '" + tableName + "';")
}

func (con *DBConnection) GetContactMD5MySQL(tableName string, ids []string, b *bytes.Buffer) {
	b.WriteString("SELECT `$id`, `$md5` FROM " + tableName + " WHERE `$id` IN (")
	for i, id := range ids {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("'" + id + "'")
	}
	b.WriteString(");")
}

func (con *DBConnection) CreateIndexMySQL(tableName string, columns []string, b *bytes.Buffer) {

	var indexName = ""
	for i, col := range columns {
		if i > 0 {
			indexName += "|"
		}
		indexName += col
	}

	b.WriteString("CREATE INDEX `" + indexName + "` ON " + tableName + "(")
	for i, col := range columns {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("`" + col + "`")
	}
	b.WriteString(");")
}

func (con *DBConnection) CreateTableMySQL(tableName string, columnMaps []map[string]string, b *bytes.Buffer) {
	var primaryKey = "id"
	if strings.HasSuffix(tableName, "contacts") {
		primaryKey = "$id"
	}

	var cols []string
	for _, colMap := range columnMaps {
		for cName, cType := range colMap {
			def := "`" + cName + "` " + con.toDBType(cType)
			if cName == primaryKey {
				def += " NOT NULL PRIMARY KEY"
			}
			cols = append(cols, def)
		}
	}

	b.WriteString("CREATE TABLE IF NOT EXISTS ")
	b.WriteString(tableName)
	b.WriteString(" (" + strings.Join(cols, ", ") + ") ")
	b.WriteString("CHARACTER SET utf16;") // Removed commented parts for clarity
}

func (con *DBConnection) AddColumnsMySQL(tableName string, columnMaps []map[string]string, b *bytes.Buffer) {
	b.WriteString("ALTER TABLE ")
	b.WriteString(tableName)

	// Iterate over the slice of maps
	isFirst := true
	for _, colMap := range columnMaps {
		for key, value := range colMap {
			if !isFirst {
				b.WriteString(", ") // Add comma before each column addition except the first
			} else {
				isFirst = false
			}

			b.WriteString(" ADD ")
			b.WriteString("`" + key + "` ") // Use backticks for MySQL identifiers
			b.WriteString(value)
		}
	}
	b.WriteString(";")
}

func (con *DBConnection) ChangeColumnsMySQL(tableName string, columns map[string]string, b *bytes.Buffer) {

	b.WriteString("ALTER TABLE ")
	b.WriteString(tableName)

	var idx = 0
	for key, value := range columns {

		b.WriteString(" MODIFY COLUMN ")
		b.WriteString("`" + key + "` ")
		b.WriteString(value)

		if idx < len(columns)-1 {
			b.WriteString(",")
		}
		idx++
	}
	b.WriteString(";")
}

func (con *DBConnection) PrepareUpsertMySQL(tableName string, columns []string, b *bytes.Buffer) {

	// Convert keys and values to string array
	var cols []string
	var insertData []string
	var updateData []string
	for _, col := range columns {
		cols = append(cols, "`"+col+"`")
		insertData = append(insertData, "?")
		updateData = append(updateData, "`"+col+"`=?")
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
	/*
		for i := 0; i < count; i++ {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(" (")
			b.WriteString(strings.Join(insertData, ","))
			b.WriteString(")")
		}
	*/
	b.WriteString(" ON DUPLICATE KEY UPDATE ")
	b.WriteString(strings.Join(updateData, ","))
	b.WriteString(";")
}
