package database

import (
	"bytes"
	"strings"
)

func (con *DBConnection) DescribeTableColumnsSQLServer(tableName string, b *bytes.Buffer) {
	b.WriteString("select column_name, data_type, character_maximum_length from information_schema.columns where table_name = '" + tableName + "';")
}

func (con *DBConnection) GetContactMD5SQLServer(tableName string, ids []string, b *bytes.Buffer) {
	b.WriteString("SELECT \"$id\", \"$md5\" FROM " + tableName + " WHERE \"$id\" IN (")
	for i, id := range ids {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("'" + id + "'")
	}
	b.WriteString(");")
}

func (con *DBConnection) CreateIndexSQLServer(tableName string, columns []string, b *bytes.Buffer) {

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

func (con *DBConnection) CreateTableSQLServer(tableName string, columnMaps []map[string]string, b *bytes.Buffer) {
	var primaryKey = "id"
	if strings.HasSuffix(tableName, "contacts") {
		primaryKey = "$id"
	}

	var cols []string
	// Iterate over the slice of maps
	for _, colMap := range columnMaps {
		for cName, cType := range colMap {
			def := "[" + cName + "] " + con.toDBType(cType) // Use brackets for SQL Server identifier compatibility
			if cName == primaryKey {
				def += " NOT NULL PRIMARY KEY"
			}
			cols = append(cols, def)
		}
	}

	b.WriteString("IF NOT EXISTS (SELECT [name] FROM sys.tables WHERE [name] = ")
	b.WriteString("'" + tableName + "'")
	b.WriteString(") BEGIN ")
	b.WriteString("CREATE TABLE ")
	b.WriteString(tableName)
	b.WriteString(" (" + strings.Join(cols, ", ") + ")")
	b.WriteString("; END;")
}

func (con *DBConnection) AddColumnsSQLServer(tableName string, columnMaps []map[string]string, b *bytes.Buffer) {
	b.WriteString("ALTER TABLE ")
	b.WriteString(tableName)
	b.WriteString(" ADD ")

	isFirst := true
	for _, colMap := range columnMaps {
		for key, value := range colMap {
			if !isFirst {
				b.WriteString(", ") // Add a comma before each new column after the first
			} else {
				isFirst = false
			}

			b.WriteString("[") // Ensure column names are enclosed in square brackets
			b.WriteString(key)
			b.WriteString("] ")
			b.WriteString(value)
		}
	}
	b.WriteString(";")
}

func (con *DBConnection) ChangeColumnsSQLServer(tableName string, columns map[string]string, b *bytes.Buffer) {
	for key, value := range columns {
		b.WriteString("ALTER TABLE ")
		b.WriteString(tableName)
		b.WriteString(" ALTER COLUMN ")
		b.WriteString("[" + key + "] ")
		b.WriteString(value)
		b.WriteString(";")
	}
}

func (con *DBConnection) PrepareUpsertSQLServer(tableName string, columns []string, b *bytes.Buffer) {

	// Convert keys and values to string array
	var primaryKey = "id"
	if strings.HasSuffix(tableName, "contacts") {
		primaryKey = "$id"
	}
	var cols []string
	var insertData []string
	var updateData []string
	for _, col := range columns {
		cols = append(cols, "["+col+"]")
		insertData = append(insertData, "?")           //+strconv.Itoa(len(columns)+idx+2))
		updateData = append(updateData, "["+col+"]=?") //+strconv.Itoa(idx+2))
	}

	// Insert data
	b.WriteString("MERGE " + tableName)
	b.WriteString(" USING ")
	b.WriteString("(")
	b.WriteString("SELECT ")
	b.WriteString("? AS ID")
	b.WriteString(") AS T")
	b.WriteString(" ON ")
	b.WriteString(tableName + ".[" + primaryKey + "]")
	b.WriteString("=")
	b.WriteString("T.ID")
	b.WriteString(" WHEN MATCHED THEN UPDATE SET ")
	b.WriteString(strings.Join(updateData, ","))
	b.WriteString(" WHEN NOT MATCHED THEN ")
	b.WriteString("INSERT")
	b.WriteString(" (")
	b.WriteString(strings.Join(cols, ","))
	b.WriteString(") ")
	b.WriteString("VALUES")
	b.WriteString(" (")
	b.WriteString(strings.Join(insertData, ","))
	b.WriteString(")")
	b.WriteString(";")
}
