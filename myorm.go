package myorm

import (
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"io"
	"strings"
	"sync"
	"time"
)

type MyOrmI interface {
	Table(string) QueryBuilderI
	EnableTestMode()
	DisableTestMode()
	EnableQueryLogger(io.Writer)
	DisableQueryLogger()
	GetDBConnection(connectionName string) (*sqlx.DB, error)
}

type QueryBuilderI interface {
	Insert(data map[string]interface{}) (int64, error)
	Update(data map[string]interface{}) error
	Delete() error
	Paginate(page int, perPage int, a interface{}) (*Paginated, error)
	First(a interface{}) error
	Get(a interface{}) error
	Select(columns ...string) QueryBuilderI
	EncryptedColumns(columns ...string) QueryBuilderI
	Connection(connectionName string) QueryBuilderI
	LeftJoin(table string, column1 string, operator string, column2 string) QueryBuilderI
	RightJoin(table string, column1 string, operator string, column2 string) QueryBuilderI
	InnerJoin(table string, column1 string, operator string, column2 string) QueryBuilderI
	FullJoin(table string, column1 string, operator string, column2 string) QueryBuilderI
	Where(column string, operator string, value interface{}) QueryBuilderI
	WhereRaw(query string, bindings ...interface{}) QueryBuilderI
	GroupBy(columns ...string) QueryBuilderI
	OrderBy(columns ...string) QueryBuilderI
	WhereEncrypted(column string, operator string, value interface{}) QueryBuilderI
	WhereIn(column string, values ...interface{}) QueryBuilderI
}

type DBConnection struct {
	DBUsername     string
	DBPassword     string
	DBProtocol     string
	DBAddress      string
	DBName         string
	DBDriver       string
	ConnectionName string
	EncryptionKey  string
}

type MyOrm struct {
	connections       map[string]*sqlx.DB
	defaultConnection string
	connectionNames   []string
	connectionEncKeys map[string]string
	testMode          bool
	logQueries        bool
	queryWriter       io.Writer
}

type QueryBuilder struct {
	selectColumns    []string
	where            [][3]interface{}
	whereIn          []whereIn
	table            string
	connection       string
	leftJoin         []join
	rightJoin        []join
	innerJoin        []join
	fullJoin         []join
	whereEncrypted   [][3]interface{}
	insert           map[string]interface{}
	encryptedColumns []string
	groupBy          []string
	orderBy          []string
	whereRaw         []whereRawStruct
	myOrm            *MyOrm
}

type whereRawStruct struct {
	RawQuery string
	Bindings []interface{}
}

type H map[string]interface{}

type Paginated struct {
	Total      int64       `json:"total"`
	TotalPages int64       `json:"total_pages"`
	Page       int         `json:"page"`
	PerPage    int         `json:"per_page"`
	Data       interface{} `json:"data"`
}

type whereIn struct {
	column string
	values []interface{}
}

type join struct {
	table    string
	operator string
	column1  string
	column2  string
}

func DBInit(cons ...DBConnection) *MyOrm {
	var orm MyOrm
	orm.connections = make(map[string]*sqlx.DB)
	orm.connectionNames = []string{}
	orm.connectionEncKeys = make(map[string]string)
	orm.testMode = false
	orm.logQueries = false

	for i, con := range cons {
		dataSourceName := fmt.Sprintf("%v:%v@%v(%v)/%v?parseTime=true", con.DBUsername, con.DBPassword, con.DBProtocol, con.DBAddress, con.DBName)
		db, err := sqlx.Open(con.DBDriver, dataSourceName)

		if err != nil {
			panic(err.Error())
		}

		if i == 0 {
			// First Connection is default connection
			orm.defaultConnection = con.ConnectionName
		}

		orm.connectionNames = append(orm.connectionNames, con.ConnectionName)
		orm.connections[con.ConnectionName] = db

		orm.connectionEncKeys[con.ConnectionName] = con.EncryptionKey
	}

	return &orm
}

// MyOrm Methods

func (o *MyOrm) Table(table string) QueryBuilderI {
	qb := QueryBuilder{
		table: table,
		myOrm: o,
	}

	return &qb
}

func (o *MyOrm) EnableTestMode() {
	o.testMode = true
}

func (o *MyOrm) DisableTestMode() {
	o.testMode = false
}

func (o *MyOrm) EnableQueryLogger(writer io.Writer) {
	o.logQueries = true
	o.queryWriter = writer
}

func (o *MyOrm) DisableQueryLogger() {
	o.logQueries = false
}

func (o *MyOrm) writeToLog(query string, bindings ...interface{}) {
	if o.logQueries {
		q0 := fmt.Sprintf("Timestamp: %v", time.Now())
		q1 := fmt.Sprintf("%v\nQuery: %v", q0, query)
		q2 := "Bindings: "
		for _, b := range bindings {
			q2 = fmt.Sprintf("%v %v", q2, b)
		}

		q3 := fmt.Sprintf("%v \n%v \n\n", q1, q2)
		data := []byte(q3)
		o.queryWriter.Write(data)
	}
}

func (o *MyOrm) GetDBConnection(connectionName string) (*sqlx.DB, error) {
	// Check connection exists
	if !ContainsStr(o.connectionNames, connectionName) {
		return nil, errors.New("invalid connection specified")
	}
	return o.connections[connectionName], nil
}

// Query Builder Methods

//Any number of columns names as strings
func (o *QueryBuilder) Select(columns ...string) QueryBuilderI {
	o.selectColumns = append(o.selectColumns, columns...)
	return o
}

func (o *QueryBuilder) Paginate(page int, perPage int, a interface{}) (*Paginated, error) {
	offset := (page - 1) * perPage
	queryString, bindings := genSearchQuery(o)

	queryString = fmt.Sprintf("%v LIMIT %v OFFSET %v", queryString, perPage, offset)
	// Get db
	db := o.myOrm.connections[o.myOrm.defaultConnection]
	if o.connection != "" {
		db = o.myOrm.connections[o.connection]
	}

	// Get total pages
	var total int64
	var err error

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		o.myOrm.writeToLog("COUNT(*) OVER() AS total")
		o.selectColumns = []string{"COUNT(*) OVER() AS total"}
		str, b := genSearchQuery(o)
		err = db.Get(&total, str, b...)
		wg.Done()
	}()

	go func() {
		o.myOrm.writeToLog(queryString, bindings)
		e := db.Select(a, queryString, bindings...)

		if e != nil {
			err = e
		}

		wg.Done()
	}()

	wg.Wait()

	if err != nil {
		return nil, err
	}

	totalPages := total / int64(perPage)
	remainder := total % int64(perPage)

	if remainder != 0 {
		totalPages++
	}

	return &Paginated{
		Total:      total,
		Page:       page,
		PerPage:    perPage,
		Data:       &a,
		TotalPages: totalPages,
	}, nil
}

func (o *QueryBuilder) EncryptedColumns(columns ...string) QueryBuilderI {
	o.encryptedColumns = append(o.encryptedColumns, columns...)
	return o
}

func (o *QueryBuilder) Insert(data map[string]interface{}) (int64, error) {
	// Get connection + enc key
	db := o.myOrm.connections[o.myOrm.defaultConnection]
	encKey := o.myOrm.connectionEncKeys[o.myOrm.defaultConnection]
	if o.connection != "" {
		db = o.myOrm.connections[o.connection]
		encKey = o.myOrm.connectionEncKeys[o.connection]
	}

	// Generate Insert Query
	columnsArr := make([]string, len(data))
	valuesArr := make([]interface{}, len(data))
	placeHolders := make([]string, len(data))

	i := 0
	for key, el := range data {
		columnsArr[i] = key
		valuesArr[i] = el

		// check if value is encrypted
		contains := ContainsStr(o.encryptedColumns, key)
		if contains {
			placeHolders[i] = fmt.Sprintf("AES_ENCRYPT(?, '%v')", encKey)
		} else {
			placeHolders[i] = "?"
		}
		i++
	}

	insertString := fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v)", o.table, strings.Join(columnsArr, ", "), strings.Join(placeHolders, ","))

	o.myOrm.writeToLog(insertString, valuesArr)
	result, err := db.Exec(insertString, valuesArr...)

	if err != nil {
		return 0, err
	}

	id, err := result.LastInsertId()

	if err != nil {
		return 0, err
	}

	return id, nil
}

func (o *QueryBuilder) Update(data map[string]interface{}) error {
	columnsArr := make([]string, len(data))
	var valuesArr []interface{}

	i := 0
	for col, value := range data {
		// name = ?
		columnsArr[i] = fmt.Sprintf("%v = ?", col)
		valuesArr = append(valuesArr, value)
		i++
	}

	// Generate Where Conditions
	whereStr, bindings := o.genWhere()

	// Update Query
	updateString := fmt.Sprintf("UPDATE %v SET %v %v", o.table, strings.Join(columnsArr, ", "), whereStr)

	// Join bindings
	valuesArr = append(valuesArr, bindings...)

	// Get connection
	db := o.myOrm.connections[o.myOrm.defaultConnection]
	if o.connection != "" {
		db = o.myOrm.connections[o.connection]
	}

	o.myOrm.writeToLog(updateString, valuesArr)
	_, err := db.Exec(updateString, valuesArr...)

	if err != nil {
		return err
	}

	return nil
}

func (o *QueryBuilder) Delete() error {
	whereStr, bindings := o.genWhere()

	deleteStr := fmt.Sprintf("DELETE FROM %v %v", o.table, whereStr)

	// Get connection
	db := o.myOrm.connections[o.myOrm.defaultConnection]
	if o.connection != "" {
		db = o.myOrm.connections[o.connection]
	}

	o.myOrm.writeToLog(deleteStr, bindings)
	_, err := db.Exec(deleteStr, bindings...)

	if err != nil {
		return err
	}

	return nil
}

//name of connection, DBConnection.ConnectionName
func (o *QueryBuilder) Connection(connectionName string) QueryBuilderI {
	// Check connection exists
	if ContainsStr(o.myOrm.connectionNames, connectionName) {
		panic("invalid connection specified")
	}
	o.connection = connectionName
	return o
}

func (o *QueryBuilder) LeftJoin(table string, column1 string, operator string, column2 string) QueryBuilderI {
	o.leftJoin = append(o.leftJoin, join{
		table:    table,
		operator: operator,
		column1:  column1,
		column2:  column2,
	})
	return o
}

func (o *QueryBuilder) RightJoin(table string, column1 string, operator string, column2 string) QueryBuilderI {
	o.rightJoin = append(o.rightJoin, join{
		table:    table,
		operator: operator,
		column1:  column1,
		column2:  column2,
	})
	return o
}

func (o *QueryBuilder) InnerJoin(table string, column1 string, operator string, column2 string) QueryBuilderI {
	o.innerJoin = append(o.innerJoin, join{
		table:    table,
		operator: operator,
		column1:  column1,
		column2:  column2,
	})
	return o
}

func (o *QueryBuilder) FullJoin(table string, column1 string, operator string, column2 string) QueryBuilderI {
	o.fullJoin = append(o.fullJoin, join{
		table:    table,
		operator: operator,
		column1:  column1,
		column2:  column2,
	})
	return o
}

//column name, operator (e.g. =, LIKE, !=, >, <), column value
func (o *QueryBuilder) Where(column string, operator string, value interface{}) QueryBuilderI {
	v := [3]interface{}{column, operator, value}
	o.where = append(o.where, v)
	return o
}

func (o *QueryBuilder) WhereRaw(query string, bindings ...interface{}) QueryBuilderI {
	o.whereRaw = append(o.whereRaw, whereRawStruct{
		RawQuery: query,
		Bindings: bindings,
	})
	return o
}

func (o *QueryBuilder) GroupBy(columns ...string) QueryBuilderI {
	o.groupBy = append(o.groupBy, columns...)
	return o
}

func (o *QueryBuilder) OrderBy(columns ...string) QueryBuilderI {
	o.orderBy = append(o.orderBy, columns...)
	return o
}

//column name, operator (e.g. =, LIKE, !=, >, <), column value
func (o *QueryBuilder) WhereEncrypted(column string, operator string, value interface{}) QueryBuilderI {
	v := [3]interface{}{column, operator, value}
	o.whereEncrypted = append(o.whereEncrypted, v)
	return o
}

func (o *QueryBuilder) WhereIn(column string, values ...interface{}) QueryBuilderI {
	vals := make([]interface{}, len(values))

	i := 0
	for _, el := range values {
		vals[i] = el
		i++
	}
	o.whereIn = append(o.whereIn, whereIn{
		column: column,
		values: vals,
	})
	return o
}

func (o *QueryBuilder) First(a interface{}) error {
	queryString, bindings := genSearchQuery(o)

	// Get db
	db := o.myOrm.connections[o.myOrm.defaultConnection]
	if o.connection != "" {
		db = o.myOrm.connections[o.connection]
	}
	o.myOrm.writeToLog(queryString+" LIMIT 1", bindings)
	err := db.Get(a, queryString+" LIMIT 1", bindings...)

	if err != nil {
		return err
	}

	return nil
}

func (o *QueryBuilder) Get(a interface{}) error {
	queryString, bindings := genSearchQuery(o)

	// Get db
	db := o.myOrm.connections[o.myOrm.defaultConnection]
	if o.connection != "" {
		db = o.myOrm.connections[o.connection]
	}

	o.myOrm.writeToLog(queryString, bindings)
	err := db.Select(a, queryString, bindings...)

	if err != nil {
		return err
	}

	return nil
}

func genSearchQuery(o *QueryBuilder) (string, []interface{}) {
	var queryArr []string

	// SELECT
	if selectQ := o.genSelect(); selectQ != "" {
		queryArr = append(queryArr, selectQ)
	}

	// TABLE
	queryArr = append(queryArr, fmt.Sprintf("FROM %v", o.table))

	// LEFT JOINS
	if leftJoinsQ := o.genJoin("LEFT"); leftJoinsQ != "" {
		queryArr = append(queryArr, leftJoinsQ)
	}

	// RIGHT JOINS
	if rightJoinQ := o.genJoin("RIGHT"); rightJoinQ != "" {
		queryArr = append(queryArr, rightJoinQ)
	}

	// INNER JOINS
	if innerJoinQ := o.genJoin("INNER"); innerJoinQ != "" {
		queryArr = append(queryArr, innerJoinQ)
	}

	// OUTER JOINS
	if outerJoinQ := o.genJoin("FULL OUTER"); outerJoinQ != "" {
		queryArr = append(queryArr, outerJoinQ)
	}

	// WHERE
	whereQ, bindings := o.genWhere()
	if whereQ != "" {
		queryArr = append(queryArr, whereQ)
	}

	// Group By
	if groupByQ := o.genGroupBy(); groupByQ != "" {
		queryArr = append(queryArr, groupByQ)
	}

	// Order By
	if orderByQ := o.genOrderBy(); orderByQ != "" {
		queryArr = append(queryArr, orderByQ)
	}

	return strings.Join(queryArr, " "), bindings
}

func (o *QueryBuilder) genGroupBy() string {
	str := strings.Join(o.groupBy, ", ")

	if str == "" {
		return ""
	}

	return fmt.Sprintf("group by %v", str)
}

func (o *QueryBuilder) genOrderBy() string {
	str := strings.Join(o.orderBy, ", ")

	if str == "" {
		return ""
	}

	return fmt.Sprintf("order by %v", str)
}

func (o *QueryBuilder) genSelect() string {
	var selectArr []string

	// If encrypted any encrypted columns then decrypt them
	if len(o.encryptedColumns) > 0 {
		// Get enc key
		encKey := o.myOrm.connectionEncKeys[o.myOrm.defaultConnection]
		if o.connection != "" {
			encKey = o.myOrm.connectionEncKeys[o.connection]
		}

		for _, col := range o.selectColumns {
			contains := ContainsStr(o.encryptedColumns, col)
			if contains {
				selectArr = append(selectArr, fmt.Sprintf("AES_DECRYPT(%v, '%v') AS %v", col, encKey, col))
			} else {
				selectArr = append(selectArr, col)
			}
		}
	} else {
		selectArr = o.selectColumns
	}

	selects := strings.Join(selectArr, ", ")

	if selects == "" {
		return "SELECT *"
	}
	return fmt.Sprintf("SELECT %v", selects)
}

func (o *QueryBuilder) genJoin(joinType string) string {
	var str []string
	var arr []join

	switch joinType {
	case "LEFT":
		arr = o.leftJoin
		break
	case "RIGHT":
		arr = o.rightJoin
	case "INNER":
		arr = o.innerJoin
		break
	case "FULL OUTER":
		arr = o.fullJoin
		break
	}

	for _, join := range arr {
		// ["projects", "users.id", "=", "projects.user_id"]
		// LEFT JOIN projects ON users.id = projects.user_id
		str = append(str, fmt.Sprintf("%v JOIN %v ON %v %v %v", joinType, join.table, join.column1, join.operator, join.column2))
	}

	return strings.Join(str, " ")
}

func (o *QueryBuilder) genWhere() (string, []interface{}) {
	//Where
	str := make([]string, len(o.where)+len(o.whereIn)+len(o.whereEncrypted)+len(o.whereRaw))
	var values []interface{}

	i := 0
	for _, wArr := range o.where {
		// ["column", "=", "?"]
		wStr := []string{fmt.Sprintf("%v", wArr[0]), fmt.Sprintf("%v", wArr[1]), "?"}
		// column = ?
		str[i] = strings.Join(wStr, " ")
		values = append(values, wArr[2])
		i++
	}

	//WhereEncrypted
	encKey := o.myOrm.connectionEncKeys[o.myOrm.defaultConnection]
	if o.connection != "" {
		encKey = o.myOrm.connectionEncKeys[o.connection]
	}
	for _, wArr := range o.whereEncrypted {
		wStr := []string{fmt.Sprintf("AES_DECRYPT(%v, '%v')", wArr[0], encKey), fmt.Sprintf("%v", wArr[1]), "?"}
		// AES_DECRYPT(col, key) = ?
		str[i] = strings.Join(wStr, " ")
		values = append(values, wArr[2])
		i++
	}

	//WhereIn
	for _, w := range o.whereIn {
		if len(w.values) == 0 {
			i++
			continue
		}
		// column IN (?,?)
		qMarks := make([]string, len(w.values))
		j := 0
		for _, val := range w.values {
			qMarks[j] = "?"
			values = append(values, val)
			j++
		}

		str[i] = fmt.Sprintf("%v IN (%v)", w.column, strings.Join(qMarks, ","))
		i++
	}

	//Where Raw
	for _, w := range o.whereRaw {
		str[i] = w.RawQuery
		values = append(values, w.Bindings...)
		i++
	}

	if len(str) == 0 || len(values) == 0 {
		return "", nil
	}

	return fmt.Sprintf("WHERE %v", strings.Join(str, " AND ")), values
}

// Helper Functions
func ContainsStr(haystack []string, needle string) bool {
	for _, el := range haystack {
		if el == needle {
			return true
		}
	}

	return false
}

// Mocking Functions

// Mock Query Builder

type MockQueryBuilderTemplate struct {
	FuncInsert   func(data map[string]interface{}) (int64, error)
	FuncUpdate   func(data map[string]interface{}) error
	FuncDelete   func() error
	FuncPaginate func(page int, perPage int, a interface{}) (*Paginated, error)
	FuncFirst    func(a interface{}) error
	FuncGet      func(a interface{}) error
}

func (m MockQueryBuilderTemplate) Insert(data map[string]interface{}) (int64, error) {
	return m.FuncInsert(data)
}

func (m MockQueryBuilderTemplate) Update(data map[string]interface{}) error {
	return m.FuncUpdate(data)
}

func (m MockQueryBuilderTemplate) Delete() error {
	return m.Delete()
}

func (m MockQueryBuilderTemplate) Paginate(page int, perPage int, a interface{}) (*Paginated, error) {
	return m.Paginate(page, perPage, a)
}

func (m MockQueryBuilderTemplate) First(a interface{}) error {
	return m.FuncFirst(a)
}

func (m MockQueryBuilderTemplate) Get(a interface{}) error {
	return m.FuncGet(a)
}

func (m MockQueryBuilderTemplate) Select(...string) QueryBuilderI {
	return m
}

func (m MockQueryBuilderTemplate) EncryptedColumns(...string) QueryBuilderI {
	return m
}

func (m MockQueryBuilderTemplate) Connection(string) QueryBuilderI {
	return m
}

func (m MockQueryBuilderTemplate) LeftJoin(string, string, string, string) QueryBuilderI {
	return m
}

func (m MockQueryBuilderTemplate) RightJoin(string,  string, string, string) QueryBuilderI {
	return m
}

func (m MockQueryBuilderTemplate) InnerJoin(string, string, string, string) QueryBuilderI {
	return m
}

func (m MockQueryBuilderTemplate) FullJoin(string, string, string, string) QueryBuilderI {
	return m
}

func (m MockQueryBuilderTemplate) Where(string, string, interface{}) QueryBuilderI {
	return m
}

func (m MockQueryBuilderTemplate) WhereRaw(string, ...interface{}) QueryBuilderI {
	return m
}

func (m MockQueryBuilderTemplate) GroupBy(...string) QueryBuilderI {
	return m
}

func (m MockQueryBuilderTemplate) OrderBy(...string) QueryBuilderI {
	return m
}

func (m MockQueryBuilderTemplate) WhereEncrypted(string, string, interface{}) QueryBuilderI {
	return m
}

func (m MockQueryBuilderTemplate) WhereIn(string, ...interface{}) QueryBuilderI {
	return m
}

type MockOrmTemplate struct {
	QueryBuilderMock    QueryBuilderI
	FuncTable           func(string) *QueryBuilder
	FuncEnableTestMode  func()
	FuncDisableTestMode func()
	FuncGetDBConnection func(string) (*sqlx.DB, error)
}

func (i *MockOrmTemplate) GetDBConnection(s string) (*sqlx.DB, error) {
	return i.GetDBConnection(s)
}

func (i *MockOrmTemplate) EnableTestMode() {
	i.FuncEnableTestMode()
}

func (i *MockOrmTemplate) DisableTestMode() {
	i.DisableTestMode()
}

func (i *MockOrmTemplate) EnableQueryLogger(writer io.Writer) {
	i.EnableQueryLogger(writer)
}

func (i *MockOrmTemplate) DisableQueryLogger() {
	i.DisableQueryLogger()
}

func (i *MockOrmTemplate) Table(string) QueryBuilderI {
	return i.QueryBuilderMock
}

func GenerateOrmMock(m MockQueryBuilderTemplate) MockOrmTemplate {
	return MockOrmTemplate{
		FuncTable:           nil,
		FuncEnableTestMode:  nil,
		FuncDisableTestMode: nil,
		QueryBuilderMock:    m,
	}
}