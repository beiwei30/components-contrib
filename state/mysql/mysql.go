// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mysql

import (
	"encoding/json"
	"fmt"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"

	"database/sql"
	"strconv"
	"time"
)

const (
	// configurations to connect to Mysql, either a data source name represent by URL
	connectionURLKey = "url"
	// , or separated keys:
	userKey     = "user"
	passwordKey = "password"
	networkKey  = "network"
	addrKey     = "addr"
	databaseKey = "database"

	// other general settings for DB connections
	maxIdleConnsKey    = "maxIdleConns"
	maxOpenConnsKey    = "maxOpenConns"
	connMaxLifetimeKey = "connMaxLifetime"
	connMaxIdleTimeKey = "connMaxIdleTime"

	tableName = "state"
)

type MySQL struct {
	db     *sql.DB
	logger logger.Logger
}

func NewMySQLStateStore(logger logger.Logger) *MySQL {
	return &MySQL{logger: logger}
}

func (m *MySQL) Init(metadata state.Metadata) error {
	p := metadata.Properties
	// either init from URL or from separated keys
	if url, ok := p[connectionURLKey]; ok && url != "" {
		db, err := initDBFromURL(url)
		if err != nil {
			return err
		}
		m.db = db
	} else {
		db, err := initDBFromConfig(p[userKey], p[passwordKey], p[networkKey], p[addrKey], p[databaseKey])
		if err != nil {
			return err
		}
		m.db = db
	}

	if err := propertyToInt(p, maxIdleConnsKey, m.db.SetMaxIdleConns); err != nil {
		return err
	}

	if err := propertyToInt(p, maxOpenConnsKey, m.db.SetMaxOpenConns); err != nil {
		return err
	}

	if err := propertyToDuration(p, connMaxIdleTimeKey, m.db.SetConnMaxIdleTime); err != nil {
		return err
	}

	if err := propertyToDuration(p, connMaxLifetimeKey, m.db.SetConnMaxLifetime); err != nil {
		return err
	}

	if err := m.db.Ping(); err != nil {
		return errors.Wrap(err, "unable to ping the DB")
	}

	if err := m.ensureStateTable(); err != nil {
		return err
	}

	return nil
}

func (m *MySQL) Get(req *state.GetRequest) (*state.GetResponse, error) {
	if req.Key == "" {
		return nil, fmt.Errorf("missing key in get operation")
	}

	if err := state.CheckRequestOptions(req.Options); err != nil {
		return nil, err
	}

	m.logger.Debugf("Getting state value %q from MySQL", req.Key)

	var value string
	var etag int
	err := m.db.QueryRow(fmt.Sprintf("SELECT value, etag FROM %s WHERE `key` = $1", tableName), req.Key).Scan(&value, &etag)
	if err != nil {
		if err == sql.ErrNoRows {
			return &state.GetResponse{}, nil
		}

		return nil, err
	}

	resp := &state.GetResponse{
		Data:     []byte(value),
		ETag:     strconv.Itoa(etag),
		Metadata: req.Metadata,
	}

	return resp, nil
}

func (m *MySQL) Set(req *state.SetRequest) error {
	return state.SetWithOptions(m.setValue, req)
}

func (m *MySQL) setValue(req *state.SetRequest) error {
	if req.Key == "" {
		return fmt.Errorf("missing key in set operation")
	}

	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	m.logger.Debugf("Setting state value %q to MySQL", req.Key)

	// Convert to json string
	bt, _ := utils.Marshal(req.Value, json.Marshal)
	value := string(bt)

	var result sql.Result
	if req.ETag == nil {
		s := fmt.Sprintf("INSERT INTO %s (`key`, value) VALUES ($1, $2) ON DUPLICATE KEY UPDATE value = $2", tableName)
		result, err = m.db.Exec(s, req.Key, value)
	} else {
		var etag int
		etag, err = strconv.Atoi(*req.ETag)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}

		s := fmt.Sprintf("UPDATE %s SET value = $1 WHERE `key` = $2 AND etag = $3", tableName)
		result, err = m.db.Exec(s, value, req.Key, etag)
	}

	if err != nil {
		return err
	}

	return m.verify(result)
}

func (m *MySQL) Delete(req *state.DeleteRequest) error {
	return state.DeleteWithOptions(m.deleteValue, req)
}

func (m *MySQL) deleteValue(req *state.DeleteRequest) error {
	if req.Key == "" {
		return fmt.Errorf("missing key in delete operation")
	}

	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	m.logger.Debugf("Deleting state value %q from MySQL", req.Key)

	var result sql.Result
	if req.ETag == nil {
		s := fmt.Sprintf("DELETE FROM %s WHERE `key` = $1", tableName)
		result, err = m.db.Exec(s, req.Key)
	} else {
		var etag int
		etag, err = strconv.Atoi(*req.ETag)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}

		s := fmt.Sprintf("DELETE FROM %s WHERE `key` = $1 AND etag = $2", tableName)
		result, err = m.db.Exec(s, req.Key, etag)
	}

	if err != nil {
		return err
	}

	return m.verify(result)
}

func (m *MySQL) BulkDelete(req []state.DeleteRequest) error {
	return m.executeMulti(nil, req)
}

func (m *MySQL) BulkGet(req []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	return false, nil, nil
}

func (m *MySQL) BulkSet(req []state.SetRequest) error {
	return m.executeMulti(req, nil)
}

func (m *MySQL) Multi(request *state.TransactionalStateRequest) error {
	var deletes []state.DeleteRequest
	var sets []state.SetRequest
	for _, req := range request.Operations {
		switch req.Operation {
		case state.Upsert:
			if set, ok := req.Request.(state.SetRequest); ok {
				sets = append(sets, set)
			} else {
				return fmt.Errorf("expecting set request")
			}

		case state.Delete:
			if del, ok := req.Request.(state.DeleteRequest); ok {
				deletes = append(deletes, del)
			} else {
				return fmt.Errorf("expecting delete request")
			}

		default:
			return fmt.Errorf("unsupported operation: %s", req.Operation)
		}
	}

	if len(sets) > 0 || len(deletes) > 0 {
		return m.executeMulti(sets, deletes)
	}

	return nil
}

func (m *MySQL) Close() error {
	if m.db != nil {
		return m.db.Close()
	}

	return nil
}

func (m *MySQL) executeMulti(sets []state.SetRequest, deletes []state.DeleteRequest) error {
	m.logger.Debug("Executing multiple MySQL operations")

	tx, err := m.db.Begin()
	if err != nil {
		return err
	}

	if len(deletes) > 0 {
		for _, d := range deletes {
			err = m.Delete(&d)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
		}
	}

	if len(sets) > 0 {
		for _, s := range sets {
			err = m.Set(&s)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
		}
	}

	err = tx.Commit()

	return err
}

func (m *MySQL) ensureStateTable() error {
	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"`key`      VARCHAR(64) NOT NULL PRIMARY KEY,"+
		"value      JSON,"+
		"insertdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"+
		"updatedate TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"+
		"etag       INT);", tableName)
	_, err := m.db.Exec(ddl)

	if err != nil {
		return err
	}

	return nil
}

func (m *MySQL) verify(result sql.Result) error {
	n, err := result.RowsAffected()
	if err != nil {
		m.logger.Error(err)
		return err
	}

	if n == 0 {
		noRowErr := state.NewETagError(state.ETagMismatch, nil)
		m.logger.Error(noRowErr)
		return noRowErr
	}

	if n > 1 {
		tooManyRowsErr := errors.New("database operation failed: more than one row affected, expected one")
		m.logger.Error(tooManyRowsErr)
		return tooManyRowsErr
	}

	return nil
}

func propertyToInt(props map[string]string, key string, setter func(int)) error {
	if v, ok := props[key]; ok {
		if i, err := strconv.Atoi(v); err == nil {
			setter(i)
		} else {
			return errors.Wrapf(err, "error converitng %s:%s to int", key, v)
		}
	}

	return nil
}

func propertyToDuration(props map[string]string, key string, setter func(time.Duration)) error {
	if v, ok := props[key]; ok {
		if d, err := time.ParseDuration(v); err == nil {
			setter(d)
		} else {
			return errors.Wrapf(err, "error converitng %s:%s to int", key, v)
		}
	}

	return nil
}

func initDBFromURL(url string) (*sql.DB, error) {
	if _, err := mysql.ParseDSN(url); err != nil {
		return nil, errors.Wrapf(err, "illegal Data Source Name (DNS) specified by %s", connectionURLKey)
	}

	db, err := sql.Open("mysql", url)
	if err != nil {
		return nil, errors.Wrap(err, "error opening DB connection")
	}

	return db, nil
}

func initDBFromConfig(user, passwd, net, addr, db string) (*sql.DB, error) {
	config := mysql.NewConfig()
	config.User = user
	config.Passwd = passwd
	config.Net = net
	config.Addr = addr
	config.DBName = db
	dns := config.FormatDSN()

	ret, err := sql.Open("mysql", dns)
	if err != nil {
		return nil, errors.Wrap(err, "error opening DB connection")
	}

	return ret, nil
}
