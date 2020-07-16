<?php

namespace DBoho\IO;

use PDO;
use PDOException;
use PDOStatement;

/**
 * Class DataAccess.
 */
class DataAccess
{
    private $driver;
    /**
     * @var PDO
     */
    protected $pdo;
    private $quote;

    /**
     * @param PDO $pdo
     */
    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        if ($this->driver === 'pgsql') {
            $this->quote = '"';
        } else {
            $this->quote = '`';
        }

    }

    /**
     * Run a <code>SELECT</code> statement on a database table.
     *
     * @param string $table table name the select should work on
     * @param array|string $cols the name or the array of column names that should be selected
     * @param array $filter an associative array of filter conditions. The key are the column name, the values
     *                              compared values. all key value pairs will be chained with a logical `AND`. E.g.:
     *                              <code>['title'=>'Simple Dataaccess', 'price'=>'10.0']</code>
     * @param string $orderBy columns the result should be ordered by. For now (see Issue #3)only ascending is
     *                              supported
     * @return array returns an array containing all rows in the result set. Each row is an associative array
     *                              indexed with the column names
     * @throws PDOException on select statement failed
     */
    public function select($table, $cols = [], $filter = [], $orderBy = '')
    {
        $cols = is_string($cols) ? [$cols] : $cols;
        $fields_allowed = $this->getTableColumns($table);
        $cols = $this->filter($fields_allowed, $cols);
        $sqlCols = empty($cols) ? '*' : implode(',', $this->quoteIdentifiers($cols));

        $fields = $this->filterKeys($fields_allowed, $filter);
        $escapedFields = $this->quoteIdentifiers($fields);

        $statement = $this->implodeBindFields($escapedFields, ' AND ', 'w_');
        $sqlWhere = $statement !== false ? ' WHERE ' . $statement : '';

        $sqlOrder = $this->createOrderByStatement($table, $orderBy);

        $sql = 'SELECT ' . $sqlCols . ' FROM ' . $this->quoteIdentifiers($table) . $sqlWhere . $sqlOrder;
        $bind = $this->bindValues($fields, $filter, array(), 'w_');
        return $this->run($sql, $bind);
    }

    /**
     * Run a <code>INSERT INTO</code> on a database table
     *
     * @param string $table table name the insert should run on
     * @param array $data an associative array indexed with column names
     * @param bool $notThrowOnEmptyData if true this method wil not throw an exception, when no data or no valid data
     *                                 was provided but return a <code>0</code>.
     * @return int return number of inserted rows or false
     * @throws PDOException on insert failed
     */
    public function insert($table, $data, $notThrowOnEmptyData = false)
    {
        $isMultiple = $data !== null && is_array($data) && is_array($data[array_keys($data)[0]]);
        if (!$isMultiple) {
            $data = [$data];
        }
        $requestFields = $data[0];

        $fields_allowed = $this->getTableColumns($table);
        $fields = $this->filterKeys($fields_allowed, $requestFields);
        if (count($fields) === 0) {
            if($notThrowOnEmptyData === false){
                throw new PDOException('empty request');
            } else {
                return 0;
            }
        }

        $fieldCount = count($fields);
        $rowCount = count($data);
        $insertPlaceholder = $this->generateInsertPlaceholder($fieldCount, $rowCount);
        $insertValues = $this->filterInsertValues($data, $fields);

        $escapedFields = $this->quoteIdentifiers($fields);
        $sqlCols = ' (' . implode(', ', $escapedFields) . ')';

        $sql = 'INSERT INTO ' . $this->quoteIdentifiers($table) . $sqlCols . ' VALUES ' . $insertPlaceholder . ';';
        return $this->run($sql, $insertValues);
    }

    /**
     * Update one or more rows in the database table
     *
     * @param string $table name of the table in the database
     * @param array $data an associative array indexed with column names and the values that should be updated
     * @param array $filter an associative array of filter conditions. The key are the column name, the values
     *                              compared values. all key value pairs will be chained with a logical `AND`. E.g.:
     *                              <code>['id'=>'1']</code>
     * @param bool $notThrowOnEmptyData if true this method wil not throw an exception, when no data or no valid data
     *                                 was provided but return a <code>0</code>.
     * @return int number of affected rows or false if update failed
     * @throws PDOException on update failed
     */
    public function update($table, $data, $filter = [], $notThrowOnEmptyData = false)
    {

        $fields_allowed = $this->getTableColumns($table);
        $fields = $this->filterKeys($fields_allowed, $data);
        if (count($fields) === 0) {
            if($notThrowOnEmptyData === false){
                throw new PDOException('empty request');
            } else {
                return 0;
            }
        }
        $escapedFields = $this->quoteIdentifiers($fields);
        $statement = $this->implodeBindFields($escapedFields, ',', 'u_');
        $sets = ' SET ' . $statement;

        $whereFields = $this->filterKeys($fields_allowed, $filter);
        $escapedWhereFields = $this->quoteIdentifiers($whereFields);
        $statement = $this->implodeBindFields($escapedWhereFields, ' AND ', 'w_');
        $whereStatement = $statement !== false ? ' WHERE ' . $statement : '';

        $sql = 'UPDATE ' . self::quoteIdentifiers($table) . $sets . $whereStatement;

        $bind = $this->bindValues($fields, $data, array(), 'u_');
        $bind = $this->bindValues($whereFields, $filter, $bind, 'w_');
        return $this->run($sql, $bind);
    }

    /**
     * Delete rows on a database table
     *
     * @param string $table name of the database table
     * @param array $filter an associative array of filter conditions. The key are the column name, the values
     *                              compared values. all key value pairs will be chained with a logical `AND`. E.g.:
     *                              <code>['id'=>'1']</code>
     *
     * @return int number of affected rows
     * @throws PDOException on delete failed
     */
    public function delete($table, $filter = [])
    {
        $whereFields = $this->filterKeysForTable($table, $filter);
        $escapedWhereFields = $this->quoteIdentifiers($whereFields);
        $statement = $this->implodeBindFields($escapedWhereFields, ' AND ');
        $whereStatement = $statement !== false ? ' WHERE ' . $statement : '';

        $sql = 'DELETE FROM ' . self::quoteIdentifiers($table) . $whereStatement;
        $bind = $this->bindValues($whereFields, $filter);
        return $this->run($sql, $bind);
    }

    /**
     * Quote one or an array of identifiers with backticks
     *
     * @param array|string $names one or more identifiers that should be quoted
     * @return array|string quoted identifiers
     */
    public function quoteIdentifiers($names)
    {
        if (is_array($names)) {
            foreach ($names as $key => $value) {
                $var = $this->quoteIdentifiers($value);
                $names[$key] = $var;

            }
            return $names;
        }
        $result = $this->quote . preg_replace('#\\\*' . $this->quote . '#', $this->quote . $this->quote,
                $names) . $this->quote;
        $result = preg_replace('#\.#', $this->quote . '.' . $this->quote, $result);
        return $result;
    }

    /**
     * create a filtered* sql <code>ORDER BY</code> statement out of an string
     * * \* filter the column name from the whitelist of allowed column names
     *
     * @param string $table name of the table in the database
     * @param string $orderBy the statement e.g.: <code>price ASC</code>
     * @return string extracted <code>ORDER BY</code> statement e.g.: <code>ORDER BY price ASC</code>
     * @throws PDOException
     * @see DataAccess::filter
     */
    public function createOrderByStatement($table, $orderBy)
    {
        if ($orderBy == '') {
            return $orderBy;
        }
        $direction = '';
        if (1 === preg_match('/\s*(.*?)\s*(asc|desc|default)/i', $orderBy, $m)) {
            $orderBy = $m[1];
            $direction = strtoupper($m[2]);
        }
        $this->filterKeysForTable($table, [$orderBy]);
        return ' ORDER BY ' . self::quoteIdentifiers($orderBy) . ' ' . $direction . ' ';
    }

    /**
     * filter the keys of an associative array as column names for a specific table
     *
     * @param array $fields_allowed array of fields allowed
     * @param array $params associative array indexed with column names
     * @return array non associative array with the filtered column names as values
     * @throws PDOException
     * @see DataAccess::filter
     */
    public function filterKeys($fields_allowed, $params)
    {
        if (!is_array($params)) {
            return [];
        }
        $params = array_keys($params);
        return $this->filter($fields_allowed, $params);
    }

    /**
     * filter the keys of an associative array as column names for a specific table
     *
     * @param string $table database table to query for allowed fields
     * @param array $params associative array indexed with column names
     * @return array non associative array with the filtered column names as values
     * @throws PDOException
     * @see DataAccess::filter
     */
    public function filterKeysForTable($table, $params)
    {
        return $this->filterKeys($this->getTableColumns($table), $params);
    }

    /**
     * prepare and execute sql statement on the pdo. Run PDO::fetchAll on select, describe or pragma statements
     *
     * @param string $sql This must be a valid SQL statement for the target database server.
     * @param array $bind [optional]
     *                     An array of values with as many elements as there are bound parameters in the SQL statement
     *                     being executed
     * @param bool $shouldThrow if throw PDOException if prepare or execute failed otherwise return false (default true )
     * @return array|false|int|PDOStatement <ul>
     *                     <li> associative array of results if sql statement is select, describe or pragma
     *                     <li> the number of rows affected by a delete, insert, update or replace statement
     *                     <li> the executed PDOStatement otherwise</ul>
     *                     <li> false only if execution failed and the PDO::ERRMODE_EXCEPTION was unset</ul>
     * @throws PDOException if prepare or execute will fail and $shouldThrow is True
     * @see PDO::execute
     * @see PDO::prepare
     */
    public function run($sql, $bind = array(), $shouldThrow = true)
    {
        $sql = trim($sql);
        $statement = $this->pdo->prepare($sql);
        if ($statement !== false and ($statement->execute($bind) !== false)) {
            if (preg_match('/^(select|describe|pragma) /i', $sql)) {
                return $statement->fetchAll(PDO::FETCH_ASSOC);
            } elseif (preg_match('/^(delete|insert|update|replace) /i', $sql)) {
                return $statement->rowCount();
            } else {
                return $statement;
            }
        }
        if ($shouldThrow) {
            throw new PDOException($this->pdo->errorCode() . ' ' . ($statement === false ? 'prepare' : 'execute') . ' failed');
        }
        return false;
    }

    /**
     * filter an array of column names based on a whitelist queried from the database using <code>PRAGMA</code>,
     * <code>DESCRIBE</code> or <code>SELECT column_name FROM information_schema.columns</code> depending on the
     * PDO::ATTR_DRIVER_NAME
     *
     * @param string $table database table to query for allowed fields
     * @param array $columns array of column names
     * @return array filtered array of column names
     */
    public function filterForTable($table, $columns)
    {
        return $this->filter($this->getTableColumns($table), $columns);
    }

    /**
     * filter an array of column names based on a whitelist provided
     *
     * @param array $fields_allowed array of fields allowed
     * @param array $columns array of column names
     * @return array filtered array of column names
     */
    public function filter($fields_allowed, $columns)
    {
        if (!is_array($columns)) {
            return [];
        }

        return array_values(array_intersect($columns, $fields_allowed));
    }

    /**
     * insert bind placeholders for a sql statement
     *
     * @param array $escapedFields array with column names the index will use for placeholder and can be prefixed with
     *                              $keyPrefix
     * @param string $glue the glue between the bind placeholders
     * @param string $keyPrefix prefix for placeholder names
     * @return string|false statement with binded column placeholders
     */
    public function implodeBindFields($escapedFields, $glue, $keyPrefix = '')
    {
        if (count($escapedFields) == 0) {
            return false;
        }
        $statement = '';//' WHERE ' . implode(',', $escapedFields) . ' = :' . implode(',', array_keys($fields));
        $first = true;
        foreach ($escapedFields as $key => $value) {
            if ($first) {
                $first = false;
            } else {
                $statement .= $glue;
            }
            $statement .= $value . ' = :' . $keyPrefix . $key;
        }
        return $statement;
    }

    /**
     * create bind array indexed with placeholder ids prefixed with $keyPrefix
     *
     * @param array $fields array with column names the index will use for placeholder and can be prefixed with
     *                              $keyPrefix
     * @param array $params associative array indexed with column names
     * @param array $bind [output]
     *                              associative array the results will be appended to
     * @param string $keyPrefix prefix for placeholder names
     * @return array bind array
     */
    public function bindValues($fields, $params, $bind = [], $keyPrefix = '')
    {
        foreach ($fields as $key => $field) {
            $bind[':' . $keyPrefix . $key] = $params[$field];
        }
        return $bind;
    }

    /**
     * Query the available columns for a database table
     * @param string $table name of database table
     * @return array of column names
     */
    public function getTableColumns($table)
    {
        $driver = $this->getDriverName();
        $bind = [];
        if ($driver == 'sqlite') {
            $table = $this->quoteIdentifiers($table);
            $sql = 'PRAGMA table_info(' . $table . ');';
            $key = 'name';
        } elseif ($driver == 'mysql') {
            $table = $this->quoteIdentifiers($table);
            $sql = 'DESCRIBE ' . $table . ';';
            $key = 'Field';
        } else {
            $bind[] = $table;
            $sql = 'SELECT column_name FROM information_schema.columns WHERE ';
            if ($driver == 'pgsql') {
                $bind = explode('.', $table, 2);
                if (count($bind) == 2) {
                    $sql .= 'table_schema = ? AND ';
                }
            }
            $sql .= 'table_name = ? ;';
            $key = 'column_name';
        }

        $fields = [];
        if (is_array($list = $this->run($sql, $bind, false))) {
            foreach ($list as $record) {
                $fields[] = $record[$key];
            }
            return $fields;
        }
        return $fields;
    }

    /**
     * Returns the ID of the last inserted row or sequence value
     *
     * @param string $name [optional]
     *                     Name of the sequence object from which the ID should be returned.
     * @return string last insert id
     * @see \PDO::lastInsertId()
     */
    public function getLastInsertId($name = null)
    {
        return $this->pdo->lastInsertId($name);
    }

    /**
     * Returns the name of the driver
     * @return string Driver name
     */
    public function getDriverName()
    {
        return $this->driver;
    }

    /**
     * generate insert placeholder for parameter binding based on field and row count
     * @param $fieldCount integer
     * @param $rowCount integer
     * @return string the placeholder
     */
    public function generateInsertPlaceholder($fieldCount, $rowCount)
    {
        $insertPlaceholder = '(' . implode(',', array_fill(0, $fieldCount, '?')) . ')';
        $insertPlaceholder = implode(',', array_fill(0, $rowCount, $insertPlaceholder));
        return $insertPlaceholder;
    }

    /**
     * filter data rows for fields (array keys) allowed
     *
     * @param $data array rows of assoc arrays
     * @param $fields array keys allowed for assoc array
     * @return array filtered values
     */
    public function filterInsertValues($data, $fields)
    {
        $insertValues = array();
        $field_keys = array_flip($fields);
        foreach ($data as $key => $values) {
            $filteredValues = array_intersect_key($values, $field_keys);
            $emptyFields = array_fill_keys($fields, null);
            $filledValues = array_values(array_merge($emptyFields, $filteredValues));
            $insertValues = array_merge($insertValues, $filledValues);
        }
        return $insertValues;
    }
}
