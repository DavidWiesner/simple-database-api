<?php

namespace DBoho\IO;

use PDO;
use PDOException;

/**
 * Class DataAccess.
 */
class DataAccess
{
    /**
     * @var \PDO
     */
    private $pdo;

    /**
     * @param \PDO $pdo
     */
    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    }

    /**
     * Run a <code>SELECT</code> statement on a database table.
     *
     * @param string       $table   table name the select should work on
     * @param array|string $cols    the name or the array of column names that should be selected
     * @param array        $filter  an associative array of filter conditions. The key are the column name, the values
     *                              compared values. all key value pairs will be chained with a logical `AND`. E.g.:
     *                              <code>['title'=>'Simple Dataaccess', 'price'=>'10.0']</code>
     * @param string       $orderBy columns the result should be ordered by. For now (see Issue #3)only ascending is
     *                              supported
     * @return array returns an array containing all rows in the result set. Each row is an associative array indexed
     *                              with the column names
     * @throws PDOException
     */
    public function select($table, $cols = [], $filter = [], $orderBy = '')
    {
        $cols = is_string($cols) ? [$cols] : $cols;
        $cols = $this->filter($table, $cols);
        $sqlCols = empty($cols) ? '*' : implode(',', $this->quoteIdentifiers($cols));

        $fields = $this->filterKeys($table, $filter);
        $escapedFields = $this->quoteIdentifiers($fields);

        $statement = $this->implodeBindFields($escapedFields, ' AND ', 'w_');
        $sqlWhere = $statement !== false ? ' WHERE ' . $statement : '';

        $sqlOrder = $this->createOrderByStatement($table, $orderBy);

        $sql = 'SELECT ' . $sqlCols . ' FROM ' . self::quoteIdentifiers($table) . $sqlWhere . $sqlOrder;
        $bind = $this->bindValues($fields, $filter, array(), 'w_');
        return $this->run($sql, $bind);
    }

    /**
     * Run a <code>INSERT INTO</code> on a database table
     *
     * @param string $table table name the insert should run on
     * @param array  $data  an associative array indexed with column names
     * @return int return the number of affected rows
     * @throws PDOException
     */
    public function insert($table, $data)
    {
        if ($data == null or !is_array($data)) {
            throw new PDOException('empty request');
        }

        $isMultiple = is_array($data[array_keys($data)[0]]);
        $requestFields = $isMultiple ? $data[0] : $data;

        $fields = $this->filterKeys($table, $requestFields);
        $escapedFields = $this->quoteIdentifiers($fields);
        $fieldCount = count($escapedFields);
        $insertPlaceholder = '(' . implode(',', array_fill(0, $fieldCount, '?')) . ')';
        if ($isMultiple) {
            $insertPlaceholder = implode(',', array_fill(0, count($data), $insertPlaceholder));
            $insertValues = array();
            foreach ($data as $key => $values) {
                $insertValues = array_merge($insertValues, array_values($values));
            }
        } else {
            $insertValues = array_values($data);
        }

        $sqlCols = ' (' . implode($escapedFields, ', ') . ')';
        $sql = 'INSERT INTO ' . self::quoteIdentifiers($table) . $sqlCols . ' VALUES ' . $insertPlaceholder . ';';

        return $this->run($sql, $insertValues);
    }

    /**
     * Update one or more rows in the database table
     *
     * @param string $table         name of the table in the database
     * @param array  $data          an associative array indexed with column names and the values that should be updated
     * @param array  $filter        an associative array of filter conditions. The key are the column name, the values
     *                              compared values. all key value pairs will be chained with a logical `AND`. E.g.:
     *                              <code>['id'=>'1']</code>
     * @return int number of affected rows
     * @throws PDOException
     */
    public function update($table, $data, $filter = [])
    {
        if ($data == null) {
            throw new PDOException('empty request');
        }

        $fields = $this->filterKeys($table, $data);
        $escapedFields = $this->quoteIdentifiers($fields);
        $statement = $this->implodeBindFields($escapedFields, ',', 'u_');
        $sets = ' SET ' . $statement;

        $whereFields = $this->filterKeys($table, $filter);
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
     * @param string $table         name of the database table
     * @param array  $filter        an associative array of filter conditions. The key are the column name, the values
     *                              compared values. all key value pairs will be chained with a logical `AND`. E.g.:
     *                              <code>['id'=>'1']</code>
     *
     * @return int number of affected rows
     */
    public function delete($table, $filter = [])
    {
        $whereFields = $this->filterKeys($table, $filter);
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
    public static function quoteIdentifiers($names)
    {
        if (is_array($names)) {
            foreach ($names as $key => $value) {
                $names[$key] = '`' . preg_replace('#\\\*`#', '``', $value) . '`';
            }
            return $names;
        }
        return '`' . preg_replace('#\\\*`#', '``', $names) . '`';
    }

    /**
     * create a filtered* sql <code>ORDER BY</code> statement out of an string
     * * \* filter the column name from the whitelist of allowed column names
     *
     * @see DataAccess::filter
     * @param string $table   name of the table in the database
     * @param string $orderBy the statement e.g.: <code>price ASC</code>
     * @return string extracted <code>ORDER BY</code> statement e.g.: <code>ORDER BY price ASC</code>
     * @throws PDOException
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
        $this->filterKeys($table, [$orderBy]);
        return ' ORDER BY ' . self::quoteIdentifiers($orderBy) . ' ' . $direction . ' ';
    }

    /**
     * filter the keys of an associative array as column names for a specific table
     *
     * @see DataAccess::filter
     * @param string $table  name of a table in the database
     * @param array  $params associative array indexed with column names
     * @return array non associative array with the filtered column names as values
     * @throws PDOException
     */
    private function filterKeys($table, $params)
    {
        $params = array_keys($params);
        return $this->filter($table, $params);
    }

    /**
     * prepare and execute sql statement on the pdo. Run PDO::fetchAll on select, describe or pragma statements
     *
     * @see PDO::prepare
     * @see PDO::execute
     * @param string $sql  This must be a valid SQL statement for the target database server.
     * @param array  $bind [optional]
     *                     An array of values with as many elements as there are bound parameters in the SQL statement
     *                     being executed
     * @return array|int|\PDOStatement|false <ul>
     *                     <li> associative array of results if sql statement is select, describe or pragma
     *                     <li> the number of rows affected by a delete, insert, update or replace statement
     *                     <li> the executed PDOStatement otherwise</ul>
     *                     <li> false only if execution failed and the PDO::ERRMODE_EXCEPTION was unset</ul>
     * @throws PDOException
     */
    public function run($sql, $bind = array())
    {
        $sql = trim($sql);
        $statement = $this->pdo->prepare($sql);
        if ($statement->execute($bind) !== false) {
            if (preg_match('/^(select|describe|pragma) /i', $sql)) {
                return $statement->fetchAll(PDO::FETCH_ASSOC);
            } elseif (preg_match('/^(delete|insert|update|replace) /i', $sql)) {
                return $statement->rowCount();
            } else {
                return $statement;
            }
        }
        return false;
    }

    /**
     * filter an array of column names based on a whitelist queried from the database using <code>PRAGMA</code>,
     * <code>DESCRIBE</code> or <code>SELECT column_name FROM information_schema.columns</code> depending on the
     * PDO::ATTR_DRIVER_NAME
     *
     * @param string $table   name of the table
     * @param array  $columns array of column names
     * @return array filtered array of column names
     */
    public function filter($table, $columns)
    {
        $driver = $this->pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        $table = self::quoteIdentifiers($table);
        if ($driver == 'sqlite') {
            $sql = 'PRAGMA table_info(' . $table . ');';
            $key = 'name';
        } elseif ($driver == 'mysql') {
            $sql = 'DESCRIBE ' . $table . ';';
            $key = 'Field';
        } else {
            $sql = 'SELECT column_name FROM information_schema.columns WHERE table_name = ' . $table . ';';
            $key = 'column_name';
        }

        if (is_array($list = $this->run($sql))) {
            $fields = [];
            foreach ($list as $record) {
                $fields[] = $record[$key];
            }
            return array_values(array_intersect($columns, $fields));
        }
        return [];
    }

    /**
     * insert bind placeholders for a sql statement
     *
     * @param array  $escapedFields array with column names the index will use for placeholder and can be prefixed with
     *                              $keyPrefix
     * @param string $glue          the glue between the bind placeholders
     * @param string $keyPrefix     prefix for placeholder names
     * @return string|false statement with binded column placeholders
     */
    protected function implodeBindFields($escapedFields, $glue, $keyPrefix = '')
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
     * @param array  $fields        array with column names the index will use for placeholder and can be prefixed with
     *                              $keyPrefix
     * @param array  $params        associative array indexed with column names
     * @param array  $bind          [output]
     *                              associative array the results will be appended to
     * @param string $keyPrefix     prefix for placeholder names
     * @return array bind array
     */
    protected function bindValues($fields, $params, $bind = [], $keyPrefix = '')
    {
        foreach ($fields as $key => $field) {
            $bind[':' . $keyPrefix . $key] = $params[$field];
        }
        return $bind;
    }
}