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
     * @var \Psr\Log\LoggerInterface
     */
    private $logger;

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
     * @param $table
     * @param $params
     * @return array
     * @throws PDOException
     * @deprecated To be removed soon, use select instead
     */
    public function getAll($table, $params)
    {
        $orderBy = isset($params['sort']) ? $params['sort'] : '';
        return $this->select($table, [], $params, $orderBy);
    }

    /**
     * @param $table
     * @param array|string $cols
     * @param array $params
     * @param string $orderBy
     * @return array with one object
     * @throws PDOException
     */
    public function select($table, $cols = [], $params = [], $orderBy = '')
    {
        $cols = is_string($cols) ? [$cols] : $cols;
        $cols = $this->filter($table, $cols);
        $sqlCols = empty($cols) ? '*' : implode(',', $this->quoteIdentifiers($cols));

        $fields = $this->filterKeys($table, $params);
        $escapedFields = $this->quoteIdentifiers($fields);

        $statement = $this->implodeBindFields($escapedFields, ' AND ', 'w_');
        $sqlWhere = $statement !== false ? ' WHERE ' . $statement : '';

        $sqlOrder = $this->createOrderByStatement($table, $orderBy);

        $sql = 'SELECT ' . $sqlCols . ' FROM ' . self::quoteIdentifiers($table) . $sqlWhere . $sqlOrder;
        $bind = $this->bindValues($fields, $params, array(), 'w_');
        return $this->run($sql, $bind);
    }

    /**
     * @param string $table
     * @param array $data
     * @return int
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
     * @param string $table
     * @param array $data
     * @param array $filter
     * @return bool
     * @throws PDOException
     */
    public function update($table, $data, $filter=[])
    {
        // if no data to update or not key set = return false
        if ($data == null) { //} || !isset($filter[implode(',', array_flip($filter))])) {
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
        return (bool)$this->run($sql, $bind);
    }

    /**
     * @param $table
     * @param $filter
     * @return bool
     */
    public function delete($table, $filter=[])
    {
        $whereFields = $this->filterKeys($table, $filter);
        $escapedWhereFields = $this->quoteIdentifiers($whereFields);
        $statement = $this->implodeBindFields($escapedWhereFields, ' AND ');
        $whereStatement = $statement !== false ? ' WHERE ' . $statement : '';

        $sql = 'DELETE FROM ' . self::quoteIdentifiers($table) . $whereStatement;
        $bind = $this->bindValues($whereFields, $filter);
        return $this->run($sql, $bind);
    }

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
     * @param $table
     * @param $orderBy
     * @return string
     */
    public function createOrderByStatement($table, $orderBy)
    {
        if ($orderBy == '') {
            return $orderBy;
        }
        $direction = '';
        if (1 === preg_match('/\s*(.*)\s*(asc|desc|default)/i', $orderBy, $m)) {
            $orderBy = $m[1];
            $direction = strtoupper($m[2]);
        }
        $this->filterKeys($table, [$orderBy]);
        return ' ORDER BY ' . self::quoteIdentifiers($orderBy) . ' ' . $direction . ' ';
    }

    /**
     * @param $table
     * @param $params
     * @return array
     * @throws PDOException
     */
    private function filterKeys($table, $params)
    {
        $params = array_keys($params);
        return $this->filter($table, $params);
    }

    /**
     * @param $sql
     * @param array $bind
     * @return array|bool|int|\PDOStatement
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
     * @param $table
     * @param $params
     * @return array
     */
    public function filter($table, $params)
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

        if (false !== ($list = $this->run($sql))) {
            $fields = array();
            foreach ($list as $record) {
                $fields[] = $record[$key];
            }
            return array_values(array_intersect($params, $fields));
        }
        return array();
    }

    /**
     * @param array $escapedFields
     * @param string $glue
     * @param string $keyPrefix
     * @return string|false
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
     * @param $fields
     * @param $params
     * @param array $bind
     * @param string $keyPrefix
     * @return array
     */
    protected function bindValues($fields, $params, $bind = array(), $keyPrefix = '')
    {
        foreach ($fields as $key => $field) {
            $bind[':' . $keyPrefix . $key] = $params[$field];
        }
        return $bind;
    }
}
