<?php
/**
 * Created by PhpStorm.
 * User: Jordy
 * Date: 2019/12/6
 * Time: 5:28 PM
 */

namespace All\Mysql;

use Ali\InstanceTrait;
use All\Exception\Exception;
use All\Exception\MysqlException;

class Mysql extends DriverAbstract
{
    use InstanceTrait {
        getInstance as private _getInstance;
    }

    const DB_TYPE_MASTER = 1;
    const DB_TYPE_SLAVE = 2;

    protected $distinct = false;
    protected $forUpdate = false;
    protected $ignore = false;
    protected $config;
    protected $masterConfig;
    protected $slaveConfig;
    protected $pdo = [];

    protected $fetchMode = \PDO::FETCH_ASSOC;

    protected $inTrans = false;
    protected $transCount = 0;

    /**
     * @param array $config
     * @return static
     */
    public static function getInstance(array $config)
    {
        return self::_getInstance($config);
    }

    protected function __construct(array $config)
    {
        if (isset($config['master'])) {
            $this->masterConfig = $config['master'];
            if (!empty($config['slaves']) && is_array($config['slaves'])) {
                $randKey = array_rand($config['slaves']);
                $this->slaveConfig = $config['slaves'][$randKey];
            }
        } else {
            $this->masterConfig = $config;
        }
        $this->config = $config;
    }

    //-------- 地平线 --------//

    /**
     * @return bool|mixed
     * @throws Exception
     */
    public function fetch()
    {
        $this->type = self::TYPE_SELECT;
        $sql = $this->sqlConcat();
        $sth = $this->_execute($sql, $this->params);
        if (!$sth) {
            return false;
        }
        return $sth->fetch($this->fetchMode);
    }

    /**
     * @return array|bool
     * @throws Exception
     */
    public function fetchAll()
    {
        $this->type = self::TYPE_SELECT;
        $sql = $this->sqlConcat();
        $sth = $this->_execute($sql, $this->params);
        if (!$sth) {
            return false;
        }
        return $sth->fetchAll($this->fetchMode);
    }

    /**
     * @param array $data
     * @return $this
     */
    public function insert(array $data)
    {
        $this->type = self::TYPE_INSERT;
        $this->data = $data;
        return $this;
    }

    /**
     * @param array $data
     * @return $this
     */
    public function insertMulti(array $data)
    {
        $this->type = self::TYPE_INSERT_MULTI;
        $this->data = $data;
        return $this;
    }

    /**
     * @param array $data
     * @return $this
     */
    public function update(array $data)
    {
        $this->type = self::TYPE_UPDATE;
        $this->data = $data;
        return $this;
    }

    /**
     * @param array $data
     * @param $index
     * @return $this
     */
    public function updateMulti(array $data, $index)
    {
        $this->type = self::TYPE_UPDATE_MULTI;
        $this->data = $data;
        $this->index = $index;
        return $this;
    }

    /**
     * @return $this
     */
    public function delete()
    {
        $this->type = self::TYPE_DELETE;
        return $this;
    }

    /**
     * @param array $data
     * @return $this
     */
    public function replace(array $data)
    {
        $this->type = self::TYPE_REPLACE;
        $this->data = $data;
        return $this;
    }

    /**
     * @return bool|int
     * @throws Exception
     */
    public function exec()
    {
        if ($this->debug) {
            $this->showSql($this->_getSql());
        }
        $type = $this->type;
        $sql = $this->sqlConcat();
        $sth = $this->_execute($sql, $this->params);
        if (!$sth) {
            return false;
        }
        if (in_array($type, [self::TYPE_UPDATE, self::TYPE_UPDATE_MULTI, self::TYPE_DELETE])) {
            return $sth->rowCount();
        }
        return true;
    }

    /**
     * @return bool|string
     * @throws Exception
     */
    public function lastInsertId()
    {
        $result = $this->exec();
        if (!$result) {
            return false;
        }
        return $this->getConnection(self::DB_TYPE_MASTER)->lastInsertId();
    }

    /**
     * @param $sql
     * @param array $params
     * @return bool
     * @throws Exception
     */
    public function execute($sql, array $params = [])
    {
        $sth = $this->_execute($sql, $params);
        if (!$sth) {
            return false;
        }
        return true;
    }

    //-------- 地平线 --------//

    public function distinct()
    {
        $this->distinct = true;
        return $this;
    }

    public function ignore()
    {
        $this->ignore = true;
        return $this;
    }

    public function forUpdate()
    {
        $this->forUpdate = true;
        return $this;
    }

    public function getSql()
    {
        $sql = $this->_getSql();
        $this->reset();
        return $sql;
    }

    protected function _getSql()
    {
        $prepareSql = $this->sqlConcat();
        $params = $this->params;

        $sqlArr = explode('?', $prepareSql);
        $sql = array_shift($sqlArr);
        foreach ($sqlArr as $key => $value) {
            $sql .= $this->addSqlParam($params[$key]) . $value;
        }

        return $sql;
    }

    public function setFetchMode($fetchMode)
    {
        $this->fetchMode = $fetchMode;
        return $this;
    }

    //-------- 地平线 --------//

    /**
     * @return bool
     * @throws Exception
     */
    public function begineTrans()
    {
        $this->transCount++;
        if ($this->transCount == 1) {
            $this->inTrans = true;
            return $this->getConnection(self::DB_TYPE_MASTER)->beginTransaction();
        }
        return true;
    }

    /**
     * @return bool
     * @throws Exception
     */
    public function rollbackTrans()
    {
        $this->transCount--;
        if ($this->transCount == 0) {
            $this->inTrans = false;
            return $this->getConnection(self::DB_TYPE_MASTER)->rollBack();
        }
        return true;
    }

    /**
     * @return bool
     * @throws Exception
     */
    public function commitTrans()
    {
        $this->transCount--;
        if ($this->transCount == 0) {
            $this->inTrans = false;
            return $this->getConnection(self::DB_TYPE_MASTER)->commit();
        }
        return true;
    }

    //-------- 地平线 --------//

    protected function getSelectSql()
    {
        //SELECT $fields FROM $table X JOIN $join ON $condition WHERE $where GROUP BY $group HAVING $having ORDER BY $order LIMIT $offset,$limit FOR UPDATE
        $sql = 'SELECT ';
        if ($this->distinct) {
            $sql .= 'DISTINCT ';
        }
        $sql .= $this->fields . ' FROM ' . $this->getTable();

        if ($this->join) {
            $sql .= $this->getJoin();
        }
        if ($this->where) {
            $sql .= ' ' . $this->where;
        }
        if ($this->groupBy) {
            $sql .= ' ' . $this->groupBy;
        }
        if ($this->orderBy) {
            $sql .= ' ' . $this->orderBy;
        }
        if ($this->record) {
            $sql .= ' ' . $this->getLimit();
        }
        if ($this->forUpdate) {
            $sql .= ' FOR UPDATE';
        }
        return $sql;
    }

    protected function getInsertSql()
    {
        //INSERT IGNORE INTO $table ($fields) VALUES $values;
        if ($this->ignore) {
            $sql = 'INSERT IGNORE INTO';
        } else {
            $sql = 'INSERT INTO';
        }
        $sql .= ' ' . $this->getTable();
        $fields = array_keys($this->data);
        $values = array_values($this->data);
        $sql .= '(' . implode(',', $this->escapeFields($fields)) . ') VALUES(' . implode(',',
                array_fill(0, count($values), '?')) . ')';
        $this->params = $values;
        return $sql;
    }

    protected function getUpdateSql()
    {
        //UPDATE $table SET $field=$value WHERE $where
        $sql = 'UPDATE ' . $this->getTable();
        $values = [];
        $fieldValue = [];
        foreach ($this->data as $field => $value) {
            $fieldValue[] = $this->escapeField($field) . '=?';
            $values[] = $value;
        }
        $sql .= ' SET ' . implode(',', $fieldValue);
        $this->params = array_merge($values, $this->params);
        if ($this->where) {
            $sql .= ' ' . $this->where;
        }
        return $sql;
    }

    protected function getDeleteSql()
    {
        //DELETE FROM $table WHERE $where ORDER BY $order LIMIT $limit
        $sql = 'DELETE ';
        $sql .= 'FROM ' . $this->getTable();
        if ($this->where) {
            $sql .= ' ' . $this->where;
        }
        if ($this->orderBy) {
            $sql .= ' ' . $this->orderBy;
        }
        if ($this->record) {
            $sql .= ' ' . $this->getLimit();
        }
        return $sql;
    }

    protected function getReplaceSql()
    {
        //REPLACE INTO $table ($fields) VALUES $values;
        $sql = 'REPLACE INTO';
        $sql .= ' ' . $this->getTable();
        $fields = array_keys($this->data);
        $values = array_values($this->data);
        $sql .= '(' . implode(',', $this->escapeFields($fields)) . ') VALUES(' . implode(',',
                array_fill(0, count($values), '?')) . ')';
        $this->params = $values;
        return $sql;
    }

    protected function getInsertMultiSql()
    {
        //INSERT IGNORE INTO $table ($fields) VALUES $values;
        if ($this->ignore) {
            $sql = 'INSERT IGNORE INTO';
        } else {
            $sql = 'INSERT INTO';
        }
        $sql .= ' ' . $this->getTable();
        $fields = array_keys(current($this->data));
        $valueArr = [];
        foreach ($this->data as $item) {
            $valueArr[] = '(' . implode(',', array_fill(0, count($item), '?')) . ')';
            foreach ($item as $v) {
                $this->params[] = $v;
            }
        }
        $sql .= '(' . implode(',', $this->escapeFields($fields)) . ') VALUES' . implode(',', $valueArr);
        return $sql;
    }

    protected function getUpdateMultiSql()
    {
        //UPDATE $table SET $field = CASE $index WHEN $condition1 THEN $value1 WHEN $condition2 THEN $value2 END,$field2 = CASE ... END WHERE $index IN ($indexValues)
        $indexValues = [];
        $fieldValues = [];

        foreach ($this->data as $item) {
            foreach ($item as $field => $value) {
                if ($field == $this->index) {
                    $indexValues[] = $value;
                } else {
                    if (!isset($fieldValues[$field])) {
                        $fieldValues[$field] = [];
                    }
                    $fieldValues[$field][] = $value;
                }
            }
        }

        $sql = 'UPDATE ' . $this->getTable();
        $indexField = $this->escapeField($this->index);
        $sets = [];
        $params = [];
        foreach ($fieldValues as $field => $values) {
            $when = '';
            foreach ($values as $k => $v) {
                $when .= ' WHEN ? THEN ?';
                $params[] = $indexValues[$k];
                $params[] = $v;
            }
            $sets[] = $this->escapeField($field) . ' = CASE ' . $indexField . $when . ' END';
        }
        $sql .= ' SET ' . implode(',', $sets);
        $this->params = array_merge($params, $this->params);
        $this->where($this->index, $indexValues, 'IN');
        $sql .= ' ' . $this->where;

        return $sql;
    }

    //-------- 地平线 --------//

    protected function sqlConcat()
    {
        switch ($this->type) {
            case self::TYPE_SELECT:
                $sql = $this->getSelectSql();
                break;
            case self::TYPE_INSERT:
                $sql = $this->getInsertSql();
                break;
            case self::TYPE_UPDATE:
                $sql = $this->getUpdateSql();
                break;
            case self::TYPE_DELETE:
                $sql = $this->getDeleteSql();
                break;
            case self::TYPE_REPLACE:
                $sql = $this->getReplaceSql();
                break;
            case self::TYPE_INSERT_MULTI:
                $sql = $this->getInsertMultiSql();
                break;
            case self::TYPE_UPDATE_MULTI:
                $sql = $this->getUpdateMultiSql();
                break;
            default:
                $sql = $this->getSelectSql();
                break;
        }

        return $sql;
    }

    protected function getTable()
    {
        $table = '';
        foreach ($this->table as $item) {
            if ($table) {
                $table .= ',';
            }
            $table .= $item['table'];
            if (!empty($item['alias'])) {
                $table .= ' AS ' . $item['alias'];
            }
        }
        return $table;
    }

    protected function getJoin()
    {
        $table = '';
        foreach ($this->join as $item) {
            if (!empty($item['type'])) {
                $table .= ' ' . $item['type'];
            }
            $table .= ' JOIN ' . $item['table'];
            if (!empty($item['alias'])) {
                $table .= ' AS ' . $item['alias'];
            }
            $table .= ' ON ' . $item['condition'];
        }
        return $table;
    }

    protected function getLimit()
    {
        if ($this->offset) {
            $limitSql = 'LIMIT ?,?';
            $this->params[] = $this->offset;
            $this->params[] = $this->record;
        } elseif ($this->page) {
            $offset = ($this->page - 1) * $this->record;
            $limitSql = 'LIMIT ?,?';
            $this->params[] = $offset;
            $this->params[] = $this->record;
        } else {
            $limitSql = 'LIMIT ?';
            $this->params[] = $this->record;
        }
        return $limitSql;
    }

    protected function whereConcat($where, $params, $connector)
    {
        if ($this->where) {
            if (substr($this->where, -1) == '(') {
                $this->where .= $where;
            } else {
                $this->where .= ' ' . ($connector ? $connector . ' ' : '') . $where;
            }
        } else {
            $this->where = 'WHERE ' . $where;
        }
        foreach ($params as $param) {
            $this->params[] = $param;
        }
    }

    /**
     * @param $field
     * @param $value
     * @param $operator
     * @return array
     * @throws Exception
     */
    protected function whereValueArray($field, $value, $operator)
    {
        $params = [];
        $operator = $operator ?: 'IN';
        switch ($operator) {
            case 'IN':
            case 'NOT IN':
                $count = count($value);
                $where = $field . ' ' . $operator . ' (' . implode(',', array_fill(0, $count, '?')) . ')';
                foreach ($value as $item) {
                    $params[] = $item;
                }
                break;
            case 'BETWEEN':
            case 'NOT BETWEEN':
                $param1 = isset($value[0]) ? $value[0] : '';
                $param2 = isset($value[1]) ? $value[1] : '';
                $where = $field . ' BETWEEN ? AND ?';
                $params[] = $param1;
                $params[] = $param2;
                break;
            default:
                throw new Exception('Invalid operator!');
                break;
        }

        return [$where, $params];
    }

    /**
     * @param $field
     * @param $value
     * @param $operator
     * @return array
     * @throws Exception
     */
    protected function whereValueString($field, $value, $operator)
    {
        $params = [];
        $operator = $operator ?: '=';
        switch ($operator) {
            case '=':
            case '!=':
            case '<>':
            case '>':
            case '>=':
            case '<':
            case '<=':
            case 'LIKE':
            case 'NOT LIKE':
                $where = $field . ' ' . $operator . ' ?';
                $params[] = $value;
                break;
            case 'IS NULL':
            case 'IS NOT NULL':
                $where = $field . ' ' . $operator;
                break;
            default:
                throw new Exception('Invalid operator!');
                break;
        }

        return [$where, $params];
    }

    protected function escapeTable($table)
    {
        return '`' . strtr($table, '`', '') . '`';
    }

    protected function escapeFields(array $fields)
    {
        $escapedFields = [];
        foreach ($fields as $field) {
            if (strpos($field, ' ') !== false ||
                strpos($field, '(') !== false ||
                strpos($field, '\'') !== false ||
                strpos($field, '"') !== false) {
                $escapedFields[] = $field;
            } else {
                $escapedFields[] = $this->escapeField($field);
            }
        }
        return $escapedFields;
    }

    protected function escapeField($field)
    {
        $field = strtr(trim($field), '`', '');
        $strpos = strpos($field, '.');
        if ($strpos !== false) {
            $table = substr($field, 0, $strpos);
            $tableField = substr($field, $strpos + 1);
            $field = $tableField == '*' ? '`' . $table . '`.*' : '`' . $table . '`.`' . $tableField . '`';
        } else {
            $field = $field == '*' ? '*' : '`' . $field . '`';
        }
        return $field;
    }

    protected function addSqlParam($value)
    {
        if (is_null($value)) {
            $string = '';
        } elseif (is_bool($value)) {
            $string = $value ? 1 : 0;
        } elseif (is_int($value) || is_float($value)) {
            $string = $value;
        } elseif (is_array($value)) {
            $arr = [];
            foreach ($value as $k => $v) {
                if (is_int($v) || is_float($v)) {
                    $arr[] = $v;
                } else {
                    $arr[] = '\'' . $v . '\'';
                }
            }
            $string = implode(',', $arr);
        } else {
            $string = '\'' . $value . '\'';
        }
        return $string;
    }

    protected function showSql($sql)
    {
        if (!$this->debug) {
            return;
        }
        $output = <<<EOT
<pre>
$sql
</pre>
EOT;

        echo $output;
    }

    public function reset()
    {
        parent::reset();
        $this->distinct = false;
        $this->ignore = false;
        $this->forUpdate = false;
    }

    //-------- 地平线 --------//

    /**
     * 连接数据库
     * @param $config
     * @return \PDO
     * @throws Exception
     */
    protected function connect($config)
    {
        $dsn = sprintf('mysql:host=%s;port=%s;dbname=%s', $config['host'], $config['port'], $config['dbname']);
        $charset = empty($config['charset']) ? 'utf8' : $config['charset'];
        $options = array(
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES ' . $charset
        );
        if (!empty($this->config['connect_timeout'])) {
            $options[\PDO::ATTR_TIMEOUT] = $this->config['connect_timeout'];
        }
        if (!empty($this->config['timeout'])) {
            $options[\PDO::ATTR_TIMEOUT] = $this->config['timeout'];
        }
        if (!empty($this->config['is_persistent'])) {
            $options[\PDO::ATTR_PERSISTENT] = true;
        }
        try {
            $pdo = new \PDO($dsn, $config['username'], $config['password'], $options);
        } catch (\PDOException $e) {
            throw new Exception($e->getMessage(), $e->getCode());
        }
        return $pdo;
    }

    /**
     * @param $sql
     * @param $params
     * @return bool|\PDOStatement
     * @throws Exception
     */
    protected function _execute($sql, $params)
    {
        $pdo = $this->getConnection($this->getDbType($this->type));
        try {
            $sth = $pdo->prepare($sql);
            foreach ($params as $key => $value) {
                if (is_int($value)) {
                    $valueType = \PDO::PARAM_INT;
                } elseif (is_bool($value)) {
                    $valueType = \PDO::PARAM_BOOL;
                } elseif (is_null($value)) {
                    $valueType = \PDO::PARAM_NULL;
                } else {
                    $valueType = \PDO::PARAM_STR;
                }
                $sth->bindValue($key + 1, $value, $valueType);
            }
            $sth->execute();
        } catch (\PDOException $e) {
            $MysqlException = new MysqlException($e->getMessage(), $e->getCode());
            $MysqlException->setPrepareSql($sql);
            $MysqlException->setParams($params);
            $config = $this->getConfig($this->type);
            $MysqlException->setHost($config['host']);
            $MysqlException->setPort($config['port']);
            throw $MysqlException;
        }
        $this->reset();
        return $sth;
    }

    /**
     * 获取数据库连接
     * @param $dbType
     * @return \PDO
     * @throws Exception
     */
    protected function getConnection($dbType)
    {
        if (!empty($this->pdo[$dbType])) {
            return $this->pdo[$dbType];
        }
        $config = $this->getConfig($this->type);
        $this->pdo[$dbType] = $this->connect($config);
        return $this->pdo[$dbType];
    }

    /**
     * 配置信息
     * @param $type
     * @return mixed
     * @throws Exception
     */
    protected function getConfig($type)
    {
        if (self::DB_TYPE_SLAVE == $this->getDbType($type)) {
            $config = $this->getSlaveConfig();
        } else {
            $config = $this->getMasterConfig();
        }
        return $config;
    }

    /**
     * DB主从类型
     * @param $type
     * @return int
     */
    protected function getDbType($type)
    {
        if (in_array($type, [self::TYPE_SELECT])) {
            return self::DB_TYPE_SLAVE;
        } else {
            return self::DB_TYPE_MASTER;
        }
    }

    /**
     * DB的主配置
     * @return mixed
     * @throws Exception
     */
    protected function getMasterConfig()
    {
        if (!$this->masterConfig) {
            throw new Exception('Invalid Config');
        }
        return $this->masterConfig;
    }

    /**
     * DB的从配置
     * @return mixed
     * @throws Exception
     */
    protected function getSlaveConfig()
    {
        if (!$this->slaveConfig) {
            $this->slaveConfig = $this->masterConfig;
        }
        if (!$this->slaveConfig) {
            throw new Exception('Invalid Config');
        }
        return $this->slaveConfig;
    }
}