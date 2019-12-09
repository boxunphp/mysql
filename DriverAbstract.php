<?php
/**
 * Created by PhpStorm.
 * User: Jordy
 * Date: 2019/12/6
 * Time: 5:30 PM
 */

namespace All\Mysql;

abstract class DriverAbstract implements DriverInterface
{
    const TYPE_SELECT = 1;
    const TYPE_INSERT = 2;
    const TYPE_UPDATE = 3;
    const TYPE_DELETE = 4;
    const TYPE_REPLACE = 5;
    const TYPE_INSERT_MULTI = 6;
    const TYPE_UPDATE_MULTI = 7;

    protected $table = [];
    protected $join = [];
    protected $fields = '*';
    protected $where = '';
    protected $params = [];
    protected $orderBy = '';
    protected $groupBy = '';
    protected $offset;
    protected $page;
    protected $record;
    protected $index;
    protected $type;

    protected $data = [];
    protected $debug = false;

    public function table($table, $alias = null, $condition = null)
    {
        $data = [];
        $data['table'] = $this->escapeTable($table);
        if ($alias) {
            $data['alias'] = $this->escapeTable($alias);
        }
        if ($condition) {
            list($leftValue, $rightValue) = array_map('trim', explode('=', $condition));
            $leftValue = $this->escapeField($leftValue);
            $rightValue = $this->escapeField($rightValue);
            if ($this->where) {
                $this->where .= ' AND ' . $leftValue . ' = ' . $rightValue;
            } else {
                $this->where = 'WHERE ' . $leftValue . ' = ' . $rightValue;
            }
        }
        $this->table[] = $data;
        return $this;
    }

    public function join($table, $condition, $alias = null, $type = null)
    {
        $data = [];
        $data['table'] = $this->escapeTable($table);
        if ($alias) {
            $data['alias'] = $this->escapeTable($alias);
        }
        $type = strtoupper($type);
        $data['type'] = $type && in_array($type, ['LEFT', 'RIGHT', 'OUTER', 'INNER']) ? $type : '';
        list($leftValue, $rightValue) = array_map('trim', explode('=', $condition));
        $leftValue = $this->escapeField($leftValue);
        $rightValue = $this->escapeField($rightValue);
        $data['condition'] = $leftValue . ' = ' . $rightValue;

        $this->join[] = $data;
        return $this;
    }

    public function leftJoin($table, $condition, $alias = null)
    {
        return $this->join($table, $condition, $alias, 'LEFT');
    }

    public function rightJoin($table, $condition, $alias = null)
    {
        return $this->join($table, $condition, $alias, 'RIGHT');
    }

    public function outerJoin($table, $condition, $alias = null)
    {
        return $this->join($table, $condition, $alias, 'OUTER');
    }

    public function innerJoin($table, $condition, $alias = null)
    {
        return $this->join($table, $condition, $alias, 'INNER');
    }

    public function fields($fields)
    {
        if ($fields) {
            if (!is_array($fields)) {
                $fields = explode(',', $fields);
            }
            $this->fields = implode(',', $this->escapeFields($fields));
        }
        return $this;
    }

    /**
     * @param $field
     * @param $value
     * @param null $operator
     * @param null $connector
     * @return $this
     */
    public function where($field, $value, $operator = null, $connector = null)
    {
        $field = $this->escapeField($field);
        if (is_array($value)) {
            list($where, $params) = $this->whereValueArray($field, $value, $operator);
        } else {
            list($where, $params) = $this->whereValueString($field, $value, $operator);
        }
        $connector = $connector ?: 'AND';
        $this->whereConcat($where, $params, $connector);
        return $this;
    }

    public function orWhere($field, $value, $operator = null)
    {
        return $this->where($field, $value, $operator, 'OR');
    }

    public function beginWhereGroup()
    {
        $this->whereConcat('(', [], 'AND');
        return $this;
    }

    public function orBeginWhereGroup()
    {
        $this->whereConcat('(', [], 'OR');
        return $this;
    }

    public function endWhereGroup()
    {
        $this->where .= ')';
        return $this;
    }

    /**
     * @param $field
     * @param $beginValue
     * @param $endValue
     * @return $this
     */
    public function between($field, $beginValue, $endValue)
    {
        return $this->where($field, [$beginValue, $endValue], 'BETWEEN');
    }

    public function notBetween($field, $beginValue, $endValue)
    {
        return $this->where($field, [$beginValue, $endValue], 'NOT BETWEEN');
    }

    public function orBetween($field, $beginValue, $endValue)
    {
        return $this->where($field, [$beginValue, $endValue], 'BETWEEN', 'OR');
    }

    public function orNotBetween($field, $beginValue, $endValue)
    {
        return $this->where($field, [$beginValue, $endValue], 'NOT BETWEEN', 'OR');
    }

    public function like($field, $value)
    {
        return $this->where($field, '%' . $value . '%', 'LIKE');
    }

    public function leftLike($field, $value)
    {
        return $this->where($field, $value . '%', 'LIKE');
    }

    public function orLike($field, $value)
    {
        return $this->where($field, '%' . $value . '%', 'LIKE', 'OR');
    }

    public function orLeftLike($field, $value)
    {
        return $this->where($field, $value . '%', 'LIKE', 'OR');
    }

    public function in($field, array $value)
    {
        return $this->where($field, $value, 'IN');
    }

    public function notIn($field, array $value)
    {
        return $this->where($field, $value, 'NOT IN');
    }

    public function orIn($field, array $value)
    {
        return $this->orWhere($field, $value, 'IN');
    }

    public function orNotIn($field, array $value)
    {
        return $this->orWhere($field, $value, 'NOT IN');
    }

    public function isNull($field)
    {
        return $this->where($field, null, 'IS NULL');
    }

    public function isNotNull($field)
    {
        return $this->where($field, null, 'IS NOT NULL');
    }

    public function orIsNull($field)
    {
        return $this->orWhere($field, null, 'IS NULL');
    }

    public function orIsNotNull($field)
    {
        return $this->orWhere($field, null, 'IS NOT NULL');
    }

    public function whereSql($where, array $params)
    {
        $this->whereConcat($where, $params, 'AND');
        return $this;
    }

    public function orderBy($field, $sort = null)
    {
        $field = $this->escapeField($field);
        $sort = strtoupper($sort);
        $sort = $sort && in_array($sort, ['ASC', 'DESC']) ? ' ' . $sort : '';
        if ($this->orderBy) {
            $this->orderBy .= ',' . $field . $sort;
        } else {
            $this->orderBy = 'ORDER BY ' . $field . $sort;
        }
        return $this;
    }

    public function groupBy($field)
    {
        $field = $this->escapeField($field);
        if ($this->groupBy) {
            $this->groupBy .= ',' . $field;
        } else {
            $this->groupBy = 'GROUP BY ' . $field;
        }
        return $this;
    }

    public function limit($record, $offset = 0)
    {
        $this->offset = $offset;
        $this->record = $record;
        return $this;
    }

    public function record($record)
    {
        $this->record = $record;
        return $this;
    }

    public function page($page)
    {
        $this->page = $page;
        return $this;
    }

    public function debug()
    {
        $this->debug = true;
        return $this;
    }

    //-------- 地平线 --------//

    abstract protected function sqlConcat();

    abstract protected function whereConcat($where, $params, $connector);

    abstract protected function whereValueArray($field, $value, $operator);

    abstract protected function whereValueString($field, $value, $operator);

    abstract protected function escapeTable($table);

    abstract protected function escapeFields(array $fields);

    abstract protected function escapeField($field);

    protected function reset()
    {
        $this->table = [];
        $this->join = [];
        $this->fields = '*';
        $this->where = '';
        $this->params = [];
        $this->orderBy = '';
        $this->groupBy = '';
        $this->offset = null;
        $this->page = null;
        $this->record = null;
        $this->index = null;
        $this->type = null;

        $this->data = [];
        $this->debug = false;
    }
}