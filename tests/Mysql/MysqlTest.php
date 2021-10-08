<?php
/**
 * Created by PhpStorm.
 * User: Jordy
 * Date: 2019/12/6
 * Time: 6:49 PM
 */

namespace Tests\Mysql;

use All\Mysql\Drivers\Mysql;
use PHPUnit\Framework\TestCase;

class MysqlTest extends TestCase
{
    /**
     * @var Mysql
     */
    protected $db;

    protected function setUp(): void
    {
        $config = [
            'master' => [
                'host' => 'mysql-3306',
                'port' => 3306,
                'username' => 'root',
                'password' => '123456',
                'dbname' => 'test',
                'charset' => 'utf8',
            ],
            'connect_timeout' => 1,
            'timeout' => 1,
            'is_persistent' => true,
        ];
        $this->db = Mysql::getInstance($config);

        $tableSQL = <<<EOT
CREATE TABLE `user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(45) NOT NULL,
  `create_time` int(10) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
EOT;

        $this->db->execute("DROP TABLE IF EXISTS `user`");
        $this->db->execute($tableSQL);
    }

    public function testSelectSQL()
    {
        $validSqlArr = [
            "SELECT `id`,`name` FROM `user` WHERE `id` = 10",
            "SELECT DISTINCT `id`,`name` FROM `user`",
            "SELECT COUNT(id) as count FROM `user`",
            "SELECT * FROM `user` WHERE `id` = 10",
            "SELECT * FROM `user` WHERE `id` != 10",
            "SELECT * FROM `user` WHERE `id` <> 10",
            "SELECT * FROM `user` WHERE `id` > 1",
            "SELECT * FROM `user` WHERE `id` >= 1",
            "SELECT * FROM `user` WHERE `id` < 10",
            "SELECT * FROM `user` WHERE `id` <= 10",
            "SELECT * FROM `user` WHERE `id` IN (1,2,3)",
            "SELECT * FROM `user` WHERE `id` NOT IN (1,2,3)",
            "SELECT * FROM `user` WHERE `id` BETWEEN 1 AND 10",
            "SELECT * FROM `user` WHERE `name` LIKE '%abc%'",
            "SELECT * FROM `user` WHERE `name` LIKE 'abc%'",
            "SELECT * FROM `user` WHERE `name` IS NULL",
            "SELECT * FROM `user` WHERE `name` IS NOT NULL",
            "SELECT * FROM `user` WHERE `id` IN (1,2,3) AND `name` LIKE '%abc%'",
            "SELECT * FROM `user` WHERE `id` IN (1,2,3) OR `name` LIKE '%abc%'",
            "SELECT * FROM `user` WHERE `create_time` BETWEEN '2019-01-01 12:00:00' AND '2019-10-01 12:00:00'",

            // LIMIT
            "SELECT * FROM `user` LIMIT 10",
            "SELECT * FROM `user` LIMIT 0,10",
            "SELECT * FROM `user` LIMIT 2,10",

            // GROUP BY
            "SELECT * FROM `user` GROUP BY `level`",
            "SELECT *,COUNT(id) as count FROM `user` GROUP BY `level`",

            // JOIN
            "SELECT `u`.`id`,`u`.`name`,`d`.`content` FROM `user` AS `u` JOIN `user_detail` AS `d` ON `u`.`id` = `d`.`id` WHERE `u`.`id` = 10",
            "SELECT `u`.`id`,`u`.`name`,`d`.`content` FROM `user` AS `u` LEFT JOIN `user_detail` AS `d` ON `u`.`id` = `d`.`id` WHERE `u`.`id` = 10",
            "SELECT `u`.`id`,`u`.`name`,`d`.`content` FROM `user` AS `u` RIGHT JOIN `user_detail` AS `d` ON `u`.`id` = `d`.`id` WHERE `u`.`id` = 10",
            "SELECT `u`.`id`,`u`.`name`,`d`.`content` FROM `user` AS `u`,`user_detail` AS `d` WHERE `u`.`id` = `d`.`id` AND `u`.`id` = 10",

            // BeginGroup
            "SELECT * FROM `user` WHERE `id` IN (1,2,3,4) AND (`name` LIKE '%abc%' OR `level` = 1)",
        ];

        $sqlArr = [
            $this->db->table('user')->fields('id,name')->where('id', 10)->getSql(),
            $this->db->table('user')->distinct()->fields('id,name')->getSql(),
            $this->db->table('user')->fields('COUNT(id) as count')->getSql(),
            $this->db->table('user')->where('id', 10)->getSql(),
            $this->db->table('user')->where('id', 10, '!=')->getSql(),
            $this->db->table('user')->where('id', 10, '<>')->getSql(),
            $this->db->table('user')->where('id', 1, '>')->getSql(),
            $this->db->table('user')->where('id', 1, '>=')->getSql(),
            $this->db->table('user')->where('id', 10, '<')->getSql(),
            $this->db->table('user')->where('id', 10, '<=')->getSql(),
            $this->db->table('user')->in('id', [1, 2, 3])->getSql(),
            $this->db->table('user')->notIn('id', [1, 2, 3])->getSql(),
            $this->db->table('user')->between('id', 1, 10)->getSql(),
            $this->db->table('user')->like('name', 'abc')->getSql(),
            $this->db->table('user')->leftLike('name', 'abc')->getSql(),
            $this->db->table('user')->isNull('name')->getSql(),
            $this->db->table('user')->isNotNull('name')->getSql(),
            $this->db->table('user')->in('id', [1, 2, 3])->like('name', 'abc')->getSql(),
            $this->db->table('user')->in('id', [1, 2, 3])->orLike('name', 'abc')->getSql(),
            $this->db->table('user')->between('create_time', '2019-01-01 12:00:00', '2019-10-01 12:00:00')->getSql(),

            // LIMIT
            $this->db->table('user')->record(10)->getSql(),
            $this->db->table('user')->page(1)->record(10)->getSql(),
            $this->db->table('user')->limit(10, 2)->getSql(),

            // GROUP BY
            $this->db->table('user')->groupBy('level')->getSql(),
            $this->db->table('user')->fields('*,COUNT(id) as count')->groupBy('level')->getSql(),

            // JOIN
            $this->db->table('user', 'u')->join(
                'user_detail',
                'u.id=d.id',
                'd'
            )->fields('u.id,u.name,d.content')->where('u.id', 10)->getSql(),
            $this->db->table('user', 'u')->leftJoin(
                'user_detail',
                'u.id=d.id',
                'd'
            )->fields('u.id,u.name,d.content')->where('u.id', 10)->getSql(),
            $this->db->table('user', 'u')->rightJoin(
                'user_detail',
                'u.id=d.id',
                'd'
            )->fields('u.id,u.name,d.content')->where('u.id', 10)->getSql(),
            $this->db->table('user', 'u')->table(
                'user_detail',
                'd',
                'u.id=d.id'
            )->fields('u.id,u.name,d.content')->where('u.id', 10)->getSql(),

            // BeginGroup
            $this->db->table('user')->where('id', [1, 2, 3, 4])->beginWhereGroup()->like(
                'name',
                'abc'
            )->orWhere('level', 1)->endWhereGroup()->getSql(),
        ];

        foreach ($sqlArr as $key => $sql) {
            $this->assertEquals($validSqlArr[$key], $sql);
        }
    }

    public function testInsertSQL()
    {
        $validSqlArr = [
            "INSERT INTO `user`(`id`,`name`) VALUES(1,'abc')",
            "INSERT IGNORE INTO `user`(`id`,`name`) VALUES(1,'abc')",
        ];

        $data = ['id' => 1, 'name' => 'abc'];

        $sqlArr = [
            $this->db->table('user')->insert($data)->getSql(),
            $this->db->table('user')->insert($data)->ignore()->getSql(),
        ];

        foreach ($sqlArr as $key => $sql) {
            $this->assertEquals($validSqlArr[$key], $sql);
        }
    }

    public function testInsertMultiSQL()
    {
        $validSqlArr = [
            "INSERT INTO `user`(`id`,`name`) VALUES(1,'a'),(2,'b'),(3,'c')",
            "INSERT IGNORE INTO `user`(`id`,`name`) VALUES(1,'a'),(2,'b'),(3,'c')",
        ];

        $data = [
            ['id' => 1, 'name' => 'a'],
            ['id' => 2, 'name' => 'b'],
            ['id' => 3, 'name' => 'c'],
        ];

        $sqlArr = [
            $this->db->table('user')->insertMulti($data)->getSql(),
            $this->db->table('user')->insertMulti($data)->ignore()->getSql(),
        ];

        foreach ($sqlArr as $key => $sql) {
            $this->assertEquals($validSqlArr[$key], $sql);
        }
    }

    public function testUpdateSQL()
    {
        $validSqlArr = [
            "UPDATE `user` SET `name`='cba',`create_time`=100 WHERE `id` = 10",
            "UPDATE `user` SET `name`='cba',`create_time`=100 WHERE `id` = 10 AND `level` = 1",
        ];

        $data = ['name' => 'cba', 'create_time' => 100];

        $sqlArr = [
            $this->db->table('user')->update($data)->where('id', 10)->getSql(),
            $this->db->table('user')->where('id', 10)->update($data)->where('level', 1)->getSql(),
        ];

        foreach ($sqlArr as $key => $sql) {
            $this->assertEquals($validSqlArr[$key], $sql);
        }
    }

    public function testUpdateMultiSQL()
    {
        $validSqlArr = [
            "UPDATE `user` SET `name` = CASE `id` WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN 'c' END,`create_time` = CASE `id` WHEN 1 THEN 101 WHEN 2 THEN 102 WHEN 3 THEN 103 END WHERE `id` IN (1,2,3)",
        ];

        $data = [
            ['id' => 1, 'name' => 'a', 'create_time' => 101],
            ['id' => 2, 'name' => 'b', 'create_time' => 102],
            ['id' => 3, 'name' => 'c', 'create_time' => 103],
        ];

        $sqlArr = [
            $this->db->table('user')->updateMulti($data, 'id')->getSql(),
        ];

        foreach ($sqlArr as $key => $sql) {
            $this->assertEquals($validSqlArr[$key], $sql);
        }
    }

    public function testReplaceSQL()
    {
        $validSqlArr = [
            "REPLACE INTO `user`(`id`,`name`) VALUES(1,'abc')",
        ];

        $data = ['id' => 1, 'name' => 'abc'];

        $sqlArr = [
            $this->db->table('user')->replace($data)->getSql(),
        ];

        foreach ($sqlArr as $key => $sql) {
            $this->assertEquals($validSqlArr[$key], $sql);
        }
    }

    public function testDeleteSQL()
    {
        $validSqlArr = [
            "DELETE FROM `user` WHERE `id` = 10 AND `level` = 1"
        ];

        $sqlArr = [
            $this->db->table('user')->where('id', 10)->where('level', 1)->delete()->getSql()
        ];

        foreach ($sqlArr as $key => $sql) {
            $this->assertEquals($validSqlArr[$key], $sql);
        }
    }

    public function testIncrementSQL()
    {
        $validSqlArr = [
            "UPDATE `user` SET `click_count`=`click_count`+10,`read_count`=`read_count`+1 WHERE `id` = 10",
            "UPDATE `user` SET `click_count`=`click_count`+10,`read_count`=`read_count`-3 WHERE `id` = 10",
        ];

        $sqlArr = [
            $this->db->table('user')->where('id', 10)->increment(['click_count'=>10,'read_count'=>1])->getSql(),
            $this->db->table('user')->where('id', 10)->increment(['click_count'=>10,'read_count'=>-3])->getSql(),
        ];

        foreach ($sqlArr as $key => $sql) {
            $this->assertEquals($validSqlArr[$key], $sql);
        }
    }

    public function testData()
    {
        // INSERT
        $this->assertTrue($this->db->execute('TRUNCATE TABLE `user`'));
        $data = ['id' => 1, 'name' => 'a', 'create_time' => 101];
        $this->assertTrue($this->db->table('user')->insert($data)->exec());
        $data = ['id' => 2, 'name' => 'b', 'create_time' => 102];
        $this->assertEquals(2, $this->db->table('user')->insert($data)->lastInsertId());
        $this->assertEquals($data, $this->db->table('user')->where('id', 2)->fetch());

        // UPDATE
        $updateData = ['name' => 'b', 'create_time' => 1102];
        $this->assertEquals(1, $this->db->table('user')->where('id', 2)->update($updateData)->exec());
        $this->assertEquals(
            ['id' => 2, 'name' => 'b', 'create_time' => 1102],
            $this->db->table('user')->where('id', 2)->fetch()
        );

        // REPLACE
        $replaceData = $data = ['id' => 2, 'name' => 'bb', 'create_time' => 2102];
        $this->assertEquals(1, $this->db->table('user')->replace($replaceData)->exec());
        $this->assertEquals(
            ['id' => 2, 'name' => 'bb', 'create_time' => 2102],
            $this->db->table('user')->where('id', 2)->fetch()
        );

        // INSERT MULTI
        $data = [
            ['name' => 'c', 'create_time' => 103],
            ['name' => 'e', 'create_time' => 104],
            ['name' => 'f', 'create_time' => 105],
        ];
        $this->assertEquals(3, $this->db->table('user')->insertMulti($data)->lastInsertId());
        $this->assertEquals(['count' => 5], $this->db->table('user')->fields('COUNT(*) as count')->fetch());

        // UPDATE MULTI
        $data = [
            ['id' => 3, 'name' => 'ccc', 'create_time' => 1103],
            ['id' => 4, 'name' => 'eee', 'create_time' => 1104],
            ['id' => 5, 'name' => 'fff', 'create_time' => 1105],
        ];
        $this->assertEquals(3, $this->db->table('user')->updateMulti($data, 'id')->exec());
        $this->assertEquals($data, $this->db->table('user')->where('id', [3, 4, 5])->fetchAll());
    }
}
