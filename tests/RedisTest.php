<?php
/**
 * Created by PhpStorm.
 * User: Jordy
 * Date: 2019/12/3
 * Time: 4:15 PM
 */

namespace RedisTest;

use All\Redis\Redis;
use PHPUnit\Framework\TestCase;

class RedisTest extends TestCase
{
    /** @var  Redis */
    private $redis;

    public function setUp()
    {
        $this->redis = Redis::getInstance([
            'host' => $GLOBALS['REDIS_HOST'],
            'port' => $GLOBALS['REDIS_PORT'],
            'timeout' => 1.0, //s
        ]);
    }

    public function testStrings()
    {
        $k1 = 'k1';
        $k2 = 'k2';
        $this->assertTrue($this->redis->set($k1, 1));
        $this->assertEquals($this->redis->get($k1), 1);
        $this->assertEquals($this->redis->exists($k1), 1);
        $this->assertTrue($this->redis->set($k2, 2));
        $this->assertEquals($this->redis->del($k1, $k2), 2);
        $this->assertFalse($this->redis->get($k1));
        $this->assertFalse($this->redis->get($k2));

        $this->assertTrue($this->redis->set($k1, 1));
        $this->assertEquals($this->redis->incr($k1), 2);
        $this->assertEquals($this->redis->incrBy($k1, 3), 5);
        $this->assertEquals($this->redis->incrByFloat($k1, 2.7), 7.7);
        $this->assertEquals($this->redis->get($k1), 7.7);

        $this->assertTrue($this->redis->setex($k1, 3600, 1));
        $this->assertTrue($this->redis->psetex($k2, 3600, 2));
        $ttl = $this->redis->ttl($k1);
        $pttl = $this->redis->pttl($k2);
        $this->assertTrue($ttl > 0);
        $this->assertTrue($pttl > 0);

        $data = $this->redis->mget([$k1, $k2]);
        $this->assertEquals(count($data), 2);
        $this->assertEquals($data[0], 1);
        $this->assertEquals($data[1], 2);

        $this->assertEquals($this->redis->del($k2), 1);

        $this->assertFalse($this->redis->setnx($k1, 10));
        $this->assertTrue($this->redis->setnx($k2, 20));
        $this->assertEquals($this->redis->get($k1), 1);
        $this->assertEquals($this->redis->get($k2), 20);

        $this->assertTrue($this->redis->mset([$k1 => 5, $k2 => 6]));
        $this->assertEquals($this->redis->ttl($k1), -1);
        $this->assertEquals($this->redis->ttl($k2), -1);

        $this->assertTrue($this->redis->expire($k1, 3600));
        $this->assertTrue($this->redis->pexpire($k2, 3600));
        $this->assertTrue($this->redis->ttl($k1) > 0);
        $this->assertTrue($this->redis->pttl($k2) > 0);

        $this->assertTrue($this->redis->expireAt($k1, time() + 3600));
        $this->assertTrue($this->redis->pexpireAt($k2, time() * 1000 + 3600));
        $this->assertTrue($this->redis->ttl($k1) > 0 && $this->redis->ttl($k1) <= 3600);
        $this->assertTrue($this->redis->pttl($k2) > 0 && $this->redis->pttl($k2) <= 3600);
    }

    public function testHashes()
    {
        $key = 'hash';
        $h1 = 'a';
        $h2 = 'b';
        $h3 = 'c';

        $this->redis->del($key);
        $this->assertEquals($this->redis->hSet($key, $h1, 1), 1);
        $this->assertEquals($this->redis->hDel($key, $h1), 1);
        $this->assertTrue($this->redis->hMSet($key, [$h1 => 1, $h2 => 2]));
        $this->assertEquals($this->redis->hGet($key, $h1), 1);
        $this->assertEquals($this->redis->hGet($key, $h2), 2);
        $this->assertEquals($this->redis->hIncrBy($key, $h1, 3), 4);
        $this->assertEquals($this->redis->hIncrBy($key, $h1, -2), 2);
        $this->assertEquals($this->redis->hIncrBy($key, $h1, -3), -1);
        $this->assertEquals($this->redis->hIncrByFloat($key, $h2, -3.3), -1.3);
        $this->assertTrue($this->redis->hExists($key, $h1));
        $this->assertTrue($this->redis->hExists($key, $h2));
        $this->assertEquals($this->redis->hLen($key), 2);
        $this->assertEquals($this->redis->hKeys($key), [$h1, $h2]);
        $this->assertEquals($this->redis->hVals($key), [-1, -1.3]);
        $this->assertEquals($this->redis->hGetAll($key), [$h1 => -1, $h2 => -1.3]);
        $this->assertEquals($this->redis->hGetAll($key), $this->redis->hMGet($key, [$h1, $h2]));
        $this->assertFalse($this->redis->hSetNx($key, $h1, 1));
        $this->assertTrue($this->redis->hSetNx($key, $h3, 3));
    }

    public function testLists()
    {
        $key = 'list';
        $key2 = '$list2';
        $v1 = 1;
        $v2 = 2;
        $v3 = 3;
        $v4 = 4;
        $v5 = 5;
        $v6 = 6;

        $this->redis->del($key);
        $this->redis->del($key2);
        $this->assertEquals($this->redis->lPush($key, $v1), 1);
        $this->assertEquals($this->redis->lPush($key, $v2), 2);
        $this->assertEquals($this->redis->lPush($key, $v3), 3);
        $this->assertEquals($this->redis->rPop($key), $v1);
        $this->assertEquals($this->redis->rPop($key), $v2);
        $this->assertEquals($this->redis->rPop($key), $v3);
        $this->assertEquals($this->redis->rPush($key, $v1), 1);
        $this->assertEquals($this->redis->rPush($key, $v2), 2);
        $this->assertEquals($this->redis->rPush($key, $v3), 3);
        $this->assertEquals($this->redis->lPop($key), $v1);
        $this->assertEquals($this->redis->lPop($key), $v2);
        $this->assertEquals($this->redis->lPop($key), $v3);


        $this->assertEquals($this->redis->rPush($key, $v1), 1);
        $this->assertEquals($this->redis->rPush($key, $v2), 2);
        $this->assertEquals($this->redis->rPush($key, $v3), 3);

        $this->assertEquals($this->redis->lGet($key, 0), $v1);
        $this->assertEquals($this->redis->lGet($key, 1), $v2);
        $this->assertEquals($this->redis->lGet($key, 2), $v3);
        $this->assertFalse($this->redis->lGet($key, 10));
        $this->assertEquals($this->redis->lIndex($key, 0), $v1);
        $this->assertEquals($this->redis->lIndex($key, 1), $v2);
        $this->assertEquals($this->redis->lIndex($key, 2), $v3);
        $this->assertFalse($this->redis->lIndex($key, 10));

        $this->assertEquals($this->redis->lRange($key, 1, 2), [$v2, $v3]);
        $this->assertEquals($this->redis->lLen($key), 3);
        $this->redis->rPush($key, $v2);
        $this->redis->rPush($key, $v2);
        $this->redis->rPush($key, $v2);
        $this->assertEquals($this->redis->lLen($key), 6);
        $this->assertEquals($this->redis->lRem($key, $v2, 2), 2);
        $this->assertEquals($this->redis->lLen($key), 4);
        $this->assertEquals($this->redis->lRange($key, 0, 3), [$v1, $v3, $v2, $v2]);
        $this->assertTrue($this->redis->lTrim($key, 1, 2));
        $this->assertEquals($this->redis->lRange($key, 0, 1), [$v3, $v2]);


        $this->redis->rPush($key2, $v4, $v5, $v6);
        $this->assertEquals($this->redis->lRange($key2, 0, 2), [$v4, $v5, $v6]);

        $this->assertEquals($this->redis->rPoplPush($key2, $key), $v6);
        $this->assertEquals($this->redis->lRange($key, 0, -1), [$v6, $v3, $v2]);
        $this->assertEquals($this->redis->lRange($key2, 0, -1), [$v4, $v5]);
    }

    public function testSets()
    {
        $key = 'set';
        $v1 = 1;
        $v2 = 2;
        $v3 = 3;
        $v4 = 4;
        $v5 = 5;
        $v6 = 6;
        $this->redis->del($key);
        $this->assertEquals($this->redis->sAdd($key, $v1), 1);
        $this->assertEquals($this->redis->sAdd($key, $v2, $v3), 2);
        $this->assertEquals($this->redis->sAdd($key, $v2, $v3), 0);
        $this->assertEquals($this->redis->sCard($key), 3);
        $this->assertEquals($this->redis->sSize($key), 3);
        $this->assertTrue($this->redis->sIsMember($key, $v1));
        $this->assertFalse($this->redis->sIsMember($key, $v4));
        $this->assertEquals($this->redis->sMembers($key), [$v1, $v2, $v3]);
        $this->assertEquals($this->redis->sRem($key, $v1, $v3), 2);
        $this->assertEquals($this->redis->sMembers($key), [$v2]);
        $this->assertEquals($this->redis->sAdd($key, $v4, $v5, $v6), 3);
        $this->assertTrue($this->redis->sPop($key) > 0);
        $this->assertEquals($this->redis->sCard($key), 3);
        $this->assertTrue($this->redis->sRandMember($key) > 0);
        $this->assertEquals($this->redis->sCard($key), 3);
    }

    public function testSortedSets()
    {
        $key = 'sorted_set';
        $s1 = 1;
        $s2 = 2;
        $s3 = 3;
        $s4 = 4;
        $s5 = 5;
        $s6 = 6;
        $v1 = 'a';
        $v2 = 'b';
        $v3 = 'c';
        $v4 = 'd';
        $v5 = 'e';
        $v6 = 'f';
        $this->redis->del($key);
        $this->assertEquals($this->redis->zAdd($key, $s1, $v1), 1);
        $this->assertEquals($this->redis->zAdd($key, $s2, $v2, $s3, $v3), 2);
        $this->assertEquals($this->redis->zCard($key), 3);
        $this->assertEquals($this->redis->zSize($key), 3);
        $this->assertEquals($this->redis->zAdd($key, $s5, $v5, $s4, $v4, $s6, $v6), 3);
        $this->assertEquals($this->redis->zCount($key, 2, 4), 3);
        $this->assertEquals($this->redis->zRange($key, 2, 4), [$v3, $v4, $v5]);
        $this->assertEquals($this->redis->zRange($key, 2, 4, true), [$v3 => $s3, $v4 => $s4, $v5 => $s5]);
        $this->assertEquals($this->redis->zRevRange($key, 2, 4), [$v4, $v3, $v2]);
        $this->assertEquals($this->redis->zRevRange($key, 2, 4, true), [$v4 => $s4, $v3 => $s3, $v2 => $s2]);
        $this->assertEquals($this->redis->zRangeByScore($key, 2, 4), [$v2, $v3, $v4]);
        $this->assertEquals($this->redis->zRangeByScore($key, 2, 4, ['withscores' => true]),
            [$v2 => $s2, $v3 => $s3, $v4 => $s4]);
        $this->assertEquals($this->redis->zRevRangeByScore($key, 4, 2), [$v4, $v3, $v2]);
        $this->assertEquals($this->redis->zRevRangeByScore($key, 4, 2, ['withscores' => true]),
            [$v4 => $s4, $v3 => $s3, $v2 => $s2]);

        $this->assertEquals($this->redis->zRank($key, $v3), 2);
        $this->assertEquals($this->redis->zRevRank($key, $v3), 3);
        $this->assertEquals($this->redis->zScore($key, $v4), $s4);
        $this->assertEquals($this->redis->zRem($key, $v4, $v5), 2);
        $this->assertEquals($this->redis->zRange($key, 0, -1, true), [$v1 => $s1, $v2 => $s2, $v3 => $s3, $v6 => $s6]);
        $this->assertEquals($this->redis->zRemRangeByScore($key, 1, 2), 2);
        $this->assertEquals($this->redis->zRange($key, 0, -1, true), [$v3 => $s3, $v6 => $s6]);
        $this->assertEquals($this->redis->zRemRangeByRank($key, 1, 1), 1);
        $this->assertEquals($this->redis->zRange($key, 0, -1, true), [$v3 => $s3]);
        $this->assertEquals($this->redis->zIncrBy($key, 3.7, $v3), 6.7);
    }

    public function testTransactions()
    {
        $k1 = 'm1';
        $k2 = 'm2';
        $v1 = 1;
        $v2 = 2;
        $this->redis->del($k1, $k2);
        $redis = $this->redis->multi();
        $redis->set($k1, $v1)->set($k2, $v2);
        $this->assertEquals($redis->exec(), [0 => true, 1 => true]);

        $this->assertEquals($this->redis->get($k1), $v1);
        $this->assertEquals($this->redis->get($k2), $v2);
    }

}