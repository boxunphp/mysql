# php-mysql
PHP Mysql Client

## Configure

```php
$config = [
    'master' => [
        'host' => 'mysql-3306',
        'port' => 3306,
        'username' => 'root',
        'password' => '123456',
        'dbname' => 'test',
        'charset' => 'utf8',
    ],
    'slaves' => [
        [
            'host' => 'mysql-3307',
            'port' => 3307,
            'username' => 'root',
            'password' => '123456',
            'dbname' => 'test',
            'charset' => 'utf8',
        ],
        [
            'host' => 'mysql-3308',
            'port' => 3308,
            'username' => 'root',
            'password' => '123456',
            'dbname' => 'test',
            'charset' => 'utf8',
        ],
    ],
    'connect_timeout' => 1,
    'timeout' => 1,
    'is_persistent' => true,
];
```