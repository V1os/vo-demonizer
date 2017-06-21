<?php

use app\commands\daemons\consumer\StoreQueueController;
use app\commands\daemons\WatcherController;
use yii\log\FileTarget;
use yii\gii\Module;
use yii\redis\Cache as RedisCache;

define('CONNECT_DB_STORE', 'postgredbStore');
define('CONNECT_DB_INCY', 'db');

$params = require __DIR__ . '/params.php';

$config = [
    'id' => 'basic-console',
    'basePath' => dirname(__DIR__),
    'bootstrap' => ['log'],
    'controllerNamespace' => 'app\commands',
    'controllerMap' => [
        'dm' => [ 'class' => DemoController::class ],
        'wt' => [ 'class' => WatcherController::class ],
        'consumer-sq' => [ 'class' => QueueController::class]
    ],
    'components' => [
        'cache' => [
            'class' => RedisCache::class,
            'redis' => [
                'hostname' => 'localhost',
                'port' => 6379,
                'database' => 0,
            ]
        ],
        'log' => [
            'flushInterval' => 4,
            'targets'       => [
                [
                    'class'          => FileTarget::class,
                    'levels'         => ['error', 'warning'],
                    'except'         => ['yii\web\HttpException:*', 'yii\i18n\I18N\*', 'yii\redis\*', 'yii\base\*'],
                    'prefix'         => function () {
                        return sprintf('[%s]', Yii::$app->id);
                    },
                    'logVars'        => [],
                    'exportInterval' => 2,
                ],
            ],
        ],
        'db' => require __DIR__ . '/db/db_store.php'
    ],
    'params' => $params,
];

if (YII_ENV_DEV) {
    $config['bootstrap'][] = 'gii';
    $config['modules']['gii'] = [
        'class' => Module::class,
    ];
}

return $config;
