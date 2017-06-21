<?php
require_once __DIR__ . '/../vendor/autoload.php';
define('HOST', 'localhost');
define('PORT', 5672);
define('USER', 'catalog');
define('PASS', 'catalog');
define('VHOST', 'catalog');
// If this is enabled you can see AMQP output on the CLI
define('AMQP_DEBUG', false);