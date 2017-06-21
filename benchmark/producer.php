<?php
/**
 * Usage:
 *  php producer.php 10000
 * The integer arguments tells the script how many messages to publish.
 */
use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Message\AMQPMessage;

include __DIR__ . '/config.php';
$exchange = 'catalog_queue';
$queue = 'catalog_queue';
$max = isset($argv[1]) ? (int)$argv[1] : 1;
$rout_key = $queue = isset($argv[2]) ? $argv[2] : 'goods';

$conn = new AMQPConnection(HOST, PORT, USER, PASS, VHOST);
$ch = $conn->channel();
$ch->queue_declare($queue, false, true, false, false);
$ch->exchange_declare($exchange, 'direct', false, true, false);
$ch->queue_bind($queue, $exchange);

//$msg_body = sprintf('{"entity":"%s","id":"16943012","action":"insert"}', $rout_key);
$time = microtime(true);
// Publishes $max messages using $msg_body as the content.
for ($i = 0; $i < $max; $i++) {
    $msg_body = sprintf('{"id":"%s","action":"insert"}', mt_rand(1000000, 16000000), $rout_key);
    $msg = new AMQPMessage($msg_body);
    $ch->basic_publish($msg, $exchange, $rout_key);
}
echo microtime(true) - $time, "\n";
$ch->close();
$conn->close();