<?php

namespace app\commands\daemons;

use app\extend\daemon\DaemonController;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use yii\base\InvalidParamException;

/**
 * Base class queue
 * @property array                           $arguments
 * @property \PhpAmqpLib\Channel\AMQPChannel $queue
 */
class BaseQueueController extends DaemonController
{
    const SHORT_KEY_REPUBLISH   = 'S';
    const SHORT_KEY_MAX_FLAG    = 'I';

    const TYPE_EX_FANOUT        = 'fanout';
    const TYPE_EX_DIRECT        = 'direct';
    const TYPE_EX_TOPIC         = 'topic';

    const LOG_CATEGORY_NAME     = 'queue_%s';
    const TIMEOUT_CHANEL        = 10;

    // EXCHANGE params
    const EXCHANGE_NAME         = 'catalog_queue';
    const EXCHANGE_TYPE         = self::TYPE_EX_DIRECT;     // default DIRECT
    const EXCHANGE_PASSIVE      = false;                    // сервер вернет ОК, если таже точка задекларирована, иначе ошибка
    const EXCHANGE_DURABLE      = true;                     // признак устойчивости точки (сохраняет на диск)
    const EXCHANGE_AUTO_DELETE  = false;                    // да - точка обмена удаляеться если все очереди обработаны подписчиками

    // QUEUE params
    const QUEUE_NAME            = '';                       // 'catalog_queue'; if routing key exist should be empty
    const QUEUE_PASSIVE         = false;                    // сервер вернет ОК, если очередь существует, иначе ошибка
    const QUEUE_DURABLE         = true;                     // признак устойчивости точки (сохраняет на диск)
    const QUEUE_EXCLUSIVE       = false;                    // да - эксклюзивная очередь, доступка только текущему соеденнению (не может быть экскл.)
    const QUEUE_AUTO_DELETE     = true;                    // да - автоудаления очереди после использования ее всеми подписчиками
    const QUEUE_NO_WAIT         = false;                    // да - не отвечать на сообщения очереди

    // CONSUMER params
    const CONSUME_TAG           = '';                       // тег для подписчика, локальный для канала,
                                                            // несколько клиентов(подписчиков) могут иметь тот же тег
    const CONSUME_NO_LOCAL      = false;                    // да - не отправляет сообщения в сообщения которое их опубликовало (локальное)
    const CONSUME_NO_ACK        = false;                    // да - сервер не ждет подтверждения и удаляет не дожидаясь статуса отправки
    const CONSUME_EXCLUSIVE     = false;                    // да - запросить доступ только для этого подписчика
    const CONSUME_NO_WAIT       = false;                    // да - не отвечать на данный метод, если ошибка - обрыв канала или соеденения, клиент не
                                                            // ждет ответа

    /** @var  string */
    protected $exchange = self::EXCHANGE_NAME;
    /** @var  string */
    protected $type = self::EXCHANGE_TYPE;
    /** @var  string Dead-letter exchange router name */
    protected $dlx;
    /** @var  int Max length queue */
    protected $maxLength;
    /** @var  int Max length queue on bytes */
    protected $maxBytes;
    /** @var  int [0..255] Max priority queue */
    protected $maxPriority;
    /** @var  string Name queue to declare */
    protected $queueName = self::QUEUE_NAME;
    /** @var array */
    protected $bindingKeys = [];
    /** @var $connection AMQPStreamConnection */
    protected $connection;
    /** @var $connection AMQPChannel */
    protected $channel;

    /**
     * @param $sJobName
     * @return bool
     * @throws \yii\base\ExitException
     */
    protected function doJob($sJobName) {
        $message = 'Base controller job';
        $this->stdout($message);
        \Yii::info($message, sprintf(self::LOG_CATEGORY_NAME, $sJobName));
        $this->halt(self::EXIT_CODE_NORMAL);

        return true;
    }

    /** @inheritdoc */
    protected function defineJobs() {
        return [];
    }

    /**
     * @return AMQPChannel
     * @throws InvalidParamException
     */
    protected function getQueue() {
        if ($this->channel === null) {
            $this->connect();

            $this->channel->exchange_declare(
                $this->exchange,
                $this->type,
                self::EXCHANGE_PASSIVE,
                self::EXCHANGE_DURABLE,
                self::EXCHANGE_AUTO_DELETE);

            list($queueName, ,) = $this->channel->queue_declare(
                $this->queueName,
                self::QUEUE_PASSIVE,
                self::QUEUE_DURABLE,
                self::QUEUE_EXCLUSIVE,
                self::QUEUE_AUTO_DELETE,
                self::QUEUE_NO_WAIT,
                $this->getArguments());

            foreach ($this->bindingKeys as $bindingKey) {
                $this->channel->queue_bind(
                    $queueName,
                    $this->exchange,
                    $bindingKey);
            }

            $this->channel->basic_consume(
                $queueName,
                '', //@todo PID process
                self::CONSUME_NO_LOCAL,
                self::CONSUME_NO_ACK,
                self::CONSUME_EXCLUSIVE,
                self::CONSUME_NO_WAIT,
                [$this, 'doJob']);
        }

        return $this->channel;
    }

    /**
     * Good job
     *
     * @param AMQPMessage $oJobMessage
     */
    protected function ask(AMQPMessage $oJobMessage) {
        $oJobMessage->delivery_info['channel']->basic_ack($oJobMessage->delivery_info['delivery_tag']);
    }

    /**
     * Bad job, on rework
     *
     * @param AMQPMessage $oJobMessage
     */
    protected function nAsk(AMQPMessage $oJobMessage) {
        $oJobMessage->delivery_info['channel']->basic_nack($oJobMessage->delivery_info['delivery_tag']);
    }

    /**
     * Return arguments queue for declaration
     * @return array
     */
    private function getArguments(){
        $args = [];

        /*  Назначить точку для сброса не обработанных сообщений */
        if ($this->dlx) {
            $args['x-dead-letter-exchange'] = [self::SHORT_KEY_REPUBLISH, $this->exchange];
            $args['x-dead-letter-routing-key'] = [self::SHORT_KEY_REPUBLISH, $this->dlx];
        }

        /* Максимальное значения сообщений в очередиб , если перелимит в точку для сброса выше */
        if ($this->maxLength) {
            $args['x-max-length'] = [self::SHORT_KEY_MAX_FLAG, $this->maxLength];
        }

        /* Максимальная вместительность очереди в байтах, если перелимит в точку для сброса выше */
        if ($this->maxBytes) {
            $args['x-max-length-bytes'] = [self::SHORT_KEY_MAX_FLAG, $this->maxBytes];
        }

        if ($this->maxPriority) {
            $args['x-max-priority'] = [self::SHORT_KEY_MAX_FLAG, $this->maxPriority];
        }

        return $args;
    }

    /**
     * Connect to steam rabbit
     * @throws \yii\base\InvalidParamException
     */
    private function connect(){
        if ($this->connection === null) {
            if (isset(\Yii::$app->params['rabbit'])) {
                $rabbit = \Yii::$app->params['rabbit'];
            } else {
                throw new InvalidParamException('Bad config RabbitMQ');
            }
            $this->connection = new AMQPStreamConnection($rabbit['host'], $rabbit['port'], $rabbit['user'], $rabbit['password'], $rabbit['vhost']);
        }

        $this->channel = $this->connection->channel();
    }
}