<?php
namespace app\commands\daemons\consumer;

use app\commands\daemons\BaseQueueController;
use app\models\incy\handler\FactoryIncyModel;
use app\models\store\handler\FactoryMapModel;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use yii\base\NotSupportedException;
use yii\db\ActiveQuery;
use yii\db\ActiveRecord;
use yii\helpers\ArrayHelper;
use yii\helpers\Json;
use Yii;

/** @noinspection LongInheritanceChainInspection */
class QueueController extends BaseQueueController
{
    const KEY_CATEGORY      = 'categories';
    const KEY_OPTIONS       = 'options';
    const KEY_OPTIONS_VALUES = 'options_values';
    const KEY_GOODS         = 'goods';
    const KEY_SELLERS       = 'sellers';
    const KEY_GROUPS        = 'groups';
    const KEY_PRODUCERS     = 'producers';
    const KEY_SERIES        = 'series';

    const ENTITY_IDENTITY   = 'id';
    const ENTITY_EVENT      = 'action';

    const ACTION_UPDATE     = 'update';
    const ACTION_INSERT     = 'insert';
    const ACTION_DELETE     = 'delete';
    const ACTION_PATCH      = 'patch';

    /** @var  string List by include list keys to rout queue as Entity name, example `options,categories, ...` */
    public $bKeys;

    /** @var array List binding keys like as entity store events */
    public $bindingKeys = [
        self::KEY_CATEGORY, self::KEY_GOODS,
        self::KEY_GROUPS,   self::KEY_OPTIONS,
        self::KEY_PRODUCERS, self::KEY_OPTIONS_VALUES,
        self::KEY_SELLERS, self::KEY_SERIES
    ];

    /** @var array Other options */
    private static $options = [
        'bKeys', 'help'
    ];

    /** @var array Required fields into body message from queue */
    private static $requireFieldsBody = [self::ENTITY_IDENTITY, self::ENTITY_EVENT];

    /**
     * Options controller
     * @param string $actionID
     * @return array
     */
    public function options($actionID) {
        return ArrayHelper::merge(parent::options($actionID), self::$options);
    }

    /**
     * @param AMQPMessage $oJobMessage
     * @return bool
     * @throws NotSupportedException
     * @SuppressWarnings(PHPMD.UnusedFormalParameter)
     */
    public function doJob($oJobMessage)
    {
        $result = null;
        //$this->stdout($oJobMessage->body . PHP_EOL);
        $aBodyMessage = $this->decodeMessage($oJobMessage);

        foreach ($this->bindingKeys as $sJobName) {
            /** @var ActiveQuery $oCustomAQStore */
            list ($oCustomAQStore,) = FactoryMapModel::create($sJobName); // get model to map process
            try {
                Yii::info(['body-message' => $aBodyMessage, 'data' => [/*@todo add attributes model*/]], sprintf(self::LOG_CATEGORY_NAME, $sJobName));
                $result = $this->triggerCustom($oCustomAQStore, $aBodyMessage, $sJobName);
                break; // if result is it
            } catch (\DomainException $domainException){
                continue; // if binding key more one
            } catch (\LogicException $logicException){
                Yii::warning([
                    'Error model: ' . json_encode($aBodyMessage, JSON_UNESCAPED_UNICODE),
                    json_encode(['Error Message' => $logicException->getMessage()], JSON_UNESCAPED_UNICODE)
                ], sprintf(self::LOG_CATEGORY_NAME, $sJobName));
            }
        }

        if ($result !== null && $result === true) {
            $this->ask($oJobMessage);
        } else {
            $this->nAsk($oJobMessage);
        }

        return $result;
    }

    /**
     * Subscribe to queue `some_queue`
     * @return array|bool
     * @throws \yii\base\InvalidParamException
     * @throws \PhpAmqpLib\Exception\AMQPOutOfBoundsException
     */
    protected function defineJobs()
    {
        $this->getBindingKey();
        //$this->stdout(json_encode($this->bindingKeys));
        /** @var AMQPChannel $channel */
        $channel = $this->getQueue();
        while (count($channel->callbacks) && !self::$stopFlag) {
            try {
                $channel->wait(null, true, parent::TIMEOUT_CHANEL);
                pcntl_signal_dispatch(); // врежемся в цыкл из процесса, для остановки итерации
            } catch (AMQPTimeoutException $timeout) {
                \Yii::error($timeout->getMessage());
                pcntl_signal_dispatch();
            } catch (AMQPRuntimeException $runtime) {
                \Yii::error($runtime->getMessage());
                $this->stderr(sprintf('Runtime: %s%s', $runtime->getMessage(), "\n"));
                $this->stdout(sprintf('Stopping channel and connection ..%s', "\n"));
                $this->channel->close();
                $this->connection->close();
                pcntl_signal_dispatch();
            }
        }

        pcntl_signal_dispatch();

        $this->channel = null;
        $this->connection = null;
        return false;
    }

    /**
     * Trigger event like action request name
     * @param ActiveQuery   $oCustomAQStore
     * @param array         $aBodyMessage
     * @param string        $sJobName
     * @return bool
     */
    private function triggerCustom(ActiveQuery $oCustomAQStore, array $aBodyMessage, $sJobName) {
        $aCustomAQ = $oCustomAQStore->where(['id' => $aBodyMessage[self::ENTITY_IDENTITY]])->one();

        if ($aCustomAQ === null){
            throw new \DomainException('VO210617-05 [Entity not found]');
        }

        //echo "\n" . print_r([$aCustomAQ, $aBodyMessage], true);
        $oEntityFactory = new FactoryIncyModel();
        $oEntity = $oEntityFactory->createModel($sJobName);
        $oEntity->allowNotSafeOperation();

        try {
            /** @var ActiveRecord $oModel */
            $oModel = $oEntity::findOne($aBodyMessage[self::ENTITY_IDENTITY]);
        } catch (\Exception $exception) {
            Yii::warning([
                'Wrong find model: ' . json_encode($aBodyMessage, JSON_UNESCAPED_UNICODE),
                json_encode(['Error Message' => $exception->getMessage()], JSON_UNESCAPED_UNICODE)
            ], sprintf(self::LOG_CATEGORY_NAME, $sJobName));
            $oModel = null;
        }

        if (null !== $oModel) {
            switch ($aBodyMessage[self::ENTITY_EVENT]) {
                case self::ACTION_UPDATE:
                    $oModel->load([$sJobName => $aCustomAQ], $sJobName);

                    return $oModel->save(false);
                    break;

                case self::ACTION_DELETE:
                    return (bool)$oModel->delete();
                    break;

                case self::ACTION_PATCH:
                    throw new \LogicException('VO210617-02 [Not implemented yet]');
                    break;

                case self::ACTION_INSERT:
                    throw new \LogicException('VO210617-03 [Record exist now. Check request type]');
                    break;
            }
        } else {
            if ($aBodyMessage[self::ENTITY_EVENT] === self::ACTION_INSERT) {
                $oEntity = $oEntityFactory->createModel($sJobName);
                $oEntity->load([$sJobName => $aCustomAQ], $sJobName);

                return $oEntity->save(false);
            } else {
                throw new \DomainException('VO210617-04 [Entity not found]');
            }
        }

        return false;
    }


    /**
     * Check message in correct
     * @param AMQPMessage $oJobMessage
     * @return array
     */
    private function decodeMessage(AMQPMessage $oJobMessage) {
        $aBodyMessage = Json::decode($oJobMessage->body, true);
        $aKeysNeed = array_filter(array_keys($aBodyMessage), function ($item) use ($aBodyMessage){ return in_array($item, self::$requireFieldsBody); });

        if (count($aKeysNeed) === 0 || count($aKeysNeed) !== count(self::$requireFieldsBody)) {
            throw new \InvalidArgumentException('VO210617-01 [Message queue incorrect]');
        }

        return $aBodyMessage;
    }

    /**
     * Get bind keys
     */
    private function getBindingKey(){
        if ($this->bKeys !== null) {
            $list = $this->bindingKeys;
            $this->bindingKeys = [];
            $bindingKeys = $this->getBindingKeysArgument();

            foreach ($bindingKeys as $bindingKey){
                if (in_array($bindingKey, $list)) {
                    $this->bindingKeys[] = $bindingKey;
                }
            }
        }
    }

    /**
     * Return include bind keys list from console
     * @return array
     */
    private function getBindingKeysArgument() {
        $bindingKeys = explode(',', $this->bKeys);
        array_map(function ($item) {
            return trim($item);
        }, $bindingKeys);

        return $bindingKeys;
    }
}