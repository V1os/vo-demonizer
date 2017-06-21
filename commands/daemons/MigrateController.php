<?php

namespace app\commands\daemons;

use app\extend\daemon\DaemonController;
use app\extend\Mapper;
use app\models\incy\handler\FactoryIncyModel;
use app\models\store\handler\FactoryMapModel;
use yii\db\ActiveQuery;
use yii\db\Connection;
use yii\db\Exception;
use yii\helpers\ArrayHelper;
use yii\helpers\Console;
use \Yii;

/** @noinspection MissingPropertyAnnotationsInspection */
class MigrateController extends DaemonController
{
    const BATCH_OF_PIECES       = 100;      // number of record in batch
    const BATCH_TIMEOUT_BETWEEN = 200;      // ms timeout between batch process

    const STRICT_MODE_SAFE      = true;     // if true then halt process by error code witch AR throw
    const VALIDATION_BEFORE_SAVE = false;   // validator save function BaseActiveRecord

    const CACHE_KEY_LIST_JOB    = 'listJobs';

    const SIGN_MEGA_BYTE        = 1048576;
    const SIGN_BYTE             = 8388608;
    const SIGN_MAX_TO_BATH_LOOP = 200000;   // The maximum number of records for loop type `batch`,
                                            // For more, it is bad, it is better to use `for`
    const LOOP_TYPE_FOR         = 'for';
    const LOOP_TYPE_BATCH       = 'batch';

    const LOG_CATEGORY_NAME     = 'migrate_%s';

    /** @var bool find for duplicates */
    public static $bFFD = true;
    /** @var  string List by exclude list jobs, example `options,categories` */
    public $exclude;
    /** @var  boolean Verbose each model operations */
    public $verbose;
    /**
     * @var  string $loop `for`|`batch`
     * @default 'batch'
     */
    public $loop;
    /** @var int Memory MORE - 100Mb */
    protected $memoryLimit = 838860800;
    /** @var string */
    private static $sLoop;
    /** @var array List jobs */
    private static $listJobs = [
        FactoryMapModel::MAP_RULE_NAME_CATEGORIES     => null, FactoryMapModel::MAP_RULE_NAME_OPTIONS => null,
        FactoryMapModel::MAP_RULE_NAME_OPTIONS_VALUES => null, FactoryMapModel::MAP_RULE_NAME_SELLERS => null,
        FactoryMapModel::MAP_RULE_NAME_GROUPS         => null, FactoryMapModel::MAP_RULE_NAME_GOODS => null,
    ];
    /** @var array List composite keys for validation record by extends */
    private static $validatorCompositeKeys = [
        FactoryMapModel::MAP_RULE_NAME_CATEGORIES     => ['id'], FactoryMapModel::MAP_RULE_NAME_OPTIONS => ['id'],
        FactoryMapModel::MAP_RULE_NAME_OPTIONS_VALUES => ['id'], FactoryIncyModel::MODEL_OPTIONS_SETTINGS => ['id'],
        FactoryMapModel::MAP_RULE_NAME_SELLERS        => ['id'], FactoryMapModel::MAP_RULE_NAME_GROUPS => ['id'],
        FactoryMapModel::MAP_RULE_NAME_GOODS          => ['id']
    ];
    /** @var  int Count not safe record */
    private $iSkipped;
    /** @var array List options self controller */
    private static $options = [
        'exclude', 'verbose', 'loop', 'help',
    ];

    /**
     * Options controller
     * @param string $actionID
     * @return array
     */
    public function options($actionID) {
        return ArrayHelper::merge(parent::options($actionID), self::$options);
    }

    /**
     * Process job self queue
     * @param string $sJobName
     * @return boolean
     * @throws \LogicException
     * @throws \yii\base\InvalidConfigException
     * @throws \yii\base\ExitException
     * @throws \InvalidArgumentException
     */
    protected function doJob($sJobName) {
        try {
            $this->registrationJob($sJobName);
            pcntl_signal_dispatch();

            /**
             * @var ActiveQuery $oCustomAQStore
             * @var Connection  $unbufferedConnect
             */
            list ($oCustomAQStore, $unbufferedConnect) = FactoryMapModel::create($sJobName); // get model to map process

            $iCount             = $oCustomAQStore->count(); // all records
            $iIterate           = ceil($iCount / self::BATCH_OF_PIECES); // count iterate loop by butch
            $this->iSkipped     = $iCurrentIterate = 0;
            $this->typeLoop($iCount);

            $message = sprintf('%s Migrate `%s`[%s records] %s of %s records',
                self::time(), $sJobName, $iCount, (self::$sLoop === self::LOOP_TYPE_FOR ? 'chunk' : 'batch'),
                self::BATCH_OF_PIECES);

            $this->stdout("\n");
            Yii::info($message, sprintf(self::LOG_CATEGORY_NAME, $sJobName));
            Console::startProgress($iCurrentIterate, 100, $message, Console::getScreenSize(true)[0]/2); // start on 0

            ///// test multi start
            /*$cache = Yii::$app->cache;
            $listJobs = $cache->get(self::CACHE_KEY_LIST_JOB);
            //$this->stdout(json_encode($listJobs) . PHP_EOL. PHP_EOL);
            Console::updateProgress(mt_rand(1, 20), 100);
            sleep(1);
            Console::updateProgress(mt_rand(20, 80), 100);
            sleep(1);
            Console::updateProgress(mt_rand(80, 100), 100);
            sleep(mt_rand(1, 3));
            $message = sprintf('%s #%s%s `%s` migrated %s[%s skipped][%sMb memory usage]',
                 self::time(), $iCount - $this->iSkipped, "\t", $sJobName,"\t", $this->iSkipped, $this->getMemory());
            Console::endProgress($message, false);
            $this->flushJob($sJobName); // finish executed job

            return true;*/
            ///// test end

            switch (self::$sLoop) {
                case self::LOOP_TYPE_FOR:
                    $oCustomAQStore->limit(self::BATCH_OF_PIECES);
                    for ($i = 0; $i < $iIterate; $i++) {
                        $offset = $i * self::BATCH_OF_PIECES;
                        $oCustomAQStore->offset($offset);
                        /** @var array $aChunks */
                        $aChunks = $oCustomAQStore->all($unbufferedConnect);
                        if (!self::$stopFlag) {
                            $this->mappedData($aChunks, $sJobName);
                        } else {
                            $this->stdout(PHP_EOL . 'Stop!!');
                            break;
                        }
                        usleep(self::BATCH_TIMEOUT_BETWEEN);
                        pcntl_signal_dispatch(); // врежемся в цыкл из процесса, для остановки итерации
                        $iCurrentIterate++;
                        Console::updateProgress(ceil(($iCurrentIterate * 100) / $iIterate), 100);
                        unset($aChunks);
                    }
                    break;
                case self::LOOP_TYPE_BATCH:
                    /** @var array $aCustomAQStore */
                    foreach ($oCustomAQStore->batch(self::BATCH_OF_PIECES, $unbufferedConnect) as $aCustomAQStore) {
                        // process first butch data
                        if (!self::$stopFlag) {
                            $this->mappedData($aCustomAQStore, $sJobName);
                        } else {
                            $this->stdout(PHP_EOL . 'Stop!!');
                            break;
                        }
                        usleep(self::BATCH_TIMEOUT_BETWEEN);
                        pcntl_signal_dispatch(); // врежемся в цыкл из процесса, для остановки итерации
                        $iCurrentIterate++;
                        Console::updateProgress(ceil(($iCurrentIterate * 100) / $iIterate), 100);
                    }
                    break;
                default:
                    $this->stdout(sprintf('%s!!! Loop type %s not implemented', "\n", self::$sLoop) . PHP_EOL);
                    break;
            }
            $unbufferedConnect->close();
            pcntl_signal_dispatch();

            $message = sprintf('%s #%s%s `%s` migrated %s[%s skipped][%sMb memory usage]',
                self::time(), $iCount - $this->iSkipped, "\t", $sJobName,"\t", $this->iSkipped, $this->getMemory());
            Console::endProgress($message, false);
            Yii::info($message, sprintf(self::LOG_CATEGORY_NAME, $sJobName));
            $this->flushJob($sJobName); // finish executed job

            return true;
        } catch (\LogicException $logicException){
            Yii::trace($logicException->getMessage(), sprintf(self::LOG_CATEGORY_NAME, $sJobName));

            return true;
        }
    }

    /**
     * List jobs taking into those busy in child process
     * @return array
     * @throws \yii\base\ExitException
     * @throws \yii\base\InvalidConfigException
     */
    protected function defineJobs() {
        $cache = Yii::$app->get('cache');
        $listJobs = $cache->getOrSet(self::CACHE_KEY_LIST_JOB, function () {
            $list = array_diff_key(self::$listJobs, $this->getExclude());
            return array_filter($list, function ($item){
                return $item === null;
            });
        });

        if (count($listJobs) === 0) {
            $this->stdout(sprintf('%s%s Empty job, synchronous output [PID%d]!', "\n", self::time(), $this->parentPID) . PHP_EOL);
            $this->halt(self::EXIT_CODE_NORMAL);
        }

        return array_keys($listJobs);
    }

    /**
     * Registration job to manager
     * @param $sJobName string
     * @throws \yii\base\InvalidConfigException
     * @throws \LogicException
     */
    private function registrationJob($sJobName) {
        $cache = Yii::$app->get('cache');
        $listJobs = $cache->get(self::CACHE_KEY_LIST_JOB);

        if (!array_key_exists($sJobName, $listJobs) // if not exist then was processed
            || null !== $listJobs[$sJobName])       // if not null then in progress
        {
            throw new \LogicException('VO15062017-00 [Task is performed by another process]');
        }

        $aJobByPID = array_flip(self::$currentJobs);
        $listJobs[$sJobName] = array_key_exists($sJobName, $aJobByPID) ? $aJobByPID[$sJobName] : $this->parentPID;
        $cache->set(self::CACHE_KEY_LIST_JOB, $listJobs);
    }

    /**
     * Remove job before process
     * @param string $sJobName
     * @throws \yii\base\ExitException
     * @throws \yii\base\InvalidConfigException
     */
    private function flushJob($sJobName) {
        $cache = Yii::$app->get('cache');
        $listJobs = $cache->get(self::CACHE_KEY_LIST_JOB);
        $killPID = $listJobs[$sJobName];

        unset($listJobs[$sJobName]);
        $cache->set(self::CACHE_KEY_LIST_JOB, $listJobs);
        $this->typeLoop(-1);

        if (count($listJobs) === 0) {
            $this->stdout(sprintf('%s%s Finished [PID%d]!', "\n", self::time(), $killPID) . PHP_EOL);
            Yii::trace(sprintf('%s Finished [PID%d]!', self::time(), $killPID), sprintf(self::LOG_CATEGORY_NAME, $sJobName));
            //exit code if all job is executed
            $this->halt(self::EXIT_CODE_NORMAL);
        }
    }

    /**
     * Iterate chunk data
     * @param array  $aChunkData
     * @param string $sJobName
     * @throws \InvalidArgumentException
     * @throws \yii\base\ExitException
     */
    private function mappedData(array $aChunkData, $sJobName) {
        foreach ($aChunkData as $aData) {
            $aMappedList = Mapper::run($aData, $sJobName);
            $this->setMappedData($aMappedList, $sJobName);
            pcntl_signal_dispatch();
        }
    }

    /**
     * Put data
     * @param array  $aMappedList
     * @param string $sNameJob
     * @throws \yii\base\ExitException
     * @throws \InvalidArgumentException
     */
    private function setMappedData(array $aMappedList, $sNameJob) {
        foreach ($aMappedList as $sEntity => $aDataEntity) {
            $oEntityFactory = new FactoryIncyModel();
            $oEntity = $oEntityFactory->createModel($sEntity);
            $oEntity->allowNotSafeOperation();
            try {
                if (self::$bFFD) {
                    $fConditions = function ($sEntity, $aDataEntity) {
                        $aConditions = [];
                        if (array_key_exists($sEntity, self::$validatorCompositeKeys)) {
                            foreach ((array)self::$validatorCompositeKeys[$sEntity] as $ik) {
                                $aConditions[$ik] = array_key_exists($ik, $aDataEntity) ? $aDataEntity[$ik] : null;
                            }
                        }

                        return array_filter($aConditions, function ($var) {
                            return $var !== null;
                        });
                    };
                    $aConditions = $fConditions($sEntity, $aDataEntity);
                    if (count($aConditions) > 0) {
                        try {
                            $oModel = $oEntity::findOne($aConditions);
                        } catch (Exception $exception) {
                            Yii::warning([
                                'Wrong find model: ' . json_encode($aConditions, JSON_UNESCAPED_UNICODE),
                                json_encode(['Error Message' => $exception->getMessage()], JSON_UNESCAPED_UNICODE)
                            ], sprintf(self::LOG_CATEGORY_NAME, $sEntity));
                            $oModel = null;
                        }
                        if (null !== $oModel) {
                            if ($sNameJob === $sEntity) {
                                $this->iSkipped++;
                            }
                            continue;
                        }
                    }
                }
                if (!$oEntity->load([$sEntity => $aDataEntity], $sEntity)) {
                    if ($sNameJob === $sEntity) {
                        $this->iSkipped++;
                    }
                    Yii::warning('Wrong load model: ' . json_encode($oEntity->errors, JSON_UNESCAPED_UNICODE),
                        sprintf(self::LOG_CATEGORY_NAME, $sEntity));
                    continue;
                }
                if (!$oEntity->save(self::VALIDATION_BEFORE_SAVE)) {
                    if ($sNameJob === $sEntity) {
                        $this->iSkipped++;
                    }
                    Yii::warning([
                        'Wrong save model: ' . json_encode($oEntity->errors, JSON_UNESCAPED_UNICODE),
                        json_encode(['DATASET' => [$sEntity => $aDataEntity]], JSON_UNESCAPED_UNICODE)
                    ], sprintf(self::LOG_CATEGORY_NAME, $sEntity));
                    continue;
                }
                if ($this->verbose !== null) {
                    $this->stdout(sprintf('%s%s %s DATA: %s',
                        "\n", self::time(), $sEntity, json_encode($aDataEntity, JSON_UNESCAPED_UNICODE)));
                }
            } catch (\Exception $exception) {
                $this->stdout("\n" . $exception->getMessage() . $exception->getTraceAsString());
                if ($sNameJob === $sEntity) {
                    $this->iSkipped++;
                }
                Yii::error('Model exception: ' . $exception->getMessage(), sprintf(self::LOG_CATEGORY_NAME, $sEntity));
                if (self::STRICT_MODE_SAFE) {
                    $this->halt(self::EXIT_CODE_ERROR); //exit code if all job is executed
                }
            }
        }
    }

    /**
     * Memory usage
     * @return float
     */
    private function getMemory() {
        return round(memory_get_usage() / self::SIGN_MEGA_BYTE, 3);
    }

    /**
     * Return exclude list from console
     * @return array
     */
    private function getExclude() {
        if (null !== $this->exclude) {
            $exclude = explode(',', $this->exclude);

            return array_combine(
                array_map(function ($item) {
                    return trim($item);
                }, $exclude),
                array_fill(0, substr_count($this->exclude, ',') + 1, null)
            );
        }

        return [];
    }

    /**
     * Return time by format
     * @param int $iTime
     * @return string
     */
    private static function time($iTime = null) {
        $iTime = $iTime ? : microtime(true);

        return sprintf('[%s] ', date('H:i:s', $iTime));
    }

    /**
     * Check count records in order to recheck type loop processed
     * @param int $iCountRecords
     */
    private function typeLoop($iCountRecords) {
        self::$sLoop = $this->loop;

        if ($iCountRecords === -1) {
            self::$sLoop = null;
        } else {
            if ($this->loop === null) {
                if ($iCountRecords > self::SIGN_MAX_TO_BATH_LOOP) {
                    self::$sLoop = self::LOOP_TYPE_FOR;
                } else {
                    self::$sLoop = self::LOOP_TYPE_BATCH;
                }
            }
        }
    }
}