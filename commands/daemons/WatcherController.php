<?php
namespace app\commands\daemons;

use app\extend\daemon\controllers\WatcherDaemonController;

/**
 * @property array $daemonsList
 */
class WatcherController extends WatcherDaemonController
{
    protected $sleep = 10;

    /**
     * @return array
     */
    protected function getDaemonsList()
    {
        return [
            ['daemon' => 'daemons/migrate', 'enabled' => true]
        ];
    }
}