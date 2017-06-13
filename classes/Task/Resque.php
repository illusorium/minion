<?php defined('SYSPATH') or die('No direct script access.');

class Task_Resque extends Minion_Task {

    protected $_options = array(
        'queue' => 'minion',
        'logging' => Resque_Worker::LOG_NONE
    );

    public function _execute(array $params)
    {
        $queue = $params['queue'];
        $logLevel = $params['logging'];

        $redisBackend = getenv('REDIS_BACKEND');
        if (!empty($redisBackend)) {
            Resque::setBackend($redisBackend);
        }

        $interval = max(1, (int) getenv('INTERVAL'));
        $count    = max(1, (int) getenv('COUNT'));

        if ($count > 1) {
            for ($i = 0; $i < $count; ++$i) {
                $pid = pcntl_fork();
                if ($pid == -1) {
                    die("Could not fork worker $i\n");
                }

                if (!$pid) {
                    // Child, start the worker
                    $queues = explode(',', $queue);
                    $worker = new Resque_Worker($queues);
                    $worker->logLevel = $logLevel;
                    fwrite(STDOUT, "*** Starting worker $worker\n");
                    $worker->work($interval);
                    break;
                }
            }
        } else {

            // Start a single worker
            $queues = explode(',', $queue);
            $worker = new Resque_Worker($queues);
            $worker->logLevel = $logLevel;

            $pidFile = getenv('PIDFILE');
            if ($pidFile) {
                file_put_contents($pidFile, getmypid()) or die("Could not write PID information to $pidFile");
            }

            fwrite(STDOUT, "*** Starting worker $worker\n");
            $worker->work($interval);
        }
    }
}
