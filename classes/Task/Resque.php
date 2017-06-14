<?php defined('SYSPATH') or die('No direct script access.');

class Task_Resque extends Minion_Task {

    protected $queues;
    protected $interval;
    protected $logLevel;

    protected $_options = array(
        'queue' => 'minion',
        'logging' => Resque_Worker::LOG_NONE
    );

    public function _execute(array $params)
    {
        $this->queues = explode(',', $params['queue']);
        $this->logLevel = $params['logging'];

        $redisBackend = getenv('REDIS_BACKEND');
        if (!empty($redisBackend)) {
            Resque::setBackend($redisBackend);
        }

        $this->interval = max(1, (int) getenv('INTERVAL'));
        $count          = max(1, (int) getenv('COUNT'));

        if ($count > 1) {

            for ($i = 0; $i < $count; ++$i) {
                $pid = pcntl_fork();
                if ($pid == -1) {
                    die("Could not fork worker $i\n");
                }

                if (!$pid) {
                    // Child, start the worker
                    $this->work();
                    break;
                }
            }
        } else {

            $pidFile = getenv('PIDFILE');
            if ($pidFile) {
                file_put_contents($pidFile, getmypid()) or die("Could not write PID information to $pidFile");
            }

            // Start a single worker
            $this->work();
        }
    }


    protected function work()
    {
        $worker = new Resque_Worker($this->queues);
        $worker->logLevel = $this->logLevel;
        fwrite(STDOUT, "*** Starting worker $worker\n");
        $worker->work($this->interval);
    }
}
