<?php defined('SYSPATH') or die('No direct script access.');
/**
 * Interface that all minion tasks must implement
 * Implemented queueing minion tasks via php-resque
 */
abstract class Kohana_Minion_Task {

    /**
     * If a task called with this parameter, it will be queued (instead of being executed immediately)
     *
     * php index.php --task=task:name --resque
     */
    protected static $queuingParameter = 'resque';


    /**
     * Whether to allow passing custom queue name under $this->customQueueParameter ("--queue") parameter
     *
     * php index.php --task=task:name --resque --queue=customQueue
     */
    protected $allowCustomQueues = false;


    /**
     * Name of parameter containing custom queue name
     * !!! Works only if $this->allowCustomQueues === true
     *
     * Example:
     *     $this->customQueueParameter = 'my-custom-queue',
     * Then if call
     *     php index.php --task=task:name --resque --my-custom-queue=customQ,
     * then task will be added to customQ queue
     */
    protected $customQueueParameter = 'queue';


    /**
     * Default queue name
     */
    protected $queueName = 'minion';


    /**
     * Job id in php-resque
     */
    protected $token;


    /**
     * Parameters for saving job output
     */
    protected $outputFileParameter = 'save';
    protected $outputFile;

	 /**
	 * The separator used to separate different levels of tasks
	 * @var string
	 */
	public static $task_separator = ':';

	/**
	 * Converts a task (e.g. db:migrate to a class name)
	 *
	 * @param string $task Task name
	 * @return string Class name
	 */
	public static function convert_task_to_class_name($task)
	{
		$task = trim($task);

		if (empty($task))
			return '';

		return 'Task_'.implode('_', array_map('ucfirst', explode(Minion_Task::$task_separator, $task)));
	}

	/**
	 * Gets the task name of a task class / task object
	 *
	 * @param  string|Minion_Task The task class / object
	 * @return string             The task name
	 */
	public static function convert_class_to_task($class)
	{
		if (is_object($class))
		{
			$class = get_class($class);
		}

		return strtolower(str_replace('_', Minion_Task::$task_separator, substr($class, 5)));
	}

	/**
	 * Factory for loading minion tasks
	 *
	 * @param  array $options An array of command line options. It should contain the 'task' key
	 * @throws Minion_Exception_InvalidTask
	 * @return Minion_Task The Minion task
	 */
	public static function factory($options)
	{
		if (($task = Arr::get($options, 'task')) !== NULL)
		{
			unset($options['task']);
		}
		else if (($task = Arr::get($options, 0)) !== NULL)
		{
			// The first positional argument (aka 0) may be the task name
			unset($options[0]);
		}
		else
		{
			// If we didn't get a valid task, generate the help
			$task = 'help';
		}

		$class = Minion_Task::convert_task_to_class_name($task);

		if ( ! class_exists($class))
		{
			throw new Minion_Exception_InvalidTask(
				"Task ':task' is not a valid minion task",
				array(':task' => $class)
			);
		}

		$class = new $class;

		if ( ! $class instanceof Minion_Task)
		{
			throw new Minion_Exception_InvalidTask(
				"Task ':task' is not a valid minion task",
				array(':task' => $class)
			);
		}

		$class->set_options($options);

        if (get_class($class) === 'Task_Resque')
        {
            /**
             * If there are any DB queries in application bootstrap file, there's need to reset existing DB connections
             * in order to prevent ErrorException "Error while sending QUERY packet".
             *
             * @see https://github.com/chrisboulton/php-resque/issues/269
             */
            if (array_key_exists('database', Kohana::modules())) {
                foreach (Database::$instances as $k => $v) {
                    unset(Database::$instances[$k]);
                }
            }
        } else {
            // check if the task has parameters with same names as queue options
            if (array_key_exists(self::$queuingParameter, $class->_options))
            {
                if (in_array(self::$queuingParameter, $class->get_accepted_options()))
                {
                    throw new Minion_Exception_InvalidTask(
                        'Queue parameter :param is used in task options',
                        array(':param' => self::$queuingParameter)
                    );
                }

                if ($class->allowCustomQueues AND in_array($class->customQueueParameter, $class->get_accepted_options()))
                {
                    throw new Minion_Exception_InvalidTask(
                        'Queue parameter :param is used in task options',
                        array(':param' => $class->customQueueParameter)
                    );
                }

                if (in_array($class->outputFileParameter, $class->get_accepted_options()))
                {
                    throw new Minion_Exception_InvalidTask(
                        'Parameter :param is used in task options',
                        array(':param' => $class->outputFileParameter)
                    );
                }
            }
        }

        // Show the help page for this task if requested
		if (array_key_exists('help', $options))
		{
			$class->_method = '_help';
		}

        return $class;
	}

	/**
	 * The list of options this task accepts and their default values.
	 *
	 *     protected $_options = array(
	 *         'limit' => 4,
	 *         'table' => NULL,
	 *     );
	 *
	 * @var array
	 */
	protected $_options = array();

	/**
	 * Populated with the accepted options for this task.
	 * This array is automatically populated based on $_options.
	 *
	 * @var array
	 */
	protected $_accepted_options = array();

	protected $_method = '_execute';

	// TODO declared __construct() public to prevent PHP Fatal error during job executing:
	// Call to protected Kohana_Minion_Task::__construct() from context "Resque_Job";
	public function __construct()
	{
		// Populate $_accepted_options based on keys from $_options
		$this->_accepted_options = array_keys($this->_options);
    }

	/**
	 * The file that get's passes to Validation::errors() when validation fails
	 * @var string|NULL
	 */
	protected $_errors_file = 'validation';

	/**
	 * Gets the task name for the task
	 *
	 * @return string
	 */
	public function __toString()
	{
		static $task_name = NULL;

		if ($task_name === NULL)
		{
			$task_name = Minion_Task::convert_class_to_task($this);
		}

		return $task_name;
	}

	/**
	 * Sets options for this task
	 *
	 * $param  array  the array of options to set
	 * @return $this
	 */
	public function set_options(array $options)
	{
		foreach ($options as $key => $value)
		{
			$this->_options[$key] = $value;
		}

		return $this;
	}

	/**
	 * Get the options that were passed into this task with their defaults
	 *
	 * @return array
	 */
	public function get_options()
	{
		return (array) $this->_options;
	}

	/**
	 * Get a set of options that this task can accept
	 *
	 * @return array
	 */
	public function get_accepted_options()
	{
		return (array) $this->_accepted_options;
	}

	/**
	 * Adds any validation rules/labels for validating _options
	 *
	 *     public function build_validation(Validation $validation)
	 *     {
	 *         return parent::build_validation($validation)
	 *             ->rule('paramname', 'not_empty'); // Require this param
	 *     }
	 *
	 * @param  Validation $validation the validation object to add rules to
	 *
	 * @return Validation
	 */
	public function build_validation(Validation $validation)
	{
		// Add a rule to each key making sure it's in the task
		foreach ($validation->data() as $key => $value)
		{
			$validation->rule($key, array($this, 'valid_option'), array(':validation', ':field'));
		}

		return $validation;
	}

	/**
	 * Returns $_errors_file
	 *
	 * @return string
	 */
	public function get_errors_file()
	{
		return $this->_errors_file;
	}

	public function execute()
    {
        $class = get_class($this);
        if (
            !array_key_exists(self::$queuingParameter, $this->_options)
            OR $class == 'Task_Resque' // prevent worker process from being queued
        ) {

            // instantly execute Minion_Task (standard Kohana minion behaviour)
            $this->executeWithoutQueue();

        } else {

            // unset this option to prevent "is not a valid option for this task!" error
            // when worker will start executing the task
            unset($this->_options[self::$queuingParameter]);

            if ($this->allowCustomQueues) {

                if (array_key_exists($this->customQueueParameter, $this->_options)) {
                    $queue = $this->_options[$this->customQueueParameter];
                    if (!empty($queue)) {
                        $this->queueName = $queue;
                    }

                    // unset this option to prevent "is not a valid option for this task!" error
                    // when worker will start executing the task
                    unset($this->_options[$this->customQueueParameter]);
                }
            }

            if (array_key_exists($this->outputFileParameter, $this->_options)) {

                $saveOutput =
                    is_null($this->_options[$this->outputFileParameter])
                    || !empty($this->_options[$this->outputFileParameter]);

                if (!empty($saveOutput)) {
                    // create tmp file for job output
                    $this->outputFile = tempnam(sys_get_temp_dir(), uniqid('job_' . time() . '_'));
                    $this->set_options(array('outputFile' => $this->outputFile));
                }
                unset($this->_options[$this->outputFileParameter]);
            }

            try {
                // Queuing this task
                $this->token = Resque::enqueue($this->queueName, $class, $this->_options, true);

                $return = array(
                    'id' => $this->token
                );
                if (!empty($saveOutput)) {
                    $return['file'] = $this->outputFile;
                }

                if (PHP_SAPI == 'cli') {
                    Minion_CLI::write('Created job: ' . Minion_CLI::color($return['id'], 'green'));
                    if (!empty($return['file'])) {
                        Minion_CLI::write('Redirect output to file: ' . Minion_CLI::color($return['file'], 'green'));
                    }
                } else {
                    return $return;
                }

            } catch (Exception $e) {
                Minion_CLI::write("Error queueing $class: {$e->getMessage()}");
                Minion_CLI::write("Executing $class instantly");
                $this->executeWithoutQueue();
            }
        }
    }


	/**
	 * Execute the task with the specified set of options
	 *
	 * @return void
	 */
	public function executeWithoutQueue()
	{
		$options = $this->get_options();

		// Validate $options
		$validation = Validation::factory($options);
		$validation = $this->build_validation($validation);

		if ( $this->_method != '_help' AND ! $validation->check())
		{
			echo View::factory('minion/error/validation')
				->set('task', Minion_Task::convert_class_to_task($this))
				->set('errors', $validation->errors($this->get_errors_file()));
		}
		else
		{
			// Finally, run the task
			$method = $this->_method;
			echo $this->{$method}($options);
		}
	}

	abstract protected function _execute(array $params);

	/**
	 * Outputs help for this task
	 *
	 * @return void
	 */
	protected function _help(array $params)
	{
		$tasks = $this->_compile_task_list(Kohana::list_files('classes/task'));

		$inspector = new ReflectionClass($this);

		list($description, $tags) = $this->_parse_doccomment($inspector->getDocComment());

		$view = View::factory('minion/help/task')
			->set('description', $description)
			->set('tags', (array) $tags)
			->set('task', Minion_Task::convert_class_to_task($this));

		echo $view;
	}


	public function valid_option(Validation $validation, $option)
	{
		if ( ! in_array($option, $this->_accepted_options))
		{
			$validation->error($option, 'minion_option');
		}
	}

	/**
	 * Parses a doccomment, extracting both the comment and any tags associated
	 *
	 * Based on the code in Kodoc::parse()
	 *
	 * @param string $comment The comment to parse
	 * @return array First element is the comment, second is an array of tags
	 */
	protected function _parse_doccomment($comment)
	{
		// Normalize all new lines to \n
		$comment = str_replace(array("\r\n", "\n"), "\n", $comment);

		// Remove the phpdoc open/close tags and split
		$comment = array_slice(explode("\n", $comment), 1, -1);

		// Tag content
		$tags        = array();

		foreach ($comment as $i => $line)
		{
			// Remove all leading whitespace
			$line = preg_replace('/^\s*\* ?/m', '', $line);

			// Search this line for a tag
			if (preg_match('/^@(\S+)(?:\s*(.+))?$/', $line, $matches))
			{
				// This is a tag line
				unset($comment[$i]);

				$name = $matches[1];
				$text = isset($matches[2]) ? $matches[2] : '';

				$tags[$name] = $text;
			}
			else
			{
				$comment[$i] = (string) $line;
			}
		}

		$comment = trim(implode("\n", $comment));

		return array($comment, $tags);
	}

	/**
	 * Compiles a list of available tasks from a directory structure
	 *
	 * @param  array $files Directory structure of tasks
	 * @param  string $prefix
	 * @return array Compiled tasks
	 */
	protected function _compile_task_list(array $files, $prefix = '')
	{
		$output = array();

		foreach ($files as $file => $path)
		{
			$file = substr($file, strrpos($file, DIRECTORY_SEPARATOR) + 1);

			if (is_array($path) AND count($path))
			{
				$task = $this->_compile_task_list($path, $prefix.$file.Minion_Task::$task_separator);

				if ($task)
				{
					$output = array_merge($output, $task);
				}
			}
			else
			{
				$output[] = strtolower($prefix.substr($file, 0, -strlen(EXT)));
			}
		}

		return $output;
	}


    // method to be executed before perform()
    public function setUp()
    {
        /** @var Resque_Job $job */
        $job = $this->job;

        $this->token = $job->payload['id'];
        $args = $job->getArguments();

        if (array_key_exists('outputFile', $args)) {
            $this->outputFile = $args['outputFile'];
            unset($args['outputFile']);
        }

        $this->_options = $args + $this->_options;
    }


    // php-resque job classes must implement perform() method
    public function perform()
    {
        if (!empty($this->outputFile)) {
            // save job output to buffer
            Minion_CLI::write("Starting job " . Minion_CLI::color($this->token, 'yellow') . " with output buffering");
            Minion_CLI::$useEcho = true;
            ob_start();
        }

        $this->execute();

        if (!empty($this->outputFile)) {
            file_put_contents($this->outputFile, ob_get_clean());
            Minion_CLI::$useEcho = false;
            Minion_CLI::write("Finished job " . Minion_CLI::color($this->token, 'yellow'));
        }
    }


    // method to be executed after perform()
    public function tearDown()
    {
//        if (!empty($this->outputFile)) {
//            unlink($this->outputFile);
//        }
    }
}
