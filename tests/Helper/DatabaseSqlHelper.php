<?php

namespace PaulMillband\SqlLibrary\tests\Helper;

use mysqli;

class DatabaseSqlHelper
{
    private static $instance;
    private $connection;

    protected function __clone()
    {
    }

    /**
     * !!*WARNING:: keep protected as this class is a singleton*!!
     */
    protected function __construct()
    {
        if (!function_exists('mysqli_connect')) {
            die( "\nEnable Mysqli support in your PHP installation\n");
        }
        $env = parse_ini_file(__DIR__.'/../../.env');
        $this->connection = new mysqli($env['DB_HOST'], $env['DB_USERNAME'], $env['DB_PASSWORD'], $env['DB_DATABASE']);
    }

    public static function getInstance()
    {
        if (!isset(self::$instance)) {
            self::$instance = new self;
        }

        return self::$instance;
    }

    /**
     * @return mysqli
     */
    public function getConnection() :mysqli
    {
        return $this->connection;
    }
}