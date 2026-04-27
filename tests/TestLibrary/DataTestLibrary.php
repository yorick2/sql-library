<?php

namespace PaulMillband\SqlLibrary\tests\TestLibrary;

use mysqli;
use PaulMillband\SqlLibrary\tests\Helper\CsvFileDataHelper;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseSqlHelper;
use PaulMillband\SqlLibrary\tests\Helper\DataHelper;
use PHPUnit\Framework\TestCase;

class DataTestLibrary extends TestCase
{
    protected mysqli $db;

    public function setUp(): void
    {
        parent::setUp();
        $this->db = DatabaseSqlHelper::getInstance()->getConnection();
    }

    /**
     * @param string $tableName
     * @param string $localFilePath
     * @return DataTestLibrary
     */
    public function compareTableToCsvData(string $tableName, string $localFilePath, array $header=[], string $separator=','): DataTestLibrary
    {
        $data=DatabaseSqlHelper::getInstance()->getConnection()
            ->query('SELECT * FROM `'.$tableName.'`')
            ->fetch_all(MYSQLI_ASSOC);
        $missing=DataHelper::getDataMissing(
            CsvFileDataHelper::getDataFromCsv($localFilePath, $header, $separator),
            $data,
            false
        );
        $this->assertEquals(
            0,
            count($missing),
            'Compare database table to csv data`'.$tableName.'`'
        );
        return $this;
    }
}