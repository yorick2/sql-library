<?php

namespace PaulMillband\SqlLibrary\tests\TestLibrary;

use mysqli;
use PaulMillband\SqlLibrary\tests\Helper\CsvFileDataHelper;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseSqlHelper;
use PaulMillband\SqlLibrary\tests\Helper\DataHelper;
use PaulMillband\SqlLibrary\tests\TestCase;

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
        $databaseData=DatabaseSqlHelper::getInstance()->getConnection()
            ->query('SELECT * FROM `'.$tableName.'`')
            ->fetch_all(MYSQLI_ASSOC);
        $csvData=CsvFileDataHelper::getDataFromCsv($localFilePath, $header, $separator);
        $this->assertEquals(
            count($csvData),
            count($databaseData),
            "csv and database have differing number of rows"
        );
        $missing=DataHelper::getDataMissing(
            $csvData,
            $databaseData,
            false
        );
        $this->assertEquals(
            0,
            count($missing),
            "Database table `$tableName` didnt match csv data"
        );
        return $this;
    }

    /**
     * @param string $query
     * @param string $localFilePath
     * @return DataTestLibrary
     */
    public function compareSqlQueryToCsvData(string $query, string $localFilePath, array $header=[], string $separator=','): DataTestLibrary
    {
        $databaseData=DatabaseSqlHelper::getInstance()->getConnection()
            ->query($query)
            ->fetch_all(MYSQLI_ASSOC);
        $missing=DataHelper::getDataMissing(
            CsvFileDataHelper::getDataFromCsv($localFilePath, $header, $separator),
            $databaseData,
            false
        );
        $this->assertEquals(
            0,
            count($missing),
            "Database query didnt match csv data\n\nquery:\n$query"
        );
        return $this;
    }
}