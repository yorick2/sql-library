<?php

namespace PaulMillband\SqlLibrary\tests\Importer;

use mysqli;
use PaulMillband\SqlLibrary\Importer\OneManyImporter;
use PaulMillband\SqlLibrary\Importer\SimpleImporter;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseHelper;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseSqlHelper;
use PaulMillband\SqlLibrary\tests\Helper\FileHelper;
use PaulMillband\SqlLibrary\tests\TestLibrary\DataTestLibrary;
use PaulMillband\SqlLibrary\tests\TestCase;

class OneManyImporterTest extends TestCase
{
    protected mysqli $db;
    protected string $file='/var/lib/mysql-files/one-many.csv';
    protected string $localFile='../mysql-files/one-many.csv';
    protected string $fileNewTable1='/var/lib/mysql-files/one-many-new-table-file1.csv';
    protected string $fileNewTable2='/var/lib/mysql-files/one-many-new-table-file2.csv';
    protected string $localFileNewTableDesired='../mysql-files/one-many-new-table-desired.csv';
    protected string $fileNewTable2Test2='/var/lib/mysql-files/one-many-new-table-file2-test2.csv';
    protected string $localFileNewTableDesiredTest2='../mysql-files/one-many-new-table-desired-test2.csv';

    protected function setUp(): void
    {
        parent::setUp();
        (new DatabaseHelper())->dropTables();
        $this->createTables();
        (new DatabaseHelper())->dropTables();
        $this->createTables();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        (new DatabaseHelper())->dropTables();
    }

    protected function createTables(): void
    {
        $this->db = DatabaseSqlHelper::getInstance()->getConnection();
        $query=<<<SQL
            CREATE TABLE `table1` (
                `id` int NOT NULL AUTO_INCREMENT,
                `text1` varchar(255) ,
                `text1b` varchar(255),
                PRIMARY KEY (id),
                UNIQUE (text1)
            );
            CREATE TABLE `table2` (
                `id` int NOT NULL AUTO_INCREMENT,
                `table1_id` int,
                `text2` varchar(255),
                `text2b` varchar(255),
                `text2c` varchar(255),
                PRIMARY KEY (id)
            );
SQL;
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        do {
        } while ($this->db->next_result());
        $this->assertEquals(
                2,
                $this->db->query('SHOW TABLES')->num_rows,
            "check number of tables in '".(new DatabaseHelper())->getDatabaseName()."' database'"
        );
    }

    public function test_getSimpleOneManySqlText_works(): void
    {
        $this->assertFilesExistLocally([__DIR__.'/'.$this->localFile]);
        $this->assertFilesExistOnSqlServer([$this->file]);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);
        $query=OneManyImporter::getSqlText(
            'table1',
            '`text1`,`text1b`',
            'new.`text1`,new.`text1b`',
            'table2',
            '`table1_id`,`text2`,`text2b`',
            'id,NEW.`text2`,NEW.`text2b`',
            'text1',
            '`text1` = VALUES(`text1`),`text1b` = VALUES(`text1b`)',
            $this->file,
            '`text1`,`text1b`,`text2`,`text2b`',
            1,
            ',',
            '',
            'temp'
        );
        error_reporting(E_ALL);
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result())
        {
            // dont delete while statement
        }
        $this->assertEquals(3, $this->db->query('SELECT * FROM `table1`')->num_rows);
        $this->assertEquals(6, $this->db->query('SELECT * FROM `table2`')->num_rows);

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData('table1', __DIR__.'/'.$this->localFile,[],',',false)
            ->compareTableToCsvData('table2', __DIR__.'/'.$this->localFile,[],',',false);
    }

    public function test_getSimpleOneManyAddNewTableSqlText_works(): void
    {
        $this->assertFilesExistLocally([
            __DIR__.'/'.$this->localFileNewTableDesired
        ]);
        $this->assertFilesExistOnSqlServer([
            $this->fileNewTable1,
            $this->fileNewTable2
            ]);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);
        $query=SimpleImporter::getSqlText(
            'table1',
                $this->fileNewTable1,
                '`text1`,`text1b`',
                1,
                ',',
                ''
        )
            .OneManyImporter::getaddNewTableSql(
                'table2',
                'table1',
                'text1',
                'text1',
                'table1_id',
                'id',
                $this->fileNewTable2,
                '`text1`,`text2`,`text2b`',
                false,
                1,
                ',',
                '',
                'temp'
            );
        error_reporting(E_ALL);
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result())
        {
            //   dont delete while statement
        }
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData('table2', __DIR__.'/'.$this->localFileNewTableDesired);
    }

    public function test_getSimpleOneManyAddNewTableSqlText_works_test2(): void
    {
        $this->assertFilesExistLocally([__DIR__.'/'.$this->localFileNewTableDesiredTest2]);
        $this->assertFilesExistOnSqlServer([
            $this->fileNewTable1,
            $this->fileNewTable2Test2
        ]);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);
        $query=SimpleImporter::getSqlText(
                'table1',
                $this->fileNewTable1,
                '`text1`,`text1b`',
                1,
                ',',
                ''
            )
            .OneManyImporter::getaddNewTableSql(
                'table2',
                'table1',
                'text2',
                'text1',
                'table1_id',
                'id',
                $this->fileNewTable2Test2,
                '`text2`,`text2b`,`text2c`',
                true,
                1,
                ',',
                '',
                'temp'
            );
        error_reporting(E_ALL);
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result())
        {
            //   dont delete while statement
        }
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData('table2', __DIR__.'/'.$this->localFileNewTableDesiredTest2);
    }
}
