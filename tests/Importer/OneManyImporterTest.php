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
    protected string $file='/var/lib/mysql-files/data/one-many.csv';
    protected string $localFile='../data/one-many.csv';
    protected string $file2='/var/lib/mysql-files/data/one-many2.csv';
    protected string $localFile2='../data/one-many2.csv';
    protected string $fileNewTable1='/var/lib/mysql-files/data/one-many-new-table-file1.csv';
    protected string $fileNewTable2='/var/lib/mysql-files/data/one-many-new-table-file2.csv';
    protected string $localFileNewTableDesired='../data/one-many-new-table-desired.csv';
    protected string $fileNewTable2Test2='/var/lib/mysql-files/data/one-many-new-table-file2-test2.csv';
    protected string $localFileNewTableDesiredTest2='../data/one-many-new-table-desired-test2.csv';

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
                `text1` varchar(255),
                `text1b` varchar(255),
                `table2_id` varchar(255) ,
                `table3_id` varchar(255) ,
                `table4_id` varchar(255) ,
                PRIMARY KEY (id)
            );
            CREATE TABLE `table2` (
                `id` int NOT NULL AUTO_INCREMENT,
                `text2` varchar(255),
                `text2b` varchar(255),
                `text2c` varchar(255),
                PRIMARY KEY (id)
            );
            CREATE TABLE `table3` (
                `id` int NOT NULL AUTO_INCREMENT,
                `text3` varchar(255),
                `text3b` varchar(255),
                `text3c` varchar(255),
                PRIMARY KEY (id)
            );
            CREATE TABLE `table4` (
                `id` int NOT NULL AUTO_INCREMENT,
                `text4` varchar(255),
                `text4b` varchar(255),
                `text4c` varchar(255),
                PRIMARY KEY (id)
            );
SQL;
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        do {
        } while ($this->db->next_result());
        $this->assertEquals(
                4,
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
        $query=OneManyImporter::getSimpleImportSqlText(
            'table1',
            ['text1', 'text1b'],
            ['table2_id'],
            ['table2'],
            [
                ['text2','text2b']
            ],
            $this->file,
            ['text1','text1b','text2','text2b'],
            1,
            1,
            ',',
            '',
            'tempTable'
        );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result())
        {
            // dont delete while statement
        }
        $this->assertEquals(6, $this->db->query('SELECT * FROM `table1`')->num_rows);
        $this->assertEquals(6, $this->db->query('SELECT * FROM `table2`')->num_rows);

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData('table1', __DIR__.'/'.$this->localFile,[],',',false)
            ->compareTableToCsvData('table2', __DIR__.'/'.$this->localFile,[],',',false);
    }

    public function test_getSimpleOneManySqlText_works2(): void
    {
        $this->assertFilesExistLocally([__DIR__.'/'.$this->localFile]);
        $this->assertFilesExistOnSqlServer([$this->file]);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table3` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table4` LIMIT 1')->num_rows);
        $query=OneManyImporter::getSimpleImportSqlText(
            'table1',
            ['text1', 'text1b'],
            ['table2_id', 'table3_id', 'table4_id'],
            ['table2', 'table3', 'table4'],
            [['text2','text2b'], ['text3','text3b'], ['text4','text4b']],
            $this->file2,
            ['text1','text1b','text2','text2b','text3','text3b','text4','text4b'],
            1,
            1,
            ',',
            '',
            'tempTable'
        );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result())
        {
            // dont delete while statement
        }
        $this->assertEquals(5, $this->db->query('SELECT * FROM `table1`')->num_rows);
        $this->assertEquals(5, $this->db->query('SELECT * FROM `table2`')->num_rows);
        $this->assertEquals(5, $this->db->query('SELECT * FROM `table3`')->num_rows);
        $this->assertEquals(5, $this->db->query('SELECT * FROM `table4`')->num_rows);

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData('table1', __DIR__.'/'.$this->localFile2,[],',',false)
            ->compareTableToCsvData('table2', __DIR__.'/'.$this->localFile2,[],',',false)
            ->compareTableToCsvData('table3', __DIR__.'/'.$this->localFile2,[],',',false)
            ->compareTableToCsvData('table4', __DIR__.'/'.$this->localFile2,[],',',false);
    }

    public function test_getComplexOneManyImporterSqlText_works(): void
    {
        $this->assertFilesExistLocally([__DIR__.'/'.$this->localFile]);
        $this->assertFilesExistOnSqlServer([$this->file]);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table3` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table4` LIMIT 1')->num_rows);
        $query=OneManyImporter::getComplexImportSqlText(
            'table1',
            ['text1', 'text1b'],
            ['table2_id', 'table3_id', 'table4_id'],
            ['text2', 'text3', 'text4'],
            ['table2', 'table3', 'table4'],
            [
                ['text2','text2b'],
                ['text3','text3b'],
                ['text4','text4b']
            ],
            $this->file2,
            ['text1','text1b','text2','text2b','text3','text3b','text4','text4b'],
            1,
            ',',
            '',
            'tempTable'
        );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result())
        {
            // dont delete while statement
        }
        $this->assertEquals(5, $this->db->query('SELECT * FROM `table1`')->num_rows);
        $this->assertEquals(4, $this->db->query('SELECT * FROM `table2`')->num_rows);
        $this->assertEquals(3, $this->db->query('SELECT * FROM `table3`')->num_rows);
        $this->assertEquals(3, $this->db->query('SELECT * FROM `table4`')->num_rows);

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData('table1', __DIR__.'/'.$this->localFile2,[],',',false)
            ->compareTableToCsvData('table2', __DIR__.'/'.$this->localFile2,[],',',false)
            ->compareTableToCsvData('table3', __DIR__.'/'.$this->localFile2,[],',',false)
            ->compareTableToCsvData('table4', __DIR__.'/'.$this->localFile2,[],',',false);
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
            'table2',
                $this->fileNewTable1,
                ['text2','text2b'],
                1,
                ',',
                ''
        );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result())
        {
            //   dont delete while statement
        }
        $query=OneManyImporter::getaddNewTableSql(
                'table1',
                'table2',
                'text2',
                'text2',
                'table2_id',
                'id',
                $this->fileNewTable2,
                ['text2','text1','text1b'],
                false,
                1,
                ',',
                '',
                'tempTable'
            );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result())
        {
            //   dont delete while statement
        }
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData('table1', __DIR__.'/'.$this->localFileNewTableDesired);
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
                'table2',
                $this->fileNewTable1,
                ['text2','text2b'],
                1,
                ',',
                ''
            );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result())
        {
            //   dont delete while statement
        }
        $query=OneManyImporter::getaddNewTableSql(
                'table1',
                'table2',
                'text1',
                'text2',
                'table2_id',
                'id',
                $this->fileNewTable2Test2,
                ['text1','text1b'],
                true,
                1,
                ',',
                '',
                'tempTable'
            );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result())
        {
            //   dont delete while statement
        }
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData('table1', __DIR__.'/'.$this->localFileNewTableDesiredTest2);
    }
}
