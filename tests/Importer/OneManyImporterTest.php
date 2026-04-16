<?php

namespace PaulMillband\SqlLibrary\tests\Importer;

use mysqli;
use PaulMillband\SqlLibrary\Importer\OneManyImporter;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseHelper;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseSqlHelper;
use PaulMillband\SqlLibrary\tests\TestLibrary\DataTestLibrary;
use PHPUnit\Framework\TestCase;

class OneManyImporterTest extends TestCase
{
    protected mysqli $db;
    protected $file1='/tmp/data/one-many.csv';
    protected $file1ForPhpunit='../data/one-many.csv';

    protected $fileCommas = '/tmp/data/one-many-commas.tsv';


    protected function setUp(): void
    {
        parent::setUp();
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
        $query=<<<EOF
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
                PRIMARY KEY (id)
            );
EOF;
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
            $this->file1,
            '`text1`,`text1b`,`text2`,`text2b`',
            1,
            ',',
            'temp'
        );
        error_reporting(E_ALL);
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result())
        {
            // dont delete while statement
        }
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData('table1', __DIR__.'/'.$this->file1ForPhpunit)
            ->compareCsvDataToTable('table2', __DIR__.'/'.$this->file1ForPhpunit);
    }

    public function test_getSplitRecordsWithCommasSqlText(): void
    {
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
            $this->fileCommas,
            '`text1`,`text1b`,`text2`,`text2b`',
            1,
            '\t',
            'temp'
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()){;}

        $table='table2';
        $splitCol='text2';
        $query=OneManyImporter::getSplitRecordsWithCommasSqlText(
            $table,
            $splitCol,
            '`table1_id`,`text2b`',
            '',
            'temp',
            100
        );
        $result = $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()){;}

        $result=$this->db
            ->query('SELECT * FROM `'.$table.'` WHERE `'.$splitCol.'` LIKE "%,%"');
        $this->assertEquals(0, $result->num_rows, "test has no rows containing a comma in column $splitCol");

        $result=$this->db
            ->query("SELECT * FROM `$table` WHERE text1='test1' AND text1b='foo' AND $splitCol='foo' ");
        $this->assertEquals(1, $result->num_rows, "test has successfully created the 1st split from row 1");
        $result=$this->db
            ->query("SELECT * FROM `$table` WHERE text1='test1' AND text1b='foo' AND $splitCol='foo2' ");
        $this->assertEquals(1, $result->num_rows, "test has successfully created the 2nd split from row 1");
        $result=$this->db
            ->query("SELECT * FROM `$table` WHERE text1='test1' AND text1b='foo' AND $splitCol='foo3' ");
        $this->assertEquals(1, $result->num_rows, "test has successfully created the 3rd split from row 1");
        $result=$this->db
            ->query("SELECT * FROM `$table` WHERE text1='test1' AND text1b='foo' AND $splitCol='foo3' ");
        $this->assertEquals(1, $result->num_rows, "test has successfully created the 4th split from row 1");

        $result=$this->db
            ->query("SELECT * FROM `$table` WHERE text1='test6' AND text1b='' AND $splitCol='foobar'");
        $this->assertEquals(1, $result->num_rows, "test has successfully created the 1st split from row 6");
        $result=$this->db
            ->query("SELECT * FROM `$table` WHERE text1='test6' AND text1b='' AND $splitCol='foobar2'");
        $this->assertEquals(1, $result->num_rows, "test has successfully created the 2nd split from row 6");
    }
}
