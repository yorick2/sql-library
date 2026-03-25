<?php

namespace PaulMillband\SqlLibrary\tests\Importer;

use mysqli;
use PaulMillband\SqlLibrary\tests\Helper\CsvFileDataHelper;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseHelper;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseSqlHelper;
use PaulMillband\SqlLibrary\tests\Helper\DataHelper;
use PaulMillband\SqlLibrary\tests\TestLibrary\DataTestLibrary;
use PHPUnit\Framework\TestCase;
use PaulMillband\SqlLibrary\Importer\ManyManyImporter;

class ManyManyImporterTest extends TestCase
{
    protected mysqli $db;
     protected $localFileSimple = '../data/many-many-simple.tsv';
    protected $fileSimple = '/tmp/data/many-many-simple.tsv';
    protected $fileCommas = '/tmp/data/many-many-commas.tsv';

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
            CREATE TABLE `link` (
                `table1_id` int NOT NULL,
                `table2_id` int
            );
            CREATE TABLE `table1` (
                `id` int NOT NULL AUTO_INCREMENT,
                `text1` varchar(255),
                `text1b` varchar(255),
                `text1c` varchar(255),
                PRIMARY KEY (id)
            );
            CREATE TABLE `table2` (
                `id` int NOT NULL AUTO_INCREMENT,
                `text2` varchar(255),
                PRIMARY KEY (id)
            )
EOF;
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        do {
        } while ($this->db->next_result());
        $this->assertEquals($this->db->query('SHOW TABLES')->num_rows,3);
    }

    /**
     * function only for flat single file 1-1 imports
     */
    public function test_getSimpleManyManySqlText_works(): void
    {
        $this->assertEquals(0, $this->db->query('SELECT * FROM `link` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);
        $query=ManyManyImporter::getSimpleManyManySqlText(
            100,
            'link',
            '`table1_id`,`table2_id`',
            'table1',
            '`id`,`text1`,`text1b`,`text1c`',
            'NEW.`id`,NEW.`text1`,new.`text1b`,new.`text1c`',
            'table2',
            'id,text2',
            'NEW.`id`,NEW.`text2`',
            $this->fileSimple,
            '`text1`,`text1b`,`text1c`,`text2`'
        );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()){;}

        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `link` LIMIT 1')->num_rows);
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);

        $lowestId=(int) $this->db
            ->query('SELECT MIN(`table1_id`) FROM `link`')
            ->fetch_column( 0 );
        $this->assertEquals(100, $lowestId);

        $lowestId=(int) $this->db
            ->query('SELECT MIN(`table2_id`) FROM `link`')
            ->fetch_column( 0 );
        $this->assertEquals(100, $lowestId);

        $lowestId=(int) $this->db
            ->query('SELECT MIN(`id`) FROM `table1`')
            ->fetch_column( 0 );
        $this->assertEquals(100, $lowestId);

        $lowestId=(int) $this->db
            ->query('SELECT MIN(`id`) FROM `table2`')
            ->fetch_column( 0 );
        $this->assertEquals(100, $lowestId);

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                'table1',
                __DIR__.'/'.$this->localFileSimple,
                ['text1', 'text1b', 'text1c', 'text2'],
                "\t"
            )
            ->compareTableToCsvData(
                'table2',
                __DIR__.'/'.$this->localFileSimple,
                ['text1', 'text1b', 'text1c', 'text2'],
                "\t"
            );
    }

    public function test_getSplitRecordsWithCommasSqlText(): void
    {
        $this->assertEquals(0, $this->db->query('SELECT * FROM `link` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);
        $query=ManyManyImporter::getSplitRecordsWithCommasSqlText(
            1,
            'link',
            '`table1_id`,`table2_id`',
            'table1',
            '`id`,`text1`,`text1b`,`text1c`',
            'NEW.`id`,NEW.`text1`,new.`text1b`,new.`text1c`',
            'table2',
            'id,text2',
            'NEW.`id`,NEW.`text2`',
            $this->fileCommas,
            '`text1`,`text1b`,`text1c`,`text2`'
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()){;}

        $tableName='table1';
        $splitCol='text1c';
        $query=ManyManyImporter::getSplitRecordsWithCommasSqlText(
            'link',
            '`table1_id`,`table2_id`',
            'new.`id`, new.`table2_id`',
            $tableName,
            $splitCol,
            '`text1`,`text1b`',
            'table2_id',
            '',
        );
        $result = $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()){;}

        $result=$this->db
            ->query('SELECT * FROM `table1` WHERE `'.$splitCol.'` LIKE "%,%"');
        $this->assertEquals(0, $result->num_rows, "test has no rows containing a comma in column $splitCol");

        $result=$this->db
            ->query("SELECT * FROM `table1` WHERE text1='test1' AND text1b='foo' AND $splitCol='foo' ");
        $this->assertEquals(1, $result->num_rows, "test has successfully created the 1st split from row 1");
        $result=$this->db
            ->query("SELECT * FROM `table1` WHERE text1='test1' AND text1b='foo' AND $splitCol='foo2' ");
        $this->assertEquals(1, $result->num_rows, "test has successfully created the 2nd split from row 1");
        $result=$this->db
            ->query("SELECT * FROM `table1` WHERE text1='test1' AND text1b='foo' AND $splitCol='foo3' ");
        $this->assertEquals(1, $result->num_rows, "test has successfully created the 3rd split from row 1");
        $result=$this->db
            ->query("SELECT * FROM `table1` WHERE text1='test1' AND text1b='foo' AND $splitCol='foo3' ");
        $this->assertEquals(1, $result->num_rows, "test has successfully created the 4th split from row 1");

        $result=$this->db
            ->query("SELECT * FROM `table1` WHERE text1='test6' AND text1b='' AND $splitCol='foobar'");
        $this->assertEquals(1, $result->num_rows, "test has successfully created the 1st split from row 6");
        $result=$this->db
            ->query("SELECT * FROM `table1` WHERE text1='test6' AND text1b='' AND $splitCol='foobar2'");
        $this->assertEquals(1, $result->num_rows, "test has successfully created the 2nd split from row 6");

    }
}
