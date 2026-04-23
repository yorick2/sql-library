<?php

namespace PaulMillband\SqlLibrary\tests\Importer;

use mysqli;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseHelper;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseSqlHelper;
use PaulMillband\SqlLibrary\tests\TestLibrary\DataTestLibrary;
use PHPUnit\Framework\TestCase;
use PaulMillband\SqlLibrary\Importer\SimpleImporter;

class SimpleImporterTest extends TestCase
{
    protected mysqli $db;
     protected $localFile = '../data/simple.tsv';
    protected $file = '/tmp/data/simple.tsv';
    protected $fileCommaColumn = '/tmp/data/simpleCommas.tsv';
    protected $fileCommaMultipleColumns = '/tmp/data/simpleCommaMultipleColumns.tsv';
    protected $localFileCommasDesired = '../data/simpleCommas.tsv';
    protected $localFileCommaMultipleColumnsDesired = '../data/simpleCommaMultipleColumns.tsv';

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
        $query = <<<EOF
            CREATE TABLE `table1` (
                `id` int NOT NULL AUTO_INCREMENT,
                `text1` varchar(255),
                `text1b` varchar(255),
                `text1c` varchar(255),
                PRIMARY KEY (id)
            );
EOF;
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        do {
        } while ($this->db->next_result());
        $this->assertEquals($this->db->query('SHOW TABLES')->num_rows, 1);
    }

    public function test_getSimpleManyManySqlText_works(): void
    {
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $query = SimpleImporter::getSqlText(
            'table1',
            $this->file,
            '`text1`,`text1b`,`text1c`',
        );
        $result = $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        do {
        } while ($this->db->next_result());

        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData('table1', __DIR__.'/'.$this->localFile, ['text1','text1b','text1c'], "\t");
    }

    public function test_getSplitRecordsWithCommaColumnSqlText(): void
    {
        $table='table1';
        $splitCol='text1b';

        $this->assertEquals(0, $this->db->query("SELECT * FROM `$table` LIMIT 1")->num_rows);
        $query = SimpleImporter::getSqlText(
            $table,
            $this->fileCommaColumn,
            '`text1`,`text1b`,`text1c`',
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()){;}

        $query=SimpleImporter::getSplitRecordsWithCommasSqlText(
            $table,
            $splitCol,
            '`text1`,`text1c`',
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
            ->query('SELECT * FROM `'.$table.'`');
        $this->assertEquals(9, $result->num_rows, "not the right number of rows");

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $table,
                __DIR__.'/'.$this->localFileCommasDesired,
                [],
                "\t"
            );
    }

    public function test_getSplitRecordsWithCommasMultipleColumnsSqlText(): void
    {
        $table= 'table1';
        $splitCols=['text1b','text1c'];
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$table` LIMIT 1")->num_rows);
        $query = SimpleImporter::getSqlText(
            $table,
            $this->fileCommaMultipleColumns,
            '`text1`,`text1b`,`text1c`',
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()){;}

        $query=SimpleImporter::getSplitRecordsWithMultipleCommaColumnsSqlText(
            $table,
            $splitCols,
            '`text1`',
            '',
            'temp',
            100
        );
        $result = $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()){;}

        $result=$this->db
            ->query('SELECT * FROM `'.$table.'` WHERE `'.$splitCols[0].'` LIKE "%,%"');
        $this->assertEquals(0, $result->num_rows, "test has no rows containing a comma in column $splitCols[0]");

        $result=$this->db
            ->query('SELECT * FROM `'.$table.'`');
        $this->assertEquals(6, $result->num_rows, "not the right number of rows");

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $table,
                __DIR__.'/'.$this->localFileCommasDesired,
                [],
                "\t"
            );
    }
}