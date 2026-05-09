<?php

namespace PaulMillband\SqlLibrary\tests\Importer;

use mysqli;
use PaulMillband\SqlLibrary\Importer\SimpleImporter;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseHelper;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseSqlHelper;
use PaulMillband\SqlLibrary\tests\TestLibrary\DataTestLibrary;
use PHPUnit\Framework\TestCase;
use PaulMillband\SqlLibrary\Importer\ManyManyImporter;

class ManyManyImporterTest extends TestCase
{
    protected mysqli $db;
    protected string $localFileSimple = '../data/many-many-simple.tsv';
    protected string $localFileComplexDesired = '../data/many-many-complex-desired.tsv';
    protected string $fileSimple = '/tmp/data/many-many-simple.tsv';
    protected string $fileComplex1 = '/tmp/data/many-many-complex-1.tsv';
    protected string $fileComplex2 = '/tmp/data/many-many-complex-2.tsv';
    protected string $fileComplexPivot = '/tmp/data/many-many-complex-pivot.tsv';
    protected string $fileComplexDesired = '/tmp/data/many-many-complex-desired.tsv';

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
        $query=ManyManyImporter::getSqlText(
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

    public function test_getPivotTableImportSqlText_works(): void
    {
        $this->assertEquals(0, $this->db->query('SELECT * FROM `link` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);

        $query = 'ALTER TABLE `table1` ADD COLUMN `myRef1` INT;'
            .'ALTER TABLE `table2` ADD COLUMN `myRef2` INT;'
            .SimpleImporter::getSqlText(
                'table1',
                $this->fileComplex1,
                '`myRef1`,`text1`,`text1b`,`text1c`',
            )
            .SimpleImporter::getSqlText(
                'table2',
                $this->fileComplex2,
                '`myRef2`,`text2`',
            );
        $result = $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        do {
        } while ($this->db->next_result());

        $query=ManyManyImporter::getPivotTableImportSqlText(
            'link',
            $this->fileComplexPivot,
            ['table1_id','table2_id'],
            ['table1','table2'],
            ['myRef1','myRef2'],
            1,
            '\t'
        );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()){;}

        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `link` LIMIT 1')->num_rows);
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                'link',
                __DIR__.'/'.$this->localFileComplexDesired,
                [],
                "\t"
            );
    }

}
