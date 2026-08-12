<?php

namespace PaulMillband\SqlLibrary\tests\Importer;

use mysqli;
use PaulMillband\SqlLibrary\Importer\SimpleImporter;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseHelper;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseSqlHelper;
use PaulMillband\SqlLibrary\tests\Helper\FileHelper;
use PaulMillband\SqlLibrary\tests\TestLibrary\DataTestLibrary;
use PaulMillband\SqlLibrary\tests\TestCase;
use PaulMillband\SqlLibrary\Importer\ManyManyImporter;

class ManyManyImporterTest extends TestCase
{
    protected mysqli $db;
    protected string $localFileSimple = '../data/many-many-simple.tsv';
    protected string $localFileComplex = '../data/many-many-complex.tsv';
    protected string $localFileComplex2 = '../data/many-many-complex-2.tsv';
    protected string $localFileComplexPivot3 = '../data/many-many-complex-pivot3.tsv';
    protected string $localFileComplexDesired = '../data/many-many-complex-desired.tsv';
    protected string $localFileComplexDesired2 = '../data/many-many-complex-desired2.tsv';
    protected string $fileSimple = '/var/lib/mysql-files/data/many-many-simple.tsv';
    protected string $fileComplex = '/var/lib/mysql-files/data/many-many-complex.tsv';
    protected string $fileComplex1 = '/var/lib/mysql-files/data/many-many-complex-1.tsv';
    protected string $fileComplex2 = '/var/lib/mysql-files/data/many-many-complex-2.tsv';
    protected string $fileComplex3 = '/var/lib/mysql-files/data/many-many-complex-3.tsv';
    protected string $fileComplex4 = '/var/lib/mysql-files/data/many-many-complex-4.tsv';
    protected string $fileComplexPivot = '/var/lib/mysql-files/data/many-many-complex-pivot.tsv';
    protected string $fileComplexPivot2 = '/var/lib/mysql-files/data/many-many-complex-pivot2.tsv';

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
        $query=<<<SQL
            CREATE TABLE `link` (
                `id` int NOT NULL AUTO_INCREMENT,
                `table1_id` int NOT NULL,
                `table2_id` int,
                `table3_id` int,
                `table4_id` int,
                PRIMARY KEY (id)
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
                `text2b` varchar(255),
                PRIMARY KEY (id)
            );
            CREATE TABLE `table3` (
                `id` int NOT NULL AUTO_INCREMENT,
                `text3` varchar(255),
                `text3b` varchar(255),
                PRIMARY KEY (id)
            );
            CREATE TABLE `table4` (
                `id` int NOT NULL AUTO_INCREMENT,
                `text4` varchar(255),
                `text4b` varchar(255),
                PRIMARY KEY (id)
            );
SQL;
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        do {
        } while ($this->db->next_result());
        $this->assertEquals(5, $this->db->query('SHOW TABLES')->num_rows);
    }

    /**
     * function only for flat single file 1-1 imports
     */
    public function test_getSimpleManyManySqlText_works(): void
    {
        $this->assertFilesExistLocally([__DIR__.'/'.$this->localFileSimple]);
        $this->assertFilesExistOnSqlServer([$this->fileSimple]);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `link` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);
        $query=ManyManyImporter::getSimpleImportSqlText(
            'link',
            ['table1_id','table2_id'],
            ['table1','table2'],
            [
                ['text1','text1b','text1c'],
                ['text2']
            ],
            $this->fileSimple,
            ['text1','text1b','text1c','text2'],
            100,
        );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()){;}

        $this->assertEquals(6, $this->db->query('SELECT * FROM `link`')->num_rows);
        $this->assertEquals(6, $this->db->query('SELECT * FROM `table1`')->num_rows);
        $this->assertEquals(6, $this->db->query('SELECT * FROM `table2`')->num_rows);

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

    /**
     * imports for complex databases NOT flat 1-1 databases
     */
    public function test_getManyManySqlText_works(): void
    {
        $this->assertFilesExistLocally([__DIR__.'/'.$this->localFileComplex]);
        $this->assertFilesExistOnSqlServer([$this->fileComplex]);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `link` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table3` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table4` LIMIT 1')->num_rows);
        $query=ManyManyImporter::getComplexImportSqlText(
            'link',
            ['table1_id','table2_id','table3_id','table4_id'],
            ['table1','table2','table3','table4'],
            [
                ['text1','text1b'],
                ['text2','text2b'],
                ['text3','text3b'],
                ['text4','text4b'],
            ],
            ['text1','text2','text3','text4'],
            $this->fileComplex,
            ['text1','text1b','text2','text2b','text3','text3b','text4','text4b'],
            1,
            '\t',
            '',
            'temp'
    );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()){;}

        $this->assertEquals(6, $this->db->query('SELECT * FROM `link`')->num_rows);
        $this->assertEquals(3, $this->db->query('SELECT * FROM `table1`')->num_rows);
        $this->assertEquals(4, $this->db->query('SELECT * FROM `table2`')->num_rows);
        $this->assertEquals(3, $this->db->query('SELECT * FROM `table3`')->num_rows);
        $this->assertEquals(3, $this->db->query('SELECT * FROM `table4`')->num_rows);

        $query= 'SELECT l.*';
        for ($i = 1; $i <= 4; $i++) {
            $query.=",t$i.`text$i`";
        }
        $query.="\nFROM\n  `link` l\n";
        for ($i = 1; $i <= 4; $i++) {
         $query .=<<<SQL
        INNER JOIN
    `table$i` t$i
ON
    l.table{$i}_id=t$i.id
SQL;
        }
        (new DataTestLibrary($this->name()))
            ->compareSqlQueryToCsvData(
                $query,
                __DIR__.'/'.$this->localFileComplex,
                [],
                "\t"
            )
            ->compareTableToCsvData(
                'table1',
                __DIR__.'/'.$this->localFileComplex,
                [],
                "\t",
                false
            )
            ->compareTableToCsvData(
                'table2',
                __DIR__.'/'.$this->localFileComplex,
                [],
                "\t",
                false
            )
            ->compareTableToCsvData(
                'table3',
                __DIR__.'/'.$this->localFileComplex,
                [],
                "\t",
                false
            );
    }

    public function test_getImportNewTableAndLinkTableSqlText()
    {
        $this->assertFilesExistLocally([__DIR__.'/'.$this->localFileComplex]);
        $this->assertFilesExistOnSqlServer([$this->fileComplex]);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `link` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);
        $query = SimpleImporter::getSqlText(
            'table1',
            $this->fileComplex1,
            ['id','text1','text1b','text1c'],
        );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        do {
        } while ($this->db->next_result());

        $query = ManyManyImporter::getImportNewTableAndLinkTableSqlText(
            'table2',
            'table2_id',
            'link',
            [],
            [],
            'table1',
            'table1_id',
            '`id` = NEW.`id`',
            $this->fileComplex2,
            ['id','text2'],
            0,
            ',',
            ''
        );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        do {
        } while ($this->db->next_result());
        $this->assertEquals(4, $this->db->query('SELECT * FROM `link`')->num_rows);
        $this->assertEquals(4, $this->db->query('SELECT * FROM `table1`')->num_rows);
        $this->assertEquals(4, $this->db->query('SELECT * FROM `table2`')->num_rows);
        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                'table1',
                __DIR__.'/'.$this->localFileComplex2,
                ['id','text2'],
                "\t",
                false
            )
            ->compareTableToCsvData(
                'link',
                __DIR__.'/'.$this->localFileComplexPivot3,
                ['table1_id','table2_id'],
                "\t",
                false
            );
    }

    public function test_getPivotTableImportSqlText_works(): void
    {
        $this->assertFilesExistLocally([__DIR__.'/'.$this->localFileComplexDesired]);
        $this->assertFilesExistOnSqlServer([
            $this->fileComplex1,
            $this->fileComplex2,
            $this->fileComplexPivot
        ]);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `link` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);

        $query = 'ALTER TABLE `table1` ADD COLUMN `myRef1` INT;'
            .'ALTER TABLE `table2` ADD COLUMN `myRef2` INT;'
            .SimpleImporter::getSqlText(
                'table1',
                $this->fileComplex1,
                ['myRef1','text1','text1b','text1c'],
            )
            .SimpleImporter::getSqlText(
                'table2',
                $this->fileComplex2,
                ['myRef2','text2'],
            );
        $this->db->multi_query($query);
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

    public function test_getPivotTableImportSqlText_worksWith4Columns(): void
    {
        $this->assertFilesExistLocally([__DIR__.'/'.$this->localFileComplexDesired2]);
        $this->assertFilesExistOnSqlServer([
            $this->fileComplex1,
            $this->fileComplex2,
            $this->fileComplex3,
            $this->fileComplex4,
            $this->fileComplexPivot2
        ]);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `link` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);
$query=<<<SQL
            ALTER TABLE `table1` ADD COLUMN `myRef1` INT;
            ALTER TABLE `table2` ADD COLUMN `myRef2` INT;
            ALTER TABLE `table3` ADD COLUMN `myRef3` INT;
            ALTER TABLE `table4` ADD COLUMN `myRef4` INT;
SQL
            .SimpleImporter::getSqlText(
                'table1',
                $this->fileComplex1,
                ['myRef1','text1','text1b','text1c'],
            )
            .SimpleImporter::getSqlText(
                'table2',
                $this->fileComplex2,
                ['myRef2','text2'],
            )
            .SimpleImporter::getSqlText(
                'table3',
                $this->fileComplex3,
                ['myRef3','text3'],
            )
            .SimpleImporter::getSqlText(
                'table4',
                $this->fileComplex4,
                ['myRef4','text4'],
            );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        do {
        } while ($this->db->next_result());

        $query=ManyManyImporter::getPivotTableImportSqlText(
            'link',
            $this->fileComplexPivot2,
            ['table1_id','table2_id','table3_id','table4_id'],
            ['table1','table2','table3','table4'],
            ['myRef1','myRef2','myRef3','myRef4'],
            1,
            '\t'
        );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()){;}

        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `link` LIMIT 1')->num_rows);
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table1` LIMIT 1')->num_rows);
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table2` LIMIT 1')->num_rows);
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table3` LIMIT 1')->num_rows);
        $this->assertNotEquals(0, $this->db->query('SELECT * FROM `table4` LIMIT 1')->num_rows);

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                'link',
                __DIR__.'/'.$this->localFileComplexDesired2,
                [],
                "\t"
            );
    }

}
