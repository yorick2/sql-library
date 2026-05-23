<?php

namespace PaulMillband\SqlLibrary\tests\Editor;

use mysqli;
use PaulMillband\SqlLibrary\Editor\ManyManyDatabaseEditor;
use PaulMillband\SqlLibrary\Importer\ManyManyImporter;
use PaulMillband\SqlLibrary\misc\misc;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseHelper;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseSqlHelper;
use PaulMillband\SqlLibrary\tests\Helper\FileHelper;
use PaulMillband\SqlLibrary\tests\TestLibrary\DataTestLibrary;
use PaulMillband\SqlLibrary\tests\TestCase;

class ManyManyDatabaseEditorTest extends TestCase
{
    protected mysqli $db;

    protected string $file='/tmp/data/many-many.csv';
    protected string $localFile='../data/many-many.csv';
    protected string $fileCommaColumn = '/tmp/data/many-many-editor-commas.tsv';
    protected string $localFileCommasDesired = '../data/many-many-editor-commas-desired.tsv';
    protected string $localFileCommasDesiredLink = '../data/many-many-editor-commas-desired-link.tsv';
    protected string $fileCommaMultipleColumns = '/tmp/data/many-many-editor-comma-multiple-columns.tsv';
    protected string $localFileCommaMultipleColumnsDesired = '../data/many-many-editor-comma-multiple-columns-desired1.tsv';
    protected string $localFileCommaMultipleColumnsDesiredLink = '../data/many-many-editor-comma-multiple-columns-desired-link.tsv';
    protected string $fileCharacterColumn = '/tmp/data/many-many-editor-character.tsv';
    protected string $localFileCharacterDesired = '../data/many-many-editor-character-desired.tsv';
    protected string $localFileCharacterDesiredLink = '../data/many-many-editor-character-desired-link.tsv';
    protected string $fileCharacterMultipleColumns = '/tmp/data/many-many-editor-character-multiple-columns.tsv';
    protected string $localFileCharacterMultipleColumnsDesired = '../data/many-many-editor-character-multiple-columns-desired1.tsv';
    protected string $localFileCharacterMultipleColumnsDesiredLink = '../data/many-many-editor-character-multiple-columns-desired-link.tsv';
    protected string $fileRemoveLaterDuplicates = '/tmp/data/many-many-editor-remove-later-duplicates1.tsv';
    protected string $fileRemoveLaterDuplicates2 = '/tmp/data/many-many-editor-remove-later-duplicates2.tsv';
    protected string $localFileRemoveLaterDuplicatesDesired = '../data/many-many-editor-remove-later-duplicates-desired1.tsv';
    protected string $localFileRemoveLaterDuplicatesDesiredLink = '../data/many-many-editor-remove-later-duplicates-desired-link.tsv';


    protected string $linkTable = 'link';
    protected string $table1 = 'table1';
    protected string $table2 = 'table2';
    protected string $table3 = 'table3';
    protected string $table4 = 'table4';

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
                `table2_id` int,
                `table3_id` int,
                `table4_id` int
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
EOF;
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        do {
        } while ($this->db->next_result());
        $this->assertEquals(
            5,
            $this->db->query('SHOW TABLES')->num_rows,
            "check number of tables in '".(new DatabaseHelper())->getDatabaseName()."' database'"
        );
    }
    public function test_getSplitRecordsWithCommaColumnSqlText_table1(): void
    {
        $this->assertFilesExistLocally([
            __DIR__.'/'.$this->localFileCommasDesired,
            __DIR__.'/'.$this->localFileCommasDesiredLink
        ]);
        $this->assertFilesExistOnSqlServer([$this->fileCommaColumn]);

        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table1` LIMIT 1")->num_rows);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table2` LIMIT 1")->num_rows);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table3` LIMIT 1")->num_rows);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table4` LIMIT 1")->num_rows);

        $query=ManyManyImporter::getComplexImportSqlText(
            100,
            $this->linkTable,
            ['table1_id','table2_id','table3_id','table4_id'],
            [$this->table1, $this->table2, $this->table3, $this->table4],
            [
                '`id`,`text1`,`text1b`,`text1c`',
                '`id`,`text2`,`text2b`',
                '`id`,`text3`,`text3b`',
                '`id`,`text4`,`text4b`',
            ],
            [
                'NEW.`id`,NEW.`text1`,new.`text1b`,new.`text1c`',
                'NEW.`id`,NEW.`text2`,new.`text2b`',
                'NEW.`id`,NEW.`text3`,new.`text3b`',
                'NEW.`id`,NEW.`text4`,new.`text4b`',
            ],
            ['text1','text2','text3','text4'],
            $this->fileCommaColumn,
            '`text1`,`text1b`,`text1c`,`text2`,`text2b`,`text3`,`text3b`,`text4`,`text4b`',
            1,
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            // needed to run the multi_query
        }

        $splitCol = 'text1';
        $linkTableLinkColumn='table1_id';
        $query=misc::getTableColumnNamesStringExcluding($this->linkTable, [$linkTableLinkColumn],'`,`');
        $remainingColumnsInLinkTable=[];
        $this->db->multi_query($query);
        do {
            // Store first result set
            if ($result = $this->db -> store_result()) {
                while ($row = $result -> fetch_row()) {
                    $remainingColumnsInLinkTable[] = $row[0];
                }
                $result -> free_result();
            }
        } while ($this->db->next_result());
        $query = ManyManyDatabaseEditor::getSplitRecordsWithCommasSqlText(
            $this->table1,
            'id',
            $this->linkTable,
            $linkTableLinkColumn,
            '`'.$remainingColumnsInLinkTable[0].'`',
            $splitCol,
            '`text1b`,`text1c`',
            '',
            'temp',
            1000
        );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()) {
            // needed to run the multi_query
        }

        $result = $this->db
            ->query('SELECT * FROM `' . $this->table1 . '` WHERE `' . $splitCol . '` LIKE "%,%"');
        $this->assertEquals(0, $result->num_rows, "test has no rows containing a comma in column $splitCol");

        $result = $this->db
            ->query('SELECT * FROM `' . $this->table1 . '`');
        $this->assertEquals(9, $result->num_rows, "not the right number of rows");
        $result = $this->db
            ->query('SELECT * FROM `' . $this->linkTable . '`');
        $this->assertEquals(9, $result->num_rows, "not the right number of rows");

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $this->table1,
                __DIR__ . '/' . $this->localFileCommasDesired,
                [],
                "\t"
            )
            ->compareTableToCsvData(
                $this->linkTable,
                __DIR__ . '/' . $this->localFileCommasDesiredLink,
                [],
                "\t"
            );
    }

    public function test_getSplitRecordsWithCommasMultipleColumnsSqlText_table1(): void
    {
        $splitCols = ['text1b', 'text1c'];
        $this->assertFilesExistLocally([__DIR__.'/'.$this->file]);
        $this->assertFilesExistOnSqlServer([$this->localFileCommaMultipleColumnsDesired]);

        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table1` LIMIT 1")->num_rows);
        $query=ManyManyImporter::getSqlText(
            100,
            'link',
            '`table1_id`,`table2_id`',
            $this->table1,
            '`id`,`text1`,`text1b`,`text1c`',
            'NEW.`id`,NEW.`text1`,new.`text1b`,new.`text1c`',
            $this->table2,
            'id,text2',
            'NEW.`id`,NEW.`text2`',
            $this->file,
            '`text2`,`text1`,`text1b`,`text1c`'
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }

        $query = ManyManyDatabaseEditor::getSplitRecordsWithMultipleCommaColumnsSqlText(
            $this->table1,
            $splitCols,
            '`text1`',
            '',
            'temp',
            100
        );
        $result = $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()) {
            ;
        }

        $result = $this->db
            ->query('SELECT * FROM `' . $this->table1 . '` WHERE `' . $splitCols[0] . '` LIKE "%,%"');
        $this->assertEquals(0, $result->num_rows, "test has no rows containing a comma in column $splitCols[0]");

        $result = $this->db
            ->query('SELECT * FROM `' . $this->table1 . '`');
        $this->assertEquals(6, $result->num_rows, "not the right number of rows");

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $this->table1,
                __DIR__ . '/' . $this->localFileCommaMultipleColumnsDesired,
                [],
                "\t"
            );
    }

    public function test_getSplitRecordsWithCharacterColumnSqlText_table1(): void
    {
        $this->assertFilesExistOnSqlServer([$this->localFileCharacterDesired]);
        $this->assertFilesExistLocally([
            __DIR__.'/'.$this->localFileCharacterDesired,
            __DIR__.'/'.$this->localFileCharacterDesiredLink
        ]);
        $this->assertFilesExistOnSqlServer([$this->fileCharacterColumn]);

        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table1` LIMIT 1")->num_rows);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table2` LIMIT 1")->num_rows);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table3` LIMIT 1")->num_rows);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table4` LIMIT 1")->num_rows);

        $splitCharacter=';';
        $query=ManyManyImporter::getComplexImportSqlText(
            100,
            $this->linkTable,
            ['table1_id','table2_id','table3_id','table4_id'],
            [$this->table1, $this->table2, $this->table3, $this->table4],
            [
                '`id`,`text1`,`text1b`,`text1c`',
                '`id`,`text2`,`text2b`',
                '`id`,`text3`,`text3b`',
                '`id`,`text4`,`text4b`',
            ],
            [
                'NEW.`id`,NEW.`text1`,new.`text1b`,new.`text1c`',
                'NEW.`id`,NEW.`text2`,new.`text2b`',
                'NEW.`id`,NEW.`text3`,new.`text3b`',
                'NEW.`id`,NEW.`text4`,new.`text4b`',
            ],
            ['text1','text2','text3','text4'],
            $this->fileCharacterColumn,
            '`text1`,`text1b`,`text1c`,`text2`,`text2b`,`text3`,`text3b`,`text4`,`text4b`',
            1,
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            // needed to run the multi_query
        }

        $splitCol = 'text1';
        $linkTableLinkColumn='table1_id';
        $query=misc::getTableColumnNamesStringExcluding($this->linkTable, [$linkTableLinkColumn],'`,`');
        $remainingColumnsInLinkTable=[];
        $this->db->multi_query($query);
        do {
            // Store first result set
            if ($result = $this->db -> store_result()) {
                while ($row = $result -> fetch_row()) {
                    $remainingColumnsInLinkTable[] = $row[0];
                }
                $result -> free_result();
            }
        } while ($this->db->next_result());
        $query = ManyManyDatabaseEditor::getSplitRecordsWithCharacterSqlText(
            $this->table1,
            'id',
            $this->linkTable,
            $linkTableLinkColumn,
            '`'.$remainingColumnsInLinkTable[0].'`',
            $splitCol,
            '`text1b`,`text1c`',
            $splitCharacter,
            '',
            'temp',
            1000
        );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()) {
            // needed to run the multi_query
        }

        $result = $this->db
            ->query('SELECT * FROM `' . $this->table1 . '` WHERE `' . $splitCol . '` LIKE "%'.$splitCharacter.'%"');
        $this->assertEquals(0, $result->num_rows, "test has no rows containing a $splitCharacter in column $splitCol");

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $this->table1,
                __DIR__ . '/' . $this->localFileCharacterDesired,
                [],
                "\t"
            )
            ->compareTableToCsvData(
                $this->linkTable,
                __DIR__ . '/' . $this->localFileCharacterDesiredLink,
                [],
                "\t"
            );
    }

    public function test_getSplitRecordsWithCharacterMultipleColumnsSqlText_table1(): void
    {
        $splitCols = ['text1b', 'text1c'];
        $this->assertFilesExistLocally([__DIR__.'/'.$this->file]);
        $this->assertFilesExistOnSqlServer([$this->localFileCharacterMultipleColumnsDesired]);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table1` LIMIT 1")->num_rows);
        $query=ManyManyImporter::getSqlText(
            100,
            'link',
            '`table1_id`,`table2_id`',
            $this->table1,
            '`id`,`text1`,`text1b`,`text1c`',
            'NEW.`id`,NEW.`text1`,new.`text1b`,new.`text1c`',
            $this->table2,
            'id,text2',
            'NEW.`id`,NEW.`text2`',
            $this->file,
            '`text2`,`text1`,`text1b`,`text1c`'
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }

        $query = ManyManyDatabaseEditor::getSplitRecordsWithMultipleCharacterColumnsSqlText_table1(
            $this->table1,
            $splitCols,
            '`text1`',
            ';',
            '',
            'temp',
            100
        );
        $result = $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()) {
            ;
        }

        $result = $this->db
            ->query('SELECT * FROM `' . $this->table1 . '` WHERE `' . $splitCols[0] . '` LIKE "%;%"');
        $this->assertEquals(0, $result->num_rows, "test has no rows containing a comma in column $splitCols[0]");

        $result = $this->db
            ->query('SELECT * FROM `' . $this->table1 . '`');
        $this->assertEquals(6, $result->num_rows, "not the right number of rows");

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $this->table1,
                __DIR__ . '/' . $this->localFileCharacterMultipleColumnsDesired,
                [],
                "\t"
            );
    }

    public function test_getRemoveLaterDuplicatesSqlText_table1(){
        $duplicateColumn="text1";
        $this->assertFilesExistLocally([__DIR__.'/'.$this->file]);
        $this->assertFilesExistOnSqlServer([$this->localFileRemoveLaterDuplicatesDesired]);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table1` LIMIT 1")->num_rows);
        $query=ManyManyImporter::getSqlText(
            100,
            'link',
            '`table1_id`,`table2_id`',
            $this->table1,
            '`id`,`text1`,`text1b`,`text1c`',
            'NEW.`id`,NEW.`text1`,new.`text1b`,new.`text1c`',
            $this->table2,
            'id,text2',
            'NEW.`id`,NEW.`text2`',
            $this->file,
            '`text2`,`text1`,`text1b`,`text1c`'
        )
            .ManyManyDatabaseEditor::getRemoveLaterDuplicatesSqlText(
                $this->table1,
                $duplicateColumn,
                'id',
                true
            );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }

        $this->assertEquals(
            0,
            $this->db
                ->query("SELECT `$duplicateColumn`, COUNT(*) c FROM `$this->table1` GROUP BY `$duplicateColumn` HAVING c > 1;")
                ->num_rows,
            'duplicates still exist'
        );
        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $this->table1,
                __DIR__ . '/' . $this->localFileRemoveLaterDuplicatesDesired,
                [],
                "\t"
            );
    }

    public function test_getSplitRecordsWithCommaColumnSqlText_table2(): void
    {
        $splitCol = 'text1b';
        $this->assertFilesExistLocally([__DIR__.'/'.$this->file]);
        $this->assertFilesExistOnSqlServer([$this->localFileCommasDesired]);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table1` LIMIT 1")->num_rows);
        $query=ManyManyImporter::getSqlText(
            100,
            'link',
            '`table1_id`,`table2_id`',
            $this->table1,
            '`id`,`text1`,`text1b`,`text1c`',
            'NEW.`id`,NEW.`text1`,new.`text1b`,new.`text1c`',
            $this->table2,
            'id,text2',
            'NEW.`id`,NEW.`text2`',
            $this->file,
            '`text2`,`text1`,`text1b`,`text1c`'
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }

        $query = ManyManyDatabaseEditor::getSplitRecordsWithCommasSqlText(
            $this->table1,
            $splitCol,
            '`text1`,`text1c`',
            '',
            'temp',
            100
        );
        $result = $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()) {
            ;
        }

        $result = $this->db
            ->query('SELECT * FROM `' . $this->table1 . '` WHERE `' . $splitCol . '` LIKE "%,%"');
        $this->assertEquals(0, $result->num_rows, "test has no rows containing a comma in column $splitCol");

        $result = $this->db
            ->query('SELECT * FROM `' . $this->table1 . '`');
        $this->assertEquals(9, $result->num_rows, "not the right number of rows");

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $this->table1,
                __DIR__ . '/' . $this->localFileCommasDesired,
                [],
                "\t"
            );
    }

    public function test_getSplitRecordsWithCommasMultipleColumnsSqlText_table2(): void
    {
        $splitCols = ['text1b', 'text1c'];
        $this->assertFilesExistLocally([__DIR__.'/'.$this->fileSimple]);
        $this->assertFilesExistOnSqlServer([$this->localFileCommaMultipleColumnsDesired]);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table1` LIMIT 1")->num_rows);
        $query=ManyManyImporter::getSqlText(
            100,
            'link',
            '`table1_id`,`table2_id`',
            $this->table1,
            '`id`,`text1`,`text1b`,`text1c`',
            'NEW.`id`,NEW.`text1`,new.`text1b`,new.`text1c`',
            $this->table2,
            'id,text2',
            'NEW.`id`,NEW.`text2`',
            $this->fileSimple,
            '`text2`,`text1`,`text1b`,`text1c`'
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }

        $query = ManyManyDatabaseEditor::getSplitRecordsWithMultipleCommaColumnsSqlText(
            $this->table1,
            $splitCols,
            '`text1`',
            '',
            'temp',
            100
        );
        $result = $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()) {
            ;
        }

        $result = $this->db
            ->query('SELECT * FROM `' . $this->table1 . '` WHERE `' . $splitCols[0] . '` LIKE "%,%"');
        $this->assertEquals(0, $result->num_rows, "test has no rows containing a comma in column $splitCols[0]");

        $result = $this->db
            ->query('SELECT * FROM `' . $this->table1 . '`');
        $this->assertEquals(6, $result->num_rows, "not the right number of rows");

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $this->table1,
                __DIR__ . '/' . $this->localFileCommaMultipleColumnsDesired,
                [],
                "\t"
            );
    }

    public function test_getSplitRecordsWithCharacterColumnSqlText_table2(): void
    {
        $splitCol = 'text1b';
        $this->assertFilesExistLocally([__DIR__.'/'.$this->file]);
        $this->assertFilesExistOnSqlServer([$this->localFileCharacterDesired]);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table1` LIMIT 1")->num_rows);
        $query=ManyManyImporter::getSqlText(
            100,
            'link',
            '`table1_id`,`table2_id`',
            $this->table1,
            '`id`,`text1`,`text1b`,`text1c`',
            'NEW.`id`,NEW.`text1`,new.`text1b`,new.`text1c`',
            $this->table2,
            'id,text2',
            'NEW.`id`,NEW.`text2`',
            $this->fileSimple,
            '`text2`,`text1`,`text1b`,`text1c`'
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }

        $query = ManyManyDatabaseEditor::getSplitRecordsWithCharacterSqlText(
            $this->table1,
            $splitCol,
            '`text1`,`text1c`',
            ';',
            '',
            'temp',
            100
        );
        $result = $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()) {
            ;
        }

        $result = $this->db
            ->query('SELECT * FROM `' . $this->table1 . '` WHERE `' . $splitCol . '` LIKE "%;%"');
        $this->assertEquals(0, $result->num_rows, "test has no rows containing a comma in column $splitCol");

        $result = $this->db
            ->query('SELECT * FROM `' . $this->table1 . '`');
        $this->assertEquals(9, $result->num_rows, "not the right number of rows");

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $this->table1,
                __DIR__ . '/' . $this->localFileCharacterDesired,
                [],
                "\t"
            );
    }

    public function test_getSplitRecordsWithCharacterMultipleColumnsSqlText_table2(): void
    {
        $splitCols = ['text1b', 'text1c'];
        $this->assertFilesExistLocally([__DIR__.'/'.$this->fileSimple]);
        $this->assertFilesExistOnSqlServer([$this->localFileCharacterMultipleColumnsDesired]);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table1` LIMIT 1")->num_rows);
        $query=ManyManyImporter::getSqlText(
            100,
            'link',
            '`table1_id`,`table2_id`',
            $this->table1,
            '`id`,`text1`,`text1b`,`text1c`',
            'NEW.`id`,NEW.`text1`,new.`text1b`,new.`text1c`',
            $this->table2,
            'id,text2',
            'NEW.`id`,NEW.`text2`',
            $this->fileSimple,
            '`text2`,`text1`,`text1b`,`text1c`'
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }

        $query = ManyManyDatabaseEditor::getSplitRecordsWithMultipleCharacterColumnsSqlText_table2(
            $this->table1,
            $splitCols,
            '`text1`',
            ';',
            '',
            'temp',
            100
        );
        $result = $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()) {
            ;
        }

        $result = $this->db
            ->query('SELECT * FROM `' . $this->table1 . '` WHERE `' . $splitCols[0] . '` LIKE "%;%"');
        $this->assertEquals(0, $result->num_rows, "test has no rows containing a comma in column $splitCols[0]");

        $result = $this->db
            ->query('SELECT * FROM `' . $this->table1 . '`');
        $this->assertEquals(6, $result->num_rows, "not the right number of rows");

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $this->table1,
                __DIR__ . '/' . $this->localFileCharacterMultipleColumnsDesired,
                [],
                "\t"
            );
    }

    public function test_getRemoveLaterDuplicatesSqlText_table2(){
        $duplicateColumn="text1";
        $this->assertFilesExistLocally([__DIR__.'/'.$this->fileSimple]);
        $this->assertFilesExistOnSqlServer([$this->localFileRemoveLaterDuplicatesDesired]);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table1` LIMIT 1")->num_rows);
        $query=ManyManyImporter::getSqlText(
            100,
            'link',
            '`table1_id`,`table2_id`',
            $this->table1,
            '`id`,`text1`,`text1b`,`text1c`',
            'NEW.`id`,NEW.`text1`,new.`text1b`,new.`text1c`',
            $this->table2,
            'id,text2',
            'NEW.`id`,NEW.`text2`',
            $this->fileSimple,
            '`text2`,`text1`,`text1b`,`text1c`'
        )
            .ManyManyDatabaseEditor::getRemoveLaterDuplicatesSqlText(
                $this->table1,
                $duplicateColumn,
                'id',
                true
            );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }

        $this->assertEquals(
            0,
            $this->db
                ->query("SELECT `$duplicateColumn`, COUNT(*) c FROM `$this->table1` GROUP BY `$duplicateColumn` HAVING c > 1;")
                ->num_rows,
            'duplicates still exist'
        );
        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $this->table1,
                __DIR__ . '/' . $this->localFileRemoveLaterDuplicatesDesired,
                [],
                "\t"
            );
    }
}