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

    protected string $file='/var/lib/mysql-files/many-many-simple.tsv';
    protected string $localFile='../mysql-files/many-many.tsv';
    protected string $fileCommaColumn = '/var/lib/mysql-files/many-many-editor-commas.tsv';
    protected string $localFileCommasDesired = '../mysql-files/many-many-editor-commas-desired.tsv';
    protected string $localFileCommasDesiredLink = '../mysql-files/many-many-editor-commas-desired-link.tsv';
    protected string $fileSplitMultipleColumns = '/var/lib/mysql-files/many-many-editor-split-multiple-columns.tsv';
    protected string $localFileSplitMultipleColumnsDesired = '../mysql-files/many-many-editor-split-multiple-columns-desired.tsv';
    protected string $localFileSplitMultipleColumnsDesiredLink = '../mysql-files/many-many-editor-split-multiple-columns-desired-link.tsv';
    protected string $fileCharacterColumn = '/var/lib/mysql-files/many-many-editor-character.tsv';
    protected string $localFileCharacterDesired = '../mysql-files/many-many-editor-character-desired.tsv';
    protected string $localFileCharacterDesiredLink = '../mysql-files/many-many-editor-character-desired-link.tsv';
    protected string $fileRemoveLaterDuplicates = '/var/lib/mysql-files/many-many-editor-remove-later-duplicates.tsv';
    protected string $localFileRemoveLaterDuplicatesDesired1 = '../mysql-files/many-many-editor-remove-later-duplicates-desired1.tsv';
    protected string $localFileRemoveLaterDuplicatesDesiredLink1 = '../mysql-files/many-many-editor-remove-later-duplicates-desired-link1.tsv';
    protected string $localFileRemoveLaterDuplicatesDesired2 = '../mysql-files/many-many-editor-remove-later-duplicates-desired2.tsv';
    protected string $localFileRemoveLaterDuplicatesDesiredLink2 = '../mysql-files/many-many-editor-remove-later-duplicates-desired-link2.tsv';
    protected string $localFileRemoveLaterDuplicatesDesired3 = '../mysql-files/many-many-editor-remove-later-duplicates-desired3.tsv';
    protected string $localFileRemoveLaterDuplicatesDesiredLink3 = '../mysql-files/many-many-editor-remove-later-duplicates-desired-link3.tsv';


    protected string $linkTable = 'link';
    protected array $allLinkTableColumns = ['table1_id','table2_id','table3_id','table4_id'];
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
        $result = $this->db
            ->query("SELECT * FROM $this->table1 LIMIT 1")
            ->fetch_assoc();
        $this->assertFalse(key_exists('old_id',$result));
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
        $this->assertEquals(
            5,
            $this->db->query('SHOW TABLES')->num_rows,
            "check number of tables in '".(new DatabaseHelper())->getDatabaseName()."' database'"
        );
    }
    public function test_getSplitRecordsWithCommaColumnSqlText_table1(): void
    {
        $splitCol = 'text1';
        $linkTableLinkColumn='table1_id';
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
            $this->allLinkTableColumns,
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
        $query=misc::getTableColumnNamesStringExcluding($this->linkTable, [$linkTableLinkColumn,'id'],'`,`');
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
        $result = $this->db
            ->query("SELECT * FROM $this->table1 LIMIT 1")
            ->fetch_assoc();
        $this->assertFalse(key_exists('old_id',$result));
    }

    public function test_getSplitRecordsWithCharacterColumnSqlText_table1(): void
    {
        $splitCharacter=';';

        $this->assertFilesExistLocally([
            __DIR__.'/'.$this->localFileCharacterDesired,
            __DIR__.'/'.$this->localFileCharacterDesiredLink
        ]);
        $this->assertFilesExistOnSqlServer([$this->fileCharacterColumn]);

        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table1` LIMIT 1")->num_rows);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table2` LIMIT 1")->num_rows);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table3` LIMIT 1")->num_rows);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table4` LIMIT 1")->num_rows);

        $query=ManyManyImporter::getComplexImportSqlText(
            100,
            $this->linkTable,
            $this->allLinkTableColumns,
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
        $query=misc::getTableColumnNamesStringExcluding($this->linkTable, [$linkTableLinkColumn,'id'],'`,`');
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

    public function test_getSplitMultipleColumnsSqlText(): void
    {
        $splitCharacterArray=[',',';'];

        $this->assertFilesExistLocally([
            __DIR__.'/'.$this->localFileSplitMultipleColumnsDesired,
            __DIR__.'/'.$this->localFileSplitMultipleColumnsDesiredLink
        ]);
        $this->assertFilesExistOnSqlServer([$this->fileSplitMultipleColumns]);

        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table1` LIMIT 1")->num_rows);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table2` LIMIT 1")->num_rows);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table3` LIMIT 1")->num_rows);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table4` LIMIT 1")->num_rows);

        $query=ManyManyImporter::getComplexImportSqlText(
            100,
            $this->linkTable,
            $this->allLinkTableColumns,
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
            $this->fileSplitMultipleColumns,
            '`text1`,`text1b`,`text1c`,`text2`,`text2b`,`text3`,`text3b`,`text4`,`text4b`',
            1,
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            // needed to run the multi_query
        }

        $splitColsArray = ['text1','text1b'];
        $linkTableLinkColumn='table1_id';
        $query=misc::getTableColumnNamesStringExcluding($this->linkTable, [$linkTableLinkColumn,'id'],'`,`');
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
        $query = ManyManyDatabaseEditor::getSplitRecordsWithMultipleColumnsSqlText(
            $this->table1,
            'id',
            $this->linkTable,
            $linkTableLinkColumn,
            '`'.$remainingColumnsInLinkTable[0].'`',
            $splitColsArray,
            '`text1c`',
            $splitCharacterArray,
            '',
            'temp',
            1000
        );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()) {
            // needed to run the multi_query
        }

        for ($i = 0; $i < $this->count($splitColsArray); $i++) {
            $result = $this->db
                ->query('SELECT * FROM `' . $this->table1 . '` WHERE `' . $splitColsArray[$i] . '` LIKE "%'.$splitCharacterArray[$i].'%"');
            $this->assertEquals(0, $result->num_rows, "test has no rows containing a '$splitCharacterArray[$i]' in column `$splitColsArray[$i]`");
        }
        $this->assertEquals(
            0,
            $this->db
                ->query((new misc())->getAllDuplicatesForColumnsSqlText('link',$this->allLinkTableColumns))
                ->num_rows,
            'duplicates still exist in table "link". '
        );
        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $this->table1,
                __DIR__ . '/' . $this->localFileSplitMultipleColumnsDesired,
                [],
                "\t"
            )
            ->compareTableToCsvData(
                $this->linkTable,
                __DIR__ . '/' . $this->localFileSplitMultipleColumnsDesiredLink,
                [],
                "\t"
            );
        $result = $this->db
            ->query("SELECT * FROM $this->table1 LIMIT 1")
            ->fetch_assoc();
        $this->assertFalse(key_exists('old_id',$result));
    }

    public function test_getRemoveLaterDuplicatesSqlText(){
        $duplicateColumn="text1";
        $this->assertFilesExistLocally([
            __DIR__.'/'.$this->localFileRemoveLaterDuplicatesDesired1,
            __DIR__.'/'.$this->localFileRemoveLaterDuplicatesDesiredLink1
        ]);
        $this->assertFilesExistOnSqlServer([$this->fileRemoveLaterDuplicates]);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table1` LIMIT 1")->num_rows);
        $query=ManyManyImporter::getSimpleImportSqlText(
            100,
            'link',
            '`table1_id`,`table2_id`',
            $this->table1,
            '`id`,`text1`,`text1b`,`text1c`',
            'NEW.`id`,NEW.`text1`,new.`text1b`,new.`text1c`',
            $this->table2,
            'id,text2',
            'NEW.`id`,NEW.`text2`',
            $this->fileRemoveLaterDuplicates,
            '`text2`,`text1`,`text1b`,`text1c`'
        )
        ."\n-- insert duplicate rows\n"
            .(new misc())->getInsertAllWhereInArrayWithDuplicatesSqlText(
                'link',
                $this->allLinkTableColumns,
                'link',
                $this->allLinkTableColumns,
                'id',
                [1,4,3,3,2,4]
            );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }
        $query=ManyManyDatabaseEditor::getReassignAndRemoveDuplicatesSqlText(
                ['text1'],
                $this->table1,
                'id',
                $this->linkTable,
                'table1_id',
                ['table2_id','table3_id','table4_id'],
                'id',
                true,
                'temp'
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
        $this->assertEquals(
            0,
            $this->db
                ->query((new misc())->getAllDuplicatesForColumnsSqlText('link',$this->allLinkTableColumns))
                ->num_rows,
            'duplicates still exist in table "link". '
        );
        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $this->table1,
                __DIR__ . '/' . $this->localFileRemoveLaterDuplicatesDesired1,
                [],
                "\t"
            )
            ->compareTableToCsvData(
                $this->linkTable,
                __DIR__ . '/' . $this->localFileRemoveLaterDuplicatesDesiredLink1,
                [],
                "\t"
            );
    }

    public function test_getRemoveLaterDuplicatesSqlText_2(){
        $duplicateColumns=['text1','text1b'];
        $this->assertFilesExistLocally([
            __DIR__.'/'.$this->localFileRemoveLaterDuplicatesDesired2,
            __DIR__.'/'.$this->localFileRemoveLaterDuplicatesDesiredLink2
        ]);
        $this->assertFilesExistOnSqlServer([$this->fileRemoveLaterDuplicates]);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table1` LIMIT 1")->num_rows);
        $query=ManyManyImporter::getSimpleImportSqlText(
            100,
            'link',
            '`table1_id`,`table2_id`',
            $this->table1,
            '`id`,`text1`,`text1b`,`text1c`',
            'NEW.`id`,NEW.`text1`,new.`text1b`,new.`text1c`',
            $this->table2,
            'id,text2',
            'NEW.`id`,NEW.`text2`',
            $this->fileRemoveLaterDuplicates,
            '`text2`,`text1`,`text1b`,`text1c`'
        )
            ."\n-- insert duplicate rows\n"
            .(new misc())->getInsertAllWhereInArrayWithDuplicatesSqlText(
                'link',
                $this->allLinkTableColumns,
                'link',
                $this->allLinkTableColumns,
                'id',
                [1,4,3,3,2,4]
            );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }
        $query=ManyManyDatabaseEditor::getReassignAndRemoveDuplicatesSqlText(
            $duplicateColumns,
            $this->table1,
            'id',
            $this->linkTable,
            'table1_id',
            ['table2_id','table3_id','table4_id'],
            'id',
            true,
            'temp'
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }
        $this->assertEquals(
            0,
            $this->db
                ->query((new misc())->getAllDuplicatesForColumnsSqlText($this->table1, $duplicateColumns))
                ->num_rows,
            'duplicates still exist in table "$this->table1". '
        );
        $this->assertEquals(
            0,
            $this->db
                ->query((new misc())->getAllDuplicatesForColumnsSqlText('link',$this->allLinkTableColumns))
                ->num_rows,
            'duplicates still exist in table "link". '
        );
        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $this->table1,
                __DIR__ . '/' . $this->localFileRemoveLaterDuplicatesDesired2,
                [],
                "\t"
            )
            ->compareTableToCsvData(
                $this->linkTable,
                __DIR__ . '/' . $this->localFileRemoveLaterDuplicatesDesiredLink2,
                [],
                "\t"
            );
    }

    public function test_getRemoveLaterDuplicatesSqlText_3(){
        $duplicateColumns=['text1','text1b','text1c'];
        $this->assertFilesExistLocally([
            __DIR__.'/'.$this->localFileRemoveLaterDuplicatesDesired3,
            __DIR__.'/'.$this->localFileRemoveLaterDuplicatesDesiredLink3
        ]);
        $this->assertFilesExistOnSqlServer([$this->fileRemoveLaterDuplicates]);
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$this->table1` LIMIT 1")->num_rows);
        $query=ManyManyImporter::getSimpleImportSqlText(
            100,
            'link',
            '`table1_id`,`table2_id`',
            $this->table1,
            '`id`,`text1`,`text1b`,`text1c`',
            'NEW.`id`,NEW.`text1`,new.`text1b`,new.`text1c`',
            $this->table2,
            'id,text2',
            'NEW.`id`,NEW.`text2`',
            $this->fileRemoveLaterDuplicates,
            '`text2`,`text1`,`text1b`,`text1c`'
        )
            ."\n-- insert duplicate rows\n"
            .(new misc())->getInsertAllWhereInArrayWithDuplicatesSqlText(
                'link',
                $this->allLinkTableColumns,
                'link',
                $this->allLinkTableColumns,
                'id',
                [1,4,3,3,2,4]
            );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }
        $query=ManyManyDatabaseEditor::getReassignAndRemoveDuplicatesSqlText(
            $duplicateColumns,
            $this->table1,
            'id',
            $this->linkTable,
            'table1_id',
            ['table2_id','table3_id','table4_id'],
            'id',
            true,
            'temp'
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }
        $this->assertEquals(
            0,
            $this->db
                ->query((new misc())->getAllDuplicatesForColumnsSqlText($this->table1,$duplicateColumns))
                ->num_rows,
            'duplicates still exist in table "link". '
        );
        $this->assertEquals(
            0,
            $this->db
                ->query((new misc())->getAllDuplicatesForColumnsSqlText('link',$this->allLinkTableColumns))
                ->num_rows,
            'duplicates still exist in table "link". '
        );

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $this->table1,
                __DIR__ . '/' . $this->localFileRemoveLaterDuplicatesDesired3,
                [],
                "\t"
            )
            ->compareTableToCsvData(
                $this->linkTable,
                __DIR__ . '/' . $this->localFileRemoveLaterDuplicatesDesiredLink3,
                [],
                "\t"
            );
    }
}