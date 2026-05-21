<?php

namespace PaulMillband\SqlLibrary\tests\Editor;

use mysqli;
use PaulMillband\SqlLibrary\Editor\SimpleDatabaseEditor;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseHelper;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseSqlHelper;
use PaulMillband\SqlLibrary\tests\Helper\FileHelper;
use PaulMillband\SqlLibrary\tests\TestLibrary\DataTestLibrary;
use PaulMillband\SqlLibrary\tests\TestCase;
use PaulMillband\SqlLibrary\Importer\SimpleImporter;

class SimpleDatabaseEditorTest extends TestCase
{
    protected mysqli $db;
    protected string $file = '/tmp/data/simple.tsv';
    protected string $fileCommaColumn = '/tmp/data/editor-commas.tsv';
    protected string $localFileCommasDesired = '../data/editor-commas-desired.tsv';
    protected string $fileCommaMultipleColumns = '/tmp/data/editor-comma-multiple-columns.tsv';
    protected string $localFileCommaMultipleColumnsDesired = '../data/editor-comma-multiple-columns-desired.tsv';
    protected string $fileCharacterColumn = '/tmp/data/editor-character.tsv';
    protected string $localFileCharacterDesired = '../data/editor-character-desired.tsv';
    protected string $fileCharacterMultipleColumns = '/tmp/data/editor-character-multiple-columns.tsv';
    protected string $localFileCharacterMultipleColumnsDesired = '../data/editor-character-multiple-columns-desired.tsv';
    protected string $fileAsPrevious = '/tmp/data/editor-set-column-as-previous.tsv';
    protected string $localFileAsPreviousDesired = '../data/editor-set-column-as-previous-desired.tsv';
    protected string $localFileAsPreviousDescDesired = '../data/editor-set-column-as-previous-desc-desired.tsv';
    protected string $localFileTriggerDesired = '../data/editor-trigger-desired.tsv';
    protected string $localFileTriggerIfElseDesired = '../data/editor-trigger-if-else-desired.tsv';
    protected string $fileRemoveLaterDuplicates = '/tmp/data/editor-remove-later-duplicates.tsv';
    protected string $localFileRemoveLaterDuplicatesDesired = '../data/editor-remove-later-duplicates-desired.tsv';

    protected function setUp(): void
    {
        parent::setUp();
        $localFiles = [
            __DIR__.'/'.$this->localFileCommasDesired,
            __DIR__.'/'.$this->localFileCommaMultipleColumnsDesired,
            __DIR__.'/'.$this->localFileCharacterDesired,
            __DIR__.'/'.$this->localFileCharacterMultipleColumnsDesired,
            __DIR__.'/'.$this->localFileAsPreviousDesired,
            __DIR__.'/'.$this->localFileAsPreviousDescDesired,
            __DIR__.'/'.$this->localFileTriggerDesired,
            __DIR__.'/'.$this->localFileTriggerIfElseDesired,
            __DIR__.'/'.$this->localFileRemoveLaterDuplicatesDesired
        ];
        $remoteFiles = [
            $this->file,
            $this->fileCommaColumn,
            $this->fileCommaMultipleColumns,
            $this->fileCharacterColumn,
            $this->fileCharacterMultipleColumns,
            $this->fileAsPrevious,
            $this->fileRemoveLaterDuplicates
        ];
        $this->assertFilesExistLocally($localFiles);
        $this->assertFilesExistOnSqlServer($remoteFiles);
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
    public function test_getSplitRecordsWithCommaColumnSqlText(): void
    {
        $table = 'table1';
        $splitCol = 'text1b';

        $this->assertEquals(0, $this->db->query("SELECT * FROM `$table` LIMIT 1")->num_rows);
        $query = SimpleImporter::getSqlText(
            $table,
            $this->fileCommaColumn,
            '`text1`,`text1b`,`text1c`',
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }

        $query = SimpleDatabaseEditor::getSplitRecordsWithCommasSqlText(
            $table,
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
            ->query('SELECT * FROM `' . $table . '` WHERE `' . $splitCol . '` LIKE "%,%"');
        $this->assertEquals(0, $result->num_rows, "test has no rows containing a comma in column $splitCol");

        $result = $this->db
            ->query('SELECT * FROM `' . $table . '`');
        $this->assertEquals(9, $result->num_rows, "not the right number of rows");

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $table,
                __DIR__ . '/' . $this->localFileCommasDesired,
                [],
                "\t"
            );
    }

    public function test_getSplitRecordsWithCommasMultipleColumnsSqlText(): void
    {
        $table = 'table1';
        $splitCols = ['text1b', 'text1c'];
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$table` LIMIT 1")->num_rows);
        $query = SimpleImporter::getSqlText(
            $table,
            $this->fileCommaMultipleColumns,
            '`text1`,`text1b`,`text1c`',
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }

        $query = SimpleDatabaseEditor::getSplitRecordsWithMultipleCommaColumnsSqlText(
            $table,
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
            ->query('SELECT * FROM `' . $table . '` WHERE `' . $splitCols[0] . '` LIKE "%,%"');
        $this->assertEquals(0, $result->num_rows, "test has no rows containing a comma in column $splitCols[0]");

        $result = $this->db
            ->query('SELECT * FROM `' . $table . '`');
        $this->assertEquals(6, $result->num_rows, "not the right number of rows");

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $table,
                __DIR__ . '/' . $this->localFileCommaMultipleColumnsDesired,
                [],
                "\t"
            );
    }

    public function test_getSplitRecordsWithCharacterColumnSqlText(): void
    {
        $table = 'table1';
        $splitCol = 'text1b';

        $this->assertEquals(0, $this->db->query("SELECT * FROM `$table` LIMIT 1")->num_rows);
        $query = SimpleImporter::getSqlText(
            $table,
            $this->fileCharacterColumn,
            '`text1`,`text1b`,`text1c`',
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }

        $query = SimpleDatabaseEditor::getSplitRecordsWithCharacterSqlText(
            $table,
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
            ->query('SELECT * FROM `' . $table . '` WHERE `' . $splitCol . '` LIKE "%;%"');
        $this->assertEquals(0, $result->num_rows, "test has no rows containing a comma in column $splitCol");

        $result = $this->db
            ->query('SELECT * FROM `' . $table . '`');
        $this->assertEquals(9, $result->num_rows, "not the right number of rows");

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $table,
                __DIR__ . '/' . $this->localFileCharacterDesired,
                [],
                "\t"
            );
    }

    public function test_getSplitRecordsWithCharacterMultipleColumnsSqlText(): void
    {
        $table = 'table1';
        $splitCols = ['text1b', 'text1c'];
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$table` LIMIT 1")->num_rows);
        $query = SimpleImporter::getSqlText(
            $table,
            $this->fileCharacterMultipleColumns,
            '`text1`,`text1b`,`text1c`',
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }

        $query = SimpleDatabaseEditor::getSplitRecordsWithMultipleCharacterColumnsSqlText(
            $table,
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
            ->query('SELECT * FROM `' . $table . '` WHERE `' . $splitCols[0] . '` LIKE "%;%"');
        $this->assertEquals(0, $result->num_rows, "test has no rows containing a comma in column $splitCols[0]");

        $result = $this->db
            ->query('SELECT * FROM `' . $table . '`');
        $this->assertEquals(6, $result->num_rows, "not the right number of rows");

        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $table,
                __DIR__ . '/' . $this->localFileCharacterMultipleColumnsDesired,
                [],
                "\t"
            );
    }

    public function test_getSetColumnValueAsLastValueWhenNotSetSqlText()
    {
        $table = 'table1';
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$table` LIMIT 1")->num_rows);
        $query = SimpleImporter::getSqlText(
            $table,
            $this->fileAsPrevious,
            '`text1`,`text1b`,`text1c`',
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }

        $query = SimpleDatabaseEditor::getSetColumnValueAsLastValueWhenNotSetSqlText(
            $table,
            'text1b',
            'id',
            true,
            'temp'
        );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()) {
            ;
        }
        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $table,
                __DIR__ . '/' . $this->localFileAsPreviousDesired,
                [],
                "\t"
            );
    }

    public function test_Desc_getSetColumnValueAsLastValueWhenNotSetSqlText()
    {
        $table = 'table1';
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$table` LIMIT 1")->num_rows);
        $query = SimpleImporter::getSqlText(
            $table,
            $this->fileAsPrevious,
            '`text1`,`text1b`,`text1c`',
        );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }

        $query = SimpleDatabaseEditor::getSetColumnValueAsLastValueWhenNotSetSqlText(
            $table,
            'text1b',
            'id',
            false,
            'temp'
        );
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        while ($this->db->next_result()) {
            ;
        }
        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $table,
                __DIR__ . '/' . $this->localFileAsPreviousDescDesired,
                [],
                "\t"
            );
    }

    public function test_getTriggerSqlText()
    {
        $table = 'table1';
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$table` LIMIT 1")->num_rows);
        $query = SimpleDatabaseEditor::getTriggerSqlText(
                $table,
                'testTrigger',
            'SET new.`text1c` = concat(new.`text1c`, "banana");'
            ).
            SimpleImporter::getSqlText(
                $table,
                $this->file,
                '`text1`,`text1b`,`text1c`',
            );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }
        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $table,
                __DIR__ . '/' . $this->localFileTriggerDesired,
                [],
                "\t"
            );
    }

    public function test_getTriggerIfElseSqlText()
    {
        $table = 'table1';
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$table` LIMIT 1")->num_rows);
        $query = SimpleDatabaseEditor::getTriggerIfElseSqlText(
                $table,
                'testTrigger',
                'new.`text1`="test1"',
                'SET new.`text1c` = "no.1";',
                'SET new.`text1c` = "other";'
            ).
            SimpleImporter::getSqlText(
                $table,
                $this->file,
                '`text1`,`text1b`,`text1c`',
            );
        $this->db->multi_query($query);
        while ($this->db->next_result()) {
            ;
        }
        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $table,
                __DIR__ . '/' . $this->localFileTriggerIfElseDesired,
                [],
                "\t"
            );
    }

    public function test_getRemoveLaterDuplicatesSqlText(){
        $table = 'table1';
        $duplicateColumn="text1";
        $this->assertEquals(0, $this->db->query("SELECT * FROM `$table` LIMIT 1")->num_rows);
        $query = SimpleImporter::getSqlText(
                $table,
                $this->fileRemoveLaterDuplicates,
                '`text1`,`text1b`,`text1c`',
                1,
                '\t',
                ''
            )
            .SimpleDatabaseEditor::getRemoveLaterDuplicatesSqlText(
                $table,
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
                ->query("SELECT `$duplicateColumn`, COUNT(*) c FROM `$table` GROUP BY `$duplicateColumn` HAVING c > 1;")
                ->num_rows,
            'duplicates still exist'
        );
        (new DataTestLibrary($this->name()))
            ->compareTableToCsvData(
                $table,
                __DIR__ . '/' . $this->localFileRemoveLaterDuplicatesDesired,
                [],
                "\t"
            );
    }
}