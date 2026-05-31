<?php

namespace PaulMillband\SqlLibrary\Importer;

class OneManyImporter
{
    /**
     * generate SQL to import a tsv or csv into a one-many database as two tables
     *
     * @param string $table1 string e.g. 'table1'
     * @param string $columnsForTable1 e.g. '`column1`,`column2`'
     * @param string $valueColumnsForTable1 e.g. 'NEW.`column1`,NEW.`column2`'
     * @param string $table2 e.g. 'table2'
     * @param string $columnsForTable2 e.g. '`table1_id`,`column3`,`column4`'
     * @param string $valueColumnsForTable2 e.g. 'id, NEW.`column3`,NEW.`column4`'
     * @param string $linkColumnForTable2,e.g. `column2`'
     * @param string $onDuplicateText how to fill columns when duplicate e.g. '`text1` = VALUES(`text1`), `text1b` = VALUES(`text1b`)'
     * @param string $filePath file path e.g. '__DIR__./src/test.tsv'
     * @param string $fileColumns e.g. 'column1,column2'
     * @param int $ignoreLinesQty No. of lines to ignore in the file e.g. for the header
     * @param string $fileDelimiter e.g. '\t'
     * @param string $additionalFileImportCommand e.g. "set simple = REGEXP_REPLACE(Word,'[1234]*','')"
     * @param string $tempTable e.g. 'temp'
     * @return string
     */
    static function getSqlText(
        string $table1,
        string $columnsForTable1,
        string $valueColumnsForTable1,
        string $table2,
        string $columnsForTable2,
        string $valueColumnsForTable2,
        string $linkColumnForTable2,
        string $onDuplicateText,
        string $filePath,
        string $fileColumns,
        int    $ignoreLinesQty=0,
        string $fileDelimiter='\t',
        string $additionalFileImportCommand='',
        string $tempTable='temp'
    )
    {
        $ignoreLinesText='';
        if($ignoreLinesQty>0){
            $ignoreLinesText="IGNORE $ignoreLinesQty LINES\n";
        }
        return <<<SQL
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table1`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table2`;

        CREATE TABLE `$tempTable` AS (
                SELECT *
                FROM `$table1`
                NATURAL JOIN `$table2`
                LIMIT 0
        );

        CREATE TRIGGER `from_load_data_to_table1` AFTER INSERT ON `$tempTable`
        FOR EACH ROW
           INSERT INTO `$table1` ($columnsForTable1)
             VALUES ($valueColumnsForTable1)
             ON DUPLICATE KEY UPDATE $onDuplicateText;

        CREATE TRIGGER `from_load_data_to_table2` AFTER INSERT ON `$tempTable`
        FOR EACH ROW
           INSERT INTO `$table2` ($columnsForTable2)
             SELECT $valueColumnsForTable2
             FROM `$table1`
             WHERE `$linkColumnForTable2` = NEW.`$linkColumnForTable2`;

        LOAD DATA INFILE '$filePath'
        IGNORE INTO TABLE `$tempTable`
        FIELDS TERMINATED BY '$fileDelimiter'
        $ignoreLinesText($fileColumns)
        $additionalFileImportCommand;
        
    # cleanup
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table1`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table2`;
SQL;
    }

    /**
     * generate SQL to import a tsv or csv into a one-many database
     *
     * @param string $destinationTable string e.g. 'table1'
     * @param string $columnsForTable e.g. '`column1`,`column2`'
     * @param string $valueColumnsForTable e.g. 'NEW.`column1`,NEW.`column2`
     * @param string $referenceTable string e.g. 'table1'
     * @param string $linkColumnInTable2,e.g. `column2`'
     * @param string $onDuplicateText how to fill columns when duplicate e.g. '`text1` = VALUES(`text1`), `text1b` = VALUES(`text1b`)'
     * @param string $filePath file path e.g. '__DIR__./src/test.tsv'
     * @param string $fileColumns e.g. 'column1,column2'
     * @param int $ignoreLinesQty No. of lines to ignore in the file e.g. for the header
     * @param string $fileDelimiter e.g. '\t'
     * @param string $additionalFileImportCommand e.g. "set simple = REGEXP_REPLACE(Word,'[1234]*','')"
     * @param string $tempTable e.g. 'temp'
     * @return string
     */
    static function getAddNewTableSql(
        string $destinationTable,
        string $referenceTable,
        string $fileLinkToReferenceTable,
        string $referenceTableLinkToFile,
        string $destinationTableLinkToReferenceTable,
        string $referenceTableLinkToDestinationTable,
        string $filePath,
        string $fileColumns,
        bool   $fileLinkColumnExists = false,
        int    $ignoreLinesQty=0,
        string $fileDelimiter='\t',
        string $additionalFileImportCommand='',
        string $tempTable='temp'
    )
    {
        $ignoreLinesText='';
        if($ignoreLinesQty>0){
            $ignoreLinesText="IGNORE $ignoreLinesQty LINES\n";
        }
        $selections = "d.*";
        $dropfileLinkColumn='';
        if($fileLinkColumnExists === false){
            // not wanted in destination folder
            $dropfileLinkColumn="ALTER TABLE `$tempTable` DROP COLUMN `$fileLinkToReferenceTable`;";

            // adds a column with the column type of $referenceTableLinkToFile, but named $fileLinkToReferenceTable
            $selections = "d.*, r.`$referenceTableLinkToFile` As `$fileLinkToReferenceTable`";
        }
        return <<<SQL
        DROP TABLE IF EXISTS `$tempTable`;

        CREATE TABLE `$tempTable` AS (
            SELECT $selections
            FROM `$destinationTable` d
            NATURAL JOIN `$referenceTable` r
            LIMIT 0
        );
        
        LOAD DATA INFILE '$filePath'
            IGNORE INTO TABLE `$tempTable`
            FIELDS TERMINATED BY '$fileDelimiter'
            $ignoreLinesText($fileColumns)
            $additionalFileImportCommand;

        UPDATE `$tempTable` t
            INNER JOIN `$referenceTable` r
                ON t.$fileLinkToReferenceTable = r.$referenceTableLinkToFile
            SET t.$destinationTableLinkToReferenceTable = r.$referenceTableLinkToDestinationTable;

        $dropfileLinkColumn
        INSERT INTO `$destinationTable`
            SELECT *
            FROM `$tempTable`;

    # cleanup
        DROP TABLE IF EXISTS `$tempTable`;
SQL;
    }
}
