<?php

namespace PaulMillband\SqlLibrary\Importer;

class OneManyImporter
{
    /**
     * generate SQL to import a tsv or csv into a one-many database
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
     * @param int $ignoreLines No. of lines to ignore in the file e.g. for the header
     * @param string $fileDelimiter e.g. '\t'
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
        int $ignoreLines=0,
        string $fileDelimiter='\t',
        string $tempTable='temp'
    )
    {
        $ignoreLinesText='';
        if($ignoreLines>0){
            $ignoreLinesText="IGNORE $ignoreLines LINES\n";
        }
        return <<<EOF
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `from_load_data`;
        DROP TRIGGER IF EXISTS `from_load_data_2`;

        CREATE TABLE `$tempTable` AS
                SELECT *
                FROM `$table1`
                NATURAL JOIN `$table2`
                LIMIT 0;

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
        $ignoreLinesText($fileColumns);
        
    # cleanup
--         DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table1`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table2`;
EOF;
    }


    static function getSplitRecordsWithCommasSqlText(
        string $table,
        string $tableColumnToSplit,
        string $remainingColumnsInTable,
        string $additionalLoopCommand='',
        string $tempTable='temp',
        int    $maxIterations=10000
    ) : string
    {
        return <<<EOF
        DROP TABLE IF EXISTS `$tempTable`;

        CREATE TABLE `$tempTable` AS (
            SELECT *
                FROM `$table`
                WHERE `$tableColumnToSplit` LIKE '%,%'
        );

        DROP PROCEDURE IF EXISTS temp_insert_split_by_comma;

        CREATE PROCEDURE temp_insert_split_by_comma()
        BEGIN
            DECLARE i INT DEFAULT 0;
            DECLARE row_count INT DEFAULT 1;
            WHILE i < $maxIterations AND row_count > 0 DO
                    # infinite loop safeguard
                    SET i = i + 1;
                    # add first word in each row to the word
                    INSERT INTO `$table` (`$tableColumnToSplit`, $remainingColumnsInTable)
                        SELECT REGEXP_REPLACE(`$tableColumnToSplit`,'^.*,',''),
                               $remainingColumnsInTable
                        FROM `$tempTable`
                        WHERE `$tableColumnToSplit` LIKE '%,%';
                    # remove current word
                    UPDATE `$tempTable`
                        SET `$tableColumnToSplit` = REGEXP_REPLACE(`$tableColumnToSplit`,',[^,]*$','');
                    # additional loop commands
                      $additionalLoopCommand
                    # count comma entries
                    SET row_count = (SELECT COUNT(*) FROM `$tempTable` WHERE `$tableColumnToSplit` LIKE '%,%' LIMIT 3);
            END WHILE;
        END;
        
        CALL temp_insert_split_by_comma();
        DROP PROCEDURE IF EXISTS temp_insert_split_by_comma;

        # insert the last item in the comma seperated list
        INSERT INTO `$table` (`$tableColumnToSplit`, $remainingColumnsInTable)
            SELECT `$tableColumnToSplit`,
                    $remainingColumnsInTable
            FROM `$tempTable` as t;

        # remove the original comma listed rows
        DELETE FROM `$table`
           WHERE `$tableColumnToSplit` LIKE '%,%';

       DROP TABLE IF EXISTS `$tempTable`;
EOF;
    }
}
