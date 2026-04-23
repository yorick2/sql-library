<?php

namespace PaulMillband\SqlLibrary\Importer;

class SimpleImporter
{
    /**
     * generate SQL to import a tsv or csv into a single table
     *
     * @param $table string e.g. 'table1'
     * @param $filePath string file path e.g. '__DIR__./src/test.tsv'
     * @param $fileColumns string e.g. 'column1,column2'
     * @param int $ignoreLinesInt how many rows to ignore from table
     * @param $fileDelimiter string e.g. '\t'
     * @return string
     */
    static function getSqlText(
        string $table,
        string $filePath,
        string $fileColumns,
        int    $ignoreLinesInt = 0,
        string $fileDelimiter = '\t'
    )
    {
        $ignoreLinesText = '';
        if ($ignoreLinesInt) {
            $ignoreLinesText = "IGNORE $ignoreLinesInt LINES\n";
        }
        return <<<EOF
            LOAD DATA INFILE '$filePath'
            IGNORE INTO TABLE `$table`
            FIELDS TERMINATED BY '$fileDelimiter'
            $ignoreLinesText($fileColumns);
EOF;
    }

    /**
     * @param string $table
     * @param string $tableColumnToSplit
     * @param string $remainingColumnsInTable e.g. '`table1_id`,`text2b`'
     * @param string $additionalLoopCommand
     * @param string $tempTable
     * @param int $maxIterations
     * @return string
     */
    static function getSplitRecordsWithCommasSqlText(
        string $table,
        string $tableColumnToSplit,
        string $remainingColumnsInTable,
        string $additionalLoopCommand = '',
        string $tempTable = 'temp',
        int    $maxIterations = 10000
    ): string
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

    /**
     * all table columns to be split need to have the same number of commas
     *
     * |column1|column2|column3|
     * |-------|-------|-------|
     * |1,2,3  |5,3,6  |8,7,4 |
     *
     * @param string $table
     * @param array $tableMultipleColumnsToSplit e.g. ['column1','column2','column3',...]
     * @param string $remainingColumnsInTable e.g. '`table1_id`,`text2b`'
     * @param string $additionalLoopCommand
     * @param string $tempTable
     * @param int $maxIterations
     * @return string
     */
    static function getSplitRecordsWithMultipleCommaColumnsSqlText(
        string $table,
        array $tableMultipleColumnsToSplit,
        string $remainingColumnsInTable,
        string $additionalLoopCommand = '',
        string $tempTable = 'temp',
        int    $maxIterations = 10000
    ): string
    {
        $tableMultipleColumnsToSplitString = '`'.implode('`,`', $tableMultipleColumnsToSplit).'`';
        $query = <<<EOF
        DROP TABLE IF EXISTS `$tempTable`;

        CREATE TABLE `$tempTable` AS (
            SELECT *
                FROM `$table`
                WHERE `$tableMultipleColumnsToSplit[0]` LIKE '%,%'
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
                        INSERT INTO `$table` ($tableMultipleColumnsToSplitString, $remainingColumnsInTable)
                        SELECT 
EOF;
        for ($i = 0; $i < count($tableMultipleColumnsToSplit); $i++) {
            $query .= "REGEXP_REPLACE(`$tableMultipleColumnsToSplit[$i]`,'^.*,',''),\n";
        }

        $query .= <<<EOF
                               $remainingColumnsInTable
                        FROM `$tempTable`
                        WHERE `$tableMultipleColumnsToSplit[0]` LIKE '%,%';
                        # remove current word
                        
EOF;

        for ($i = 0; $i < count($tableMultipleColumnsToSplit); $i++) {
            $query .= "UPDATE `$tempTable` SET `$tableMultipleColumnsToSplit[$i]` = REGEXP_REPLACE(`$tableMultipleColumnsToSplit[$i]`,',[^,]*$','');\n";
        }

        $query .= <<<EOF
                    # additional loop commands
                      $additionalLoopCommand
                    # count comma entries
                    SET row_count = (SELECT COUNT(*) FROM `$tempTable` WHERE `$tableMultipleColumnsToSplit[0]` LIKE '%,%' LIMIT 3);
            END WHILE;
        END;
        
        CALL temp_insert_split_by_comma();
        DROP PROCEDURE IF EXISTS temp_insert_split_by_comma;

        # insert the last item in the comma seperated list
        INSERT INTO `$table` ($tableMultipleColumnsToSplitString, $remainingColumnsInTable)
            SELECT $tableMultipleColumnsToSplitString,
                    $remainingColumnsInTable
            FROM `$tempTable` as t;
        # remove the original comma listed rows
        DELETE FROM `$table`
           WHERE `$tableMultipleColumnsToSplit[0]` LIKE '%,%';

       DROP TABLE IF EXISTS `$tempTable`;
EOF;
        return $query;
    }
}
