<?php

namespace PaulMillband\SqlLibrary\Editor;

class SimpleDatabaseEditor
{


    /**
     * set each rows value for a given column to the value of the column in the previous row
     * @param string $table
     * @param string $column
     * @param string $orderingColumn
     * @param boolean $orderAsc
     * @return string
     */
    static function getSetColumnValueAsLastValueIfNotSetSqlTriggerText(
        string $table,
        string $column,
        string $orderingColumn='id',
        bool $orderAsc=true,
        string $tempTable = 'temp'
    ): string
    {
        if ($orderAsc){
            return "SET new.`$table` = (SELECT `$table` from `$column` ORDER BY `$orderingColumn` ASC LIMIT 1);";
        }
        return "SET new.`$table` = (SELECT `$table` from `$column` ORDER BY `$orderingColumn` DESC LIMIT 1);";
    }

        /**
         * set each rows value for a given column to the value of the column in the previous row
         * @param string $table
         * @param string $column
         * @param string $orderingColumn
         * @param boolean $orderAsc
         * @return string
         */
        static function getSetColumnValueAsLastValueWhenNotSetSqlText(
            string $table,
            string $column,
            string $orderingColumn='id',
            bool $orderAsc=true,
            string $tempTable = 'temp'
        ): string
        {
        $newColumn = "new_$column";
        $ascText = "Asc";
        $where = "$orderingColumn > t.$orderingColumn AND $column != 0";
        if($orderAsc === true){
            $ascText = "DESC";
            $where = "$orderingColumn < t.$orderingColumn AND $column != 0";
        }
        return <<<SQL
        CREATE TABLE $tempTable AS
        SELECT t.$orderingColumn, (
            SELECT $column
            FROM `$table`
            WHERE $where
            ORDER BY $orderingColumn $ascText
            LIMIT 1
        ) AS $newColumn
        FROM `$table` t
        WHERE t.$column = 0;

        UPDATE $tempTable set $newColumn = 0 where $newColumn is NULL;

        UPDATE `$table` t
        JOIN $tempTable temp ON t.$orderingColumn = temp.$orderingColumn
        SET t.$column = temp.$newColumn;

        DROP TABLE $tempTable;
SQL;
    }

    /**
     * @param string $table
     * @param string $triggerName
     * @param string $action finish with a ;
     * @return string
     */
    static function getTriggerSqlText(
        string $table,
        string $triggerName,
        string $action
    ): string
    {
        return <<<SQL
            DROP TRIGGER IF EXISTS `$triggerName`;

            CREATE TRIGGER `$triggerName`
            BEFORE INSERT ON `$table`
            FOR EACH ROW
            BEGIN
                $action
            END;
            
SQL;
    }

    /**
     * @param string $table
     * @param string $triggerName
     * @param string $IfStatement
     * @param string $ifAction finish with a ;
     * @param string $elseAction finish with a ;
     * @return string
     */
    static function getTriggerIfElseSqlText(
        string $table,
        string $triggerName,
        string $IfStatement,
        string $ifAction,
        string $elseAction
    ): string
    {
        return <<<SQL
            CREATE TRIGGER `$triggerName`
            BEFORE INSERT ON `$table`
            FOR EACH ROW
            BEGIN
                IF $IfStatement THEN
                    $ifAction
                ELSE
                    $elseAction
                END IF;
            End;
SQL;
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
        return self::getSplitRecordsWithCharacterSqlText(
            $table,
            $tableColumnToSplit,
            $remainingColumnsInTable,
            ',',
            $additionalLoopCommand,
            $tempTable = 'temp',
            $maxIterations = 10000
        );
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
        return self::getSplitRecordsWithMultipleCharacterColumnsSqlText(
            $table,
            $tableMultipleColumnsToSplit,
            $remainingColumnsInTable,
            ',',
            $additionalLoopCommand,
            $tempTable,
            $maxIterations
        );
    }

    /**
     * @param string $table
     * @param string $tableColumnToSplit
     * @param string $remainingColumnsInTable e.g. '`table1_id`,`text2b`'
     * @param string $character the character to split with
     * @param string $additionalLoopCommand
     * @param string $tempTable
     * @param int $maxIterations
     * @return string
     */
    static function getSplitRecordsWithCharacterSqlText(
        string $table,
        string $tableColumnToSplit,
        string $remainingColumnsInTable,
        string $character = ',',
        string $additionalLoopCommand = '',
        string $tempTable = 'temp',
        int    $maxIterations = 10000
    ): string
    {
        $query=<<<SQL
        DROP TABLE IF EXISTS `$tempTable`;

        CREATE TABLE `$tempTable` AS (
            SELECT *
                FROM `$table`
                WHERE `$tableColumnToSplit` LIKE '%$character%'
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
                        SELECT REGEXP_REPLACE(`$tableColumnToSplit`,'^.*$character',''),
                               $remainingColumnsInTable
                        FROM `$tempTable`
                        WHERE `$tableColumnToSplit` LIKE '%$character%';
                    # remove current word
SQL;
            $query .= "# remove current word
UPDATE `$tempTable`
SET `$tableColumnToSplit` = REGEXP_REPLACE(`$tableColumnToSplit`,'{$character}[^$character]*$','');\n";
        $query .=<<<SQL
                    # additional loop commands
                      $additionalLoopCommand
                    # count comma entries
                    SET row_count = (SELECT COUNT(*) FROM `$tempTable` WHERE `$tableColumnToSplit` LIKE '%$character%' LIMIT 3);
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
           WHERE `$tableColumnToSplit` LIKE '%$character%';

       DROP TABLE IF EXISTS `$tempTable`;
SQL;
    return $query;
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
    static function getSplitRecordsWithMultipleCharacterColumnsSqlText(
        string $table,
        array $tableMultipleColumnsToSplit,
        string $remainingColumnsInTable,
        string $character = ',',
        string $additionalLoopCommand = '',
        string $tempTable = 'temp',
        int    $maxIterations = 10000
    ): string
    {
        $tableMultipleColumnsToSplitString = '`'.implode('`,`', $tableMultipleColumnsToSplit).'`';
        $query = <<<SQL
        DROP TABLE IF EXISTS `$tempTable`;

        CREATE TABLE `$tempTable` AS (
            SELECT *
                FROM `$table`
                WHERE `$tableMultipleColumnsToSplit[0]` LIKE '%$character%'
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
SQL;
        for ($i = 0; $i < count($tableMultipleColumnsToSplit); $i++) {
            $query .= "REGEXP_REPLACE(`$tableMultipleColumnsToSplit[$i]`,'^.*$character',''),\n";
        }

        $query .= <<<SQL
                               $remainingColumnsInTable
                        FROM `$tempTable`
                        WHERE `$tableMultipleColumnsToSplit[0]` LIKE '%$character%';
                        # remove current word
                        
SQL;

        for ($i = 0; $i < count($tableMultipleColumnsToSplit); $i++) {
            $query .= "UPDATE `$tempTable` SET `$tableMultipleColumnsToSplit[$i]` = REGEXP_REPLACE(`$tableMultipleColumnsToSplit[$i]`,'".$character.'[^'.$character."]*$','');\n";
        }

        $query .= <<<SQL
                    # additional loop commands
                      $additionalLoopCommand
                    # count comma entries
                    SET row_count = (SELECT COUNT(*) FROM `$tempTable` WHERE `$tableMultipleColumnsToSplit[0]` LIKE '%$character%' LIMIT 3);
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
           WHERE `$tableMultipleColumnsToSplit[0]` LIKE '%$character%';

       DROP TABLE IF EXISTS `$tempTable`;
SQL;
        return $query;
    }

    /**
     * remove rows where a previous row (by $orderColumn) has the same value for a given column
     * @param string $table
     * @param string $column
     * @param string $orderColumn
     * @param bool $orderAscending
     * @return string
     */
    static function getRemoveLaterDuplicatesSqlText(
        string $table,
        string $column,
        string $orderColumn='id',
        bool $orderAscending=true
    ){
        if($orderAscending){
            return <<<SQL
                DELETE FROM `$table` USING `$table`,
                    `$table` e1
                WHERE `$table`.`$orderColumn` > e1.`$orderColumn`
                    AND `$table`.`$column` = e1.`$column`;
SQL;
        }
        return <<<SQL
                DELETE FROM `$table` USING `$table`,
                    `$table` e1
                WHERE `$table`.`$orderColumn` > e1.`$orderColumn`
                    AND `$table`.`$column` = e1.`$column`;
SQL;
    }

}