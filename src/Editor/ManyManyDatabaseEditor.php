<?php

namespace PaulMillband\SqlLibrary\Editor;

class ManyManyDatabaseEditor
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
        CREATE TEMPORARY TABLE $tempTable AS
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

        DROP TEMPORARY TABLE $tempTable;
SQL;
    }



    /**
     * @param string $table
     * @param string $linkCol
     * @param string $linkTable
     * @param string $linkTableLinkCol
     * @param string $remainingColumnsInLinkTable
     * @param string $tableColumnToSplit
     * @param string $remainingColumnsInTable e.g. '`table1_id`,`text2b`'
     * @param string $additionalLoopCommand
     * @param string $tempTable
     * @param int $maxIterations
     * @return string
     */
    static function getSplitRecordsWithCommasSqlText(
        string $table,
        string $linkCol,
        string $linkTable,
        string $linkTableLinkCol,
        string $remainingColumnsInLinkTable,
        string $tableColumnToSplit,
        string $remainingColumnsInTable,
        string $additionalLoopCommand = '',
        string $tempTable = 'temp',
        int    $maxIterations = 10000
    ): string
    {
        return self::getSplitRecordsWithCharacterSqlText(
            $table,
            $linkCol,
            $linkTable,
            $linkTableLinkCol,
            $remainingColumnsInLinkTable,
            $tableColumnToSplit,
            $remainingColumnsInTable,
            ',',
            $additionalLoopCommand,
            $tempTable = 'temp',
            $maxIterations = 10000
        );
    }

    /**
     * @param string $table
     * @param string $linkCol
     * @param string $linkTable
     * @param string $linkTableLinkCol
     * @param string $remainingColumnsInLinkTable
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
        string $linkCol,
        string $linkTable,
        string $linkTableLinkCol,
        string $remainingColumnsInLinkTable,
        string $tableColumnToSplit,
        string $remainingColumnsInTable,
        string $character = ',',
        string $additionalLoopCommand = '',
        string $tempTable = 'temp',
        int    $maxIterations = 10000
    ): string
    {
        $query=<<<SQL
        ALTER TABLE `$table` ADD COLUMN `old_id` int;

        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `updatePivotTable`;

        CREATE TABLE `$tempTable` AS (
            SELECT *
                FROM `$table`
                WHERE `$tableColumnToSplit` LIKE '%$character%'
        );

        CREATE TRIGGER `updatePivotTable`
        AFTER INSERT ON `$table`
        FOR EACH ROW
        BEGIN
              INSERT INTO `$linkTable` (`$linkTableLinkCol`, $remainingColumnsInLinkTable)
                    SELECT new.$linkCol, $remainingColumnsInLinkTable
                    FROM `$linkTable`
                    WHERE `$linkTableLinkCol`=new.old_id;
        End;

        DROP PROCEDURE IF EXISTS temp_insert_split_by_comma;

        CREATE PROCEDURE temp_insert_split_by_comma()
        BEGIN
            DECLARE i INT DEFAULT 0;
            DECLARE row_count INT DEFAULT 1;
            WHILE i < $maxIterations AND row_count > 0 DO
                    -- infinite loop safeguard
                    SET i = i + 1;
                    -- add first word in each row to the word
                    INSERT INTO `$table` (`$tableColumnToSplit`, $remainingColumnsInTable,`old_id`)
                        SELECT REGEXP_REPLACE(`$tableColumnToSplit`,'^.*$character',''), $remainingColumnsInTable, `id`
                        FROM `$tempTable`
                        WHERE `$tableColumnToSplit` LIKE '%$character%';

                    -- remove current word
                    UPDATE `$tempTable`
                    SET `$tableColumnToSplit` = REGEXP_REPLACE(`$tableColumnToSplit`,'{$character}[^$character]*$','');

                    -- additional loop commands
                      $additionalLoopCommand
                    -- count comma entries
                    SET row_count = (SELECT COUNT(*) FROM `$tempTable` WHERE `$tableColumnToSplit` LIKE '%$character%' LIMIT 3);
            END WHILE;
        END;
        
        CALL temp_insert_split_by_comma();
        DROP PROCEDURE IF EXISTS temp_insert_split_by_comma;

        -- update original column to remaining value 

        UPDATE `$table`
            SET `$tableColumnToSplit` = REGEXP_REPLACE(`$tableColumnToSplit`,'^.*{$character}','');
           
        ALTER TABLE `$table` DROP COLUMN `old_id`;
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `updatePivotTable`;
SQL;
        return $query;
    }

    /**
     * @param string $table
     * @param string $linkCol
     * @param string $linkTable
     * @param string $linkTableLinkCol
     * @param string $remainingColumnsInLinkTable
     * @param array $tableColumnsToSplitArray
     * @param string $remainingColumnsInTable e.g. '`table1_id`,`text2b`'
     * @param array $charactersArray the character to split with
     * @param string $additionalLoopCommand
     * @param string $tempTable
     * @param int $maxIterations
     * @return string
     * @throws \Exception
     */
    static function getSplitRecordsWithMultipleColumnsSqlText(
        string $table,
        string $linkCol,
        string $linkTable,
        string $linkTableLinkCol,
        string $remainingColumnsInLinkTable,
        array  $tableColumnsToSplitArray,
        string $remainingColumnsInTable,
        array  $charactersArray = [',',','],
        string $additionalLoopCommand = '',
        string $tempTable = 'temp',
        int    $maxIterations = 10000
    ): string
    {
        if(count($tableColumnsToSplitArray) !== count($charactersArray)) {
            throw new \Exception('$charactersArray & $tableColumnsToSplitArray are different length');
        }
        $query=<<<SQL
        ALTER TABLE `$table` ADD COLUMN `old_id` int;

        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `updatePivotTable`;

        CREATE TABLE `$tempTable` AS (
            SELECT *
                FROM `$table`
                WHERE `$tableColumnsToSplitArray[0]` LIKE '%$charactersArray[0]%'
        );

        CREATE TRIGGER `updatePivotTable`
        AFTER INSERT ON `$table`
        FOR EACH ROW
        BEGIN
              INSERT INTO `$linkTable` (`$linkTableLinkCol`, $remainingColumnsInLinkTable)
                    SELECT new.$linkCol, $remainingColumnsInLinkTable
                    FROM `$linkTable`
                    WHERE `$linkTableLinkCol`=new.old_id;
        End;

        DROP PROCEDURE IF EXISTS temp_insert_split_by_comma;

        CREATE PROCEDURE temp_insert_split_by_comma()
        BEGIN
            DECLARE i INT DEFAULT 0;
            DECLARE row_count INT DEFAULT 1;
            WHILE i < $maxIterations AND row_count > 0 DO
                    -- infinite loop safeguard
                    SET i = i + 1;
                    -- add first word in each row to the word
                    INSERT INTO `$table` (`$tableColumnsToSplitArray[0]`
SQL;
        for ($i = 1; $i < count($tableColumnsToSplitArray) ; $i++) {
            $query.=", `$tableColumnsToSplitArray[$i]`";
        }
        $query.=<<<SQL
, $remainingColumnsInTable,`old_id`)
                            SELECT REGEXP_REPLACE(`$tableColumnsToSplitArray[0]`,'^.*$charactersArray[0]','')
SQL;
        for ($i = 1; $i < count($tableColumnsToSplitArray) ; $i++) {
            $query.=", REGEXP_REPLACE(`$tableColumnsToSplitArray[$i]`,'^.*$charactersArray[$i]','')";
        }
    $query.=<<<SQL
, $remainingColumnsInTable, `id`
                        FROM `$tempTable`
                        WHERE `$tableColumnsToSplitArray[0]` LIKE '%$charactersArray[0]%';
                    -- remove current word
                    UPDATE `$tempTable`
                    SET `$tableColumnsToSplitArray[0]` = REGEXP_REPLACE(`$tableColumnsToSplitArray[0]`,'{$charactersArray[0]}[^$charactersArray[0]]*$','')
SQL;
        for ($i = 1; $i < count($tableColumnsToSplitArray) ; $i++) {
            $query.=", `$tableColumnsToSplitArray[$i]` = REGEXP_REPLACE(`$tableColumnsToSplitArray[$i]`,'{$charactersArray[$i]}[^$charactersArray[$i]]*$','')";
        }
        $query.=";\n";
        $query.=<<<SQL
                    -- additional loop commands
                      $additionalLoopCommand
                    -- count comma entries
                    SET row_count = (SELECT COUNT(*) FROM `$tempTable` WHERE `$tableColumnsToSplitArray[0]` LIKE '%$charactersArray[0]%' LIMIT 3);
            END WHILE;
        END;
        
        CALL temp_insert_split_by_comma();
        DROP PROCEDURE IF EXISTS temp_insert_split_by_comma;

        -- update original column to remaining value 
        
        UPDATE `$table`
            SET `$tableColumnsToSplitArray[0]` = REGEXP_REPLACE(`$tableColumnsToSplitArray[0]`,'^.*{$charactersArray[0]}','')
SQL;
        for ($i = 1; $i < count($tableColumnsToSplitArray) ; $i++) {
            $query.=", `$tableColumnsToSplitArray[$i]` = REGEXP_REPLACE(`$tableColumnsToSplitArray[$i]`,'^.*{$charactersArray[$i]}','')";
        }
        $query.=";\n";
        $query .= <<<SQL
        ALTER TABLE `$table` DROP COLUMN `old_id`;
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `updatePivotTable`;
SQL;
    return $query;
    }

    /**
     * remove rows where a previous row (by $orderColumn) has the same value for a given column
     * @param string $table
     * @param string $linkColumn,
     * @param string $linkTable,
     * @param string $linkTableLinkColumn,
     * @param string $column
     * @param string $orderColumn
     * @param bool $orderAscending
     * @return string
     */
    static function getRemoveLaterDuplicatesSqlText(
        string $table,
        string $linkColumn,
        string $linkTable,
        string $linkTableLinkColumn,
        string $column,
        string $orderColumn='id',
        bool   $orderAscending=true
    ){
        if($orderAscending){
            return <<<EOL
                DELETE FROM `$table` USING `$table`,
                    `$table` e1
                WHERE `$table`.`$orderColumn` > e1.`$orderColumn`
                    AND `$table`.`$column` = e1.`$column`;
EOL;
        }
        return <<<EOL
                DELETE FROM `$table` USING `$table`,
                    `$table` e1
                WHERE `$table`.`$orderColumn` > e1.`$orderColumn`
                    AND `$table`.`$column` = e1.`$column`;
EOL;
    }

}