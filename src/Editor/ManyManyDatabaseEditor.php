<?php

namespace PaulMillband\SqlLibrary\Editor;

class ManyManyDatabaseEditor
{
    /**
     * set each rows value for a given column to the value of the column in the previous row
     * @param string $table
     * @param string $column
     * @param string $orderingColumn
     * @param bool $orderAsc
     * @param string $tempTable
     * @return string
     */
    public static function getSetColumnValueAsLastValueIfNotSetSqlTriggerText(
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
         * @param bool $orderAsc
         * @param string $tempTable
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
     * @param array $remainingColumnsInLinkTable
     * @param string $tableColumnToSplit
     * @param array $remainingColumnsInTable e.g. ['table1_id','text2b']
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
        array $remainingColumnsInLinkTable,
        string $tableColumnToSplit,
        array $remainingColumnsInTable,
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
     * @param array $remainingColumnsInTableArray e.g. ['table1_id','text2b']
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
        array  $remainingColumnsInLinkTableArray,
        string $tableColumnToSplit,
        array  $remainingColumnsInTableArray,
        string $character = ',',
        string $additionalLoopCommand = '',
        string $tempTable = 'temp',
        int    $maxIterations = 10000
    ): string
    {
        $remainingColumnsInTableString='`'.implode('`,`', $remainingColumnsInTableArray).'`';
        $remainingColumnsInLinkTableString='`'.implode('`,`', $remainingColumnsInLinkTableArray).'`';
        return<<<SQL
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
              INSERT INTO `$linkTable` (`$linkTableLinkCol`, $remainingColumnsInLinkTableString)
                    SELECT new.$linkCol, $remainingColumnsInLinkTableString
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
                    INSERT INTO `$table` (`$tableColumnToSplit`, $remainingColumnsInTableString,`old_id`)
                        SELECT REGEXP_REPLACE(`$tableColumnToSplit`,'^.*$character',''), $remainingColumnsInTableString, `id`
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
    }

    /**
     * @param string $table
     * @param string $linkCol
     * @param string $linkTable
     * @param string $linkTableLinkCol
     * @param array $remainingColumnsInLinkTable
     * @param array $tableColumnsToSplitArray
     * @param array $remainingColumnsInTableString
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
        array $remainingColumnsInLinkTableArray,
        array  $tableColumnsToSplitArray,
        array $remainingColumnsInTableArray,
        array  $charactersArray = [',',','],
        string $additionalLoopCommand = '',
        string $tempTable = 'temp',
        int    $maxIterations = 10000
    ): string
    {
        if(count($tableColumnsToSplitArray) !== count($charactersArray)) {
            throw new \Exception('$charactersArray & $tableColumnsToSplitArray are different length');
        }
        $remainingColumnsInLinkTableString='`'.implode('`,`', $remainingColumnsInLinkTableArray).'`';
        $remainingColumnsInTableString='`'.implode('`,`', $remainingColumnsInTableArray).'`';
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
              INSERT INTO `$linkTable` (`$linkTableLinkCol`, $remainingColumnsInLinkTableString)
                    SELECT new.$linkCol, $remainingColumnsInLinkTableString
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
, $remainingColumnsInTableString,`old_id`)
                            SELECT REGEXP_REPLACE(`$tableColumnsToSplitArray[0]`,'^.*$charactersArray[0]','')
SQL;
        for ($i = 1; $i < count($tableColumnsToSplitArray) ; $i++) {
            $query.=", REGEXP_REPLACE(`$tableColumnsToSplitArray[$i]`,'^.*$charactersArray[$i]','')";
        }
    $query.=<<<SQL
, $remainingColumnsInTableString, `id`
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
     * @param array $duplicateColumns
     * @param string $linkColumn,
     * @param string $linkTable,
     * @param string $linkTableLinkColumn,
     * @param array $remainingLinkTableColumns
     * @param string $orderColumn
     * @param bool $orderAscending
     * @param string $tempTable
     * @return string
     */
    static function getReassignAndRemoveDuplicatesSqlText(
        array $duplicateColumns,
        string $table,
        string $linkColumn,
        string $linkTable,
        string $linkTableLinkColumn,
        array  $remainingLinkTableColumns,
        string $orderColumn='id',
        bool   $orderAscending=true,
        string $tempTable='temp'
    ) :string
    {
        if($orderAscending){
            $direction = '>';
        }else{
            $direction = '>';
        }
        $queryAndStatement='';
        for ($i = 0; $i < count($duplicateColumns); $i++) {
            $queryAndStatement.="\n".<<<SQL
       AND a.`$duplicateColumns[$i]` <=> b.`$duplicateColumns[$i]`
SQL;
        }
            $query=<<<SQL
-- setup
DROP TABLE IF EXISTS `temp`;

-- get new and old ids
CREATE table `$tempTable` AS
    SELECT old_id, MIN(b_id) AS new_id
	FROM
  (     
      SELECT 
           a.`$linkColumn` AS old_id,
           b.$linkColumn AS b_id
       FROM `$table` a, `$table` b
       WHERE a.`$linkColumn` $direction b.`$linkColumn` $queryAndStatement
  ) AS t 
	GROUP BY old_id ;

-- update link table
UPDATE `$linkTable` as l
JOIN `$tempTable` as t ON (l.$linkTableLinkColumn = t.old_id)
SET l.$linkTableLinkColumn = t.new_id;

-- remove duplicates from link table
DELETE FROM `$linkTable` USING `$linkTable`,
    `$linkTable` l
WHERE `$linkTable`.`$orderColumn` $direction l.`$orderColumn`
    AND `$linkTable`.`$linkTableLinkColumn` <=> l.`$linkTableLinkColumn`
SQL;
        for ($i = 0; $i < count($remainingLinkTableColumns); $i++) {
            $query.="\nAND `$linkTable`.`$remainingLinkTableColumns[$i]` <=> l.`$remainingLinkTableColumns[$i]`";
        }
        $query.=";\n";
        $query.=<<<SQL

-- remove duplicates (by $orderColumn) in table
 DELETE FROM `$table` WHERE `$orderColumn` IN (
  SELECT * FROM (
      SELECT 
           a.`$orderColumn` AS `$orderColumn`
       FROM `$table` a, `$table` b
       WHERE a.`$orderColumn` > b.`$orderColumn` $queryAndStatement
  ) AS t 
	GROUP BY `$orderColumn`
);

-- cleanup
DROP TABLE IF EXISTS `$tempTable`;
SQL;
        return $query;
    }

}