<?php

namespace PaulMillband\SqlLibrary\Importer;

class ManyManyImporter
{
    /**
     * Import a tsv or csv into a many-many database table pair
     * only for flat single file 1-1 imports
     *
     * @param int $startID what id to give the first row
     * @param string $pivotTable string e.g. 'table1_table2'
     * @param string $pivotTableColumns e.g. '`table1_id`,`table2_id`'
     * @param string $table1 e.g. 'table1'
     * @param string $columnsForTable1 e.g. '`column1`,`column2`'
     * @param string $valueColumnsForTable1 e.g. 'NEW.`column1`,NEW.`column2`'
     * @param string $table2 e.g. 'table2'
     * @param string $columnsForTable2 e.g. '`column3`,`column4`'
     * @param string $valueColumnsForTable2 e.g. 'id, NEW.`column3`,NEW.`column4`'
     * @param string $filePath file path e.g. '__DIR__./src/test.tsv'
     * @param string $fileColumns e.g. 'column1,column2'
     * @param string $fileDelimiter e.g. '\t'
     * @param string $tempTable e.g. 'temp'
     * @return string
     */
    static function getSimpleManyManySqlText(
        string $startID,
        string $pivotTable,
        string $pivotTableColumns,
        string $table1,
        string $columnsForTable1,
        string $valueColumnsForTable1,
        string $table2,
        string $columnsForTable2,
        string $valueColumnsForTable2,
        string $filePath,
        string $fileColumns,
        int    $ignoreLinesInt=0,
        string $fileDelimiter='\t',
        string $tempTable='temp'
    )
    {
        $ignoreLinesText='';
        if ($ignoreLinesInt){
            $ignoreLinesText="IGNORE $ignoreLinesInt LINES\n";
        }
        return <<<EOF
        # clean workspace
        SET FOREIGN_KEY_CHECKS=0;
        TRUNCATE TABLE `$pivotTable`;
        TRUNCATE TABLE `$table1`;
        TRUNCATE TABLE `$table2`;
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table1`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table2`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table3`;

        CREATE TABLE `$tempTable` AS
                SELECT *
                FROM `$table1`
                NATURAL JOIN `$table2`
                LIMIT 0;
        ALTER TABLE `$tempTable` MODIFY `id` INT PRIMARY KEY AUTO_INCREMENT;
        ALTER TABLE `$tempTable` AUTO_INCREMENT = $startID;

        CREATE TRIGGER `from_load_data_to_table1` AFTER INSERT ON `$tempTable`
        FOR EACH ROW
           INSERT INTO `$table1` ($columnsForTable1)
             VALUES ($valueColumnsForTable1);

        CREATE TRIGGER `from_load_data_to_table2` AFTER INSERT ON `$tempTable`
        FOR EACH ROW
           INSERT INTO `$table2` ($columnsForTable2)
             SELECT $valueColumnsForTable2;

       CREATE TRIGGER `from_load_data_to_table3` AFTER INSERT ON `$tempTable`
       FOR EACH ROW
          INSERT INTO `$pivotTable` ($pivotTableColumns)
            VALUES (new.`id`,new.`id`);

        LOAD DATA INFILE '$filePath'
        IGNORE INTO TABLE `$tempTable`
        FIELDS TERMINATED BY '$fileDelimiter'
        $ignoreLinesText($fileColumns);

        # cleanup
        SET FOREIGN_KEY_CHECKS=1;
#       DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table1`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table2`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table3`;

EOF;
    }

    /**
     * Used to split rows where comma seperated lists should be individual rows, but the other data remains the same.
     * The original rows are then deleted.
     * e.g.
     * here new rows would be created int this table: 1 for foo, 1 for bar and 1 for foobar.
     * The pivot table is also updated
     *
     * |id|column1      |column2   |
     * |0|foo,bar,foobar|some text |
     *
     * @param string $pivotTable string e.g. 'table1_table2'
     * @param string $pivotTableColumns e.g. '`table1_id`,`table2_id`'
     * @param string $pivotTableValues e.g. 'NEW.`table1_id`,NEW.`table2_id`'
     * @param string $table e.g. 'table1'
     * @param string $tableColumnToSplit e.g. '`column1`'
     * @param string $remainingColumnsInTable DO NOT include the id column e.g. '`column2`,`column3`'
     * @param string $linkColumnForTable2 pivot table column for other table that is duplicated for split rows
     * e.g. if column2 is comma seperated too "UPDATE `temp` SET `column2` = REGEXP_REPLACE(`column2`,',[^,]*$','');"
     * @param string $additionalLoopCommand additional commands to add near the end of each loop e.g. 'Column2'
     * @param string $tempTable e.g. 'temp'
     * @param int $maxIterations max amount of rows to do to ensure no infinite loops
     * @return string
     */
    static function getSplitRecordsWithCommasSqlText(
        string $pivotTable,
        string $pivotTableColumns,
        string $pivotTableValues,
        string $table,
        string $tableColumnToSplit,
        string $remainingColumnsInTable,
        string $linkColumnForTable2,
        string $additionalLoopCommand,
        string $tempTable='temp',
        int    $maxIterations=10000
    )
    {
        return <<<EOF
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `temp_add_to_pivot_table`;

        CREATE TABLE `$tempTable` AS (
            SELECT *
                FROM `$table`
                WHERE `$tableColumnToSplit` LIKE '%,%'
        );
        ALTER TABLE `$table`
                ADD COLUMN `$linkColumnForTable2` INT;

        CREATE TRIGGER `temp_add_to_pivot_table` AFTER INSERT ON `$table`
            FOR EACH ROW
            INSERT INTO `$pivotTable` ($pivotTableColumns)
            VALUES ($pivotTableValues);

        DROP PROCEDURE IF EXISTS temp_insert_split_by_comma;

        CREATE PROCEDURE temp_insert_split_by_comma()
        BEGIN
            DECLARE i INT DEFAULT 0;
            DECLARE row_count INT DEFAULT 1;
            WHILE i < $maxIterations AND row_count > 0 DO
                    # infinite loop safeguard
                    SET i = i + 1;
                    # add first word in each row to the word & pivot tables
                    # note: there is a trigger 'temp_add_to_pivot_table' defined above, and fired for each row inserted into `$table`
                    INSERT INTO `$table` ($linkColumnForTable2, $tableColumnToSplit, $remainingColumnsInTable)
                        SELECT `id`,
                               REGEXP_REPLACE(`$tableColumnToSplit`,'^.*,',''),
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
        # note: there is a trigger 'temp_add_to_pivot_table' defined above, and fired for each row inserted into `$table`
        INSERT INTO `$table` ($linkColumnForTable2, $tableColumnToSplit, $remainingColumnsInTable)
            SELECT `id`,
                   `$tableColumnToSplit`,
                    $remainingColumnsInTable
            FROM `$tempTable` as t;

        # remove the original comma listed rows
        DELETE FROM `$table`
           WHERE `$tableColumnToSplit` LIKE '%,%';

        # clean up
        ALTER TABLE `$table`
            DROP COLUMN `$linkColumnForTable2`;
       #  DROP TABLE IF EXISTS `$tempTable`;
EOF;
    }
}
