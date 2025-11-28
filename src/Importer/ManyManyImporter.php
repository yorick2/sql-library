<?php

namespace PaulMillband\SqlLibrary\Importer;

class ManyManyImporter
{
    /**
     * Import a tsv or csv into a many-many database table pair,
     * where there are only one of each. This does happen
     *
     * @param string $pivotTable string e.g. 'table1_table2'
     * @param string $pivotTableColumns e.g. '`table1_id`,`table2_id`'
     * @param string $table1 e.g. 'table1'
     * @param string $columnsForTable1 e.g. '`column1`,`column2`'
     * @param string $valueColumnsForTable1 e.g. 'NEW.`column1`,NEW.`column2`'
     * @param string $table2 e.g. 'table2'
     * @param string $columnsForTable2 e.g. '`table1_id`,`column3`,`column4`'
     * @param string $valueColumnsForTable2 e.g. 'id, NEW.`column3`,NEW.`column4`'
     * @param string $filePath file path e.g. '__DIR__./src/test.tsv'
     * @param string $fileColumns e.g. 'column1,column2'
     * @param string $fileDelimiter e.g. '\t'
     * @param string $tempTable e.g. 'temp'
     * @return string
     */
    static function getSimpleManyManySqlText(
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
        string $fileDelimiter='\t',
        string $tempTable='temp'
    )
    {
        return <<<EOF
        # clean workspace
        SET FOREIGN_KEY_CHECKS=0;
        TRUNCATE TABLE `$pivotTable`;
        TRUNCATE TABLE `$table1`;
        TRUNCATE TABLE `$table2`;
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `add_to_word_table`;
        DROP TRIGGER IF EXISTS `add_to_translation_table`;
        DROP TRIGGER IF EXISTS `add_to_pivot_table`;

        CREATE TABLE `$tempTable` AS
                SELECT *
                FROM `$table1`
                NATURAL JOIN `$table2`
                LIMIT 0;

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
        IGNORE 1 LINES
        ($fileColumns);

        # cleanup
        SET FOREIGN_KEY_CHECKS=1;
        DROP TABLE IF EXISTS `$tempTable`;
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
     * @param string $table1 e.g. 'table1'
     * @param string $table1SplitColumn e.g. '`column1`'
     * @param string $remainingColumnsForTable1 DO NOT include the id column e.g. '`column2`,`column3`'
     * @param string $linkColumn pivot table column for other table that is duplicated for split rows
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
        string $table1,
        string $table1SplitColumn,
        string $remainingColumnsForTable1,
        string $linkColumn,
        string $additionalLoopCommand,
        string $tempTable='temp',
        int    $maxIterations=10000
    )
    {
        return <<<EOF
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `temp_add_to_pivot_table`;

        CREATE TABLE `$tempTable` AS
            SELECT *
                FROM `$table1`
                WHERE `$table1SplitColumn` LIKE '%,%';
        ALTER TABLE `$table1`
                ADD COLUMN `$linkColumn` INT;

        CREATE TRIGGER `temp_add_to_pivot_table` AFTER INSERT ON `$table1`
            FOR EACH ROW
            INSERT INTO `$pivotTable` ($pivotTableColumns)
            VALUES ($pivotTableValues);

        DROP PROCEDURE IF EXISTS temp_insert_split_by_comma;

        DELIMITER $$
        CREATE PROCEDURE temp_insert_split_by_comma()
        BEGIN
            DECLARE i INT DEFAULT 0;
            DECLARE row_count INT DEFAULT 1;
            WHILE i < $maxIterations AND row_count > 0 DO
                    # infinite loop safeguard
                    SET i = i + 1;
                    # add first word in each row to the word & pivot tables
                    # note: there is a trigger 'temp_add_to_pivot_table' defined above, and fired for each row inserted into `$table1`
                    INSERT INTO `$table1` ($linkColumn, $remainingColumnsForTable1)
                        SELECT `id`,
                               REGEXP_REPLACE(`$table1SplitColumn`,'^.*,',''),
                               $remainingColumnsForTable1
                        FROM `$tempTable` as t
                        WHERE `$table1SplitColumn` LIKE '%,%';
                    # remove current word
                    UPDATE `$tempTable`
                        SET `$table1SplitColumn` = REGEXP_REPLACE(`$table1SplitColumn`,',[^,]*$','');
                    # additional loop commands
                      $additionalLoopCommand
                    # count comma entries
                    SET row_count = (SELECT COUNT(*) FROM `$tempTable` WHERE `$table1SplitColumn` LIKE '%,%' LIMIT 3);
                END WHILE;
        END$$
        DELIMITER ;

        CALL temp_insert_split_by_comma();

        DROP PROCEDURE IF EXISTS temp_insert_split_by_comma;

        # insert the last item in the comma seperated list
        # note: there is a trigger 'temp_add_to_pivot_table' defined above, and fired for each row inserted into `$table1`
        INSERT INTO `$table1` ($linkColumn, $remainingColumnsForTable1)
            SELECT `id`,
                   `$table1SplitColumn`,
                    $remainingColumnsForTable1
            FROM `$tempTable` as t;

        # remove the original comma listed rows
        DELETE FROM `$table1`
           WHERE `$table1SplitColumn` LIKE '%,%';

        # clean up
        ALTER TABLE `$table1`
            DROP COLUMN `$linkColumn`;
        DROP TABLE IF EXISTS `$tempTable`;
EOF;
    }
}
