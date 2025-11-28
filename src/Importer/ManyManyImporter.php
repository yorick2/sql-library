<?php

namespace PaulMillband\SqlLibrary\Importer;

class ManyManyImporter
{
    /**
     * Import a tsv or csv into a many-many database table pair,
     * where there are only one of each. This does happen
     *
     * @param string $tempTable e.g. 'temp'
     * @param string $fileDelimiter e.g. '\t'
     * @param string $filePath file path e.g. '__DIR__./src/test.tsv'
     * @param string $fileColumns e.g. 'column1,column2'
     * @param string $pivotTable string e.g. 'table1_table2'
     * @param string $pivotTableColumns e.g. '`table1_id`,`table2_id`'
     * @param string $destinationTable1 e.g. 'table1'
     * @param string $columnsForTable1 e.g. '`column1`,`column2`'
     * @param string $valueColumnsForTable1 e.g. 'NEW.`column1`,NEW.`column2`'
     * @param string $destinationTable2 e.g. 'table2'
     * @param string $columnsForTable2 e.g. '`table1_id`,`column3`,`column4`'
     * @param string $valueColumnsForTable2 e.g. 'id, NEW.`column3`,NEW.`column4`'
     * @param string $link e.g. 'WHERE `sometext` = NEW.`sometext`'
     * @return string
     */
    public function getSimpleManyManySqlText(
        $tempTable='temp',
        $fileDelimiter='\t',
        string $filePath,
        string $pivotTable,
        string $pivotTableColumns,
        string $fileColumns,
        string $destinationTable1,
        string $columnsForTable1,
        string $valueColumnsForTable1,
        string $destinationTable2,
        string $columnsForTable2,
        string $valueColumnsForTable2,
        string $link
    )
    {
        return <<<EOF
        # clean workspace
        SET FOREIGN_KEY_CHECKS=0;
        TRUNCATE TABLE `$pivotTable`;
        TRUNCATE TABLE `$destinationTable1`;
        TRUNCATE TABLE `$destinationTable2`;
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `add_to_word_table`;
        DROP TRIGGER IF EXISTS `add_to_translation_table`;
        DROP TRIGGER IF EXISTS `add_to_pivot_table`;

        CREATE TABLE `$tempTable` AS
                SELECT *
                FROM `$destinationTable1`
                NATURAL JOIN `$destinationTable2`
                LIMIT 0;

        CREATE TRIGGER `from_load_data_to_table1` AFTER INSERT ON `$tempTable`
        FOR EACH ROW
           INSERT INTO `$destinationTable1` ($columnsForTable1)
             VALUES ($valueColumnsForTable1);

        CREATE TRIGGER `from_load_data_to_table2` AFTER INSERT ON `$tempTable`
        FOR EACH ROW
           INSERT INTO `$destinationTable2` ($columnsForTable2)
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
}
