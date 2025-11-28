<?php

namespace PaulMillband\Importer;

class OneManyImporter
{
    /**
     * Import a tsv or csv, one-one relationships into a many-many database
     *
     * @param $tempTable string e.g. 'temp'
     * @param $fileDelimiter string e.g. '\t'
     * @param $filePath string file path e.g. '__DIR__./src/test.tsv'
     * @param $tableColumns string e.g. 'column1,column2'
     * @param $destinationTable1 string e.g. 'table1'
     * @param $columnsForTable1 string e.g. '`column1`,`column2`'
     * @param $valueColumnsForTable1 string e.g. 'NEW.`column1`,NEW.`column2`'
     * @param $destinationTable2 string e.g. 'table2'
     * @param $columnsForTable2 string e.g. '`table1_id`,`column3`,`column4`'
     * @param $valueColumnsForTable2 string e.g. 'id, NEW.`column3`,NEW.`column4`'
     * @param $link string e.g. 'WHERE `sometext` = NEW.`sometext`'
     * @return string
     */
    static function getSqlText(
        $tempTable='temp',
        $fileDelimiter='\t',
        string $filePath,
        string $tableColumns,
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
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `from_load_data`;
        DROP TRIGGER IF EXISTS `from_load_data_2`;

        CREATE TABLE `$tempTable` AS
                SELECT *
                FROM `$destinationTable1`
                NATURAL JOIN `$destinationTable2`
                LIMIT 0;

        CREATE TRIGGER `from_load_data_to_table1` AFTER INSERT ON `$tempTable`
        FOR EACH ROW
           INSERT INTO `$destinationTable1` ($columnsForTable1)
             VALUES ($valueColumnsForTable1)
             ON DUPLICATE KEY UPDATE `Hebrew` = VALUES(`Hebrew`);

        CREATE TRIGGER `from_load_data_to_table2` AFTER INSERT ON `$tempTable`
        FOR EACH ROW
           INSERT INTO `$destinationTable2` ($columnsForTable2)
             SELECT $valueColumnsForTable2
             FROM `$destinationTable1`
             $link;

        LOAD DATA INFILE '$filePath'
        IGNORE INTO TABLE `$tempTable`
        FIELDS TERMINATED BY '$fileDelimiter'
        IGNORE 1 LINES
        ($tableColumns);
        
        # cleanup
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table1`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table2`;
EOF;
    }
}
