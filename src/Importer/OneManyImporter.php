<?php

namespace PaulMillband\SqlLibrary\Importer;

class OneManyImporter
{
    /**
     * Import a tsv or csv, one-one relationships into a many-many database
     *
     * @param $table1 string e.g. 'table1'
     * @param $columnsForTable1 string e.g. '`column1`,`column2`'
     * @param $valueColumnsForTable1 string e.g. 'NEW.`column1`,NEW.`column2`'
     * @param $table2 string e.g. 'table2'
     * @param $columnsForTable2 string e.g. '`table1_id`,`column3`,`column4`'
     * @param $valueColumnsForTable2 string e.g. 'id, NEW.`column3`,NEW.`column4`'
     * @param $link string e.g. 'WHERE `sometext` = NEW.`sometext`'
     * @param $filePath string file path e.g. '__DIR__./src/test.tsv'
     * @param $fileColumns string e.g. 'column1,column2'
     * @param $fileDelimiter string e.g. '\t'
     * @param $tempTable string e.g. 'temp'
     * @return string
     */
    static function getSqlText(
        string $table1,
        string $columnsForTable1,
        string $valueColumnsForTable1,
        string $table2,
        string $columnsForTable2,
        string $valueColumnsForTable2,
        string $link,
        string $filePath,
        string $fileColumns,
        string $fileDelimiter='\t',
        string $tempTable='temp'
    )
    {
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
             ON DUPLICATE KEY UPDATE `Hebrew` = VALUES(`Hebrew`);

        CREATE TRIGGER `from_load_data_to_table2` AFTER INSERT ON `$tempTable`
        FOR EACH ROW
           INSERT INTO `$table2` ($columnsForTable2)
             SELECT $valueColumnsForTable2
             FROM `$table1`
             $link;

        LOAD DATA INFILE '$filePath'
        IGNORE INTO TABLE `$tempTable`
        FIELDS TERMINATED BY '$fileDelimiter'
        IGNORE 1 LINES
        ($fileColumns);
        
        # cleanup
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table1`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table2`;
EOF;
    }
}
