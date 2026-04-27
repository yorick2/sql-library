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
     * @param int    $ignoreLinesInt number of lines to ignore at the start of the file
     * @param string $fileDelimiter e.g. '\t'
     * @param string $additionalFileImportCommand e.g. "set simple = REGEXP_REPLACE(Word,'[1234]*','')"
     * @param string $tempTable e.g. 'temp'
     * @return string
     */
    static function getSqlText(
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
        string $additionalFileImportCommand='',
        string $tempTable='temp'
    ) : string
    {
        $ignoreLinesText='';
        if ($ignoreLinesInt){
            $ignoreLinesText="IGNORE $ignoreLinesInt LINES\n";
        }
        return <<<EOF
        # clean workspace
        SET FOREIGN_KEY_CHECKS=0;
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
        $ignoreLinesText($fileColumns)
        $additionalFileImportCommand;

        # cleanup
        SET FOREIGN_KEY_CHECKS=1;
#       DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table1`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table2`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table3`;

EOF;
    }

    /**
     * import pivot table based on two columns in the tables provided
     * note: only imports pivot table data
     *
     * @param string $pivotTable string e.g. 'table1_table2'
     * @param string $filePath file path e.g. '__DIR__./src/test.tsv'
     * @param array $pivotTableColumns pivot table columns in the order of the related column in the file e.g. ['table1_id','table2_id']
     * @param array $tables tables in the order of the related column in the file e.g. ['table1','table2']
     * @param array $refColumns the column the tables to match the file data to. In the order of the related column in the file e.g. ['ref','ref']
     * @param int $ignoreLinesInt number of lines to ignore at the start of the file
     * @param string $fileDelimiter e.g. '\t'
     * @param string $additionalFileImportCommand e.g. "set simple = REGEXP_REPLACE(Word,'[1234]*','')"
     * @return string
     */
    static function getPivotTableImportSqlText(
        string $pivotTable,
        string $filePath,
        array $pivotTableColumns,
        array $tables,
        array $refColumns,
        int    $ignoreLinesInt=0,
        string $fileDelimiter='\t',
        string $additionalFileImportCommand=''
    ) : string
    {
        $ignoreLinesText='';
        if ($ignoreLinesInt){
            $ignoreLinesText="IGNORE $ignoreLinesInt LINES\n";
        }
        $query = <<<EOF
            LOAD DATA INFILE '$filePath'
            IGNORE
            INTO TABLE `$pivotTable`
            FIELDS TERMINATED BY '$fileDelimiter'
            $ignoreLinesText (
EOF;
        for($i=0; $i<count($pivotTableColumns); $i++) {
            $query .= '@ref' . $i . ',';
        }
        $query = rtrim($query,',').") \n SET";
        for($i=0; $i<count($pivotTableColumns); $i++){
            $query.= "`$pivotTableColumns[$i]` = (SELECT `id` FROM `$tables[$i]` WHERE `$refColumns[$i]` = @ref$i),";
        }
        return rtrim($query,',').'
        '.$additionalFileImportCommand.';';
    }
}
