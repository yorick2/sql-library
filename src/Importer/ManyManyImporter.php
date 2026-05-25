<?php

namespace PaulMillband\SqlLibrary\Importer;

class ManyManyImporter
{
    /**
     * Import a tsv or csv into a many-many database table pair
     * for more complex imports than flat 1-1
     *
     * @param int $startID what id to give the first row
     * @param string $pivotTable string e.g. 'table1_table2'
     * @param array $pivotTableColumns e.g. [`table1_id`,`table2_id`]
     * @param string $table1 e.g. 'table1'
     * @param string $columnsForTable1 e.g. '`column1`,`column2`'
     * @param string $valueColumnsForTable1 e.g. 'NEW.`column1`,NEW.`column2`'
     * @param string $table2 e.g. 'table2'
     * @param string $columnsForTable2 e.g. '`column3`,`column4`'
     * @param string $valueColumnsForTable2 e.g. 'id, NEW.`column3`,NEW.`column4`'
     * @param array $linkColumns ['text1','text2']
     * @param string $filePath file path e.g. '__DIR__./src/test.tsv'
     * @param string $fileColumns e.g. 'column1,column2'
     * @param int    $ignoreLinesQty number of lines to ignore at the start of the file
     * @param string $fileDelimiter e.g. '\t'
     * @param string $additionalFileImportCommand e.g. "set simple = REGEXP_REPLACE(Word,'[1234]*','')"
     * @param string $tempTable e.g. 'temp'
     * @return string
     */
    static function getComplexImportSqlText(
        string $startID,
        string $pivotTable,
        array $pivotTableColumns,
        array $dataTableNames,
        array $tableColumnsForDataTables,
        array $valueColumnsForDataTables,
        array  $linkColumns,
        string $filePath,
        string $fileColumns,
        int    $ignoreLinesQty=0,
        string $fileDelimiter='\t',
        string $additionalFileImportCommand='',
        string $tempTable='temp'
    ) : string
    {
        if(count($dataTableNames)!=count($tableColumnsForDataTables)){
            throw new Exception('array lengths must be the same for $pivotTableColumns,$dataTableNames,$tableColumnsForDataTables,$valueColumnsForDataTables,$linkColumns');
        }
        if(count($dataTableNames)!=count($valueColumnsForDataTables)){
            throw new Exception('array lengths must be the same for $pivotTableColumns,$dataTableNames,$tableColumnsForDataTables,$valueColumnsForDataTables,$linkColumns');
        }
        if(count($dataTableNames)!=count($linkColumns)){
            throw new Exception('array lengths must be the same for $pivotTableColumns,$dataTableNames,$tableColumnsForDataTables,$valueColumnsForDataTables,$linkColumns');
        }
        $ignoreLinesText='';
        if ($ignoreLinesQty){
            $ignoreLinesText="IGNORE $ignoreLinesQty LINES\n";
        }
        $query=<<<SQL
        # clean workspace
        SET FOREIGN_KEY_CHECKS=0;
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table1`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table2`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table3`;
        
        CREATE TABLE `$tempTable` AS
                SELECT *
                FROM `$dataTableNames[0]`
                
SQL;
        for ($i = 1; $i < count($dataTableNames); $i++) {
            $query.="NATURAL JOIN `$dataTableNames[$i]`\n";
        }
        $query.=<<<SQL
                LIMIT 0;
        
        ALTER TABLE `$tempTable` MODIFY `id` INT PRIMARY KEY AUTO_INCREMENT;
        ALTER TABLE `$tempTable` AUTO_INCREMENT = $startID;

SQL;
        for ($i = 0; $i < count($pivotTableColumns); $i++) {
            $query.=<<<SQL

            Alter TABLE `temp` ADD COLUMN `$pivotTableColumns[$i]` int;
            
            CREATE TRIGGER `from_load_data_to_table$i`
            BEFORE INSERT ON `$tempTable`
            FOR EACH ROW
            BEGIN
                IF (SELECT COUNT(1) FROM `$dataTableNames[$i]` WHERE `$linkColumns[$i]` = new.`$linkColumns[$i]`)=0 THEN
                    INSERT INTO `$dataTableNames[$i]` ($tableColumnsForDataTables[$i])
                        VALUES ($valueColumnsForDataTables[$i]);
                END IF;
            End;
            
SQL;
        }
        $query.=<<<SQL

        LOAD DATA INFILE '$filePath'
        IGNORE INTO TABLE `$tempTable`
        FIELDS TERMINATED BY '$fileDelimiter'
        $ignoreLinesText($fileColumns)
        $additionalFileImportCommand;
        
SQL;
        for ($i = 0; $i < count($dataTableNames); $i++) {
            $query.=<<<SQL
            
            UPDATE
            `$tempTable` tmp$i
            JOIN
                `$dataTableNames[$i]` t$i
            ON
                tmp$i.$linkColumns[$i] = t$i.$linkColumns[$i]
            SET
                tmp$i.`$pivotTableColumns[$i]` = t$i.`id`;

SQL;
        }
        $query .="\nINSERT INTO `$pivotTable` (`$pivotTableColumns[0]`";
        for ($i = 1; $i < count($dataTableNames); $i++) {
            $query .= ",`$pivotTableColumns[$i]`";
        }
        $query .=")\n           SELECT `$pivotTableColumns[0]`";
        for ($i = 1; $i < count($dataTableNames); $i++) {
            $query.=",`$pivotTableColumns[$i]`";
        }
        $query.="\n".<<<SQL
        FROM `$tempTable`;
        
        # cleanup
        SET FOREIGN_KEY_CHECKS=1;
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table1`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table2`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table3`;
SQL;
        return $query;
    }

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
     * @param int    $ignoreLinesQty number of lines to ignore at the start of the file
     * @param string $fileDelimiter e.g. '\t'
     * @param string $additionalFileImportCommand e.g. "set simple = REGEXP_REPLACE(Word,'[1234]*','')"
     * @param string $tempTable e.g. 'temp'
     * @return string
     */
    static function getSimpleImportSqlText(
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
        int    $ignoreLinesQty=0,
        string $fileDelimiter='\t',
        string $additionalFileImportCommand='',
        string $tempTable='temp'
    ) : string
    {
        $ignoreLinesText='';
        if ($ignoreLinesQty){
            $ignoreLinesText="IGNORE $ignoreLinesQty LINES\n";
        }
        return <<<SQL
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
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table1`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table2`;
        DROP TRIGGER IF EXISTS `from_load_data_to_table3`;

SQL;
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
     * @param int $ignoreLinesQty number of lines to ignore at the start of the file
     * @param string $fileDelimiter e.g. '\t'
     * @param string $additionalFileImportCommand e.g. "set simple = REGEXP_REPLACE(Word,'[1234]*','')"
     * @return string
     */
    static function getPivotTableImportSqlText(
        string $pivotTable,
        string $filePath,
        array  $pivotTableColumns,
        array  $tables,
        array  $refColumns,
        int    $ignoreLinesQty=0,
        string $fileDelimiter='\t',
        string $additionalFileImportCommand=''
    ) : string
    {
        $ignoreLinesText='';
        if ($ignoreLinesQty){
            $ignoreLinesText="IGNORE $ignoreLinesQty LINES\n";
        }
        $query = <<<SQL
            LOAD DATA INFILE '$filePath'
            IGNORE
            INTO TABLE `$pivotTable`
            FIELDS TERMINATED BY '$fileDelimiter'
            $ignoreLinesText (
SQL;
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
