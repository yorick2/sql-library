<?php

namespace PaulMillband\SqlLibrary\Importer;

class ManyManyImporter
{

    /**
     * Import a tsv or csv into a many-many database table pair
     * only for flat single file 1-1 imports to empty tables
     *
     * @param string $pivotTable string e.g. 'table1_table2'
     * @param array  $pivotTableColumnsArray e.g. ['table1_id','table2_id']
     * @param array  $linkedTables e.g. ['table1','table2']
     * @param array  $columnsForLinkedTablesArray e.g. [['text1','text1b'],['text2','text2b']]
     * @param string $filePath file path e.g. '__DIR__./src/test.tsv'
     * @param array  $fileColumnsArray e.g. ['column1','column2']
     * @param int    $startID
     * @param int    $ignoreLinesQty number of lines to ignore at the start of the file
     * @param string $fileDelimiter e.g. '\t'
     * @param string $additionalFileImportCommand e.g. "set simple = REGEXP_REPLACE(Word,'[1234]*','')"
     * @param string $tempTable e.g. 'temp'
     * @return string
     */
    static function getSimpleImportSqlText(
        string $pivotTable,
        array  $pivotTableColumnsArray,
        array  $linkedTables,
        array  $columnsForLinkedTablesArray,
        string $filePath,
        array  $fileColumnsArray,
        int    $startID=1,
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
        $pivotTableColumns='`'.implode('`,`', $pivotTableColumnsArray).'`';
        $fileColumnsString='`'.implode('`,`', $fileColumnsArray).'`';
        $query=<<<SQL
        # clean workspace
        SET FOREIGN_KEY_CHECKS=0;
        DROP TABLE IF EXISTS `$tempTable`;
SQL;
        for ($i = 0; $i < count($linkedTables); $i++) {
            $query.="\n        DROP TRIGGER IF EXISTS `from_load_data_to_table$i`;";
        }
        $query.="\n";
        $query.=<<<SQL
        DROP TRIGGER IF EXISTS `from_load_data_to_pivot_table`;

        CREATE TABLE `$tempTable` AS
                SELECT *
                FROM `$linkedTables[0]`
SQL;
        for ($i = 1; $i < count($linkedTables); $i++){
            $query.="\n                NATURAL JOIN `$linkedTables[$i]`";
        }
        $query.="\n";
        $query.=<<<SQL
                LIMIT 0;
        ALTER TABLE `$tempTable` MODIFY `id` INT PRIMARY KEY AUTO_INCREMENT;
        ALTER TABLE `$tempTable` AUTO_INCREMENT = $startID;
SQL;
        $vals='';
        for ($i = 0; $i < count($linkedTables); $i++) {
            $columnsForTableString='`id`, `'.implode('`,`', $columnsForLinkedTablesArray[$i]).'`';
            $valueColumnsForTableString='NEW.`id`, NEW.`'.implode('`, NEW.`', $columnsForLinkedTablesArray[$i]).'`';
            $query.="\n\n";
            $query.=<<<SQL
                CREATE TRIGGER `from_load_data_to_table$i` AFTER INSERT ON `$tempTable`
                        FOR EACH ROW
                           INSERT INTO `$linkedTables[$i]` ($columnsForTableString)
                             VALUES ($valueColumnsForTableString);
SQL;
            $vals.='new.`id`,';
        }
        $vals=rtrim($vals,',');
        $query.="\n\n";
        $query.=<<<SQL
        CREATE TRIGGER `from_load_data_to_pivot_table` AFTER INSERT ON `$tempTable`
        FOR EACH ROW
          INSERT INTO `$pivotTable` ($pivotTableColumns)
            VALUES ($vals);

        LOAD DATA INFILE '$filePath'
        IGNORE INTO TABLE `$tempTable`
        FIELDS TERMINATED BY '$fileDelimiter'
        $ignoreLinesText ($fileColumnsString)
        $additionalFileImportCommand;

        # cleanup
        SET FOREIGN_KEY_CHECKS=1;
        DROP TABLE IF EXISTS `$tempTable`;
SQL;
        for ($i = 0; $i < count($linkedTables); $i++) {
            $query.="\n        DROP TRIGGER IF EXISTS `from_load_data_to_table$i`;";
        }
        $query.="\n";
        $query.=<<<SQL
        DROP TRIGGER IF EXISTS `from_load_data_to_pivot_table`;
SQL;
        return $query;
    }

    /**
     * Import a tsv or csv into a many-many database table pair
     * for more complex imports than flat 1-1 or tables not empty
     * note all columns in the file must have a unique name
     *
     * @param int $startID what id to give the first row
     * @param string $pivotTable string e.g. 'table1_table2'
     * @param array $pivotTableColumns e.g. ['table1_id','table2_id','table3_id','table4_id'],
     * @param array $linkedTableNames e.g. ['table1','table2','table3','table4'],
     * @param array $tableColumnsForLinkedTables e.g. [['id','text1','text1b','text1c'],['id','text2','text2b'],...]
     * @param array $valueColumnsForDataTables e.g. [['NEW.`id`','NEW.`text1`'],['NEW.`id`','NEW.`text2`'],...]
     * @param array $linkColumns ['text1','text2',...]
     * @param string $filePath file path e.g. '__DIR__./src/test.tsv'
     * @param array $fileColumns e.g. ['column1','column2']
     * @param int    $ignoreLinesQty number of lines to ignore at the start of the file
     * @param string $fileDelimiter e.g. '\t'
     * @param string $additionalFileImportCommand e.g. "set simple = REGEXP_REPLACE(Word,'[1234]*','')"
     * @param string $tempTable e.g. 'temp'
     * @return string
     */
    static function getComplexImportSqlText(
        string $pivotTable,
        array  $pivotTableColumns,
        array  $linkedTableNames,
        array  $tableColumnsForLinkedTables,
        array  $linkColumns,
        string $filePath,
        array  $fileColumns,
        int    $ignoreLinesQty=0,
        string $fileDelimiter='\t',
        string $additionalFileImportCommand='',
        string $tempTable='temp'
    ) : string
    {
        if(count($linkedTableNames)!=count($tableColumnsForLinkedTables)){
            throw new Exception('array lengths must be the same for $pivotTableColumns,$dataTableNames,$tableColumnsForDataTables,$linkColumns');
        }
        if(count($linkedTableNames)!=count($linkColumns)){
            throw new Exception('array lengths must be the same for $pivotTableColumns,$dataTableNames,$tableColumnsForDataTables,$linkColumns');
        }
        $ignoreLinesText='';
        if ($ignoreLinesQty){
            $ignoreLinesText="IGNORE $ignoreLinesQty LINES\n";
        }
        $fileColumnsString=implode(',',$fileColumns);
        $query=<<<SQL
        # clean workspace
        SET FOREIGN_KEY_CHECKS=0;
        DROP TABLE IF EXISTS `$tempTable`;
SQL;
        for ($i = 0; $i < count($pivotTableColumns); $i++) {
            $query.="\nDROP TRIGGER IF EXISTS `from_load_data_to_table$i`;";
        }
            $query.=<<<SQL
        CREATE TABLE `$tempTable` AS
                SELECT *
                FROM `$linkedTableNames[0]`
                
SQL;
        for ($i = 1; $i < count($linkedTableNames); $i++) {
            $query.="NATURAL JOIN `$linkedTableNames[$i]`\n";
        }
        $query.=<<<SQL
                LIMIT 0;
        
        ALTER TABLE `$tempTable` MODIFY `id` INT PRIMARY KEY AUTO_INCREMENT;
SQL;
        for ($i = 0; $i < count($tableColumnsForLinkedTables); $i++) {
            $tableColumnsForDataTablesImploded='`'.implode('`,`',$tableColumnsForLinkedTables[$i]).'`';
            $valueColumnsForDataTablesImploded='New.`'.implode('`, New.`',$tableColumnsForLinkedTables[$i]).'`';
            $query.=<<<SQL

            Alter TABLE `temp` ADD COLUMN `$pivotTableColumns[$i]` int;
            
            CREATE TRIGGER `from_load_data_to_table$i`
            BEFORE INSERT ON `$tempTable`
            FOR EACH ROW
            BEGIN
                IF (SELECT COUNT(1) FROM `$linkedTableNames[$i]` WHERE `$linkColumns[$i]` = new.`$linkColumns[$i]`)=0 THEN
                    INSERT INTO `$linkedTableNames[$i]` ($tableColumnsForDataTablesImploded)
                        VALUES ($valueColumnsForDataTablesImploded);
                END IF;
            End;
            
SQL;
        }
        $query.=<<<SQL

        LOAD DATA INFILE '$filePath'
        IGNORE INTO TABLE `$tempTable`
        FIELDS TERMINATED BY '$fileDelimiter'
        $ignoreLinesText($fileColumnsString)
        $additionalFileImportCommand;
        
SQL;
        for ($i = 0; $i < count($linkedTableNames); $i++) {
            $query.=<<<SQL
            
            UPDATE
            `$tempTable` tmp$i
            JOIN
                `$linkedTableNames[$i]` t$i
            ON
                tmp$i.$linkColumns[$i] = t$i.$linkColumns[$i]
            SET
                tmp$i.`$pivotTableColumns[$i]` = t$i.`id`;

SQL;
        }
        $cols=implode('`,`', $pivotTableColumns);
        $query.="\n".<<<SQL
        INSERT INTO `$pivotTable` (`$cols`)
          SELECT `$cols` 
          FROM `$tempTable`;
        
        # cleanup
        SET FOREIGN_KEY_CHECKS=1;
        DROP TABLE IF EXISTS `$tempTable`;
SQL;
        for ($i = 0; $i < count($pivotTableColumns); $i++) {
            $query.="\nDROP TRIGGER IF EXISTS `from_load_data_to_table$i`;";
        }
        return $query;
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
