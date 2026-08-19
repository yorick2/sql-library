<?php

namespace PaulMillband\SqlLibrary\Importer;

class OneManyImporter
{
    /**
     * generate SQL to import a tsv or csv into a one-many database as two tables
     * only for flat single file 1-1 imports to empty tables, and it will import duplicates
     *
     * @param int    $startID e.g. 1
     * @param string $mainTable string e.g. 'table1'
     * @param array  $columnsForMainTableArray e.g. ['text1','text1b']
     * @param array  $linkColumnForMainTableArray e.g. ['table2_id','table3_id']
     * @param array  $linkedTableNameArray ['table2','table3',]
     * @param array  $columnsForLinkedTableArray [['text2','text2b'],['text3','text3b']]
     * @param string $filePath file path e.g. '__DIR__./src/test.tsv'
     * @param array  $fileColumnsArray e.g. ['text1','text1b','text2','text2b','text3','text3b'] columns starting with @ are assigned to a variable and  are not imported e.g. @ignore_me
     * @param int    $ignoreLinesQty No. of lines to ignore in the file e.g. for the header
     * @param string $fileDelimiter e.g. '\t'
     * @param string $additionalFileImportCommand e.g. "set text1_simple = REGEXP_REPLACE(text1,'[1234]*','')"
     * @param string $tempTable e.g. 'temp'
     * @return string
     */
    static function getSimpleImportSqlText(
        string $mainTable,
        array  $columnsForMainTableArray,
        array  $linkColumnForMainTableArray,
        array  $linkedTableNameArray,
        array  $columnsForLinkedTableArray,
        string $filePath,
        array  $fileColumnsArray,
        int    $startID=1,
        int    $ignoreLinesQty=0,
        string $fileDelimiter='\t',
        string $additionalFileImportCommand='',
        string $tempTable='temp'
    )
    {
        $ignoreLinesText='';
        if($ignoreLinesQty>0){
            $ignoreLinesText="IGNORE $ignoreLinesQty LINES\n";
        }
        $fileColumnsString=implode(',' ,$fileColumnsArray);
        $columnsForMainTableString='`'.implode('`,`' ,$columnsForMainTableArray).'`';
        $valuesForMainTableString='New.`'.implode('`, New.`' ,$columnsForMainTableArray).'`';
        $query=<<<SQL
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `from_load_data_to_main_table`;
SQL;
        for ($i = 0; $i < count($linkedTableNameArray); $i++) {
            $query.="\nDROP TRIGGER IF EXISTS `from_load_data_to_table_$i`;";
        }
        $query.="\n\n";
        $query.=<<<SQL
        CREATE TABLE `$tempTable` AS (
            SELECT *
                FROM `$mainTable`
SQL;
        for ($i = 0; $i < count($linkedTableNameArray); $i++) {
            $query.="\n                NATURAL JOIN `$linkedTableNameArray[$i]`";
            }
        $query.="\n";
        $query.=<<<SQL
                LIMIT 0);
        ALTER TABLE `$tempTable` MODIFY `id` INT PRIMARY KEY AUTO_INCREMENT;
        ALTER TABLE `$tempTable` AUTO_INCREMENT = $startID;
SQL;
        for ($i = 0; $i < count($linkedTableNameArray); $i++) {
            $columnsForLinkedTablesString = '`' . implode('`,`', $columnsForLinkedTableArray[$i]) . '`';
            $valuesForLinkedTableString = 'New.`' . implode('`,New.`', $columnsForLinkedTableArray[$i]) . '`';
            $query .= "\n\n";
            $query .= <<<SQL
      CREATE TRIGGER `from_load_data_to_table_$i` AFTER INSERT ON `$tempTable`
        FOR EACH ROW
           INSERT INTO `$linkedTableNameArray[$i]` ($columnsForLinkedTablesString)
             VALUES ($valuesForLinkedTableString);
SQL;
        }
            $query.="\n\n";
        $cols='';
        $vals='';
        for ($i = 0; $i < count($linkedTableNameArray); $i++) {
            $cols.=", `$linkColumnForMainTableArray[$i]`";
            $vals.=", New.`id`";
        }
            $query.=<<<SQL
      CREATE TRIGGER `from_load_data_to_main_table` AFTER INSERT ON `$tempTable`
        FOR EACH ROW
           INSERT INTO `$mainTable` ($columnsForMainTableString $cols)
             VALUES ($valuesForMainTableString $vals);

        LOAD DATA INFILE '$filePath'
        IGNORE INTO TABLE `$tempTable`
        FIELDS TERMINATED BY '$fileDelimiter'
        $ignoreLinesText($fileColumnsString)
        $additionalFileImportCommand;
        
    # cleanup
        DROP TABLE IF EXISTS `$tempTable`;
        DROP TRIGGER IF EXISTS `from_load_data_to_main_table`;
SQL;
        for ($i = 0; $i < count($linkedTableNameArray); $i++) {
            $query.="\nDROP TRIGGER IF EXISTS `from_load_data_to_table_$i`;";
        }
        return $query;
    }

    /**
     *  generate SQL to import a tsv or csv into a one-many database as two or more tables. With the main database able
     * to have duplicates but the $linkColumnForMainTableArray are unique in linked tables.
     *
     * @param string $mainTable string e.g. 'table1'
     * @param array $columnsForMainTableArray e.g. ['text1','text1b]
     * @param array $linkColumnForMainTableArray e.g. ['table2_id', 'table3_id', 'table4_id']
     * @param array $searchColumnsInLinkedTableArray column used to create the link and also eliminate duplicates e.g. ['text1','text2']
     * @param array $linkedTableNameArray e.g. ['table2', 'table3', 'table4']
     * @param array $tableColumnsForLinkedTables e.g. [['text2','text2b'],['text3','text3b'],['text4','text4b']],
     * @param string $filePath file path e.g. '__DIR__./src/test.tsv'
     * @param array $fileColumnsArray e.g. ['column1', 'column2'] columns starting with @ are assigned to a variable and  are not imported e.g. @ignore_me
     * @param int $ignoreLinesQty No. of lines to ignore in the file e.g. for the header
     * @param string $fileDelimiter e.g. '\t'
     * @param string $additionalFileImportCommand e.g. "set simple = REGEXP_REPLACE(Word,'[1234]*','')"
     * @param string $tempTable e.g. 'temp'
     * @return string
     */
    static function getComplexImportSqlText(
        string $mainTable,
        array  $columnsForMainTableArray,
        array  $linkColumnForMainTableArray,
        array  $searchColumnsInLinkedTableArray,
        array  $linkedTableNameArray,
        array  $tableColumnsForLinkedTables,
        string $filePath,
        array  $fileColumnsArray,
        int    $ignoreLinesQty=0,
        string $fileDelimiter='\t',
        string $additionalFileImportCommand='',
        string $tempTable='temp'
    )
    {
        $ignoreLinesText='';
        if($ignoreLinesQty){
            $ignoreLinesText="IGNORE $ignoreLinesQty LINES\n";
        }
        $fileColumnsString=implode(',' ,$fileColumnsArray);
        $query=<<<SQL
    DROP TABLE IF EXISTS `$tempTable`;
    DROP TRIGGER IF EXISTS `from_load_data_to_main_table`;
SQL;
        for ($i = 0; $i < count($linkedTableNameArray); $i++) {
            $query.="\nDROP TRIGGER IF EXISTS `from_load_data_to_table_$i`;";
        }
        $query.="\n\n";
        $query.=<<<SQL
    CREATE TABLE `$tempTable` AS (
        SELECT *
            FROM `$mainTable`
SQL;
        for ($i = 0; $i < count($linkedTableNameArray); $i++) {
            $query.="\n                NATURAL JOIN `$linkedTableNameArray[$i]`";
        }
        $query.="\n";
        $query.=<<<SQL
                LIMIT 0);
        
        ALTER TABLE `$tempTable` MODIFY `id` INT PRIMARY KEY AUTO_INCREMENT;
SQL;
        for ($i = 0; $i < count($tableColumnsForLinkedTables); $i++) {
            $tableColumnsForDataTablesImploded='`'.implode('`,`',$tableColumnsForLinkedTables[$i]).'`';
            $valueColumnsForDataTablesImploded='New.`'.implode('`, New.`',$tableColumnsForLinkedTables[$i]).'`';
            $query.="\n";
            $query.=<<<SQL
            CREATE TRIGGER `from_load_data_to_table$i`
            BEFORE INSERT ON `$tempTable`
            FOR EACH ROW
            BEGIN
                IF (SELECT COUNT(1) FROM `$linkedTableNameArray[$i]` WHERE `$searchColumnsInLinkedTableArray[$i]` = new.`$searchColumnsInLinkedTableArray[$i]`)=0 THEN
                    INSERT INTO `$linkedTableNameArray[$i]` ($tableColumnsForDataTablesImploded)
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
        for ($i = 0; $i < count($linkedTableNameArray); $i++) {
            $query.=<<<SQL
            
            UPDATE
            `$tempTable` tmp$i
            JOIN
                `$linkedTableNameArray[$i]` t$i
            ON
                tmp$i.$searchColumnsInLinkedTableArray[$i] = t$i.$searchColumnsInLinkedTableArray[$i]
            SET
                tmp$i.`$linkColumnForMainTableArray[$i]` = t$i.`id`;

SQL;
    }
        $cols=implode('`,`', $columnsForMainTableArray)
            .'`,`'
            .implode('`,`', $linkColumnForMainTableArray);
$query.="\n".<<<SQL
      INSERT INTO `$mainTable` (`$cols`)
        SELECT `$cols`
        FROM `$tempTable`;
        
# cleanup
    DROP TABLE IF EXISTS `$tempTable`;
    DROP TRIGGER IF EXISTS `from_load_data_to_main_table`;
SQL;
            for ($i = 0; $i < count($linkedTableNameArray); $i++) {
                $query.="\nDROP TRIGGER IF EXISTS `from_load_data_to_table_$i`;";
            }
            return $query;
        }

    /**
     * generate SQL to import a tsv or csv into a one-many database
     *
     * @param string $destinationTable e.g. 'table2'
     * @param string $referenceTable e.g. 'table1'
     * @param string $fileLinkToReferenceTable .g. 'text2'
     * @param string $referenceTableLinkToFile e.g. 'text2'
     * @param string $destinationTableLinkToReferenceTable e.g. 'table2_id'
     * @param string $referenceTableLinkToDestinationTable e.g. 'id'
     * @param string $filePath file path e.g. '__DIR__./src/test.tsv'
     * @param array $fileColumnArray e.g. ['column1','column2'] columns starting with @ are assigned to a variable and  are not imported e.g. @ignore_me
     * @param bool $fileLinkColumnExists add link column to table
     * @param int $ignoreLinesQty No . of lines to ignore in the file e.g. for the header
     * @param string $fileDelimiter e .g. '\t'
     * @param string $additionalFileImportCommand e .g. "set simple = REGEXP_REPLACE(Word,'[1234]*','')"
     * @param string $tempTable e .g. 'temp'
     * @return string
     */
    static function getAddNewTableSql(
        string $destinationTable,
        string $referenceTable,
        string $fileLinkToReferenceTable,
        string $referenceTableLinkToFile,
        string $destinationTableLinkToReferenceTable,
        string $referenceTableLinkToDestinationTable,
        string $filePath,
        array  $fileColumnArray,
        bool   $fileLinkColumnExists = false,
        int    $ignoreLinesQty=0,
        string $fileDelimiter='\t',
        string $additionalFileImportCommand='',
        string $tempTable='temp'
    )
    {
        $ignoreLinesText='';
        if($ignoreLinesQty>0){
            $ignoreLinesText="IGNORE $ignoreLinesQty LINES\n";
        }
        $selections = "d.*";
        $dropFileLinkColumn='';
        if($fileLinkColumnExists === false){
            // not wanted in destination folder
            $dropFileLinkColumn="ALTER TABLE `$tempTable` DROP COLUMN `$fileLinkToReferenceTable`;";

            // adds a column with the column type of $referenceTableLinkToFile, but named $fileLinkToReferenceTable
            $selections = "d.*, r.`$referenceTableLinkToFile` As `$fileLinkToReferenceTable`";
        }
        $fileColumnString=implode(',' ,$fileColumnArray);
        return <<<SQL
        DROP TABLE IF EXISTS `$tempTable`;

        CREATE TABLE `$tempTable` AS (
            SELECT $selections
                FROM `$destinationTable` d
                NATURAL JOIN `$referenceTable` r
                LIMIT 0);
            
        LOAD DATA INFILE '$filePath'
            IGNORE INTO TABLE `$tempTable`
            FIELDS TERMINATED BY '$fileDelimiter'
            $ignoreLinesText($fileColumnString)
            $additionalFileImportCommand;

        UPDATE `$tempTable` t
            INNER JOIN `$referenceTable` r
                ON t.$fileLinkToReferenceTable = r.$referenceTableLinkToFile
            SET t.$destinationTableLinkToReferenceTable = r.$referenceTableLinkToDestinationTable;

        $dropFileLinkColumn
        INSERT INTO `$destinationTable`
            SELECT *
            FROM `$tempTable`;

    # cleanup
        DROP TABLE IF EXISTS `$tempTable`;
SQL;
    }
}
