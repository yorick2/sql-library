<?php

namespace PaulMillband\SqlLibrary\Importer;

class SimpleImporter
{
    /**
     * generate SQL to import a tsv or csv into a single table
     *
     * @param string $table e.g. 'table1'
     * @param string $filePath file path e.g. '__DIR__./src/test.tsv'
     * @param array $fileColumnsArray e.g. ['column1','column2'] columns starting with @ are assigned to a variable and  are not imported e.g. @ignore_me
     * @param int $ignoreLinesQty how many rows to ignore from table
     * @param string $fileDelimiter e.g. '\t'
     * @param string $additionalFileImportCommand e.g. "set simple = REGEXP_REPLACE(Word,'[1234]*','')"
     * @return string
     */
    static function getSqlText(
        string $table,
        string $filePath,
        array $fileColumnsArray,
        int    $ignoreLinesQty = 0,
        string $fileDelimiter = '\t',
        string $additionalFileImportCommand = ''
    )
    {
        $fileColumnsString=implode(',', $fileColumnsArray);
        $ignoreLinesText = '';
        if ($ignoreLinesQty) {
            $ignoreLinesText = "IGNORE $ignoreLinesQty LINES\n";
        }
        return <<<SQL
            LOAD DATA INFILE '$filePath'
            IGNORE INTO TABLE `$table`
            FIELDS TERMINATED BY '$fileDelimiter'
            $ignoreLinesText($fileColumnsString)
            $additionalFileImportCommand;
SQL;
    }
}
