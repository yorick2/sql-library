<?php

namespace PaulMillband\SqlLibrary\Importer;

class SimpleImporter
{
    /**
     * generate SQL to import a tsv or csv into a single table
     *
     * @param string $table e.g. 'table1'
     * @param string $filePath file path e.g. '__DIR__./src/test.tsv'
     * @param string $fileColumns e.g. 'column1,column2'
     * @param int $ignoreLinesInt how many rows to ignore from table
     * @param string $fileDelimiter e.g. '\t'
     * @param string $additionalFileImportCommand e.g. "set simple = REGEXP_REPLACE(Word,'[1234]*','')"
     * @return string
     */
    static function getSqlText(
        string $table,
        string $filePath,
        string $fileColumns,
        int    $ignoreLinesInt = 0,
        string $fileDelimiter = '\t',
        string $additionalFileImportCommand = ''
    )
    {
        $ignoreLinesText = '';
        if ($ignoreLinesInt) {
            $ignoreLinesText = "IGNORE $ignoreLinesInt LINES\n";
        }
        return <<<EOF
            LOAD DATA INFILE '$filePath'
            IGNORE INTO TABLE `$table`
            FIELDS TERMINATED BY '$fileDelimiter'
            $ignoreLinesText($fileColumns)
            $additionalFileImportCommand;
EOF;
    }
}
