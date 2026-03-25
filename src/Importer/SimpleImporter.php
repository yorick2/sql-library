<?php

namespace PaulMillband\SqlLibrary\Importer;

class SimpleImporter
{
    /**
     * Import a tsv or csv, one-one relationships into a many-many database
     *
     * @param $table string e.g. 'table1'
     * @param $filePath string file path e.g. '__DIR__./src/test.tsv'
     * @param $fileColumns string e.g. 'column1,column2'
     * @param int $ignoreLinesInt how many rows to ignore from table
     * @param $fileDelimiter string e.g. '\t'
     * @return string
     */
    static function getSqlText(
        string $table,
        string $filePath,
        string $fileColumns,
        int    $ignoreLinesInt=0,
        string $fileDelimiter='\t'
    )
    {
        $ignoreLinesText='';
        if ($ignoreLinesInt){
            $ignoreLinesText="IGNORE $ignoreLinesInt LINES\n";
        }
        return <<<EOF
            LOAD DATA INFILE '$filePath'
            IGNORE INTO TABLE `$table`
            FIELDS TERMINATED BY '$fileDelimiter'
            $ignoreLinesText($fileColumns);
EOF;
    }
}
