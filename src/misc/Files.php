<?php

namespace PaulMillband\SqlLibrary\misc;

class Files
{
    /**
     * @param array $remoteFiles
     * @param string $tmpTable
     * @return string
     */
    static function getMissingFilesFromSqlServer(
        array $remoteFiles,
        string $tmpTable='temp'
    ) : string
    {
        return <<<EOF
        DROP TABLE IF EXISTS `$tmpTable`;
        CREATE TABLE `$tmpTable`(
            filename varchar(255)
        );
EOF;
        for ($i = 0; $i < count($remoteFiles); $i++) {
            $query .= "\nINSERT INTO `$tmpTable` (filename) VALUES ('" . $remoteFiles[$i] . "');";
        }
        $query.= <<<EOF
        SELECT *
        FROM `$tmpTable`
        WHERE LOAD_FILE(filename) IS NULL;
        DROP TABLE IF EXISTS `$tmpTable`;
EOF;
    }
}
