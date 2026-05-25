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
        $query=<<<SQL
        DROP TABLE IF EXISTS `$tmpTable`;
        CREATE TABLE `$tmpTable`(
            filename varchar(255)
        );
SQL;
        for ($i = 0; $i < count($remoteFiles); $i++) {
            $query .= "\nINSERT INTO `$tmpTable` (filename) VALUES ('" . $remoteFiles[$i] . "');";
        }
        $query.= <<<SQL
        SELECT *
        FROM `$tmpTable`
        WHERE LOAD_FILE(filename) IS NULL;
        DROP TABLE IF EXISTS `$tmpTable`;
SQL;
        return $query;
    }
}
