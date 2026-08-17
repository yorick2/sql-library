<?php

namespace PaulMillband\SqlLibrary\tests\Helper;

class DatabaseHelper
{
    static function getDatabaseName(): string
    {
        return DatabaseSqlHelper::getInstance()
            ->getConnection()
            ->query('SELECT DATABASE()')
            ->fetch_row()[0];
    }

    static function dropTables(): void
    {
        $db = DatabaseSqlHelper::getInstance()->getConnection();
        $query=<<<SQL
            DROP TABLE IF EXISTS `tempTable`;
            DROP TABLE IF EXISTS `link`;
            DROP TABLE IF EXISTS `table1`;
            DROP TABLE IF EXISTS `table2`;
            DROP TABLE IF EXISTS `table3`;
            DROP TABLE IF EXISTS `table4`;
SQL;
        $db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        do {
        } while ($db->next_result());
        $tablesQty=$db->query('SHOW TABLES')->num_rows;
        if($tablesQty){
            error_log('table didnt empty');
        }
    }
}