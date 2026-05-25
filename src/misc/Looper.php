<?php

namespace PaulMillband\SqlLibrary\misc;

class Looper
{
    /**
     * loop while row_count > 0 and iteration limit not hit
     *
     * @param string $command additional commands to add in each loop
     * @param string $condition condition for the loop to continue
     * @param int $maxIterations max amount of rows to do to ensure no infinite loops
     * @return string
     */
    static function getWhileLoopSqlText(
        string $command,
        string $condition,
        int    $maxIterations=1000
    )
    {
        return <<<SQL
        DROP PROCEDURE IF EXISTS temp_looper;

        DELIMITER $$
        CREATE PROCEDURE temp_looper()
        BEGIN
            DECLARE i INT DEFAULT 0;
            WHILE i < $maxIterations AND $condition DO
                    # infinite loop safeguard
                    SET i = i + 1;
                    # additional loop commands
                      $command
                END WHILE;
        END$$
        DELIMITER ;

        DROP PROCEDURE IF EXISTS temp_looper;
SQL;
    }
}
