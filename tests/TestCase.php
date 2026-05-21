<?php

namespace PaulMillband\SqlLibrary\tests;

use PaulMillband\SqlLibrary\misc\Files;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseSqlHelper;
use PHPUnit\Framework\TestCase as PHPUnitTestCase;

class TestCase extends PHPUnitTestCase
{
    /**
     * @param array $remoteFiles
     * @return void
     */
    public function assertFilesExistOnSqlServer(array $remoteFiles)
    {
        $query=(new Files)->getMissingFilesFromSqlServer(
            $remoteFiles,
            'temp'
        );
        $this->db = DatabaseSqlHelper::getInstance()->getConnection();
        $this->db->multi_query($query);
        // do not remove. multi_query needs this to allow next query to run
        do {
            // Store first result set
            if ($result = $this->db -> store_result()) {
                while ($row = $result -> fetch_row()) {
                    $this->assertTrue(
                        false,
                        'file "'.$row[0].'" not found. please ensure this file is in place and readable. Read README.md file for more instructions'
                    );
                }
                $result -> free_result();
            }
        } while ($this->db->next_result());
    }

    /**
     * @param array $localFiles
     * @return void
     */
    public function assertFilesExistLocally(array $localFiles)
    {
        for ($i = 0; $i < count($localFiles); $i++) {
            $this->assertTrue(
                file_exists($localFiles[$i]),
                'file "'.$localFiles[$i].'" not found. please ensure this file is in place and readable'
            );
        }
    }
}