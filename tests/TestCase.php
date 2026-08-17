<?php

namespace PaulMillband\SqlLibrary\tests;

use PaulMillband\SqlLibrary\misc\Files;
use PaulMillband\SqlLibrary\tests\Helper\DatabaseSqlHelper;
use PHPUnit\Framework\TestCase as PHPUnitTestCase;

class TestCase extends PHPUnitTestCase
{
    protected function tearDown(): void
    {
        $result = $this->db
            ->query("Show tables")
            ->fetch_all( MYSQLI_ASSOC);
        foreach ($result as $row) {
            $this->assertNotEquals('tempTable', $row['Tables_in_test'], 'tempTable table still exists');
        }
        parent::tearDown();
    }

    /**
     * @param array $remoteFiles
     * @return void
     */
    public function assertFilesExistOnSqlServer(array $remoteFiles)
    {
        $query=(new Files)->getMissingFilesFromSqlServer(
            $remoteFiles,
            'tempTable'
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
                        "file not found: '$row[0]'.\nplease ensure this file is in place and readable. Read README.md file for more instructions\n\n"
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
                "file not found: '$localFiles[$i]'.\nPlease ensure this file is in place and readable\n\n"
            );
        }
    }
}