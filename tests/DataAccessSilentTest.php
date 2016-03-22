<?php
namespace DBohoTest\IO;

use PDO;
use PDOException;


class DataAccessSilentTest extends DataAccessTest
{

    /**
     * @expectedException PDOException
     */
    public function testUnknownTable()
    {
        return $this->dataAccess->select('unknown');
    }

    protected function setUp()
    {
        parent::setUp();
        $this->db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_SILENT);
    }
}
