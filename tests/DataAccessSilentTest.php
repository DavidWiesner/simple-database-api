<?php

namespace DBohoTest\IO;

use PDO;
use PDOException;


class DataAccessSilentTest extends DataAccessTest
{

    public function testUnknownTable()
    {
        $this->expectException(PDOException::class);
        return $this->dataAccess->select('unknown');
    }

    protected function setUp()
    {
        parent::setUp();
        $this->db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_SILENT);
    }
}
