<?php

namespace DBohoTest\IO;

use DBoho\IO\DataAccess;
use PDO;
use PDOStatement;
use PHPUnit;
use PDOException;

class PDOMock extends PDO
{
    /** @noinspection PhpMissingParentConstructorInspection */
    public function __construct()
    {
    }
}

/**
 * User: David Wiesner
 * Date: 11.03.16
 * Time: 07:52
 */
class DataAccessTest extends PHPUnit\Framework\TestCase
{
    /**
     * @var PDO
     */
    protected $db;
    /**
     * @var DataAccess
     */
    protected $dataAccess;

    public function testEscapeIdentifiers()
    {

        $firstEsc = $this->dataAccess->quoteIdentifiers('1`st');
        $secEsc = $this->dataAccess->quoteIdentifiers('2``st');

        $this->assertEquals('`1``st`', $firstEsc);
        $this->assertEquals('`2````st`', $secEsc);
    }

    public function testEscapeSchemaTable()
    {

        $firstEsc = $this->dataAccess->quoteIdentifiers('1`st.2`cd');
        $secEsc = $this->dataAccess->quoteIdentifiers('2``st.3``st');

        $this->assertEquals('`1``st`.`2``cd`', $firstEsc);
        $this->assertEquals('`2````st`.`3````st`', $secEsc);
    }

    public function testUnknownTable()
    {
        $this->expectException(PDOException::class);
        $this->expectExceptionMessageRegExp("'no such table.* unknown'");
        return $this->dataAccess->select('unknown');
    }

    public function testSelectWithoutParameter()
    {

        $data = $this->dataAccess->select('books');

        $this->assertSame([
            ['id' => '1', 'title' => 'last', 'price' => '2.0'],
            ['id' => '2', 'title' => 'first', 'price' => '1.5']
        ], $data);
    }

    public function testSelectOneColumnString()
    {

        $ids = $this->dataAccess->select('test`escp', '1`st');
        $prices = $this->dataAccess->select('books', 'price');

        $this->assertSame([['1`st' => '1'], ['1`st' => '2']], $ids);
        $this->assertSame([['price' => '2.0'], ['price' => '1.5']], $prices);
    }

    public function testSelectColumnArrayOne()
    {

        $ids = $this->dataAccess->select('test`escp', ['1`st']);

        $this->assertSame([['1`st' => '1'], ['1`st' => '2']], $ids);
    }

    public function testSelectColumnArrayMore()
    {

        $ids = $this->dataAccess->select('test`escp', ['1`st']);
        $prices = $this->dataAccess->select('books', array('price', 'id'));

        $this->assertSame([['1`st' => '1'], ['1`st' => '2']], $ids);
        $this->assertSame([['price' => '2.0', 'id' => '1'], ['price' => '1.5', 'id' => '2']], $prices);
    }

    public function testSelectColumnArrayOrder()
    {
        $prices_id = $this->dataAccess->select('books', array('price', 'id'));
        $id_prices = $this->dataAccess->select('books', array('id', 'price'));

        $this->assertSame([['price' => '2.0', 'id' => '1'], ['price' => '1.5', 'id' => '2']], $prices_id);
        $this->assertSame([['id' => '1', 'price' => '2.0'], ['id' => '2', 'price' => '1.5']], $id_prices);
    }

    public function testSelectWithOneParameter()
    {

        $data = $this->dataAccess->select('books', [], ['id' => '1']);

        $this->assertSame([['id' => '1', 'title' => 'last', 'price' => '2.0']], $data);
    }

    public function testSelectOneEscapedParameter()
    {

        $data = $this->dataAccess->select('test`escp', [], ['1`st' => '1']);

        $this->assertSame([['1`st' => '1', '2``st' => '2']], $data);
    }

    public function testSelectTwoParameter()
    {
        $data = $this->dataAccess->select('books', [], ['id' => '1', 'price' => '2']);

        $this->assertsame([['id' => '1', 'title' => 'last', 'price' => '2.0']], $data);
    }


    public function testSelectAllSortedNative()
    {
        $first = '1`st';
        $sec = '2``st';
        $sortedSecond = [[$first => '2', $sec => '1'], [$first => '1', $sec => '2']];
        $sortedFirst = [[$first => '1', $sec => '2'], [$first => '2', $sec => '1']];

        $resultSecond = $this->db->query('SELECT * FROM `test``escp` ORDER BY `2````st`', PDO::FETCH_ASSOC)->fetchAll();
        $resultFirst = $this->db->query('SELECT * FROM `test``escp` ORDER BY `1``st`', PDO::FETCH_ASSOC)->fetchAll();

        $this->assertSame($sortedSecond, $resultSecond);
        $this->assertSame($sortedFirst, $resultFirst);
    }


    public function testSelectAllSorted()
    {
        $first = '1`st';
        $sec = '2``st';

        $sortedSecond = [[$first => '2', $sec => '1'], [$first => '1', $sec => '2']];
        $sortedFirst = [[$first => '1', $sec => '2'], [$first => '2', $sec => '1']];

        $resultFirst = $this->dataAccess->select('test`escp', [], [], $first);
        $resultSecond = $this->dataAccess->select('test`escp', [], [], $sec);

        $this->assertSame($sortedFirst, $resultFirst);
        $this->assertSame($sortedSecond, $resultSecond);
    }

    public function testInsertOne()
    {
        $first = '1`st';
        $sec = '2``st';
        $data = [$first => '3', $sec => '4'];

        $rowCount = $this->dataAccess->insert('test`escp', $data);

        $this->assertEquals(1, $rowCount);
        $this->assertEquals(3, $this->dataAccess->getLastInsertId());
        $result = $this->dataAccess->select('test`escp', [], [$first => '3']);
        $this->assertSame([$data], $result);
    }


    public function testInsertFilterUnknownColumn()
    {
        $first = '1`st';
        $sec = '2``st';
        $data = [$first => '3', $sec => '4'];
        $oddData = array_merge($data, ['unknownColumn' => 'none']);

        $rowCount = $this->dataAccess->insert('test`escp', $oddData);

        $this->assertEquals(1, $rowCount);
        $this->assertEquals(3, $this->dataAccess->getLastInsertId());
        $result = $this->dataAccess->select('test`escp', [], [$first => '3']);
        $this->assertSame([$data], $result);
    }

    public function testInsertFilterFillEmpty()
    {
        $first = '1`st';
        $sec = '2``st';
        $data = [$first => '3', $sec => '4'];
        $oddData = array_merge($data, ['unknownColumn' => 'none']);

        $rowCount = $this->dataAccess->insert('test`escp', [$oddData, [$sec => '3']]);

        $this->assertEquals(2, $rowCount);
        $this->assertEquals(4, $this->dataAccess->getLastInsertId());
        $result = $this->dataAccess->select('test`escp', [], [$first => '3']);
        $this->assertSame([$data], $result);

        $result = $this->dataAccess->select('test`escp', [], [$sec => '3']);
        $this->assertSame([[$first => null, $sec => '3']], $result);
    }

    public function testInsertOnlyUnknownColumn()
    {
        $this->expectException(PDOException::class);
        $oddData = ['unknownColumn' => 'none'];

        $this->dataAccess->insert('test`escp', $oddData);
    }

    public function testInsertMultiple()
    {
        $first = '1`st';
        $sec = '2``st';
        $data = [[$first => '3', $sec => '5'], [$first => '6', $sec => '8']];

        $rowCount = $this->dataAccess->insert('test`escp', $data);

        $this->assertEquals(2, $rowCount);
        $this->assertEquals(4, $this->dataAccess->getLastInsertId());
        $result[] = $this->dataAccess->select('test`escp', [], [$first => $data[0][$first]]);
        $result[] = $this->dataAccess->select('test`escp', [], [$first => $data[1][$first]]);
        $this->assertSame([$data[0]], $result[0]);
        $this->assertSame([$data[1]], $result[1]);
    }


    public function testInsertNoDataRaiseException()
    {
        $this->expectException(PDOException::class);
        $this->expectExceptionMessage('empty request');
        $this->dataAccess->insert('table', null);
    }

    public function testInsertNoValidDataRaiseException()
    {
        $this->expectException(PDOException::class);
        $this->expectExceptionMessage('empty request');
        $this->dataAccess->insert('table', ['not_existing' => 1]);
    }

    public function testInsertNoDataSilent()
    {
        $numAffected = $this->dataAccess->insert('table', null, true);

        $this->assertEquals(0, $numAffected);
    }

    public function testInsertValidNoDataSilent()
    {
        $numAffected = $this->dataAccess->insert('table', null, true);

        $this->assertEquals(0, $numAffected);
    }


    public function testUpdateOne()
    {
        $first = '1`st';
        $sec = '2``st';

        $id = $this->dataAccess->update('test`escp', [$sec => 0], [$first => '1']);

        $this->assertEquals(true, $id);
        $result = $this->dataAccess->select('test`escp', [], [$sec => 0]);

        $this->assertSame([[$first => '1', $sec => '0']], $result);
    }


    public function testUpdateAll()
    {
        $first = '1`st';
        $sec = '2``st';

        $id = $this->dataAccess->update('test`escp', [$first => 0, $sec => 0], []);

        $this->assertEquals(true, $id);
        $result = $this->dataAccess->select('test`escp', [], [$first => 0, $sec => 0]);
        $this->assertSame([[$first => '0', $sec => '0'], [$first => '0', $sec => '0']], $result);
    }

    public function testUpdateNoDataRaiseException()
    {
        $this->expectException(PDOException::class);
        $this->expectExceptionMessage('empty request');
        $this->dataAccess->update('table', null);
    }

    public function testUpdateNoValidDataRaiseException()
    {
        $this->expectException(PDOException::class);
        $this->expectExceptionMessage('empty request');
        $this->dataAccess->update('table', ['not_existing' => 1]);
    }

    public function testUpdateNoDataSilent()
    {
        $numAffected = $this->dataAccess->update('table', null, [], true);

        $this->assertEquals(0, $numAffected);
    }

    public function testUpdateValidNoDataSilent()
    {
        $numAffected = $this->dataAccess->update('table', null, [], true);

        $this->assertEquals(0, $numAffected);
    }

    public function testDeleteNone()
    {
        $first = '1`st';
        $sec = '2``st';

        $id = $this->dataAccess->delete('test`escp', [$first => 0, $sec => 0]);

        $this->assertEquals(0, $id);
        $result = $this->dataAccess->select('test`escp');
        $this->assertSame([[$first => '1', $sec => '2'], [$first => '2', $sec => '1']], $result);
    }

    public function testDeleteOne()
    {
        $first = '1`st';
        $sec = '2``st';

        $id = $this->dataAccess->delete('test`escp', [$first => 1]);

        $this->assertEquals(1, $id);
        $result = $this->dataAccess->select('test`escp');
        $this->assertSame([[$first => '2', $sec => '1']], $result);
    }

    public function testDeleteAll()
    {

        $id = $this->dataAccess->delete('test`escp');

        $this->assertEquals(2, $id);
        $result = $this->dataAccess->select('test`escp');
        $this->assertSame([], $result);
    }

    public function testOrderByDefault()
    {
        $first = '1`st';
        $stmt = $this->dataAccess->createOrderByStatement('test`escp', $first);
        $this->assertEquals('ORDER BY `1``st`', trim($stmt));
    }

    public function testOrderByAsc()
    {
        $first = '1`st';
        $stmt = $this->dataAccess->createOrderByStatement('test`escp', $first . ' ASC');
        $this->assertEquals('ORDER BY `1``st` ASC', trim($stmt));
    }

    public function testOrderByDesc()
    {
        $first = '1`st';
        $stmt = $this->dataAccess->createOrderByStatement('test`escp', $first . ' DESC');
        $this->assertEquals('ORDER BY `1``st` DESC', trim($stmt));
    }


    public function testRunReturnStatement()
    {
        $stmt = $this->dataAccess->run('ALTER TABLE books RENAME TO dvds');
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testRunBindFailed()
    {
        $this->expectException(PDOException::class);
        $this->db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_SILENT);
        $this->dataAccess->run('ALTER TABLE books RENAME TO boooks', [':unkown' => '1']);
    }

    public function testRunBindFailedSilent()
    {
        $this->db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_SILENT);
        $result = $this->dataAccess->run('ALTER TABLE books RENAME TO boooks', [':unkown' => '1'], false);
        $this->assertEquals(false, $result);
    }

    /**
     * https://blogs.kent.ac.uk/webdev/2011/07/14/phpunit-and-unserialized-pdo-instances/
     * @backupGlobals          disabled
     * @backupStaticAttributes disabled
     */
    public function testFilterMySQL()
    {
        $db = $this->getMockBuilder(PDOMock::class)->getMock();
        $stmt = $this->createMock(PDOStatement::class);
        $db->expects($this->once())->method('getAttribute')->willReturn('mysql');
        /** @noinspection PhpParamsInspection */
        $db->expects($this->once())->method('prepare')->with('DESCRIBE `books`;')->willReturn($stmt);
        /** @var PDO $db */
        $da = new DataAccess($db);

        $da->filterForTable('books', array('title'));
    }

    /**
     * https://blogs.kent.ac.uk/webdev/2011/07/14/phpunit-and-unserialized-pdo-instances/
     * @backupGlobals          disabled
     * @backupStaticAttributes disabled
     */
    public function testQuotePostgres()
    {
        $db = $this->getMockBuilder(PDOMock::class)->getMock();
        $db->expects($this->once())->method('getAttribute')->willReturn('pgsql');
        /** @var PDO $db */
        $da = new DataAccess($db);

        $result = $da->quoteIdentifiers('ab"c.cd""fg');

        $this->assertEquals('"ab""c"."cd""""fg"', $result);
    }

    public function testQueryPostgresTableColumns()
    {
        $db = $this->getMockBuilder(PDOMock::class)->getMock();
        $stmt = $this->createMock(PDOStatement::class);
        $db->expects($this->once())->method('getAttribute')->willReturn('pgsql');
        /** @noinspection PhpParamsInspection */
        $db->expects($this->once())->method('prepare')
            ->with('SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ;')
            ->willReturn($stmt);
        /** @var PDO $db */
        $da = new DataAccess($db);

        $da->filterForTable('books.test', array('title'));
    }

    public function testQueryPostgresTableColumnsNoSchema()
    {
        $db = $this->getMockBuilder(PDOMock::class)->getMock();
        $stmt = $this->createMock(PDOStatement::class);
        $db->expects($this->once())->method('getAttribute')->willReturn('pgsql');
        /** @noinspection PhpParamsInspection */
        $db->expects($this->once())->method('prepare')
            ->with('SELECT column_name FROM information_schema.columns WHERE table_name = ? ;')
            ->willReturn($stmt);
        /** @var PDO $db */
        $da = new DataAccess($db);

        $da->filterForTable('books', array('title'));
    }

    /**
     * https://blogs.kent.ac.uk/webdev/2011/07/14/phpunit-and-unserialized-pdo-instances/
     * @backupGlobals          disabled
     * @backupStaticAttributes disabled
     */
    public function testFilterUnknownDB()
    {
        $db = $this->getMockBuilder(PDOMock::class)->getMock();
        $stmt = $this->createMock(PDOStatement::class);
        $db->expects($this->once())->method('getAttribute')->willReturn('unknownDB');
        /** @noinspection PhpParamsInspection */
        $db->expects($this->once())->method('prepare')
            ->with('SELECT column_name FROM information_schema.columns WHERE table_name = ? ;')
            ->willReturn($stmt);
        /** @var PDO $db */
        $da = new DataAccess($db);

        $da->filterForTable('books', array('title'));
    }

    public function testFilterNoArray()
    {
        /** @noinspection PhpParamsInspection */
        $result = $this->dataAccess->filterForTable('books', 'noArray');

        $this->assertSame([], $result);
    }

    protected function setUp()
    {
        parent::setUp();
        $this->db = new PDO('sqlite::memory:', '', '');
        $this->setupTable();
        $this->dataAccess = new DataAccess($this->db);
    }

    protected function setupTable()
    {
        $this->db->exec('CREATE TABLE IF NOT EXISTS books (
							id int(11) NOT NULL PRIMARY KEY,  title varchar(200) NOT NULL, price REAL NOT NULL) ;');
        $this->db->exec('ALTER TABLE books ADD PRIMARY KEY (id);');
        $this->db->exec('ALTER TABLE books MODIFY id int(11) NOT NULL AUTO_INCREMENT;');
        $this->db->exec('INSERT INTO `books` (`id`, `title`, `price`) VALUES (1, "last", 2), (2, "first", 1.50);');
        $this->db->exec('INSERT INTO `users` (`id`,`email`, `password`) ' .
            'VALUES (1, "user1", "pass1"), (2, "user2", "pass2");');
        $this->db->exec('CREATE TABLE `test``escp` (`1``st` INT, `2````st` INT NOT NULL);
							INSERT INTO `test``escp` (`1``st`, `2````st`) VALUES (1,2), (2, 1)');

    }
}
