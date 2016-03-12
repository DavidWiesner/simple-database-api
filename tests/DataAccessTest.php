<?php
use DBoho\IO\DataAccess;

/**
 * User: David Wiesner
 * Date: 11.03.16
 * Time: 07:52
 */
class DataAccessTest extends PHPUnit_Framework_TestCase
{
    /**
     * @var PDO
     */
    private $db;
    /**
     * @var DataAccess
     */
    private $dataAccess;

    public function testEscapeIdentifiers()
    {

        $firstEsc = DataAccess::quoteIdentifiers('1`st');
        $secEsc = DataAccess::quoteIdentifiers('2``st');

        $this->assertEquals('`1``st`', $firstEsc);
        $this->assertEquals('`2````st`', $secEsc);
    }

    public function testGetWithoutParameter()
    {

        $data = $this->dataAccess->select('books');

        $this->assertSame([
                ['id' => '1', 'title' => 'last', 'price' => '2'],
                ['id' => '2', 'title' => 'first', 'price' => '1']
        ], $data);
    }

    public function testGetOneColumnString()
    {

        $ids = $this->dataAccess->select('test`escp', '1`st');
        $prices = $this->dataAccess->select('books', 'price');

        $this->assertSame([['1`st' => '1'],['1`st' => '2']], $ids);
        $this->assertSame([['price' => '2'],['price' => '1']], $prices);
    }

    public function testGetColumnArrayOne()
    {

        $ids = $this->dataAccess->select('test`escp', ['1`st']);

        $this->assertSame([['1`st' => '1'],['1`st' => '2']], $ids);
    }

    public function testGetColumnArrayMore()
    {

        $ids = $this->dataAccess->select('test`escp', ['1`st']);
        $prices = $this->dataAccess->select('books', array('price', 'id'));

        $this->assertSame([['1`st' => '1'],['1`st' => '2']], $ids);
        $this->assertSame([['price' => '2', 'id'=> '1'],['price' => '1', 'id'=> '2']], $prices);
    }

    public function testGetColumnArrayOrder()
    {
        $prices_id = $this->dataAccess->select('books', array('price', 'id'));
        $id_prices = $this->dataAccess->select('books', array('id', 'price'));

        $this->assertSame([['price' => '2', 'id'=> '1'],['price' => '1', 'id'=> '2']], $prices_id);
        $this->assertSame([['id'=> '1', 'price' => '2'],['id'=> '2', 'price' => '1']], $id_prices);
    }

    public function testGetWithOneParameter()
    {

        $data = $this->dataAccess->select('books', [], ['id' => '1']);

        $this->assertSame([['id' => '1', 'title' => 'last', 'price' => '2']], $data);
    }

    public function testGetOneEscapedParameter()
    {

        $data = $this->dataAccess->select('test`escp', [], ['1`st' => '1']);

        $this->assertSame([['1`st' => '1', '2``st' => '2']], $data);
    }

    public function testGetTwoParameter()
    {
        $data = $this->dataAccess->select('books', [], ['id' => '1', 'price' => '2']);

        $this->assertsame([['id' => '1', 'title' => 'last', 'price' => '2']], $data);
    }

    public function testGetAllSortedNative()
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


    public function testGetAllSorted()
    {
        $first = '1`st';
        $sec = '2``st';

        $sortedSecond = [[$first => '2', $sec => '1'], [$first => '1', $sec => '2']];
        $sortedFirst = [[$first => '1', $sec => '2'], [$first => '2', $sec => '1']];

        $resultFirst = $this->dataAccess->getAll('test`escp', ['sort' => $first]);
        $resultSecond = $this->dataAccess->getAll('test`escp', ['sort' => $sec]);

        $this->assertSame($sortedFirst, $resultFirst);
        $this->assertSame($sortedSecond, $resultSecond);
    }


    public function testAddOne()
    {
        $first = '1`st';
        $sec = '2``st';
        $data = [$first => '3', $sec => '4'];

        $id = $this->dataAccess->insert('test`escp', $data);

        $this->assertEquals(1, $id);
        $result = $this->dataAccess->select('test`escp', [], [$first => '3']);
        $this->assertSame([$data], $result);
    }

    public function testAddMultiple()
    {
        $first = '1`st';
        $sec = '2``st';
        $data = [[$first => '3', $sec => '5'], [$first => '6', $sec => '8']];

        $id = $this->dataAccess->insert('test`escp', $data);

        $this->assertEquals(2, $id);
        $result[] = $this->dataAccess->select('test`escp', [], [$first => $data[0][$first]]);
        $result[] = $this->dataAccess->select('test`escp', [], [$first => $data[1][$first]]);
        $this->assertSame([$data[0]], $result[0]);
        $this->assertSame([$data[1]], $result[1]);
    }

    public function testUpdateOne()
    {
        $first = '1`st';
        $sec = '2``st';

        $id = $this->dataAccess->update('test`escp', [$sec => 0], [$first => '1']);

        $this->assertEquals(true, $id);
        $result = $this->dataAccess->select('test`escp', [], [$sec => 0]);

        $this->assertSame([[$first=>'1', $sec=>'0']], $result);
    }

    public function testUpdateAll()
    {
        $first = '1`st';
        $sec = '2``st';

        $id = $this->dataAccess->update('test`escp', [$first => 0, $sec => 0], []);

        $this->assertEquals(true, $id);
        $result = $this->dataAccess->select('test`escp', [], [$first => 0, $sec => 0]);
        $this->assertSame([[$first=>'0', $sec=>'0'], [$first=>'0', $sec=>'0']], $result);
    }

    public function testDeleteNone()
    {
        $first = '1`st';
        $sec = '2``st';

        $id = $this->dataAccess->delete('test`escp', [$first => 0, $sec => 0], []);

        $this->assertEquals(0, $id);
        $result = $this->dataAccess->select('test`escp');
        $this->assertSame([[$first=>'1', $sec=>'2'], [$first=>'2', $sec=>'1']], $result);
    }

    public function testDeleteOne()
    {
        $first = '1`st';
        $sec = '2``st';

        $id = $this->dataAccess->delete('test`escp', [$first => 1]);

        $this->assertEquals(1, $id);
        $result = $this->dataAccess->select('test`escp');
        $this->assertSame([[$first=>'2', $sec=>'1']], $result);
    }

    public function testDeleteAll()
    {
        $first = '1`st';
        $sec = '2``st';

        $id = $this->dataAccess->delete('test`escp');

        $this->assertEquals(2, $id);
        $result = $this->dataAccess->select('test`escp');
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
							id int(11) NOT NULL,  title varchar(200) NOT NULL, price int(11) NOT NULL);');
        $this->db->exec('INSERT INTO `books` (`id`, `title`, `price`) VALUES (1, "last", 2), (2, "first", 1);');
        $this->db->exec('INSERT INTO `users` (`id`,`email`, `password`) ' .
                'VALUES (1, "user1", "pass1"), (2, "user2", "pass2");');
        $this->db->exec('CREATE TABLE `test``escp` (`1``st` INT, `2````st` INT NOT NULL);
							INSERT INTO `test``escp` (`1``st`, `2````st`) VALUES (1,2), (2, 1)');

    }


}
