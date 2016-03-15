# DBoho\IO\DataAccess
[![Build Status](https://travis-ci.org/DavidWiesner/simple-database-api.svg?branch=master)](https://travis-ci.org/DavidWiesner/simple-database-api)

A simple API for Database Access

## Features
 * Simple API
 * prevent SQL injection with automatic whitelist column filter and pdo prepared statements

## Install with Composer
```bash
$ composer require dboho/simple-database-api
```

## Usage
```php
# select
$result = $da->select('books', ['title'], ['author'=>'Rasmus Lerdorf']);
echo(json_encode($result));

# insert
$da->insert('books', ['title'=>'Using the New DB2', 'author'=>'Don Chamberlin']);

# update
$da->update('books', ['price'=>9.80], ['id'=>1023]);

# delete
$da->delete('books', ['id'=>1021]);
```

## Automatic Whitelist Column Filter 
To prevent SQL injection all attributes that are used as column names will be filtered with a whitelist. 
This whitelist is build for each queried database table. 

### Example
Books Table

| **id** | **title**        | **author**     | **price** |
|--------|------------------|----------------|-----------|
|      1 |  Programming PHP | Rasmus Lerdorf |     39.99 |

Whitelist for the books table will contain id, title, author and price.
