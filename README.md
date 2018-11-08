# pyadt

*A wrapper for working with ADT tables in Python 3.7*

## Prequisites

 - Install the latest ODBC driver [here](http://devzone.advantagedatabase.com/dz/content.aspx?key=20)

## Installation

```
pipenv install pyadt
```

## Usage

### Creating a Conneciton

Supply the path the directory when creating a Connection object
```
import pyadt

c = pyadt.Connection("path_to_directory")
```
Connections are closed by default, so you need to open the connection
```
c.open()
```

## Running Queries

With an open connection SQL queries can be run
```
>>> query = '''INSERT INTO MyTable
...            VALUES (1, 'John', 'Smith');
...         '''
>>> c.run_query(query)
```

Variables can also be supplied
```
>>> query = '''INSERT INTO MyTable
...            VALUES (?, ?, ?);
...         '''
>>> c.run_query(query, 1, "John", "Smith")
```

## Working with Table Contnet

After an SQL `SELECT` query has ran the results are stored as attributes
of the `Connection` object
```
>>> query = '''SELECT * FROM MyTable;'''
>>> c.run_query(query)
>>> c.columns
["Id", "Name", "Surname"]
>>> c.dataset
[(1, 'John     ', 'Smith    '), (2, 'Jack     ', 'Smith    '),]
```

There's an iterator function to yield table rows in a pretty format
```
>>> i = c.iter_dataset()
>>> next(i)
{"Id": 1, "Name": "John", "Surname": "Smith"}
>>> next(i)
{"Id": 2, "Name": "Jack", "Surname": "Smith"}
```
