# pyadt.api

"""Implements the api module."""

import pyodbc

from .exceptions import *


class Connection(object):
    """The Connection class."""

    def __init__(self, datasource, *args, **kwargs):
        """Initializes a Connection instance

        Args:
            datasource(str): Path to data source
        """
        self.cnxn = None
        self.datasource = datasource
        self.isopen = False
        self.dataset = None
        self.columns = None
        self.open()

    def open(self):
        """Connects to `self.datasource`."""
        cnxn_str = (
            'DRIVER={{Advantage StreamlineSQL ODBC}};'
            'DataDirectory={};'
            'ServerTypes=1;'
        )
        self.cnxn = pyodbc.connect(
            cnxn_str.format(self.datasource), autocommit=True
        )
        self.isopen = True

    def close(self):
        """Closes connection at `self.cnxn`."""
        self.cnxn.close()
        self.isopen = False

    def run_query(self, query, *args, persist=True):
        """Executes a query with specified `args`."""
        if self.isopen:
            with self.cnxn.cursor() as cursor:
                # Queries the data table to load the data into the
                # cursor object.
                if args:
                    cursor.execute(query, (args))
                else:
                    cursor.execute(query)
                # When `persist` is True then update the class attribs.
                if persist:
                    # References data using self.dataset
                    self.dataset = cursor.fetchall()
                    # Populates self.columns with column names from the
                    # cursors description attribute.
                    self.columns = [column[0] for column in cursor.description]
        else:
            raise ClosedDataException

    def read_table(self, table):
        """Reads all data stored in `table`."""
        # Raise a ClosedDataException if connection is closed.
        self.run_query('SELECT * FROM {0};'.format(clean_table(table)))

    def iter_dataset(self):
        """Iterates over each row in `self.dataset`."""
        
        for row in self.dataset:
            row_dict = {}
            for index, item in enumerate(row):
                if type(item) is str:
                    item = item.strip()
                row_dict[self.columns[index]] = item
            yield row_dict


def clean_table(table):
    """Removes all non-alphanumerics from `table`.
    
    See 'https://stackoverflow.com/a/3247553'
    """
    return ''.join(chr for chr in table if chr.isalnum())
