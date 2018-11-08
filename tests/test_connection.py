import tempfile
import unittest

from pyadt import Connection
from pyadt import exceptions as e


class TestConnectionInit(unittest.TestCase):
    """Unit tests for Connection.__init__"""

    def test_attribute_declaration(self):
        obj = Connection("DataSource")
        self.assertEqual(obj.datasource, "DataSource")
        self.assertIsNone(obj.cnxn)
        self.assertIsNone(obj.dataset)
        self.assertIsNone(obj.columns)
        self.assertFalse(obj.isopen)

    def test_open(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            obj = Connection(tmpdir)
            obj.open()
            self.assertTrue("pyodbc.Connection" in obj.cnxn.__str__())
            self.assertTrue(obj.isopen)
            obj.cnxn.close()

    def test_close(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            obj = Connection(tmpdir)
            obj.open()
            obj.close()
            self.assertFalse(obj.isopen)

    def test_run_query_insert(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            obj = Connection(tmpdir)
            obj.open()
            create_test_table(obj)
            query = '''INSERT INTO test_table
                VALUES (1, 'Test');'''
            obj.run_query(query)
            # Eval if test_contains record
            record = return_one_record(obj)
            self.assertEqual(record[0], 1)
            self.assertEqual(record[1].strip(), "Test")
            obj.cnxn.close()

    def test_run_query_select(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            obj = Connection(tmpdir)
            obj.open()
            create_test_table(obj)
            
            # Insert a test record into test_table
            query = '''INSERT INTO test_table
                VALUES (1, 'Test');'''
            with obj.cnxn.cursor() as c:
                c.execute(query)
            
            # Invoke run_query
            query = '''SELECT * FROM test_table;'''
            obj.run_query(query)

            # Eval if obj.dataset contains test_table records
            self.assertEqual(len(obj.dataset), 1)
            self.assertEqual(obj.dataset[0][0], 1)
            self.assertEqual(obj.dataset[0][1].strip(), "Test")
            # # Eval if obj.columns contains test_table columns
            self.assertEqual(obj.columns, ["column1", "column2"])

            obj.cnxn.close()

    def test_run_query_closed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            obj = Connection(tmpdir)
            obj.open()
            create_test_table(obj)
            obj.cnxn.close()
            obj.isopen = False

            query = '''SELECT * FROM test_table;'''
            with self.assertRaises(e.ClosedDataException):
                obj.run_query(query)

    def test_iter_dataset(self):
        # TODO: This test really needs to have it's own test case
        with tempfile.TemporaryDirectory() as tmpdir:
            obj = Connection(tmpdir)
            obj.open()
            create_test_table(obj)

            # Insert test records into test_table
            query = '''INSERT INTO test_table
                VALUES (1, 'Test');'''
            with obj.cnxn.cursor() as c:
                c.execute(query)

            query = '''INSERT INTO test_table
                VALUES (2, 'Test2');'''
            with obj.cnxn.cursor() as c:
                c.execute(query)

            # Run a SELECT method to assign attribute variables.
            query = '''SELECT * FROM test_table;'''
            obj.run_query(query)

            # Create iterator
            iter_ = obj.iter_dataset()

            # Evaluate the first row
            row = next(iter_)
            self.assertEqual(row["column1"], 1)
            self.assertEqual(row["column2"], "Test")

            # Evaluate the second row
            row = next(iter_)
            self.assertEqual(row["column1"], 2)
            self.assertEqual(row["column2"], "Test2")

            obj.cnxn.close()


# Utility functions
def create_test_table(obj):
    """Create a test table using obj.cnxn"""
    with obj.cnxn.cursor() as cursor:
        cursor.execute(
            '''CREATE TABLE test_table (
            column1 integer,
            column2 char(10)
            );'''
        )

def return_one_record(obj):
    """Returns the first result from a SELECT * FROM test_table query"""
    with obj.cnxn.cursor() as cursor:
        cursor.execute('''SELECT * FROM test_table;''')
        return cursor.fetchone()
