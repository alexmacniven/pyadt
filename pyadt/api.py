import pyodbc

from . import exceptions as e

class Connection:
    """The Connection class

    Attributes:
        cnxn: A pyodbc.Connection to a data directory
        datasource: A string path the data directory
        isopen: A boolean if the connection at cnxn is open
        dataset: Data returned from previous query
        columns: Column names of data from previous query
    """

    def __init__(self, datasource):
        """Creates a new Connection instance"""
        self.datasource = datasource
        self.cnxn = None
        self.isopen = False
        self.dataset = None
        self.columns = None

    def open(self):
        """Connects to the data directory

        Create a pyodbc.Connection to the data directory and updates
        the class attributes
        """
        cnxn_str = (
            "DRIVER={{Advantage StreamlineSQL ODBC}};"
            "DataDirectory={};"
            "ServerTypes=1;"
        )
        self.cnxn = pyodbc.connect(
            cnxn_str.format(self.datasource), autocommit=True
        )
        self.isopen = True

    def close(self):
        """Closes conection to the data directory

        Invokes pyodbc.Connection.close() and updates class attributes
        """
        self.cnxn.close()
        self.isopen = False

    def run_query(self, query, *args):
        """Executes a query
        
        Args:
            query: A string SQL query
            args: Optional variables to use with the query
        """
        if self.isopen:
            with self.cnxn.cursor() as cursor:
                # Queries the data table to load the data into the
                # cursor object.
                if args:
                    cursor.execute(query, (args))
                else:
                    cursor.execute(query)
                # When query is a SELECT...
                if "SELECT" in query:
                    # Reference data using self.dataset
                    self.dataset = cursor.fetchall()
                    # Populate self.columns with column names from the
                    # cursors description attribute.
                    self.columns = [column[0] for column in cursor.description]
        else:
            raise e.ClosedDataException

    def iter_dataset(self):
        """Returns an iterator over self.dataset with nice formatting

        Example:
            Given a data table;

            | id | name    | age |
            ----------------------
            | 1  | John    | 38  |
            | 2  | Paul    | 31  |
            
            Fetching each row would result in; 
            
            (1, 'John    ', 38)

            This iterator returns each row as;

            {"id": 1, "name": "John", "age": 38}

        Note: Only use *after* a SELECT query has been executed
        """
        for row in self.dataset:
            row_dict = {}
            for index, item in enumerate(row):
                if type(item) is str:
                    item = item.strip()
                row_dict[self.columns[index]] = item
            yield row_dict
