import pandas as pd
import termtables as tt
import shutil
import math
import numpy as np
import os
import termios, sys , tty
import click

from dataframe_sql import register_temp_table, query


class FileReader:


    def read(self, fname: str) -> pd.DataFrame:

        if not os.path.isfile(fname):
            print('File not found.')
            return None, None

        df = pd.read_sas(fname, format='sas7bdat', encoding='iso-8859-1')
        df = df.fillna('')

        df = df.astype(str)

        table = fname.replace('.sas7bdat', '')
        reg = register_temp_table(df, table)

        return df, table


class Display:


    def __init__(self):
        self.COLUMN_INDEX = 0
        self.PAGE_INDEX = 0

        self.clear_screen()


    def get_dimensions_y(self) -> tuple:
        """
        Determines how many rows and columns can be display in the 
            terminal window at once.

        """

        ROW_SIZE = 2.22 # every row requires 3 lines.
        
        _, height = shutil.get_terminal_size((80, 20))
        
        rows = math.floor(height / ROW_SIZE)
        rows = int(rows)

        return rows

    def get_dimensions_x(self, data: pd.DataFrame, column_index=0) -> tuple:
        """
        Determines how many rows and columns can be display in the 
            terminal window at once.

        """

        COL_SIZE = 4 # every columns has 4 chars in formatting.
        
        width, _ = shutil.get_terminal_size((80, 20))
        
        column_sizes = self.get_max_column_sizes(data, COL_SIZE)
        column_sizes = column_sizes[column_index:]

        col_length = 0
        num_cols = 0

        while col_length < width and num_cols < len(column_sizes):
            if col_length + column_sizes[num_cols] <= width:
                col_length += column_sizes[num_cols]
                num_cols += 1
            else:
                break


        return num_cols

    def get_max_column_sizes(self, df, col_size=0) -> list:
        """
        Calculates the maximum length of each column in a dataframe.
        """

        measurer = np.vectorize(len)
        lengths = measurer(df.values.astype(str)).max(axis=0)

        for i, col in enumerate(list(df.columns)):
            if len(col) > lengths[i]:
                lengths[i] = len(col)

        lengths += col_size

        return lengths
    

    def display_page(self, data, clear=True) -> None:
        
        if clear:
            self.clear_screen()

        tt.print(
            data.values.tolist(),
            style=tt.styles.thin,
            header=list(data.columns))


    def char_input(self) -> str:

        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)

        try:
            tty.setraw(fd)
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

        return ch


    def clear_screen(self) -> None:
        os.system('cls' if os.name == 'nt' else 'clear')


    def print_options(
            self,
            rows,
            columns,
            data
        ) -> None:

        formatting = '\x1b[0;30;47m{}\x1b[0m'

        num_cols = len(list(data.columns))
        num_rows = len(data)

        reset_option = '\t' if not self.queried else '| r: reset'

        output = 'rows {}-{} ({}) | cols {}-{} ({})  \t ↑↓←→ to scroll \t q: quit | f: sql {}\t' \
                    .format(
                        (self.PAGE_INDEX*rows)+1,
                        (self.PAGE_INDEX*rows+rows if self.PAGE_INDEX*rows+rows < num_rows else num_rows),
                        num_rows,
                        (self.COLUMN_INDEX)+1,
                        (self.COLUMN_INDEX+columns if self.COLUMN_INDEX+columns < num_cols else num_cols),
                        num_cols,
                        reset_option)

        print(formatting.format(output))

    
    def handle_input(self):

        key = self.char_input()

        if key == 'q':
            return False

        elif key == 'B':
            if (self.PAGE_INDEX + 1) * self.rows < len(self.data):
                self.PAGE_INDEX += 1

        elif key == 'A':
            if self.PAGE_INDEX > 0:
                self.PAGE_INDEX -= 1

        elif key == 'D':
            if self.COLUMN_INDEX > 0:
                self.COLUMN_INDEX -= 1

        elif key == 'C':
            if self.COLUMN_INDEX+self.columns < len(list(self.data.columns)):
                self.COLUMN_INDEX += 1
            

        elif key == 'r':
            self.data = query(f'select * from {self.table_name}')
            self.queried = False

            self.COLUMN_INDEX = 0
            self.PAGE_INDEX = 0

        elif key == 'f':
            sql_query= input('sql: ')
            if sql_query is not None and sql_query.strip() != '':
                self.data = query(sql_query)
                self.queried = True

                self.COLUMN_INDEX = 0
                self.PAGE_INDEX = 0

        return True


    def render(self, table_name: str, data: pd.DataFrame):

        self.clear_screen()

        self.data = data
        self.table_name = table_name

        self.queried = False

        while True:
        
            self.rows = self.get_dimensions_y()
            
            df = self.data[(self.PAGE_INDEX * self.rows):].head(self.rows)

            self.columns = self.get_dimensions_x(df, column_index=self.COLUMN_INDEX)

            df = df.iloc[:, self.COLUMN_INDEX:(self.COLUMN_INDEX + self.columns)]

            self.display_page(df)
            self.print_options(self.rows, self.columns, self.data)

            hi = self.handle_input()
            if not hi:
                break


@click.command()
@click.option('--fname', '--f', required=True, help='input file')
def main(fname):
    
    print('Loading')
    reader = FileReader()
    data, table = reader.read(fname)

    if data is not None:
        display = Display()
        display.render(table, data)

        
