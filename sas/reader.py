import pandas as pd
import termtables as tt
import shutil
import math
import numpy as np
import os
import termios, sys , tty
import click

from dataframe_sql import register_temp_table, query


def read(fname: str) -> pd.DataFrame:

    df = pd.read_sas(fname, format='sas7bdat', encoding='iso-8859-1')
    df = df.fillna('')

    df = df.astype(str)

    table = fname.replace('.sas7bdat', '')
    reg = register_temp_table(df, table)

    return df, table


def get_dimensions(data, column_index=0) -> tuple:
    """
    Determines how many rows and columns can be display in the 
        terminal window at once.

    """

    ROW_SIZE = 3 # every row requires 3 lines.
    COL_SIZE = 4 # every columns has 4 chars in formatting.
    
    
    width, height = shutil.get_terminal_size((80, 20))
    
    column_sizes = get_max_column_sizes(data, COL_SIZE)
    column_sizes = column_sizes[column_index:]

    rows = math.floor(height / ROW_SIZE)
    rows = int(rows)

    col_length = 0
    num_cols = 0

    while col_length < width and num_cols < len(column_sizes):
        if col_length + column_sizes[num_cols] <= width:
            col_length += column_sizes[num_cols]
            num_cols += 1
        else:
            break


    return rows, num_cols


def get_max_column_sizes(df, col_size=0) -> list:
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
    

def display_page(data, clear=True) -> None:
    
    if clear:
        clear_screen()

    tt.print(
        data.values.tolist(),
        header=list(data.columns))


def char_input() -> str:

    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)

    try:
        tty.setraw(fd)
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

    return ch


def clear_screen() -> None:
    os.system('cls' if os.name == 'nt' else 'clear')


def print_options(
        page_index,
        column_index,
        rows,
        columns,
        data,
        queried
    ) -> None:

    formatting = '\x1b[0;30;47m{}\x1b[0m'

    num_cols = len(list(data.columns))
    num_rows = len(data)

    reset_option = '\t' if not queried else '| r: reset'

    output = 'rows {}-{} ({}) | cols {}-{} ({})  \t ↑↓←→ to scroll \t q: quit | f: sql {}\t' \
                .format(
                    (page_index*rows)+1,
                    (page_index*rows+rows+1 if page_index*rows+rows+1 < num_rows else num_rows),
                    num_rows,
                    (column_index)+1,
                    (column_index+columns+1 if column_index+columns+1 < num_cols else num_cols),
                    num_cols,
                    reset_option)

    print(formatting.format(output))


@click.command()
@click.option('--fname', '--f', required=True, help='input file')
def main(fname):
    clear_screen()

    print('Loading')

    data, table = read(fname)
    
    PAGE_INDEX = 0
    COLUMN_INDEX = 0

    queried = False

    while True:
        
        rows, columns = get_dimensions(data, column_index=COLUMN_INDEX)
        
        df = data[(PAGE_INDEX * rows):].head(rows)

        rows, columns = get_dimensions(df, column_index=COLUMN_INDEX)

        df = df.iloc[:, COLUMN_INDEX:(COLUMN_INDEX + columns)]

        display_page(df)
        print_options(PAGE_INDEX, COLUMN_INDEX, rows, columns, data, queried)

        
        key = char_input()

        if key == 'q':
            break

        elif key == 'B':
            if (PAGE_INDEX + 1) * rows < len(data):
                PAGE_INDEX += 1

        elif key == 'A':
            if PAGE_INDEX > 0:
                PAGE_INDEX -= 1

        elif key == 'D':
            if COLUMN_INDEX > 0:
                COLUMN_INDEX -= 1

        elif key == 'C':
            if COLUMN_INDEX+columns < len(list(data.columns)):
                COLUMN_INDEX += 1
            

        elif key == 'r':
            data = query(f'select * from {table}')
            queried = False

        elif key == 'f':
            sql_query= input('sql: ')
            if sql_query is not None and sql_query.strip() != '':
                data = query(sql_query)
                queried = True

        
        
