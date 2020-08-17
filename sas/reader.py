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

    reg = register_temp_table(df, fname.replace('.sas7bdat', ''))

    return df


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

    while col_length < width:
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
        PAGE_INDEX,
        COLUMN_INDEX,
        rows,
        columns,
        data
    ) -> None:

    formatting = '\x1b[0;30;47m{}\x1b[0m'

    output = 'rows {}-{} ({}) | cols {}-{} ({})  \t ↑↓←→ to scroll \t q: quit | f: sql \t' \
                .format(
                    (PAGE_INDEX*rows)+1,
                    (PAGE_INDEX*rows+rows+1 if PAGE_INDEX*rows+rows+1 < len(data) else len(data)),
                    len(data),
                    (COLUMN_INDEX*columns)+1,
                    (COLUMN_INDEX*columns+columns+1 if COLUMN_INDEX*columns+columns+1 < len(list(data.columns)) else len(list(data.columns))),
                    len(list(data.columns)))

    print(formatting.format(output))




@click.command()
@click.option('--fname', '--f', required=True, help='input file')
def main(fname):
    clear_screen()

    print('Loading')

    data = read(fname)
    
    PAGE_INDEX = 0
    COLUMN_INDEX = 0

    while True:
        
        
        rows, columns = get_dimensions(data, column_index=COLUMN_INDEX)
        
        df = data[(PAGE_INDEX * rows):].head(rows)

        rows, columns = get_dimensions(df, column_index=COLUMN_INDEX)

        df = df.iloc[:, COLUMN_INDEX:(COLUMN_INDEX + columns)]

        display_page(df)
        print_options(PAGE_INDEX, COLUMN_INDEX, rows, columns, data)

        
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
            COLUMN_INDEX += 1

        elif key == 'f':
            sql_query= input('sql:')
            if sql_query is not None and sql_query.strip() != '':
                data = query(sql_query)

        
        
