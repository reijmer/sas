import pandas as pd
import termtables as tt
import shutil
import math
import numpy as np
import os
import termios, sys , tty



def read(fname: str) -> pd.DataFrame:

    df = pd.read_sas(fname, format='sas7bdat', encoding='iso-8859-1')
    df = df.fillna('')

    return df


def get_dimensions(data, column_index=0) -> tuple:
    """
    Determines how many rows and columns can be display in the 
        terminal window at once.

    """

    ROW_SIZE = 3 # every row requires 3 lines.
    COL_SIZE = 10 # every columns has 4 chars in formatting.
    
    
    width, height = shutil.get_terminal_size((80, 20))
    
    column_sizes = get_max_column_sizes(data)
    column_sizes = column_sizes + COL_SIZE

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


def get_max_column_sizes(df) -> list:
    """
    Calculates the maximum length of each column in a dataframe.
    """

    measurer = np.vectorize(len)
    lengths = measurer(df.values.astype(str)).max(axis=0)

    for i, col in enumerate(list(df.columns)):
        if len(col) > lengths[i]:
            lengths[i] = len(col)

    return lengths


def page(data, direction=None, amount=None) -> pd.DataFrame:

    if direction is None and amount is None:
        return data

    return None
    

def display_page(data) -> None:

    #print(data)

    tt.print(
        data.values.tolist(),
        header=list(data.columns))


def char_input():
   fd = sys.stdin.fileno()
   old_settings = termios.tcgetattr(fd)
   try:
      tty.setraw(fd)
      ch = sys.stdin.read(1)     #This number represents the length
   finally:
      termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
   return ch


def cls():
    os.system('cls' if os.name == 'nt' else 'clear')


def main():
    cls()

    fname = 'ae.sas7bdat'

    data = read(fname)
    

    PAGE_INDEX = 0
    COLUMN_INDEX = 0

    while True:
        cls()
        #breakpoint()
        rows, columns = get_dimensions(data, column_index=COLUMN_INDEX)

        #breakpoint()
        df = data.iloc[:, COLUMN_INDEX:(COLUMN_INDEX + columns)]
        
        display_page(df[(PAGE_INDEX * rows):].head(rows))

        getch = char_input()

        if getch == 'q':
            break

        if getch == 'B':
            PAGE_INDEX += 1

        if getch == 'A':
            if PAGE_INDEX > 0:
                PAGE_INDEX -= 1

        if getch == 'D':
            if COLUMN_INDEX > 0:
                COLUMN_INDEX -= 1

        if getch == 'C':
            COLUMN_INDEX += 1


if __name__ == '__main__':

    main()
