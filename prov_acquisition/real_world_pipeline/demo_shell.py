

# Necessary packages
import sys

sys.path.append('../')
from prov_acquisition.prov_libraries import provenance_new as pr
import pandas as pd
import time
import argparse
import os
from prov_acquisition.prov_libraries import ProvenanceTracker
import numpy as np
from run import run
def main(dbname):
    output_path = 'prov_results'

    # Specify where to save the processed files as savepath
    savepath = os.path.join(output_path, 'demo')

    df = pd.DataFrame({'key1': ['K0', 'K0', 'K1', 'K2', 'K0'],
                         'key2': ['K0', np.nan, 'K0', 'K1', 'K0'],
                         'A': ['A0', 'A1', 'A2', 'A3', 'A4'],
                         'B': ['B0', 'B1', 'B2', 'B3', 'B4']
                         })
    right = pd.DataFrame({'key1': ['K0', np.nan, 'K1', 'K2', ],
                          'key2': ['K0', 'K4', 'K0', 'K0'],
                          'A': ['C0', 'C1', 'C2', 'C3'],
                          'D': ['D0', np.nan, 'D2', 'D3'],
                          'C': ['B0', 'B1', 'B2', 'B3']})
    df2 = pd.DataFrame({'key1': ['K0', 'imputato', 'K1', 'K1', 'K0'],
                        'key2': ['K0', 'K4', 'K2', 'K1', 'K0'],
                        'E': ['E1', 'E1', 'E2', 'E3', 'E4'],
                        'F': ['F0', 'F1', 'F2', 'F3', 'F4']
                        })
    print('[' + time.strftime("%d/%m-%H:%M:%S") + '] Initialization')
    # Create a new provenance document

    p = pr.Provenance(df, savepath)
    # create provanance tracker
    tracker=ProvenanceTracker.ProvenanceTracker(df, p)
    # add second df to tracker for join or union operation, provenance of second df is not tracked
    tracker.add_second_df(right)
    # add used column for next instance generation
    tracker.set_used_columns(['key2'])
    # instance generation
    tracker.df = tracker.df.append({'key2': 'K4'}, ignore_index=True)
    # set description for next operation (string in the activity in provenance visualization)
    tracker.set_description('Join1')
    # set join on columns
    tracker.set_join_op(axis=None, on=['key1','key2'])
    # join
    tracker.df = pd.merge(tracker.df, tracker.second_df, on=['key1','key2'], how='left')
    # imputation
    tracker.df = tracker.df.fillna('imputato')
    # feature transformation of column D
    tracker.df['D'] = tracker.df['D']*2
    # add second df to tracker for join or union operation
    tracker.add_second_df(df2)
    # set join on columns
    tracker.set_join_op(axis=None, on=['key1', 'key2'])
    # join 2
    tracker.df = pd.merge(tracker.df, tracker.second_df, on=['key1', 'key2'], how='left')
    # Feature transformation of column key2
    tracker.df['key2'] = tracker.df['key2']*2
    # Imputation 2
    tracker.df = tracker.df.fillna('imputato')
    # One hot encoding
    c='D'
    dummies = []
    dummies.append(pd.get_dummies(tracker.df[c]))
    df_dummies = pd.concat(dummies, axis=1)
    tracker.df = pd.concat((tracker.df, df_dummies), axis=1)
    # sto space transformation explicitiing the derivation column(automatic if D was dropped)
    tracker.stop_space_prov('D')
    print(tracker.df)
    run(dbname,savepath)
if __name__ == '__main__':
    if len(sys.argv) == 2 :
        main(sys.argv[1])
    else:
        print('[ERROR] usage: demo_shell.py <db_name> ')

