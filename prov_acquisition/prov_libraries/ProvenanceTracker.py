from prov_acquisition.prov_libraries import logic_tracker
import pandas as pd
import numpy as np
import time
class TooManyArg(Exception):
    pass
global_tracker=0    # instance of a global variable used for tracking with __setitem__


def nan_equal(col_a, col_b):
    try:
        np.testing.assert_equal(col_a, col_b)
    except AssertionError:
        return False
    return True

class ProvenanceTracker:
    """
    Class that tracks pandas operations
    """
    def __init__(self, initial_df, provenance_obj):
        """
        initial_df: starting pandas dataframe
        provenance_obj: provenance object that contains provenance methods
        """
        #
        self._df = New_df(initial_df)
        self._copy_df=New_df(initial_df.copy())
        self.provenance_obj = provenance_obj
        self.second_df = []
        global global_tracker
        global_tracker = self

        # list that contains temp columns added to dataframe
        self.col_add = []

        # boolean change detectors
        self.shape_change = False
        self.value_change = False

        # optional operators
        self.join_operation = {'axis': None, 'on': None}
        self.description = None
        self.columns_used = []

    def dataframe_is_changed(self):
        # search for any difference between the new and the old dataframe
        global global_tracker
        if self._df.shape==self._copy_df.shape:
            if not self._df.equals(self._copy_df):
                if len(logic_tracker.columns_list_difference(self._df.columns, self._copy_df.columns)) > 0:
                    # column name change
                    new_col = logic_tracker.columns_list_difference(self._df.columns, self._copy_df.columns)
                    old_col = logic_tracker.columns_list_difference(self._copy_df.columns, self._df.columns)
                    if nan_equal(self._df[new_col].values,self._copy_df[old_col].values):
                        print(f'Column {old_col} renamed in {new_col}')
                else:
                    #print('difference founded')
                    self.value_change = True
                global_tracker = self
            else:
                print('same df')
                pass
        else:
            #print('shape changed detected')

            self.shape_change = True
            global_tracker = self

    def track_provenance(self):
        global global_tracker
        if self.shape_change:
            logic_tracker.shape_change_provenance_tracking(self)
            self._copy_df=self._df.copy()
            self.shape_change = False
            global_tracker = self
        elif self.value_change:
            logic_tracker.value_change_provenance_tracking(self)
            self._copy_df=self._df.copy()
            self.value_change = False
            global_tracker = self

    @property
    def df(self):
        return self._df

    @df.setter
    def df(self, new_value):
        # override of dataframe setter to track the difference
        #print('set method launched')
        global  global_tracker
        self._df = New_df(new_value)
        if self.join_operation['axis'] is not None:
            # Union operation
            print('Union detected')
            # launch provenace
            self.provenance_obj.get_prov_union(self._df, self.join_operation['axis'],self.description)
            self.reset_description()
            # reset join op
            self.join_operation['axis'] = None
            global_tracker = self
            self._copy_df = self._df.copy()
            self.second_df = []
            return
        if self.join_operation['on'] is not None:
            # Join operation
            print('Join detected')
            #launch provenance
            print('Warning, use the principal df as left df and second df as right df. Only standard suffix will work. Duplicate rows will not work')
            self.provenance_obj.get_prov_join(self._df,self.join_operation['on'],self.description)
            self.reset_description()
            # reset join op
            self.join_operation['on'] = None
            global_tracker = self
            self._copy_df = self._df.copy()
            self.second_df = []
            return
        t = time.time()
        # if not a union or a join search for any difference
        self.dataframe_is_changed()
        print(f'dataframe is changed {time.time()-t}')
        if self.shape_change:
            # shape is changed
            logic_tracker.shape_change_provenance_tracking(self)
            self.shape_change = False
            global_tracker = self
        elif self.value_change:
            # a value is changed in the df
            logic_tracker.value_change_provenance_tracking(self)
            self.value_change = False
            global_tracker = self
        self._copy_df = self._df.copy()

    def stop_space_prov(self,col_joined, shift_period=0):
        # Optional method for Space transformations, use it to capture the provenance of a space transformation without delete any column
        print(f'Space transform, {col_joined} generated the new columns {self.col_add}')
        self.col_add = []
        if type(col_joined)==str:
            col_joined = [col_joined]
        self.provenance_obj.get_prov_space_transformation(self._df, col_joined,shift_period, self.description)
        self.reset_description()
        return

    def add_second_df(self,second_dataframe):
        # add second df for join and union operations
        self.second_df = second_dataframe
        self.provenance_obj.add_second_df(second_dataframe)

    def set_join_op(self, axis=None, on=None):
        # set the parameters for next join or union operation
        try:
            if axis != None and on != None:
                raise TooManyArg
            else:
                if axis is not None:
                    self.join_operation['axis'] = axis
                elif on is not None:
                    self.join_operation['on'] = on
        except:
            raise TooManyArg('Too many argument different from None used, use at least 1')

    def set_description(self, description):
        # set the description of the activity
        # the description is the string in the red ellipse in the provenance visualization
        self.description = description

    def reset_description(self):
        self.description = None

    def set_used_columns(self,used_columns):
        # set used columns for instance generations
        self.columns_used=used_columns

    def reset_used_columns(self):
        self.columns_used=[]

    def checkpoint(self,columns_to_check):
        print(f'Checkpoint on {columns_to_check}')
        self.provenance_obj.checkpoint(self._df,columns_to_check, self.description)
        self.reset_description()
class New_df(pd.DataFrame):
    # Override of __setitem__ of pandas, when item is set the changes are recorded
    def __setitem__(self, key, value):
        #print('Override lauched')
        pd.core.frame.DataFrame.__setitem__(self,key, value)
        #obj=inspect.stack()[1][0].f_globals['holder']
        global_tracker.dataframe_is_changed()
        global_tracker.track_provenance()

