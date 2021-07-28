

def columns_list_difference(column_list1,column_list2):
    return list(set(column_list1.tolist())-set(column_list2.tolist()))


def shape_change_provenance_tracking(prov_tracker_obj):
    old_df=prov_tracker_obj._copy_df
    new_df=prov_tracker_obj._df
    old_shape = old_df.shape
    new_shape = new_df.shape
    print(old_shape,new_shape)
    if new_shape[0] < old_shape[0] :
        #dim reduction, instance selection
        print('Instance drop detected')
        prov_tracker_obj.provenance_obj=prov_tracker_obj.provenance_obj.get_prov_dim_reduction_hash(new_df,prov_tracker_obj.description)
        prov_tracker_obj.reset_description()
    if new_shape[1] < old_shape[1] :
        dropped_col = columns_list_difference(old_df.columns, new_df.columns)
        if len(prov_tracker_obj.col_add) > 0:
        # data reduction detected, not dim reduction but space transformation
            print(f'Space transform detected, {dropped_col} generated the new column{prov_tracker_obj.col_add}.')
            print('Warning: Remember that the last dropped columns (last operation) are the columns linked to the space transform.'
                  ' If you want to use space transform without dropping columns use stop_space_prov([col_joined]) method indicating the column joined in the transformation')
            prov_tracker_obj.provenance_obj=prov_tracker_obj.provenance_obj.get_prov_space_transformation(new_df,dropped_col,0,prov_tracker_obj.description)
            prov_tracker_obj.reset_description()
            prov_tracker_obj.col_add=[]
            return

        #dim reduction, feature selection/dropped column
        print(f'Column drop detected on {dropped_col}')
        prov_tracker_obj.provenance_obj=prov_tracker_obj.provenance_obj.get_prov_dim_reduction_hash(new_df, prov_tracker_obj.description)
        prov_tracker_obj.reset_description()


    if new_shape[0] > old_shape[0] :
        # instance generation
        if type(prov_tracker_obj.columns_used)==str:
            prov_tracker_obj.columns_used = [prov_tracker_obj.columns_used]
        if len(prov_tracker_obj.columns_used)>0:
            print(f'Instance generation detected, used columns: {prov_tracker_obj.columns_used}')

            prov_tracker_obj.provenance_obj = prov_tracker_obj.provenance_obj.get_prov_instance_generation(new_df,prov_tracker_obj.columns_used,
                                                                                                          prov_tracker_obj.description)
            prov_tracker_obj.reset_used_columns()
            prov_tracker_obj.reset_description()

        else:
            print('Instance generation detected')
            prov_tracker_obj.provenance_obj = prov_tracker_obj.provenance_obj.get_prov_instance_generation(new_df,
                                                                                                           prov_tracker_obj.columns_used,
                                                                                                           prov_tracker_obj.description)
            prov_tracker_obj.reset_description()
    if new_shape[1] > old_shape[1]:
        # column augmentation
        new_columns=columns_list_difference(new_df.columns,old_df.columns)
        if len(new_columns)>0:
            prov_tracker_obj.col_add.extend(new_columns)
            prov_tracker_obj.col_add=list(set(prov_tracker_obj.col_add))
        else:
            prov_tracker_obj.col_add.append(new_columns)
            prov_tracker_obj.col_add=list(set(prov_tracker_obj.col_add))



def value_change_provenance_tracking(prov_tracker_obj):
    old_df = prov_tracker_obj._copy_df
    new_df = prov_tracker_obj._df
    if (new_df.isna().values.sum()-old_df.isna().values.sum()) < 0:
        # number of NaN values decreased, imputation case
        # search of the column where imputation occurred
        col_names = new_df.columns
        imputed_col=[]
        for col in col_names:
            if new_df[col].isna().sum() - old_df[col].isna().sum() < 0:
                #imputated column
                imputed_col.append(col)
        print(f'Imputation detected in columns: {imputed_col}')
        prov_tracker_obj.provenance_obj=prov_tracker_obj.provenance_obj.get_prov_imputation(new_df,imputed_col,prov_tracker_obj.description)
        prov_tracker_obj.reset_description()
        return

    # value transform, feature transform
    m=new_df.shape[0] #number of rows in a df
    changed_col=[]
    feature_change=False
    for col in new_df.columns:
        if new_df[col].isna().values.sum()==m and old_df[col].isna().values.sum()==m:
            continue
        if (new_df[col].isna().values.sum()-old_df[col].isna().values.sum()) > 0:
            #increased number of NaN
            changed_col.append(col)
        elif (old_df[col].dropna()==new_df[col].dropna()).sum()==0 :
            #NaN cause problems
            # all items in a column changed, feature transofmation
            feature_change=True
            changed_col.append(col)
        elif 0<(old_df[col].dropna()==new_df[col].dropna()).sum()<m-new_df[col].isna().values.sum() :
            # only some items in a column changed, value transofmation
            #if null value are present the number of rows decreased
            changed_col.append(col)
    if feature_change and len(changed_col)>0:
        #feature transform
        print(f'Feature change detected on {changed_col} columns')
        prov_tracker_obj.provenance_obj=prov_tracker_obj.provenance_obj.get_prov_feature_transformation(new_df,changed_col, prov_tracker_obj.description)
        prov_tracker_obj.reset_description()
        return
    elif not feature_change and len(changed_col)>0:
        #value transform
        print(f'Value change detected on {changed_col} columns')
        prov_tracker_obj.provenance_obj=prov_tracker_obj.provenance_obj.get_prov_value_transformation(new_df, changed_col,prov_tracker_obj.description)
        prov_tracker_obj.reset_description()
        return



