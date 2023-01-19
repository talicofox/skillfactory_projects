import os
import pandas as pd
from joblib import load
from lightfm.data import Dataset
path_ = r'f:\tmp'

def generate_feature_list(dataframe, features_name):
    """
    Generate features list for mapping 

    Parameters
    ----------
    dataframe: Dataframe
        Pandas Dataframe for Users or Q&A. 
    features_name : List
        List of feature columns name avaiable in dataframe. 
        
    Returns
    -------
    List of all features for mapping 
    """
    features = dataframe[features_name].apply(
        lambda x: ','.join(x.map(str)), axis=1)
    features = features.str.split(',')
    features = features.apply(pd.Series).stack().reset_index(drop=True)
    return features


def model_prediction(visitor_name):
    
    model = load(r'f:/tmp/model_rs.joblib')
    
    df_visitors = pd.read_csv(os.path.join(path_, 'visitors.csv'))
    item_tags = pd.read_csv(os.path.join(path_, 'items.csv'))
    df_events2 = pd.read_csv(os.path.join(path_, 'df_events2.csv'))

    tmp = df_events2[['visitorid','visitors_id_num']].drop_duplicates()
    dict_visitors = dict(zip(tmp['visitorid'].to_list(),  tmp['visitors_id_num'].to_list()))
    try:
        visitor = dict_visitors[int(visitor_name)]
    except:
        visitor = 0
    # generating features list for mapping 

    item_feature_list = generate_feature_list(
        item_tags,
        ['item_tags'])

    visitor_feature_list = generate_feature_list(
        df_visitors,
        ['visitor_tags'])

    dataset = Dataset()
    
    dataset.fit(
        set(df_visitors['visitors_id_num']), 
        set(item_tags['items_id_num']),
        item_features=item_feature_list, 
        user_features=visitor_feature_list)
   
    #print(type(item_tags['item_features']))
    '''
    items_features = dataset.build_item_features(item_tags['item_features'])
    
    visitor_features = dataset.build_user_features(df_visitors['visitor_features'])
    '''
    previous_id_num = df_events2.loc[df_events2['visitors_id_num'] == visitor][:3]['items_id_num']
    df_previous_items = item_tags.loc[item_tags['items_id_num'].isin(previous_id_num)]
    
    
    # predict
    discard_item_id = df_previous_items['items_id_num'].values.tolist()
    df_use_for_prediction = item_tags.loc[~item_tags['items_id_num'].isin(discard_item_id)]
    items_id_for_predict = df_use_for_prediction['items_id_num'].values.tolist()

    scores = model.predict(
        visitor,
        items_id_for_predict)
    
    df_use_for_prediction.loc[:, ('scores')] = scores
    df_use_for_prediction = df_use_for_prediction.sort_values(by='scores', ascending=False)[:3]
    
    return list(df_use_for_prediction['itemid'].values)

