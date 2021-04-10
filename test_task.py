import pandas as pd
from datetime import datetime
import gspread
from google.auth.transport.requests import AuthorizedSession
from oauth2client.service_account import ServiceAccountCredentials


def append_df_to_gs( df, spread_sheet: str, sheet_name: str ):
    scopes = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive',
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        r"C:\Users\dames\Downloads\pysheets-310318-70d793fa6c89.json", scope)
    gsc = gspread.authorize(credentials)
    sheet = gsc.open(spread_sheet)
    params = {'valueInputOption': 'USER_ENTERED'}
    body = {'values': df.values.tolist()}
    sheet.values_append(f'{sheet_name:str}!A1:G1', params, body)


urls = [
    'https://docs.google.com/spreadsheets/d/1Ycg7zTxds9DZnDvTrFcyNNKuTUxg6Yy6WF0a8Wc02WQ/edit#gid=1164376473',
    'https://docs.google.com/spreadsheets/d/1Ycg7zTxds9DZnDvTrFcyNNKuTUxg6Yy6WF0a8Wc02WQ/edit#gid=1840297911',
    'https://docs.google.com/spreadsheets/d/1Ycg7zTxds9DZnDvTrFcyNNKuTUxg6Yy6WF0a8Wc02WQ/edit#gid=897421023',
    'https://docs.google.com/spreadsheets/d/1Ycg7zTxds9DZnDvTrFcyNNKuTUxg6Yy6WF0a8Wc02WQ/edit#gid=1167027454',
]

transactions = pd.read_csv(
    urls[0].replace('/edit#gid=', '/export?format=csv&gid='),
    dtype=str
)
print(transactions.shape)
clients = pd.read_csv(
    urls[1].replace('/edit#gid=', '/export?format=csv&gid='),
    dtype=str
)
print(clients.shape)
managers = pd.read_csv(
    urls[2].replace('/edit#gid=', '/export?format=csv&gid='),
    dtype=str
)
print(managers.shape)
leads = pd.read_csv(
    urls[3].replace('/edit#gid=', '/export?format=csv&gid='),
    dtype=str
)
print(leads.shape)
transactions = pd.DataFrame(transactions.groupby(['transaction_id',
                                                  'created_at',
                                                  'l_client_id'])['m_real_amount'].sum()).reset_index()
merged_clients = leads.merge(clients,
                            how='left',
                            left_on=['l_manager_id','l_client_id'],
                            right_on=['l_manager_id', 'client_id']
                            )
print(merged_clients.shape)
merged_transactions = merged_clients.merge(transactions,
                                                    how='left',
                                                    left_on=['client_id'],
                                                    right_on=['l_client_id'],
                                                    indicator=True,
                                                    ).query("_merge == 'both'")
print(merged_transactions.shape)
merged_resuls = merged_clients.merge(merged_transactions[['lead_id',
                                                        'transaction_id',
                                                        'created_at',
                                                        'm_real_amount']],
                                     how='left',
                                     on='lead_id').drop_duplicates(subset='lead_id', keep='last')
print(merged_resuls.shape)
merged_resuls = merged_resuls.merge(managers,
                                   how='left',
                                   left_on='l_manager_id',
                                   right_on='manager_id').drop(['manager_id'], axis=1)
print(merged_resuls.shape)
merged_resuls['trash_lead'] = merged_resuls['l_client_id'] == '00000000-0000-0000-0000-000000000000'
merged_resuls['new_lead'] = ((~merged_resuls['l_client_id'].isin(transactions['l_client_id'])) &
                                    (transactions['m_real_amount'] == '0'))
merged_resuls['count_of_clients_week'] = ((merged_resuls['transaction_id'].notnull()) &
                                         (((pd.to_datetime(merged_resuls['created_at_x'])).dt.day - (pd.to_datetime(merged_resuls['created_at'])).dt.day) >= -7))
merged_resuls['count_of_clients_new'] = ((merged_resuls['transaction_id'].notnull()) &
                                         (((pd.to_datetime(merged_resuls['created_at_x'])).dt.day - (pd.to_datetime(merged_resuls['created_at'])).dt.day) >= 7) &
                                         (~merged_resuls['new_lead']))
merged_resuls.m_real_amount = merged_resuls[merged_resuls.count_of_clients_new].m_real_amount
pivot = pd.pivot_table(merged_resuls,
                       index=[
                             'd_utm_source',
                             'd_club',
                             'd_manager',
                             ],
                       aggfunc={
                                'lead_id': 'count',
                                'trash_lead': sum,
                                'new_lead': sum,
                                'count_of_clients_week': sum,
                                'count_of_clients_new': sum,
                                'm_real_amount': lambda x: x.astype(float).sum(),
                               }
                       )
pivot.to_excel('pivot.xlsx')
append_df_to_gs(pivot, 'test_task', 'Sheet1')