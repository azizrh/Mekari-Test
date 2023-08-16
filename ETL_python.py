import pandas as pd
import numpy as np
import datetime

employees = pd.read_csv('employees.csv')
timesheets = pd.read_csv('timesheets.csv')

tables = pd.merge(employees.rename(columns={'employe_id':'employee_id'}),timesheets,on='employee_id',how='inner')

tables = tables.dropna(subset=['checkin','checkout'])

tables['checkin'] = tables['checkin'].apply(lambda x: datetime.datetime.strptime(x, "%H:%M:%S"))
tables['checkout'] = tables['checkout'].apply(lambda x: datetime.datetime.strptime(x, "%H:%M:%S"))

tables['total_hours'] = tables['checkout']-tables['checkin']

# handling ones that checkout after a day is over, i.e after 12.00am

tables.loc[tables['checkout']<tables['checkin'],'total_hours'] = tables.loc[tables['checkout']<tables['checkin'],'total_hours'] + pd.Timedelta(days=1)

tables['total_hours'] = tables['total_hours'].apply(lambda x: x.total_seconds() / 3600)


tables['date'] = tables['date'].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d"))

# groupby total hours based on branch_id and month

grouped_total_hours = tables[['date','employee_id', 'branch_id','total_hours',]].groupby([pd.Grouper(key='date', freq='M'),'branch_id']).agg({
                                                                                                                                            'total_hours':'sum',
                                                                                                                                            'employee_id': lambda x: set([i for i in x]),
                                                                                                                                            }).reset_index()

# find salary for each employee per month and drop duplicates year-month

salary_table = tables[['date','employee_id', 'branch_id','salary',]]
salary_table['date_str'] = salary_table['date'].dt.strftime('%Y%m')

# drop duplicates to avoid multiple counting of salary
salary_table = salary_table.drop_duplicates(subset=['date_str','employee_id', 'branch_id','salary'])

# groupby total salary based on branch_id and month

grouped_salary = salary_table.groupby([pd.Grouper(key='date', freq='M'),'branch_id']).agg({
                                                                        'salary':'sum',
                                                                        'employee_id': lambda x: set([i for i in x]),
                                                                        'date_str':lambda x: set([i for i in x]),
                                                                        }).reset_index()
grouped_salary = grouped_salary.drop(columns=['date_str'])

grouped_total_hours['employee_id'] = grouped_total_hours['employee_id'].apply(lambda x: ','.join(map(str, x)))
grouped_salary['employee_id'] = grouped_salary['employee_id'].apply(lambda x: ','.join(map(str, x)))



# merge the groupby-ed salary and total hour per branch per month, and calculate the salary_per_hour

output = pd.merge(grouped_total_hours,grouped_salary,on=['date', 'branch_id','employee_id'],how='inner')
output['salary_per_hour'] = output['salary']/output['total_hours']
output['year'] = output['date'].apply(lambda x: x.strftime('%Y'))
output['month'] = output['date'].apply(lambda x: x.strftime('%m'))
output = output.drop(columns=['date','total_hours','salary','employee_id'])
output.to_csv('tables_salary_per_month.csv',index=False)