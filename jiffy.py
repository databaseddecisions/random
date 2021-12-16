def get_path(path_dict):
    """This function takes a dictionary of paths that
        that vary based on operating system and returns
        the 'correct' path, based on the OS on which the
        script is being run.
        If the 'mac_path' is chosen, the function will
        also mount the network drive as prescribed in the
        dictionary (if applicable).
    Args:
        path_dict (dict): A set of folder paths and
            mounting information (if applicable) for
            each possible OS type.
    Returns:
        path (str/regex): The folder path that matches the
            operating system.
    """

    import platform
    import os

    try:
        platform.platform().index('Linux')
        path = path_dict['linux_path']
    except:
        try:
            platform.platform().index('Windows')
            path = path_dict['windows_path']
        except:
            path = path_dict['mac_path']['path']
            try:
                os.system(path_dict['mac_path']['mount'])
            except:
                pass
        return path

# Make sure to update your path when running this code
input_path = {
    'mac_path': {
        'path':
        r"/Users/garyguinta/Downloads/jiffy_data/",
        'mount': ''
    },
    'windows_path':
    r"C:/Users/garyguinta/Downloads/jiffy_data/",
    'linux_path': ''
} 
output_path = {
    'mac_path': {
        'path':
        r"/Users/garyguinta/Downloads/jiffy_data/Outputs/",
        'mount': ''
    },
    'windows_path':
    r"C:/Users/garyguinta/Downloads/jiffy_data/Outputs",
    'linux_path': ''
}

# Import Functions
import sqlite3
import pandas as pd
from datetime import datetime as dt
import numpy as np
import statsmodels.api as sm

# Part A
# Load CSVs
line_items = pd.read_csv(get_path(input_path)+'line_items.csv')
customers = pd.read_csv(get_path(input_path)+'customers.csv')
orders = pd.read_csv(get_path(input_path)+'orders.csv')
# Convert to timestamp to put into EST
orders['order_timestamp'] = pd.to_datetime(orders['order_timestamp'], format = '%Y-%m-%d %H:%M:%S', errors ='coerce')
orders['order_timestamp'] = pd.to_datetime(orders['order_timestamp'])
orders['order_timestamp'] = (orders['order_timestamp'].dt.tz_localize('UTC')).dt.tz_convert('US/Eastern')
# Remove timezone stamp
orders['order_timestamp'] = orders['order_timestamp'].dt.tz_localize(None)
# Connect to sqlite
conn = sqlite3.connect('test_database') 
c = conn.cursor()
# Drop data into database
line_items.to_sql("order_line_measures", conn)
orders.to_sql("orders", conn)
customers.to_sql("customers", conn)
# How many orders in 2018?
# Orders including any orders that were returned
c.execute('''
    SELECT 
    count(o.order_id) 
    FROM orders o
    WHERE 1=1
    and strftime('%Y', order_timestamp) = '2018'
    ''')
orders_2018 = c.fetchall()[0][0]
print('There were '+str(orders_2018)+ ' orders in 2018')
# Orders including any orders that were returned
c.execute('''
        SELECT
        count(x.order_id)
        from (
        SELECT
        o.order_id
        , sum(olm.quantity) quantity
        FROM orders o 
        join order_line_measures olm ON olm.order_id = o.order_id
        WHERE 1=1
        and strftime('%Y', order_timestamp) = '2018'
        GROUP BY o.order_id) x
        WHERE 1=1
        and quantity >= 10
          ''')

order_quantity = c.fetchall()[0][0]
print('There were '+str(order_quantity)+ ' orders in 2018 with 10 or more items')
# Orders including any orders that were returned
c.execute('''
        SELECT
        count(distinct x.customer_uid) as distinct_customers
        from (
        SELECT
        c.customer_uid
        , o.order_id
        FROM  customers c
        JOIN orders o ON o.customer_uid = c.customer_uid
        join order_line_measures olm ON olm.order_id = o.order_id
        WHERE 1=1
        and olm.product_category = 'Sweater'
        and olm.size = 'M'
        and o.discount <> 0) as x
          ''')

customer_numbers = c.fetchall()[0][0]
print('There are '+str(customer_numbers)+ ' unique customers that have ordered a size M sweater')
# Not including returned orders in profit
c.execute('''
 select
        Month
        , Year
        , round(Max_Profit,2)
        from (SELECT
          Month
          , Year
          , sum(profit) as Company_Profit
          from (
          SELECT
              o.order_id            
              , strftime('%m', order_timestamp) as Month
              , strftime('%Y', order_timestamp) as Year
              , (sum(line_item_sales)*(1-discount))-sum(line_item_cost)+shipping_revenue-shipping_cost as profit
              FROM orders o 
              join (select order_id 
              , line_item_id
              , selling_price*quantity as line_item_sales
              , supplier_cost*quantity as line_item_cost
              from order_line_measures) olm ON olm.order_id = o.order_id
              WHERE 1=1
              and returned = FALSE
              GROUP BY o.order_id
              , strftime('%m', order_timestamp)
              , strftime('%Y', order_timestamp)
            ) as y
          GROUP BY Month, Year) as a 
        JOIN (
          SELECT
          max(Company_Profit) as max_profit
          from (
          SELECT
          Month
          , Year
          , sum(profit) as Company_Profit
          from (
          SELECT
              o.order_id            
              , strftime('%m', order_timestamp) as Month
              , strftime('%Y', order_timestamp) as Year
              , (sum(line_item_sales)*(1-discount))-sum(line_item_cost)+shipping_revenue-shipping_cost as profit
              FROM orders o 
              join (select order_id 
              , line_item_id
              , selling_price*quantity as line_item_sales
              , supplier_cost*quantity as line_item_cost
              from order_line_measures) olm ON olm.order_id = o.order_id
              WHERE 1=1
              and returned = FALSE
              GROUP BY o.order_id
              , strftime('%m', order_timestamp)
              , strftime('%Y', order_timestamp)
            ) as y
          GROUP BY Month, Year) as z) as b ON b.max_profit = a.Company_Profit
          ''')
all = c.fetchall()[0]
month = all[0]
year = all[1]
profit = all[2]
print('Most profitable month/year was in '+str(month)+'/'+str(year)+' of $'+str(profit))
# Customers who returned orders are not considered for retention
c.execute('''
    SELECT
    case when is_business = 0 then 'Non-Business' else 'Business' end as is_business
    , count(customer_uid) as total_customers
    , sum(returned) returned_customers
    , round(round(sum(returned),2)/round(count(customer_uid),2),3)*100
    from (
    SELECT
    c.customer_uid
    , c.is_business
    , case when count(o.order_id) > 1 then 1 else 0 end as returned
    from customers c
    join orders o ON o.customer_uid = c.customer_uid
    WHERE 1=1
    and returned = FALSE
    group by c.customer_uid
    , c.is_business) as x
    group by is_business
''')
all = c.fetchall()
print('The retention rate for '+str(all[0][0])+ ' customers is ' + str(all[0][3]) +'%')
print('The retention rate for '+str(all[1][0])+ ' customers is ' + str(all[1][3]) +'%')
c.close()

# Part B
# Group by date, assuming here that we aren't changing prices intraday
orders['order_timestamp'] = pd.to_datetime(orders['order_timestamp']).dt.date
# Include returned items - customer could have bought them for the price but returned due to something else, would need return reason data
# Generate naming convention
line_items['product_SKU_name'] = line_items['product_category']+'_'+line_items['color']
order_line_measures = orders[['order_id','order_timestamp','discount']].merge(line_items[['order_id','product_SKU_name','selling_price','quantity']])
order_line_measures = (order_line_measures
    .assign(actual_price = order_line_measures['selling_price']*(1-order_line_measures['discount']))
    )
final_table = order_line_measures.groupby(['product_SKU_name','actual_price'])['quantity'].sum().reset_index()
products = final_table['product_SKU_name'].unique()
price_elasticity_df_list = []
# Use ye olde Ordinary Least Squares to model elasticity 
for product in products:
   x_value = final_table[final_table['product_SKU_name']==product][['actual_price']]
   y_value = final_table[final_table['product_SKU_name']==product][['quantity']]
   X = sm.add_constant(x_value)
   est = sm.OLS(y_value, X).fit()
   int, coef = est.params
   pvalue = round(est.f_pvalue,4)
   rsq = round(est.rsquared,4)
   mean_price = np.mean(x_value)[0]
   mean_quantity = np.mean(y_value)[0]
   #PED = Slope * (mean price/mean demand)
   elasticity = round(coef*(mean_price/mean_quantity),4)
   price_elasticity_df_list += [pd.DataFrame([[product, elasticity,pvalue,rsq]], columns = ['Product_Name','Price_Elasticity','P_Value','R_Squared'])]

finale_results = pd.concat(price_elasticity_df_list).reset_index(drop=True)
finale_results = (finale_results
    #Null hypothesis: Price Has NO Effect on Demand
    .assign(Reject_Null = np.where(finale_results['P_Value']<0.05,'Reject','Accept'))
    .assign(Elasticity_Description = np.where(finale_results['Price_Elasticity']>-1,'Inelastic','Elastic'))
    )
# RSquareds are quite low, so I would take these results with a grain of salt
finale_results.to_csv(get_path(output_path)+'model_outputs.csv',index=False)
