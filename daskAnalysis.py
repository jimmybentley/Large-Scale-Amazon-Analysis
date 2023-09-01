from cProfile import run
from dask.distributed import Client, LocalCluster
import time
import json
import dask.dataframe as dd


def PA1(user_reviews_csv,products_csv):
    start = time.time()
    client = Client('127.0.0.1:8786')
    client = client.restart()
    print(client)


    reviews = dd.read_csv(user_reviews_csv)
    products = dd.read_csv(products_csv)

    q1_reviews = (reviews.isna().mean()*100).round(2).compute()
    q1_products = (products.isna().mean()*100).round(2).compute()
    #print(q1_reviews, q1_products, time.time() - start)

    merged_df = products[['price', 'asin']].merge(reviews[['overall', 'asin']], on='asin', how='inner')
    result = merged_df.drop(columns='asin').corr()['overall'].compute()
    q2 = round(result['price'], 2)
    #print(q2, time.time() - start)


    q3 = products['price'].describe().compute()[['mean','std','50%','min','max']].round(2)
    #print(q3, time.time() - start)

    def get_super_category(c):
        if c != '':
            return eval(c)[0][0]
        else:
            return ''
       
    products['categories'] = products['categories'].fillna("")
    products['super_category'] = products.categories.map(get_super_category,meta=('categories', 'object'))
    q4 = products.groupby('super_category').agg({'asin':'count'}, split_out=10).compute()['asin'].sort_values(ascending = False)
    #print(q4, time.time() - start)

    # Q5
    keys = products.asin.values
    task5_df_hashmap = dict.fromkeys(keys.compute(), 1)
    q5 = 0
    for asin in reviews.asin.values.compute():
        if asin not in task5_df_hashmap:
            q5 = 1
            break
    #print(q5, time.time() - start)

    q6 = 0
    def q6_func(series):
        for row in series.related:
            if type(row) is float:
                continue
            row = eval(row)
            for key in row:
                for asin in row[key]:
                    if asin not in task5_df_hashmap:
                        return 1
        return 0

    q6 = q6_func(products)
    #print(q6, time.time() - start)

    #q1,q2,q3,q4,q5,q6 = da.compute(q1,q2,q3,q4,q5,q6)
    end = time.time()
    runtime = end-start

    # Write your results to "results_PA1.json" here
    with open('OutputSchema_PA1.json','r') as json_file:
        data = json.load(json_file)
        print(data)

        data['q1']['products'] = json.loads(q1_products.to_json())
        data['q1']['reviews'] = json.loads(q1_reviews.to_json())
        data['q2'] = q2
        data['q3'] = json.loads(q3.to_json())
        data['q4'] = json.loads(q4.to_json())
        data['q5'] = q5
        data['q6'] = q6

    # print(data)
    with open('results_PA1.json', 'w') as outfile: json.dump(data, outfile)


    return runtime
