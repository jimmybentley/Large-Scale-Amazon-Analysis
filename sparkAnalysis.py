import os
import pyspark.sql.functions as F
import pyspark.sql.types as T
from utilities import SEED
import pyspark.ml as M
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator


def task_1(data_io, review_data, product_data):
    # Inputs:
    asin_column = 'asin'
    overall_column = 'overall'
    # Outputs:
    mean_rating_column = 'meanRating'
    count_rating_column = 'countRating'
    
    rating_stats = review_data[[asin_column, overall_column]].groupBy(asin_column).agg(
        F.avg(overall_column).alias('meanRating'), 
        F.count(overall_column).alias('countRating')
    ).join(product_data, asin_column, how='right')

    mean_meanRating, variance_meanRating, mean_countRating, variance_countRating = (
        rating_stats.agg(F.avg('meanRating'), 
                         F.variance('meanRating'), 
                         F.avg('countRating'), 
                         F.variance('countRating')).first()
    )

    numNulls_meanRating = rating_stats.where(F.col('meanRating').isNull()).count()
    numNulls_countRating = numNulls_meanRating

    count_total = rating_stats.count()


    res = {
        'count_total': None,
        'mean_meanRating': None,
        'variance_meanRating': None,
        'numNulls_meanRating': None,
        'mean_countRating': None,
        'variance_countRating': None,
        'numNulls_countRating': None
    }
    # Modify res:
    res['count_total'] = count_total
    
    res['mean_meanRating'] = mean_meanRating
    res['variance_meanRating'] = variance_meanRating
    res['numNulls_meanRating'] = numNulls_meanRating
    
    res['mean_countRating'] = mean_countRating
    res['variance_countRating'] = variance_countRating
    res['numNulls_countRating'] = numNulls_countRating  

    data_io.save(res, 'task_1')
    return res


def task_2(data_io, product_data):
    # Inputs:
    salesRank_column = 'salesRank'
    categories_column = 'categories'
    asin_column = 'asin'
    # Outputs:
    category_column = 'category'
    bestSalesCategory_column = 'bestSalesCategory'
    bestSalesRank_column = 'bestSalesRank'
    
    best_sales = product_data.select(F.explode_outer(F.col('salesRank')).alias("bestSalesCategory", "bestSalesRank"))
    numNulls_bestSalesCategory = best_sales.where(F.col('bestSalesCategory').isNull()).count()
    
    mean_bestSalesRank, variance_bestSalesRank, countDistinct_bestSalesCategory = (
        best_sales.agg(F.avg('bestSalesRank'), 
                       F.variance('bestSalesRank'),
                       F.countDistinct('bestSalesCategory')).first())

    cat_col = product_data.select(F.transform(F.col('categories'), lambda a: a[0]==1 if a[0] is None else a[0]).getItem(0).alias("category"))
    count_total = cat_col.count()
    numNulls_category = cat_col.where(F.col('category').isNull() | F.when(F.col("category") == "", True).otherwise(False)).count()
    countDistinct_category = cat_col.agg(F.countDistinct('category').alias("num_distinct_categories")).collect()[0]["num_distinct_categories"]-1 # subtract null


    res = {
        'count_total': None,
        'mean_bestSalesRank': None,
        'variance_bestSalesRank': None,
        'numNulls_category': None,
        'countDistinct_category': None,
        'numNulls_bestSalesCategory': None,
        'countDistinct_bestSalesCategory': None
    }

    res['count_total'] = count_total
    res['mean_bestSalesRank'] = mean_bestSalesRank
    res['variance_bestSalesRank'] = variance_bestSalesRank
    res['numNulls_category'] = numNulls_category
    res['countDistinct_category'] = countDistinct_category
    res['numNulls_bestSalesCategory'] = numNulls_bestSalesCategory
    res['countDistinct_bestSalesCategory'] = countDistinct_bestSalesCategory

    data_io.save(res, 'task_2')
    return res


def task_3(data_io, product_data):
    # Inputs:
    asin_column = 'asin'
    price_column = 'price'
    attribute = 'also_viewed'
    related_column = 'related'
    # Outputs:
    meanPriceAlsoViewed_column = 'meanPriceAlsoViewed'
    countAlsoViewed_column = 'countAlsoViewed'
    
    df = product_data.select(F.col('asin'), F.explode(F.col('related'))).where(F.col('key')=="also_viewed").drop('key')
    df_left_1 = df.select(F.col('asin').alias("left_asin"), F.explode(F.col('value')).alias("value"))
    df_left = df_left_1.alias("df_left").join(product_data.select(F.col(asin_column)).alias("df_right"), F.col("df_left.left_asin") == F.col('df_right.asin'), "right")
    df_left = df_left[['asin','value']]
    joined = df_left.alias("df_left").join(product_data.select(F.col(asin_column), F.col(price_column)).alias("df_right"), F.col("df_left.value") == F.col('df_right.asin'), "left")
    meanPriceAlsoViewed = joined.groupby('df_left.asin').agg(F.avg(price_column)).select(F.col(asin_column), F.col('avg(price)').alias('meanPriceAlsoViewed'))
    countAlsoViewed = df_left_1.groupby('left_asin').agg(F.count('value')) 

    
    count_total = meanPriceAlsoViewed.count()
    
    mean_meanPriceAlsoViewed, variance_meanPriceAlsoViewed = (
       meanPriceAlsoViewed.agg(F.avg(meanPriceAlsoViewed_column), 
                      F.variance(meanPriceAlsoViewed_column)).first())
    
    mean_countAlsoViewed, variance_countAlsoViewed = (
        countAlsoViewed.agg(F.avg('count(value)'), 
                       F.variance('count(value)')).first())

    numNulls_meanPriceAlsoViewed = meanPriceAlsoViewed.where(F.col(meanPriceAlsoViewed_column).isNull()).count()
    numNulls_countAlsoViewed = df_left.where(F.col('df_left.value').isNull()).count()


    res = {
        'count_total': None,
        'mean_meanPriceAlsoViewed': None,
        'variance_meanPriceAlsoViewed': None,
        'numNulls_meanPriceAlsoViewed': None,
        'mean_countAlsoViewed': None,
        'variance_countAlsoViewed': None,
        'numNulls_countAlsoViewed': None
    }
    # Modify res:
    res['count_total'] = count_total
    res['mean_meanPriceAlsoViewed'] = mean_meanPriceAlsoViewed
    res['variance_meanPriceAlsoViewed'] = variance_meanPriceAlsoViewed
    res['numNulls_meanPriceAlsoViewed'] = numNulls_meanPriceAlsoViewed
    res['mean_countAlsoViewed'] = mean_countAlsoViewed
    res['variance_countAlsoViewed'] = variance_countAlsoViewed
    res['numNulls_countAlsoViewed'] = numNulls_countAlsoViewed

    data_io.save(res, 'task_3')
    return res


def task_4(data_io, product_data):
    # Inputs:
    price_column = 'price'
    title_column = 'title'
    # Outputs:
    meanImputedPrice_column = 'meanImputedPrice'
    medianImputedPrice_column = 'medianImputedPrice'
    unknownImputedTitle_column = 'unknownImputedTitle'

    imputer = M.feature.Imputer()
    imputer.setInputCols(["price"])
    imputer.setOutputCols(["meanImputedPrice"])
    model = imputer.fit(product_data)
    imputed_data = model.transform(product_data)

    imputer.setInputCols(["price"])
    imputer.setOutputCols(["medianImputedPrice"])
    model = imputer.setStrategy("median").fit(imputed_data)
    imputed_data = model.transform(imputed_data)
    imputed_data = imputed_data[["meanImputedPrice","medianImputedPrice","title"]]
    imputed_data = imputed_data.na.fill({'title':'unknown'})
    #imputed_data.show(10)

    #summary stats
    count_total = imputed_data.count()

    mean_meanImputedPrice, variance_meanImputedPrice, mean_medianImputedPrice, variance_medianImputedPrice = (
     imputed_data.agg(F.avg('meanImputedPrice'), 
                      F.variance('meanImputedPrice'),
                      F.avg('medianImputedPrice'), 
                      F.variance('medianImputedPrice')).first())
    numUnknowns_unknownImputedTitle = imputed_data.filter(imputed_data['title'] == 'unknown').count()
    numNulls_meanImputedPrice = imputed_data.where(F.col('meanImputedPrice').isNull()).count()
    numNulls_medianImputedPrice = imputed_data.where(F.col('medianImputedPrice').isNull()).count()


    res = {
        'count_total': None,
        'mean_meanImputedPrice': None,
        'variance_meanImputedPrice': None,
        'numNulls_meanImputedPrice': None,
        'mean_medianImputedPrice': None,
        'variance_medianImputedPrice': None,
        'numNulls_medianImputedPrice': None,
        'numUnknowns_unknownImputedTitle': None
    }
    # Modify res:
    res['count_total'] = count_total
    res['mean_meanImputedPrice'] = mean_meanImputedPrice
    res['variance_meanImputedPrice'] = variance_meanImputedPrice
    res['numNulls_meanImputedPrice'] = numNulls_meanImputedPrice
    res['mean_medianImputedPrice'] = mean_medianImputedPrice
    res['variance_medianImputedPrice'] = variance_medianImputedPrice
    res['numNulls_medianImputedPrice'] = numNulls_medianImputedPrice
    res['numUnknowns_unknownImputedTitle'] = numUnknowns_unknownImputedTitle

    data_io.save(res, 'task_4')
    return res


def task_5(data_io, product_processed_data, word_0, word_1, word_2):
    # Inputs:
    title_column = 'title'
    # Outputs:
    titleArray_column = 'titleArray'
    titleVector_column = 'titleVector'
    
    df = product_processed_data.select(F.lower(F.col("title")).alias("lowered")).select(F.split(F.col('lowered'),' ').alias("titleArray"))

    word2Vec = M.feature.Word2Vec(vectorSize = 16, seed=SEED, inputCol="titleArray", outputCol="model")
    word2Vec.setMinCount(100)
    word2Vec.setNumPartitions(4)
    model = word2Vec.fit(df)


    res = {
        'count_total': None,
        'size_vocabulary': None,
        'word_0_synonyms': [(None, None), ],
        'word_1_synonyms': [(None, None), ],
        'word_2_synonyms': [(None, None), ]
    }
    # Modify res:
    res['count_total'] = df.count()
    res['size_vocabulary'] = model.getVectors().count()
    for name, word in zip(
        ['word_0_synonyms', 'word_1_synonyms', 'word_2_synonyms'],
        [word_0, word_1, word_2]
    ):
        res[name] = model.findSynonymsArray(word, 10)

    data_io.save(res, 'task_5')
    return res


def task_6(data_io, product_processed_data):
    # Inputs:
    category_column = 'category'
    # Outputs:
    categoryIndex_column = 'categoryIndex'
    categoryOneHot_column = 'categoryOneHot'
    categoryPCA_column = 'categoryPCA'

    indexer = M.feature.StringIndexer(inputCol=category_column, outputCol=categoryIndex_column)
    indexer_model = indexer.fit(product_processed_data)
    indexed_df = indexer_model.transform(product_processed_data)
    
    OHE = M.feature.OneHotEncoder(inputCol=categoryIndex_column, outputCol=categoryOneHot_column, dropLast=False)
    OHE_model = OHE.fit(indexed_df)
    OHE_df = OHE_model.transform(indexed_df)
        
    pca = M.feature.PCA(k=15, inputCol=categoryOneHot_column, outputCol=categoryPCA_column)
    pca_model = pca.fit(OHE_df)
    pca_df = pca_model.transform(OHE_df)
    
    summarizer = M.stat.Summarizer().metrics('mean')
    meanVector_categoryOneHot = OHE_df.select(summarizer.summary(featuresCol=F.col(categoryOneHot_column))).collect()[0][0][0]
    meanVector_categoryPCA = pca_df.select(summarizer.summary(featuresCol=F.col(categoryPCA_column))).collect()[0][0][0]
    count_total = indexed_df.count()


    res = {
        'count_total': None,
        'meanVector_categoryOneHot': [None, ],
        'meanVector_categoryPCA': [None, ]
    }
    # Modify res:
    res['count_total'] = count_total
    res['meanVector_categoryOneHot'] = meanVector_categoryOneHot
    res['meanVector_categoryPCA'] = meanVector_categoryPCA

    data_io.save(res, 'task_6')
    return res
    
    
def task_7(data_io, train_data, test_data):

    dt = M.regression.DecisionTreeRegressor(featuresCol='features', labelCol='overall', maxDepth=5)
    model = dt.fit(train_data)
    preds = model.transform(test_data)
    test_rmse = preds.agg((F.avg((F.col('overall') - F.col('prediction'))**2))**(1/2)).collect()[0][0]
  

    res = {
        'test_rmse': None
    }
    # Modify res:
    res['test_rmse'] = test_rmse

    data_io.save(res, 'task_7')
    return res
    
    
def task_8(data_io, train_data, test_data):
    
    max_depths = [5, 7, 9, 12]
    rmses = []
    train_data, validation_data = train_data.randomSplit([.75, .25])
    models = []

    for depth in max_depths:
        dt = M.regression.DecisionTreeRegressor(featuresCol='features', labelCol='overall', maxDepth=depth)
        model = dt.fit(train_data)
        models.append(model)
        preds = model.transform(validation_data)
        rmse = preds.agg((F.avg((F.col('overall') - F.col('prediction'))**2))**(1/2)).collect()[0][0]
        rmses.append(rmse)
    
    #best_hyperparam = max_depths[rmses.index(min(rmses))]
    best_model = models[rmses.index(min(rmses))]
    test_preds = best_model.transform(test_data)
    test_rmse = test_preds.agg((F.avg((F.col('overall') - F.col('prediction'))**2))**(1/2)).collect()[0][0]   
    

    res = {
        'test_rmse': None,
        'valid_rmse_depth_5': None,
        'valid_rmse_depth_7': None,
        'valid_rmse_depth_9': None,
        'valid_rmse_depth_12': None,
    }
    # Modify res:
    res['test_rmse'] = test_rmse
    res['valid_rmse_depth_5'] = rmses[0]
    res['valid_rmse_depth_7'] = rmses[1]
    res['valid_rmse_depth_9'] = rmses[2]
    res['valid_rmse_depth_12'] = rmses[3]

    data_io.save(res, 'task_8')
    return res

