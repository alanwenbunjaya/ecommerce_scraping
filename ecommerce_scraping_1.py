# author : alan.Bunjaya 
# created : 2021-05-4-04
# ecommerce scaraping


import json
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
import requests
import timeit
import sys


# read sys parameter for how many page to be scraped
if len(sys.argv) > 1:
    page_range = sys.argv[1]

# list of column that not needed
useless_cols = ["preorder","labels", "badges", "__typename", 'labelGroups', 'wishlist', "imageUrl", "gaKey", "catId","dep_id"]

# read excel file as dictionary of scraped category
tokped_cat = pd.read_excel('tokopedia_category.xlsx', Sheet = 'Sheet1')

start = timeit.default_timer()
appended_data=[]

# scraping segment
for i,row in tokped_cat.iterrows():
    tokped_cat_id = {'dep_id':row['dep_id']}

    # looping for every page of selected category
    for i in range(0,int(page_range)):
        data_payload = [{"operationName":"SearchProductQuery","variables":{"params":"page="+str(i+1)+"&ob=&identifier="+row['identifier']+"&sc="+str(row['dep_id'])+"&user_id=0&rows=60&start="+str(i*60+1)+"&source=directory&device=desktop&page="+str(i+1)+"&related=true&st=product&safe_search=false","adParams":"page="+str(i+1)+"&page="+str(i+1)+"&dep_id="+str(row['dep_id'])+"&ob=&ep=product&item=15&src=directory&device=desktop&user_id=0&minimum_item=15&start="+str(i*60+1)+"&no_autofill_range=5-14"},"query":"query SearchProductQuery($params: String, $adParams: String) {\n  CategoryProducts: searchProduct(params: $params) {\n    count\n    data: products {\n      id\n      url\n      imageUrl: image_url\n      imageUrlLarge: image_url_700\n      catId: category_id\n      gaKey: ga_key\n      countReview: count_review\n      discountPercentage: discount_percentage\n      preorder: is_preorder\n      name\n      price\n      original_price\n      rating\n      wishlist\n      labels {\n        title\n        color\n        __typename\n      }\n      badges {\n        imageUrl: image_url\n        show\n        __typename\n      }\n      shop {\n        id\n        url\n        name\n        goldmerchant: is_power_badge\n        official: is_official\n        reputation\n        clover\n        location\n        __typename\n      }\n      labelGroups: label_groups {\n        position\n        title\n        type\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  displayAdsV3(displayParams: $adParams) {\n    data {\n      id\n      ad_ref_key\n      redirect\n      sticker_id\n      sticker_image\n      productWishListUrl: product_wishlist_url\n      clickTrackUrl: product_click_url\n      shop_click_url\n      product {\n        id\n        name\n        wishlist\n        image {\n          imageUrl: s_ecs\n          trackerImageUrl: s_url\n          __typename\n        }\n        url: uri\n        relative_uri\n        price: price_format\n        campaign {\n          original_price\n          discountPercentage: discount_percentage\n          __typename\n        }\n        wholeSalePrice: wholesale_price {\n          quantityMin: quantity_min_format\n          quantityMax: quantity_max_format\n          price: price_format\n          __typename\n        }\n        count_talk_format\n        countReview: count_review_format\n        category {\n          id\n          __typename\n        }\n        preorder: product_preorder\n        product_wholesale\n        free_return\n        isNewProduct: product_new_label\n        cashback: product_cashback_rate\n        rating: product_rating\n        top_label\n        bottomLabel: bottom_label\n        __typename\n      }\n      shop {\n        image_product {\n          image_url\n          __typename\n        }\n        id\n        name\n        domain\n        location\n        city\n        tagline\n        goldmerchant: gold_shop\n        gold_shop_badge\n        official: shop_is_official\n        lucky_shop\n        uri\n        owner_id\n        is_owner\n        badges {\n          title\n          image_url\n          show\n          __typename\n        }\n        __typename\n      }\n      applinks\n      __typename\n    }\n    template {\n      isAd: is_ad\n      __typename\n    }\n    __typename\n  }\n}\n"},{"operationName":"CategoryTopAdsQuery","variables":{"adParams":"page="+str(i+1)+"&page="+str(i+1)+"&dep_id="+str(row['dep_id'])+"&ob=&ep=cpm&item=3&src=directory&device=desktop&user_id=0&minimum_item=3&start="+str(i*60+1)+"&no_autofill_range=5-14"},"query":"query CategoryTopAdsQuery($adParams: String) {\n  TopAdsProducts: displayAdsV3(displayParams: $adParams) {\n    data {\n      id\n      ad_ref_key\n      ad_click_url\n      redirect\n      sticker_id\n      sticker_image\n      product_click_url\n      product_wishlist_url\n      shop_click_url\n      headline {\n        button_text\n        name\n        shop {\n          city\n          __typename\n        }\n        badges {\n          image_url\n          __typename\n        }\n        description\n        image {\n          full_url\n          full_ecs\n          __typename\n        }\n        shop {\n          is_followed\n          city\n          slogan\n          domain\n          gold_shop\n          gold_shop_badge\n          id\n          image_shop {\n            xs_url\n            __typename\n          }\n          product {\n            id\n            price_format\n            image_product {\n              image_click_url\n              image_url\n              product_id\n              product_name\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      applinks\n      __typename\n    }\n    template {\n      is_ad\n      __typename\n    }\n    __typename\n  }\n}\n"}]
        response = requests.post('https://gql.tokopedia.com/', json = data_payload).content
        tokpedjson = json.loads(response)
        for i in range(0,60):
            try:
                (tokpedjson[0]['data']['CategoryProducts']['data'][i]).update(tokped_cat_id)
            except:
                pass
        appended_data = appended_data + tokpedjson[0]['data']['CategoryProducts']['data']
stop = timeit.default_timer()
print("Time:" + str(stop-start))

# save data to dataframe and drop not needed column
tokpeddf = pd.DataFrame(appended_data).drop(useless_cols, axis = 1)

# convert price data into int
tokpeddf['price']=tokpeddf['price'].str.replace('.','')
tokpeddf['price']=tokpeddf['price'].str.replace('Rp','')
tokpeddf['price']=tokpeddf['price'].astype(int)

tokpeddf['original_price']=tokpeddf['original_price'].str.replace('.','')
tokpeddf['original_price']=tokpeddf['original_price'].str.replace('Rp','')

tokpeddf['original_price'] = pd.to_numeric(tokpeddf['original_price'], errors='coerce')
tokpeddf['original_price'].fillna(0, inplace = True)

tokpeddf['merchant'] = ""

# getting merchant name from shop column
for i,row in tokpeddf.iterrows():
    tokpeddf.at[(i,'merchant')]= (row['shop'])['name']

# save transformed data into csv file using required column
tokpeddf[['name','imageUrlLarge','price','rating','merchant']].to_csv('tokped_brick.csv')