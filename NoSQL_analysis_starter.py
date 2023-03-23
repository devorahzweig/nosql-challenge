#!/usr/bin/env python
# coding: utf-8

# # Eat Safe, Love

# ## Notebook Set Up

# In[1]:


# Import dependencies
from pymongo import MongoClient
from pprint import pprint
import pandas as pd


# In[2]:


# Create an instance of MongoClient
mongo = MongoClient(port = 27017)


# In[3]:


# assign the uk_food database to a variable name
db = mongo['uk_food']


# In[4]:


# review the collections in our database
print(db.list_collection_names())


# In[5]:


# assign the collection to a variable
establishments = db['establishments']


# ## Part 3: Exploratory Analysis
# Unless otherwise stated, for each question: 
# * Use `count_documents` to display the number of documents contained in the result.
# * Display the first document in the results using `pprint`.
# * Convert the result to a Pandas DataFrame, print the number of rows in the DataFrame, and display the first 10 rows.

# ### 1. Which establishments have a hygiene score equal to 20?

# In[10]:


# Find the establishments with a hygiene score of 20
query = {'scores.Hygiene': 20}
results = establishments.find(query)

# Use count_documents to display the number of documents in the result
print('The number of establishments with a Hygiene score of 20 is:', establishments.count_documents(query))

# Display the first document in the results using pprint
pprint(results[0])


# In[11]:


# Convert the result to a Pandas DataFrame
hyg_20_df = pd.DataFrame(results)

# Display the number of rows in the DataFrame
print(f'There are {len(hyg_20_df)} rows in the dataframe.')

# Display the first 10 rows of the DataFrame
hyg_20_df.head(10)


# ### 2. Which establishments in London have a `RatingValue` greater than or equal to 4?

# In[13]:


# Find the establishments with London as the Local Authority and has a RatingValue greater 
# than or equal to 4.
query = {'$and': [{'LocalAuthorityName': {'$regex': 'London'}},{'RatingValue': {'$in':['4', '5']}}]}
results = establishments.find(query)

# Use count_documents to display the number of documents in the result
print('The number of establishments in London with a Rating Value >= 4 is: ',establishments.count_documents(query))

# Display the first document in the results using pprint
pprint(results[0])


# In[14]:


# Convert the result to a Pandas DataFrame
london_df = pd.DataFrame(results)

# Display the number of rows in the DataFrame
print(f'There are {len(london_df)} rows in the dataframe.')

# Display the first 10 rows of the DataFrame
london_df.head(10)


# ### 3. What are the top 5 establishments with a `RatingValue` rating value of '5', sorted by lowest hygiene score, nearest to the new restaurant added, "Penang Flavours"?

# In[15]:


# Search within 0.01 degree on either side of the latitude and longitude.
# Rating value must equal 5
# Sort by hygiene score

degree_search = 0.01
latitude = 51.49014200
longitude = 0.08384000

query = {'$and': [{'RatingValue': '5'},{'$and':[{'geocode.latitude': {'$gte': latitude - degree_search}},
        {'geocode.latitude': {'$lte': latitude + degree_search}}]},{'$and': [
        {'geocode.longitude': {'$gte': longitude - degree_search}},
        {'geocode.longitude': {'$lte': longitude + degree_search}}]}]}
sort = [('scores.Hygiene', 1)]  

results = establishments.find(query).sort(sort).limit(5)

# Print the results
pprint(list(results))


# In[16]:


# Convert result to Pandas DataFrame
results = establishments.find(query).sort(sort).limit(5)
results_df = pd.DataFrame(results)
results_df


# ### 4. How many establishments in each Local Authority area have a hygiene score of 0?

# In[18]:


# Create a pipeline that: 
# 1. Matches establishments with a hygiene score of 0
match_query = {'$match': {'scores.Hygiene': 0}}

# 2. Groups the matches by Local Authority
group_query = {'$group': {'_id': '$LocalAuthorityName', 'count': {'$sum': 1}}}

# 3. Sorts the matches from highest to lowest
sort_values = {'$sort': {'count': -1}}

pipeline = [match_query, group_query, sort_values]
results = list(establishments.aggregate(pipeline))

# Print the number of documents in the result
print('The number of documents in the results is: ', len(results))

# Print the first 10 results
results[:10]


# In[19]:


# Convert the result to a Pandas DataFrame
hygiene_df = pd.DataFrame(results)

# Display the number of rows in the DataFrame
print(f'There are {len(hygiene_df)} rows in the dataframe.')

# Display the first 10 rows of the DataFrame
hygiene_df.head(10)


# In[ ]:




