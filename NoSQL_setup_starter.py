#!/usr/bin/env python
# coding: utf-8

# # Eat Safe, Love

# ## Part 1: Database and Jupyter Notebook Set Up

# Import the data provided in the `establishments.json` file from your Terminal. Name the database `uk_food` and the collection `establishments`.
# 
# Within this markdown cell, copy the line of text you used to import the data from your Terminal. This way, future analysts will be able to repeat your process.
# 
# e.g.: Import the dataset with `YOUR IMPORT TEXT HERE`

# In[1]:


# Import dependencies
from pymongo import MongoClient
from pprint import pprint


# In[2]:


# Create an instance of MongoClient
mongo = MongoClient(port = 27017)


# In[3]:


# confirm that our new database was created
print(mongo.list_database_names())


# In[4]:


# assign the uk_food database to a variable name
db = mongo['uk_food']


# In[7]:


# review the collections in our new database
print(db.list_collection_names())


# In[6]:


# review the collections in our new database
pprint(db.establishments.find_one())


# In[8]:


# review a document in the establishments collection
pprint(db.establishments.find_one())


# In[9]:


# assign the collection to a variable
establishments = db['establishments']


# ## Part 2: Update the Database

# 1. An exciting new halal restaurant just opened in Greenwich, but hasn't been rated yet. The magazine has asked you to include it in your analysis. Add the following restaurant "Penang Flavours" to the database.

# In[10]:


# Create a dictionary for the new restaurant data
new_restaurant ={
    "BusinessName":"Penang Flavours",
    "BusinessType":"Restaurant/Cafe/Canteen",
    "BusinessTypeID":"",
    "AddressLine1":"Penang Flavours",
    "AddressLine2":"146A Plumstead Rd",
    "AddressLine3":"London",
    "AddressLine4":"",
    "PostCode":"SE18 7DY",
    "Phone":"",
    "LocalAuthorityCode":"511",
    "LocalAuthorityName":"Greenwich",
    "LocalAuthorityWebSite":"http://www.royalgreenwich.gov.uk",
    "LocalAuthorityEmailAddress":"health@royalgreenwich.gov.uk",
    "scores":{
        "Hygiene":"",
        "Structural":"",
        "ConfidenceInManagement":""
    },
    "SchemeType":"FHRS",
    "geocode":{
        "longitude":"0.08384000",
        "latitude":"51.49014200"
    },
    "RightToReply":"",
    "Distance":4623.9723280747176,
    "NewRatingPending":True
}


# In[11]:


# Insert the new restaurant into the collection
establishments.insert_one(new_restaurant)


# In[12]:


# Check that the new restaurant was inserted
query = {'BusinessName': 'Penang Flavours'}
results = establishments.find(query)
for result in results:
    pprint(result)


# 2. Find the BusinessTypeID for "Restaurant/Cafe/Canteen" and return only the `BusinessTypeID` and `BusinessType` fields.

# In[13]:


# Find the BusinessTypeID for "Restaurant/Cafe/Canteen" and return only the BusinessTypeID and BusinessType fields
query = {'BusinessType': 'Restaurant/Cafe/Canteen'}
fields = {'BusinessTypeID': 1, 'BusinessType': 1}

results = establishments.find(query, fields)

pprint(results[0])


# 3. Update the new restaurant with the `BusinessTypeID` you found.

# In[14]:


# Update the new restaurant with the correct BusinessTypeID
db.establishments.update_one(
    {'BusinessName': 'Penang Flavours'},
    {'$set':
        {'BusinessTypeID': 1}
    }
)


# In[15]:


# Confirm that the new restaurant was updated
query = {'BusinessName': 'Penang Flavours'}
fields = {'BusinessName': 1, 'BusinessTypeID': 1}

results = establishments.find(query, fields)
pprint(results[0])


# 4. The magazine is not interested in any establishments in Dover, so check how many documents contain the Dover Local Authority. Then, remove any establishments within the Dover Local Authority from the database, and check the number of documents to ensure they were deleted.

# In[16]:


# Find how many documents have LocalAuthorityName as "Dover"
query = {'LocalAuthorityName': 'Dover'}
print('Number of documents: ', establishments.count_documents(query))


# In[17]:


# Delete all documents where LocalAuthorityName is "Dover"
query = {'LocalAuthorityName': 'Dover'}
establishments.delete_many(query)


# In[18]:


# Check if any remaining documents include Dover
query = {'LocalAuthorityName': 'Dover'}
print('Number of documents: ', establishments.count_documents(query))


# In[19]:


# Check that other documents remain with 'find_one'
pprint(db.establishments.find_one())


# 5. Some of the number values are stored as strings, when they should be stored as numbers. Use `update_many` to convert `latitude` and `longitude` to decimal numbers.

# In[20]:


# Change the data type from String to Decimal for longitude
establishments.update_many({}, [{'$set': {'geocode.longitude':                                          {'$toDecimal': '$geocode.longitude'}}}])


# In[21]:


# Change the data type from String to Decimal for latitude
establishments.update_many({}, [{'$set': {'geocode.latitude':                                          {'$toDecimal': '$geocode.latitude'}}}])


# In[22]:


# Check that the coordinates are now numbers
document = establishments.find_one()
geocode_dict = document['geocode']

longitude_type = type(geocode_dict['longitude'])
print("The data type of 'longitude' is: ", longitude_type)

latitude_type = type(geocode_dict['latitude'])
print("The data type of 'latitude' is: ", latitude_type)


# In[ ]:




