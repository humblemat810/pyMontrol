def resolve_mongo_client(**kwarg):
    mongoClient = None
    if 'connectionStr' in kwarg:
        if kwarg['connectionStr'] is not None:
            from  pymongo import MongoClient
            mongoClient = MongoClient (kwarg['connectionStr'])
    if 'mongoClient' in kwarg:
        if kwarg['mongoClient'] is None:
            # raise ValueError('fail to resolve mongo client')
            pass
        else:
            mongoClient = kwarg['mongoClient']
    if mongoClient is None:
        raise ValueError('fail to resolve mongo client')
        pass
    return mongoClient
class external_data_ref:
    # custom class pointing to external data storage
    def deref(self):
        if self.type == 'azure_blob':
            self.connection_str
            pass
        pass
    
    pass
class data_ref:
    
    def __init__(self, db, collection, documentID = None):
        self.db, self.collection, self.documentID = db, collection, documentID
        
        pass
    def deref_data(self, **kwarg):
        mongoClient = resolve_mongo_client(**kwarg)
        data_pickled = (mongoClient[self.db][self.collection].find_one({"_id" : self.documentID}))
        
        import pickle
        unpickled_data = pickle.loads(data_pickled['data'])
        if type(unpickled_data) is external_data_ref:
            data = unpickled_data.deref()
        return 
        
        pass
    def data_insert(self, data, **kwarg):
        mongoClient = resolve_mongo_client(**kwarg)
        if self.db is None or  self.collection is None:
            raise(ValueError('wrong db or collection or documentID'))
            pass
        import pickle
        pickled_data = pickle.dumps(data)
        return(mongoClient[self.db][self.collection].insert_one({'data' : pickled_data}))
        pass
    
    def delete_data(self, **kwarg):
        mongoClient = resolve_mongo_client(**kwarg)
        return(mongoClient[self.db][self.collection].delete_one({"_id" : self.documentID}))
        pass
    pass



if __name__ == '__main__':
    import connections
    import data_ref as dr
    my_data_ref = dr.data_ref(db = 'worker', collection = 'test_worker1')
    
    insert_result = my_data_ref.data_insert(data = 'sdas', connectionStr = None, mongoClient = connections.client)
    my_data_ref.documentID = insert_result.inserted_id
    print(my_data_ref.deref_data( connectionStr = None, mongoClient = connections.client))
    my_data_ref.delete_data(connectionStr = None, mongoClient = connections.client)
    pass

