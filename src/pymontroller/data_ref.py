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
def _base_dd_insert(data, data_db, data_col, data_ref_db, data_ref_col, **kwarg):
    mongoClient = resolve_mongo_client(**kwarg)
    my_data_store_ref = data_ref(db = data_db, collection = 'data_store')
    import pickle
    from datetime import datetime
    pickled_data = pickle.dumps(data)
    raw_data_insert_result = mongoClient[data_db][data_col].insert_one({'data' : pickled_data, 'insertion_datetime' : datetime.now()})
    my_data_store_ref.documentID = raw_data_insert_result.inserted_id
    
    my_data_ref = data_ref(db = data_ref_db, collection = data_ref_col)
    data_ref_insert_result = my_data_ref.data_insert(data = my_data_store_ref, mongoClient = mongoClient)
    my_data_ref.documentID = data_ref_insert_result.inserted_id
    
    return my_data_store_ref, my_data_ref
class data_ref:
    '''
    connection is intentionally excluded from the object to avoid saving the connection itself to db
    sample use  : 
        my_data_ref = dr.data_ref(db = worker_db, collection = worker_name)
        my_data_ref.data_insert(data = worker_command.command_kill_worker(worker_name = worker_name), connectionStr = None, mongoClient = connections.client)
    
    '''
    def __init__(self, db, collection, documentID = None):
        self.db, self.collection, self.documentID = db, collection, documentID
        self.data_pointer_ref = None
        pass
    def deref_data(self, **kwarg):
        mongoClient = resolve_mongo_client(**kwarg)
        data_pickled = (mongoClient[self.db][self.collection].find_one({"_id" : self.documentID}))
        if data_pickled is None:
            from errorType import data_ref_not_exist
            raise data_ref_not_exist('db = ' + self.db + ', collection ='+  self.collection  + ', _id =' + str(self.documentID))
        import pickle
        unpickled_data = pickle.loads(data_pickled['data'])
        if type(unpickled_data) is external_data_ref:
            data = unpickled_data.deref()
        else :
            data = unpickled_data
        return  data
        
        pass
    def data_insert(self, data, **kwarg):
        mongoClient = resolve_mongo_client(**kwarg)
        if self.db is None or  self.collection is None:
            raise(ValueError('wrong db or collection or documentID'))
            pass
        import pickle
        from datetime import datetime
        pickled_data = pickle.dumps(data)
        
        return(mongoClient[self.db][self.collection].insert_one({'data' : pickled_data, 'insertion_datetime' : datetime.now()}))
        pass

    
    def dd_insert(self, data, **kwarg):
        mongoClient = resolve_mongo_client(**kwarg)
        my_data_store_ref = data_ref(db = 'eventTrigger', collection = 'data_store')
        import pickle
        from datetime import datetime
        pickled_data = pickle.dumps(data)
        raw_data_insert_result = mongoClient['eventTrigger']['data_store'].insert_one({'data' : pickled_data, 'insertion_datetime' : datetime.now()})
        my_data_store_ref.documentID = raw_data_insert_result.inserted_id
        self.data_store_ref = my_data_store_ref
        my_data_ref = data_ref(db = self.db, collection = self.collection)
        data_ref_insert_result = my_data_ref.data_insert(data = my_data_store_ref, mongoClient = mongoClient)
        my_data_ref.documentID = data_ref_insert_result.inserted_id
        self.data_pointer_ref = my_data_ref
    def dd_deref(self, **kwarg):
        if self.data_pointer_ref is None:
            assert(hasattr(self, 'documentID'))
            assert(hasattr(self, 'db'))
            assert(hasattr(self, 'collection'))
            
            mongoClient = resolve_mongo_client(**kwarg)
            data_pickled = (mongoClient[self.db][self.collection].find_one({"_id" : self.documentID}))
            import pickle
            unpickled_data = pickle.loads(data_pickled['data'])
            assert type(unpickled_data) is data_ref
            
            self.data_pointer_ref = unpickled_data
        
        data_pickled = (mongoClient["eventTrigger"]['data_packet_input'].find_one({"_id" : self.data_pointer_ref.documentID}))
        unpickled_data = pickle.loads(data_pickled['data'])
        if type(unpickled_data) is external_data_ref:
            data = unpickled_data.deref()
        else :
            data = unpickled_data
        return  data
        
                
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

