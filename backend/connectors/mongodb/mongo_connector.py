from pymongo import MongoClient

class MongoConnector:
    def __init__(self, uri):
        self.uri = uri

    def test(self):
        client = MongoClient(self.uri, serverSelectionTimeoutMS=4000, socketTimeoutMS=10000)
        try:
            client.admin.command("ping")
        finally:
            client.close()

    def list_collections(self, database):
        client = MongoClient(self.uri, serverSelectionTimeoutMS=4000, socketTimeoutMS=10000)
        try:
            return client[database].list_collection_names()
        finally:
            client.close()

    def database_name(self):
        client = MongoClient(self.uri, serverSelectionTimeoutMS=4000)
        try:
            database = client.get_default_database()
            if database is None:
                raise ValueError("L'URI MongoDB doit préciser le nom de la base, par exemple mongodb://localhost:27017/edudata_demo")
            return database.name
        finally:
            client.close()

    def sample(self, database, collection, limit=20):
        client = MongoClient(self.uri, serverSelectionTimeoutMS=4000, socketTimeoutMS=10000)
        try:
            cursor = client[database][collection].find({}, {"_id": 0}).limit(min(max(1, limit), 50)).max_time_ms(8000)
            return list(cursor)
        finally:
            client.close()

    def find(self, database, collection, filter_doc=None, limit=100, projection=None):
        client = MongoClient(self.uri, serverSelectionTimeoutMS=4000, socketTimeoutMS=10000)
        try:
            cursor = client[database][collection].find(filter_doc or {}, projection).limit(min(max(1, limit), 5000)).max_time_ms(8000)
            docs = list(cursor)
            for doc in docs:
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
            return docs
        finally:
            client.close()
