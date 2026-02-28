import pymongo
import pandas as pd
from urllib.parse import quote_plus

def connect_and_export():
    password = "Gupta@9851"  # यहाँ real password डालें
    
    try:
        encoded_password = quote_plus(password)
        
        # Simple connection without SSL parameters
        connection_string = f"mongodb+srv://aartech_ceo:{encoded_password}@cluster0.bzujt.mongodb.net/?retryWrites=true&w=majority"
        
        client = pymongo.MongoClient(connection_string)
        
        # Test connection
        client.admin.command('ping')
        print("🎉 Connected successfully!")
        
        # Find databases
        databases = client.list_database_names()
        print(f"📊 Available databases: {databases}")
        
        for db_name in databases:
            if db_name not in ['admin', 'local', 'config']:
                print(f"\n🔍 Checking database: {db_name}")
                db = client[db_name]
                collections = db.list_collection_names()
                print(f"📁 Collections: {collections}")
                
                for coll_name in collections:
                    collection = db[coll_name]
                    count = collection.count_documents({})
                    
                    if count > 0:
                        print(f"\n📄 Collection '{coll_name}': {count} documents")
                        
                        # Get sample record
                        sample = collection.find_one()
                        if sample:
                            print(f"🏷️  Sample fields: {list(sample.keys())}")
                            
                            # Export data (limit to 1000 records for now)
                            print("💾 Exporting data...")
                            data = list(collection.find().limit(1000))
                            
                            if data:
                                df = pd.DataFrame(data)
                                filename = f"{db_name}_{coll_name}_export.csv"
                                df.to_csv(filename, index=False)
                                print(f"✅ Exported {len(df)} records to: {filename}")
                                print(f"📊 Columns: {list(df.columns)}")
                            else:
                                print("❌ No data found")
        
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        print("\n🔧 Alternative solutions:")
        print("1. Try MongoDB Compass (GUI)")
        print("2. Update PyMongo: pip install --upgrade pymongo")
        print("3. Use Atlas Dashboard manual export")

# Run the function
connect_and_export()