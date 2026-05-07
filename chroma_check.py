import chromadb

client = chromadb.PersistentClient(path="./chroma_db")
collection = client.get_collection(name="equifiz_tools")

# Peek shows the first few records including metadatas
results = collection.peek(limit=5)
print(results["metadatas"])