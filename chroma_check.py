# import chromadb

# client = chromadb.PersistentClient(path="./chroma_db")
# collection = client.get_collection(name="equifiz_tools")

# # Peek shows the first few records including metadatas
# results = collection.peek(limit=5)
# print(results["metadatas"])

import chromadb
client = chromadb.PersistentClient(path="./chroma_store")
col = client.get_collection("equifiz_tools")
results = col.get(include=["metadatas"])
for tool_id, meta in zip(results["ids"], results["metadatas"]):
    print(f"{tool_id}: {meta}")