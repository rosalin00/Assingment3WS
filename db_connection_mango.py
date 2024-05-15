#-------------------------------------------------------------------------
# AUTHOR: Alondra Marin
# FILENAME: db_connection_mango.py
# SPECIFICATION: A script is designed for interacting with a MongoDB database without using
# advanced libraries. It includes functions for establishing a database connection, creating, deleting, and updating
# documents within a collection.
# FOR: CS 4250- Assignment #3
# TIME SPENT: 2 hours
#-----------------------------------------------------------*/

#importing some Python libraries
import pymongo

def connectDataBase():
    # Create a database connection object using pymongo
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["your_database_name"]
    col = db["your_collection_name"]
    return col

def createDocument(col, docId, docText, docTitle, docDate, docCat):
    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    term_count = {}
    terms = docText.lower().split()
    for term in terms:
        term_count[term] = term_count.get(term, 0) + 1
    
    # create a list of objects to include full term objects. [{"term", count, num_char}]
    term_objects = [{"term": term, "count": count, "num_char": len(term)} for term, count in term_count.items()]
    
    # produce a final document as a dictionary including all the required document fields
    document = {
        "id": docId,
        "text": docText,
        "title": docTitle,
        "date": docDate,
        "category": docCat,
        "terms": term_objects
    }

    # insert the document
    col.insert_one(document)

def deleteDocument(col, docId):
    # Delete the document from the database
    query = {"id": docId}
    col.delete_one(query)

def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    # Delete the document
    deleteDocument(col, docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    inverted_index = {}
    for document in col.find():
        for term_obj in document["terms"]:
            term = term_obj["term"]
            title = document["title"]
            count = term_obj["count"]
            if term in inverted_index:
                inverted_index[term] += f",{title}:{count}"
            else:
                inverted_index[term] = f"{title}:{count}"
    return inverted_index
