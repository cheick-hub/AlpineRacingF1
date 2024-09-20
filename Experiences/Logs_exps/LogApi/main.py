# import config
import schemas
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from services.database_service import Database_service

app_parameters = {
    "title" : "Log API",
    "description" : "API for handling logs and storing them in a database",
    "version" : "1.0.0",
    "openapi_tags" : [{"name" : "Logs"}]
} 

# connect to the database
#TODO: get the database host and port from the config file # verify that connexion is still open otherwise open it back
app = FastAPI(**app_parameters)

@app.get("/")
def read_root():
    return RedirectResponse(url="/redoc")

@app.post("/logs",tags=["Logs"], response_model=schemas.response)
def register_log(logs : schemas.log_input):
    """
    Store received logs in the database
    param<u> : logs : dict : logs to be stored in the database
    return *dict* : response message
    """
    # insert data to the database
    print(logs)
    return {"store" : True, "time" : 1000.4}


