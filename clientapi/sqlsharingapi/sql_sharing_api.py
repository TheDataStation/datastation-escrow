from typing import List
"""
There's an abstraction of a Container, which itself may contain multiple tables.
Containers can be owned by multiple Users, and represent logical groupings of
data. This could be data all generated from a task or data combined as part of a
sharing agreement.

There is an API call to register a data element (represented with a cloud
storage path/filename) to a container. 

Users can then send SharingRequests to other users that contains the query to be
computed, the contract (in the DataStation sense), and the set of containers it
wants to access. Users can accept/reject the request or modify it, where they
could add containers to the list of containers made available, modify the query,
etc. This would encapsulate conditional sharing patterns. Once all participating
Users have accepted, then the request can be carried out. If this involves
combining data, we first check if the list of containers are currently
mergeable (is_mergeable), and if so we can call merge. Merge can either simply
create a large union container (put every individual table into one giant
container) or it can combine data from containers to produce new tables in the
new container, such as with JOINs or UNIONs to append/otherwise connect tables. 
 
Finally, we can use `query_container` to run queries against containers. All API
calls will require some auth token to verify permissions. 


Gaps: how do you discover others who you want to share with?
      who do i send accept, reject, modify requests to and how do i know when all have responded?
        when all container owners have accepted
      schema--we all need to know schema and potentially need to modify them before sharing i.e. combination of some kind
      is there a distinction between user and the person calling the API, or is
          there a 1:1 mapping between User and an Auth instance
"""

"""
Auth class encapsulates all token/login info needed to authenticate user
"""
class Auth:
    pass

class User:
    def __init__(self):
        self.containers = []

    def add_container(self, container_id):
        self.containers.append(container_id)

class Container:
    def __init__(self):
        self.container_id = 0 # Unique ID for container

    # data is filename, perhaps in cloud storage
    def add_table(self, auth: Auth, data: str):
        pass

    # access pointer to table 
    def get_table(self, auth: Auth):
        pass

class SharingRequest:
    def __init__(self, query: str=None, contract=None):
        self.query = query
        self.contract = contract
        self.containers = []

    def add_container_to_request(self, container_id):
        self.containers.append(container_id)

    def remove_container(self, container_id):
        self.containers.remove(container_id)

    def set_query(self, query:str):
        self.query = query

class SQLApi:

    def __init__(self):
        pass

    def login(self) -> Auth:
        pass 

    def send_request(self, auth: Auth, req: SharingRequest, user: User):
        pass

    def accept_request(self, auth: Auth, req: SharingRequest):
        pass

    def reject_request(self, auth: Auth, req: SharingRequest):
        pass

    def modify_request(self, auth: Auth, new_req: SharingRequest, old_req:
                       SharingRequest):
        pass

    '''
    Register a new data elemenet with a user. Either create a new container (set
    container_id = None) or upload data element to already existing container
    owned by that user
    '''
    def register(self, auth: Auth, user: User, data: str, container_id = None):
        pass

    '''
    Returns whether a set of containers can be merged to create a new container
    Must have same format. 
    '''
    def is_mergeable(self, auth: Auth, containers):
        pass

    '''
    Merge set of containers. If query provided, run this query to create new tables
    from those within the two containers, i.e. UNION, JOIN, etc.
    Else, just create a new container that contains all tables from the provided
    list of containers
    Resulting container is owned by all users in list. 
    '''
    def merge(self, auth: Auth, containers, users: List[User], query=None):
        pass

    '''
    Execute SQL Query against container owned by you.
    '''
    def query_container(self, auth: Auth, container_id, query: str):
        pass


