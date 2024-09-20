import yaml

from bdd.base import BASEBDD
from utils.filters import filter_eq, filter_like


class ANALYTICS(BASEBDD):
    def __init__(self, base="ANALYTICS"):
        file = './config/analytics.yml'
        with open(file, 'r') as file:
            config = yaml.safe_load(file)
        self._create_engine(config, base)

        # Table construction
        tables = ["applications", "usagecount", "users", "applications_users", "application_users_joined"]
        self._init_table(tables)
        print(base + " DATABASE LOADED")

    def get_applications(self, Name=None):

        t = self.tables
        select = list()
        self._select(select, t.applications, ["id", "identifier"])

        query = self._create_query(select)

        query = filter_like(query, t.applications.c.identifier, Name)

        df = self._read_sql(query)

        return df

    def log_count(self,
                  identifer,
                  title,
                  version,
                  Computername,
                  UserName,
                  Misc):
        t = self.tables
        application = self.get_applications(identifer)

        if len(application) == 0:
            raise "Unknown Application"

        sqlinsert = t.usagecount.insert().values(Name=identifer,
                                                 Title=title,
                                                 Version=version,
                                                 Type='launch',
                                                 Computername=Computername,
                                                 UserName=UserName,
                                                 Misc=Misc)
        self._execute_sql(sqlinsert)

        user = self.get_user(UserName)

        if len(user) == 0:
            user = self.add_user(UserName)

        application_id = int(application.applications_id[0])
        user_id = int(user.users_id[0])
        user_developer = user.users_developer[0]
        applications_users = self.get_applications_users(application_id, user_id)

        if len(applications_users) == 0:
            self.add_applications_users(application_id, user_id, user_developer)

        return self.get_meta(application_id, user_id)

    def get_user(self, UserName):
        t = self.tables
        select = list()
        self._select(select, t.applications, ["id", "identifier", "developer"])

        query = self._create_query(select)

        query = filter_like(query, t.users.c.identifier, UserName)

        df = self._read_sql(query)

        return df

    def isValidPassword(self, UserName, password_hash):
        t = self.tables
        select = list()
        self._select(select, t.users, ["id"])

        query = self._create_query(select)

        query = filter_like(query, t.users.c.identifier, UserName)
        query = filter_like(query, t.users.c.password, password_hash.upper())

        df = self._read_sql(query)

        return bool(df.size > 0)

    def getUserType(self, UserName, application):
        t = self.tables
        select = list()
        self._select(select, t.application_users_joined, ["user_type"])

        query = self._create_query(select)

        query = filter_like(query, t.application_users_joined.c.user_identifier, UserName)
        query = filter_like(query, t.application_users_joined.c.applicationName, application)

        df = self._read_sql(query)

        usertype = "None"
        if df.size > 0:
            usertype = df.user_type[0]

        return usertype

    def add_user(self, UserName):
        t = self.tables
        sqlinsert = t.users.insert().values(identifier=UserName)

        self._execute_sql(sqlinsert)

        return self.get_user(UserName)

    def get_applications_users(self, application_id, user_id):

        t = self.tables
        select = list()
        self._select(select, t.applications_users, ["id"])

        query = self._create_query(select)
        query = filter_eq(query, t.applications_users.c.id_users, user_id)
        query = filter_eq(query, t.applications_users.c.id_applications, application_id)

        df = self._read_sql(query)
        return df

    def add_applications_users(self, application_id, user_id, user_developer):

        user_type = "user"
        if user_developer:
            user_type = "developer"

        t = self.tables
        sqlinsert = t.applications_users.insert().values(id_applications=application_id,
                                                         id_users=user_id,
                                                         user_type=user_type)
        self._execute_sql(sqlinsert)

    def get_meta(self, application_id, user_id):
        t = self.tables
        select = [t.applications_users.c.user_type.label("user_type"),
                  t.users.c.email.label("email"),
                  t.users.c.FirstName.label("FirstName"),
                  t.users.c.LastName.label("LastName"),
                  t.users.c.identifier.label("user_identifier"),
                  t.users.c.developer.label("developer"),
                  t.applications.c.name.label("applicationName"),
                  t.applications.c.url.label("url"),
                  t.applications.c.documentation.label("documentation"),
                  t.applications.c.description.label("description"),
                  t.applications.c.responsible.label("responsible"),
                  t.applications.c.identifier.label("applicationIdentifier")]
        q = self._create_query(select)

        q = q.join(t.users, t.users.c.id == t.applications_users.c.id_users)
        q = q.join(t.applications, t.applications.c.id == t.applications_users.c.id_applications)
        q = filter_eq(q, t.applications_users.c.id_users, user_id)
        q = filter_eq(q, t.applications_users.c.id_applications, application_id)
        return self._read_sql(q)
