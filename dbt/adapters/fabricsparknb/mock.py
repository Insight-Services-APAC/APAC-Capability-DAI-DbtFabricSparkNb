import dbt.adapters.fabricsparknb.catalog as catalog

class cursor:
    def __init__(self, profile):
        self.sql = ""
        self.profile = profile
        self.data = None
        pass

    def SetProfile(self, profile):
        self.profile = profile

    def execute(self, sql, bindings=None):
        self.sql = sql
        self.data = catalog.ListRelations(self.profile)
        return self.data

    def fetchall(self):
        pass

    def fetchone(self):
        pass

    def close(self):
        pass

    @property
    def description(self) -> list[str]:
        return self.data['table'].column_names

class handle: 
    def __init__(self, profile):
        self._cursor = cursor(profile)
        

    def cursor(self):
        return self._cursor

    def close(self):
        pass

    
