import itertools

class DatabaseList:

    def __init__(self):
        self.databases = {}

    def add_database(self, db, name=None, route=None):
        new_databases = self.databases.copy()
        if name is None:
            # Pick a unique name for this database
            name = db.suggest_name()
        suggestion = name
        num = itertools.count(2)
        while name in self.databases:
            name = "{}_{}".format(suggestion, next(num))
        db.name = name
        db.route = route or name
        new_databases[name] = db
        # don't mutate! that causes race conditions with live import
        self.databases = new_databases
        return db
    
