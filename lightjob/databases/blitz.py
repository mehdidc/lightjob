import os

from blitzdb import Document
from blitzdb import FileBackend

from ..db import IDKEY
from ..db import DBFILENAME
from ..utils import recur_update

from .base import GenericDB


class Job(Document):

    class Meta(Document.Meta):
        primary_key = IDKEY  # TODO should depend on self.idkey


class Blitz(GenericDB):

    def load_from_dir(self, dirname):
        self.db = FileBackend(os.path.join(dirname, DBFILENAME))

    def insert(self, d):
        self.insert_list([d])

    def insert_list(self, l):
        for j in l:
            Job(j).save(self.db)
        self.db.commit()

    def get_by_id(self, id_):
        try:
            return self.db.get(Job, {self.idkey: id_})
        except Job.DoesNotExist:
            return None

    def delete(self, d):
        for el in self.get(d):
            self.db.delete(el)

    def get(self, d):
        return self.db.filter(Job, d)

    def update(self, d, id_):
        obj = self.get_by_id(id_)
        recur_update(obj, d)
        if obj is not None:
            obj.save(self.db)
            self.db.commit()
            return True
        else:
            return False

    def close(self):
        pass
