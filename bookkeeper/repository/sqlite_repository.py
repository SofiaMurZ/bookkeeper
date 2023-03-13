"""
Репозиторий для работы с базами данных
"""

from typing import Any
from inspect import get_annotations
import sqlite3
from bookkeeper.repository.abstract_repository import AbstractRepository, T



class SQLiteRepository(AbstractRepository[T]):
    """
    Репозиторий, работающий c SQLite3
    """

    db_file: str
    cls: type
    table_name: str
    fields: dict[str, Any]

    def __init__(self, db_file: str, cls: type) -> None:
        """
        Создает репозиторий для хранения данных
        db_file: str - название файла с базой данных
        cls: type - модель, с объектами которой взаимодействует репозиторий
        """
        self.db_file = db_file
        self.cls = cls
        self.table_name = self.cls.__name__
        self.fields = get_annotations(cls, eval_str=True)
        self.fields.pop('pk')

        keys = [str(k) for k in list(self.fields.keys())]
        vals = [str(v) for v in list(self.fields.values())]

        def type_check(val: str) -> str:
            if val.find('int') != -1:
                return 'INTEGER'
            else:
                return 'TEXT'

        vals = [type_check(v) for v in vals]
        names = [str(k) + ' ' + str(v) for (k, v) in zip(keys, vals)]
        names = names + ['pk INTEGER PRIMARY KEY']
        names = ', '.join(names)
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(f'DROP TABLE IF EXISTS {self.table_name}')
            cur.execute(f'CREATE TABLE IF NOT EXISTS {self.table_name} ({names})')
        con.close()

    def add(self, obj: T) -> int:
        """
        Добавляет объект в репозиторий и возвращает его номер
        obj: объект, который добавляется
        Возвращает номер объекта
        """
        if getattr(obj, 'pk', None) != 0:
            raise ValueError(f'trying to add object {obj} with filled `pk` attribute')
        names = ', '.join(self.fields.keys())
        placeholders = ', '.join("?" * len(self.fields))
        values = tuple(getattr(obj, x) for x in self.fields.keys())
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(
                f'INSERT INTO {self.table_name} ({names}) VALUES ({placeholders})', values
            )
            obj.pk = cur.lastrowid
        con.close()
        return obj.pk

    def get(self, pk: int) -> T | None:
        """
        Получает объект из репозитория (по его номеру)
        pk: номер объекта 
        Возвращает объект из репозитория
        """
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute('PRAGMA foreign_keys = ON')
            cur.execute(
                f'SELECT * FROM {self.table_name} WHERE pk = {pk}'
            )
            tuple_obj = cur.fetchone()
        con.close()
        if tuple_obj is None:
            return None
        tuple_obj = tuple(t if t is not None else None for t in tuple_obj)
        names = (*[str(k) for k in self.fields.keys()], 'pk')
        obj = self.cls()
        for i in range(len(names)):
            setattr(obj, names[i], tuple_obj[i])
        return obj

    def get_all(self, place: dict[str, Any] | None = None) -> list[T]:
        """
        Получает все объекты из таблицы репозитория(с определенными значениями)
        place: переменная типа 'словарь'
        Возвращает список объектов
        """
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute('PRAGMA foreign_keys = ON')
            cur.execute(
                f'SELECT * FROM {self.table_name}'
            )
            tuple_objs = cur.fetchall()
        con.close()
        objs = []
        names = (*[str(k) for k in self.fields.keys()], 'pk')
        for tuple_obj in tuple_objs:
            obj = self.cls()
            for i in range(len(names)):
                setattr(obj, names[i], tuple_obj[i])
            objs.append(obj)
        if place is None:
            return objs
        objs = [
            obj for obj in objs if all(
                getattr(obj, attr) == place[attr] for attr in place.keys()
            )
        ]
        return objs

    def update(self, obj: T) -> None:
        """
        Изменяет объект в репозитории
        """
        if obj.pk == 0:
            raise ValueError('attempt to update object with unknown primary key')
        names = list(self.fields.keys())
        sets = ', '.join(f'{name} = \'{getattr(obj, name)}\'' for name in names)
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute('PRAGMA foreign_keys = ON')
            cur.execute(
                f'UPDATE {self.table_name} SET {sets} WHERE pk = {obj.pk}'
            )
        con.close()

    def delete(self, pk: int) -> None:
        """
        Удаляет объект из репозитория с указанным номером
        """
        if self.get(pk) is None:
            raise KeyError
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute('PRAGMA foreign_keys = ON')
            cur.execute(
                f'DELETE FROM {self.table_name} WHERE pk = {pk}'
            )
        con.close()
