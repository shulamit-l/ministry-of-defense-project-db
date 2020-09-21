import json
import os

import hashedindex

import operator
from typing import Any, Dict, List, Type
import db_api
from pathlib import Path

OPERATOR = {
    '=': operator.eq,
    '<=': operator.le,
    '>=': operator.ge,
    '!=': operator.ne,
    '>': operator.gt,
    '<': operator.lt
}

DB_ROOT = Path('db_files')

# for meta data table which holds all table names and their primary key
META_DATA = "meta_data"

#========================== Auxiliary function ================================

# reads a table from disk (dict from json file)
def read_file(file_name):
    with open(file_name, 'r') as json_file:
        table = json.load(json_file)
    return table

# writes a table to disk (dict to json file)
def write_to_file(file_name, data):
    with open(file_name, 'w') as json_file:
        json.dump(data, json_file, default=str)


def record_contains_the_field_provided(selection, data_table, i):
    return selection.field_name in data_table[str(i)].keys()


def record_meets_the_conditions(criteria, data_table, record, key):
    counter = 0
    for selection in criteria:
        if record_contains_the_field_provided(selection, data_table, record):
            if OPERATOR[selection.operator](data_table[str(record)][selection.field_name], (selection.value)):
                counter += 1
        if selection.field_name == key:
            if OPERATOR[selection.operator](int(record), selection.value):
                counter += 1
    return counter == len(criteria)


def match_records(data_table, criteria, key):
    list = []
    for key_of_table in data_table.keys():
        if record_meets_the_conditions(criteria, data_table, key_of_table, key):
            list.append(key_of_table)
    return list

def add_new_table_name_to_file(file_path, table_name, key_field_name):
    keys = read_file(file_path)
    keys[table_name] = key_field_name
    write_to_file(file_path, keys)

# ====================== End Auxiliary function ====================================

class DBField(db_api.DBField):
    def __init__(self, name: str, type: Type):
        self.name = name
        self.type = type


class SelectionCriteria(db_api.SelectionCriteria):
    def __init__(self, field_name: str, operator: str, value: Any):
        self.field_name = field_name
        self.operator = operator
        self.value = value



class DBTable(db_api.DBTable):
    def __init__(self, name, fields: List[DBField], key_field_name: str):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name

    
    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        file_name = f"{DB_ROOT}/{self.name}.json"
        data_table = read_file(file_name)
        list_of_match_records = match_records(data_table, criteria, self.key_field_name)
        for i, k in enumerate(list_of_match_records):
            list_of_match_records[i] = data_table[k]
        return list_of_match_records

    
    def count(self) -> int:
        file_name = f"{DB_ROOT}/{self.name}.json"
        data_table = read_file(file_name)
        
        return len(data_table)

    
    def insert_record(self, values: Dict[str, Any]) -> None:
        if self.key_field_name in values.keys():
            file_name = f"{DB_ROOT}/{self.name}.json"
            data_table = read_file(file_name)
            value = values.pop(self.key_field_name)
            
            if str(value) in data_table.keys():
                raise ValueError("Already exists")
            else:
                data_table[str(value)] = values
                write_to_file(file_name, data_table)

    
    def delete_record(self, key: Any) -> None:
        file_name = f"{DB_ROOT}/{self.name}.json"
        data_table = read_file(file_name)
        
        if str(key) not in data_table.keys():
            raise ValueError("Bad Key")
        else:
            data_table.pop(str(key))
            write_to_file(file_name, data_table)

    
    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        file_name = f"{DB_ROOT}/{self.name}.json"
        data_table = read_file(file_name)
        list_of_match_records = match_records(data_table, criteria, self.key_field_name)
        
        for key in list_of_match_records:
            del data_table[key]
        write_to_file(file_name, data_table)

    
    def get_record(self, key: Any) -> Dict[str, Any]:
        file_name = f"{DB_ROOT}/{self.name}.json"
        data_table = read_file(file_name)
        
        return data_table[str(key)]

    
    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        file_name = f"{DB_ROOT}/{self.name}.json"
        data_table = read_file(file_name)
        data_table[str(key)].update(values)
        write_to_file(file_name, data_table)

    
    def create_index(self, field_to_index: str) -> None:
        index = hashedindex.HashedIndex()
        file_name = f"{DB_ROOT}/{self.name}.json"
        data_table = read_file(file_name)
        for i, j in data_table.items():
            if field_to_index in j:
                index.add_term_occurrence(j[field_to_index], i)


# ========================= class DataBase =================================

class DataBase(db_api.DataBase):
    
    def create_table(self, table_name: str, fields, key_field_name: str) -> DBTable:
        flag = False
        for field in fields:
            if key_field_name == field.name:
                flag = True
                break
        if not flag:
            raise ValueError("Bad Key")

        file_name = f"{DB_ROOT}/{table_name}.json"
        new_dict = {}
        file_path = f"{DB_ROOT}/{META_DATA}.json"
        
        if not os.path.exists(file_path):
            empty_dict = {}
            write_to_file(file_path, empty_dict)
        
        write_to_file(file_name, new_dict)
        add_new_table_name_to_file(file_path, table_name, key_field_name)
        new_table = DBTable(table_name, fields, key_field_name)
        
        return new_table

    def get_table(self, table_name: str) -> DBTable:
        file_path = f"{DB_ROOT}\\{META_DATA}.json"
        
        if not os.path.exists(file_path):
            raise KeyError("Table not exists")
        
        data_table = read_file(file_path)
        
        if table_name not in list(data_table.keys()):
            raise KeyError("Table not exists")
        
        key = data_table[table_name]
        db_table = DBTable(table_name, [], key)
        
        return db_table

    def delete_table(self, table_name: str) -> None:
        file_name = f"{DB_ROOT}/{table_name}.json"
        os.remove(file_name)
        file_path = f"{DB_ROOT}/{META_DATA}.json"
        dict_of_tables_name = read_file(file_path)
        dict_of_tables_name.pop(table_name)
        write_to_file(file_path, dict_of_tables_name)

    def get_tables_names(self) -> List[Any]:
        file_path = f"{DB_ROOT}/{META_DATA}.json"
        tables_names = []
        
        for root, dirs, files in os.walk(DB_ROOT):
            for file in files:
                tables_names.append(file[:-5])
        
        dict_of_tables_name = read_file(file_path)
        
        return list(dict_of_tables_name.keys())

    
    def num_tables(self) -> int:
        file_path = f"{DB_ROOT}\\{META_DATA}.json"
        
        if not os.path.exists(file_path):
            return 0
        
        dict_of_tables_name = read_file(file_path)
        
        return len(dict_of_tables_name)
