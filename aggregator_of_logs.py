# !/usr/local/bin/python3
import csv
import os
from utils import *

be_in = lambda x, array: x in array
be_not_in = lambda x, array: not x in array
be_equal = lambda x,y: x == y
be_not_equal = lambda x,y: x != y
count_in_group = lambda x, params: len(x)
is_not_empty = lambda x, params: bool(x)
has_more_then_one = lambda x, params: len(x) > 1
ed = lambda x: x

class Condition:
    def __init__(self, index, func, params=[], description=""):
        self.index = index
        self.func = func
        self.params = params
        self.description = description

    def satisfy(self, log):
        r = self.func(log[self.index], self.params)
        return r


class SetOfFields:
    # fields ("field_name", field_index)
    def __init__(self, fields):
        field_sorted = sorted(fields, key=lambda x: x[1])
        self.indexes = [field[1] for field in field_sorted]
        self.names = [field[0] for field in field_sorted]

    # TODO: rename on select_from_set
    def filter_by_set(self, log):
        return [log[i] for i in self.indexes]

    # должен возварщать порядковые номера names из self.names
    def get_indexes_by_names(self, names):
        # we must sorted names only one time
        names = sorted(list(set(names)))
        return [self.names.index(name) for name in names]

    def get_index_by_name(self, name):
        return self.names.index(name)


class Group:
    def __init__(self, indexes, logs):
        self.indexes = indexes
        if indexes:
            # possible values of key to different group
            self.values = list(
                set(
                    [tuple([log[i] for i in indexes]) for log in logs]
                )
            )

            groups_as_dict = dict()

            for value in self.values:
                group_of_logs = [
                    log for log in logs 
                    if all([log[ind] == value_el for ind, value_el in zip(self.indexes, value)])
                ]
                groups_as_dict[value] = group_of_logs
            
            self.groups_as_dict = groups_as_dict
        
        else:
            self.indexes = []
            self.values = []
            self.groups_as_dict = dict()

    def apply_func(self, func, params=[]):
        self.func_value_by_group_key = {k: func(v, params) for (k, v) in self.groups_as_dict.items()}
        return self.func_value_by_group_key
        
    def reverse_func_value_by_keys(self):
        self.groups_key_by_func_value = {
            v: [k for k in self.func_value_by_group_key.keys() if self.func_value_by_group_key[k] == v]
            for v in self.func_value_by_group_key.values()
        }
        return self.groups_key_by_func_value

    def get_logs_by_func_value(self):
        if not self.groups_key_by_func_value:
            self.reverse_func_value_by_keys()
        self.logs_by_func_value = {
            func_value: [self.groups_as_dict[k] for k in  list_of_key] 
            for func_value,list_of_key in self.groups_key_by_func_value
        }
        return self.logs_by_func_value

    def get_logs_by_list_in_groups_with_keys(self, list_of_keys):
        collected_logs = []
        for k in list_of_keys:
            collected_logs.extend(self.groups_as_dict[k])
        return collected_logs
 
    def func_values_are(self, func_values, descr_a='a', descr_b='b', func_for_compare='', dump_all=False, prefix=''):
        if not func_for_compare:
            func_for_compare = lambda a, b=0: a - b

        if not self.func_value_by_group_key:
            print("ERROR: no apply func to groups")

        no_keys_in_b, diff_a_b_values, eq_a_b_values = compare_dicts(self.func_value_by_group_key, func_values, func_for_compare)
        no_keys_in_a, diff_b_a_values, eq_b_a_values = compare_dicts(func_values, self.func_value_by_group_key, func_for_compare)

        if len(diff_a_b_values) != len(diff_b_a_values):
            print("something is broken")
            res = False

        elif not (no_keys_in_b or no_keys_in_a or diff_a_b_values):
            res = True

        else:
            res = False

        print_description(res, diff_a_b_values, no_keys_in_a, no_keys_in_b, eq_a_b_values, descr_a, descr_b, dump_all=dump_all, prefix=prefix)

        return res, diff_a_b_values, no_keys_in_a, no_keys_in_b, eq_a_b_values


class LogReaderAndFilter:

    # conditions []Condition
    # set_of_fields SetOfFields
    def __init__(self, conditions, set_of_fields):
        self.conditions = conditions
        self.set_of_fields = set_of_fields

    def satisfy_conditions(self, log):
        return all([condition.satisfy(log) for condition in self.conditions])

    def apply_conditions(self, logs):
        return [log for log in logs if self.satisfy_conditions(log)]

    def apply_set_filter(self, logs):
        return [self.set_of_fields.filter_by_set(log) for log in logs]

    def apply_all(self, logs):
        if self.conditions:
            logs = self.apply_conditions(logs)
        if self.set_of_fields:
            logs = self.apply_set_filter(logs)
        return logs

    def read_and_filter(self, file_name):
        with open(file_name, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            filtred_logs = self.apply_all(reader)
        return filtred_logs

    def apply_filter_to_dir(self, dir_path):
        print("apply_filter_to_dir", dir_path)
        files_logs = os.listdir(dir_path)
        interested_logs = []
        for file_name in files_logs:
            file_path = dir_path + "/" + file_name
            fresh_logs = self.read_and_filter(file_path)
            # print(file_name, len(fresh_logs))
            interested_logs.extend(fresh_logs)
            # FIXME we have very much logs and ssp_data_many_adslot works very slowly for all
            # break
        return interested_logs


class Schema:
    def __init__(self, file_name='', fields_for_schema=[]):
        if file_name:
            with open(file_name, newline='') as f:
                lines = f.readlines()
                self.fields = [(line.split(':')[0], j) for j, line in enumerate(lines)]
        elif fields_for_schema:
            self.fields = [(f, j) for j, f in enumerate(fields_for_schema)]
        else:
            print("ERROR: for definition Schema need file_name or fields_for_schema")

        self.name_by_index = {field[1]: field[0] for field in self.fields}
        self.index_by_name = {field[0]: field[1] for field in self.fields}

    def get_field_by_name(self, name):
        return (name, self.index_by_name[name])

    def get_set_of_fields_by_names(self, names):
        return SetOfFields([self.get_field_by_name(name) for name in names])

    def get_all_names(self):
        return [f[0] for f in self.fields]


class Data:
    common_logs_dir = "logs/"
    common_schemas_dir = "schemas/"
    # simple_conditions [(func, params, name_field)]
    # names_of_fields_for_set [field_name, ..]
    # names_of_fields_for_group [field_name, ..]
    def __init__(self, logs_dir='',logs=[], schema_file='',fields_for_schema=[], simple_conditions=[], names_of_fields_for_set=[],  names_of_fields_for_group=[]):
        if logs_dir:
            self.logs_dir = self.common_logs_dir + logs_dir
        
        if schema_file:
            self.schema_file = self.common_schemas_dir + schema_file
            self.schema = Schema(file_name=self.schema_file)
        elif fields_for_schema:
            self.schema = Schema(fields_for_schema=fields_for_schema)
        else:
            print("ERROR: for define Data need data to define schema")

        if simple_conditions:
            self.conditions = [
                Condition(
                    self.schema.index_by_name[simple_condition[2]], 
                    simple_condition[0],
                    simple_condition[1],
                    simple_condition[2]
                ) for simple_condition in simple_conditions]
        else:
            self.conditions = []

        if names_of_fields_for_set:
            self.set_of_fields = self.schema.get_set_of_fields_by_names(names_of_fields_for_set)
        else:
            self.set_of_fields = self.schema.get_set_of_fields_by_names(self.schema.get_all_names())

        # после выжимки схема меняется!
        if names_of_fields_for_group:
            self.ind_of_fields_for_group = self.set_of_fields.get_indexes_by_names(names_of_fields_for_group)
        else:
            self.ind_of_fields_for_group = []

    def create_filter(self):
        self.filter = LogReaderAndFilter(self.conditions, self.set_of_fields)

    def apply_filter_to_dir(self):
        self.filtred_logs = self.filter.apply_filter_to_dir(self.logs_dir)

    def apply_filter_to_logs(self, logs):
        self.filtred_logs = self.filter.apply_all(logs)
    
    def create_groups(self, func, params=[]):
        if not self.filtred_logs:
            print("ERROR: there is no filtred_logs")

        self.group = Group(self.ind_of_fields_for_group, self.filtred_logs)
        self.group.apply_func(func, params=params)
        return self.group

    def recreate_groups(self, func, names_of_fields_for_group=[], params=[]):
        if names_of_fields_for_group:
            self.ind_of_fields_for_group = self.set_of_fields.get_indexes_by_names(names_of_fields_for_group)
        else:
            self.ind_of_fields_for_group = []
        return self.create_groups(func, params)

    def filtred_logs_by_func_value_on_group(self, func_for_interested_value, func_for_bad_value, condition_on_logs_in_group):
        if not (self.group and self.group.func_value_by_group_key):
            print("ERROR! you do not create group yet!")
            return
        bad_keys = [ k for k,v in self.group.func_value_by_group_key.items() if func_for_bad_value(v)]
        if bad_keys:
            print("There are func_for_bad_value!")

        interested_keys = [ k for k,v in self.group.func_value_by_group_key.items() if func_for_interested_value(v)]
        good_logs = []
        bad_logs = []

        for k in interested_keys:
            logs_in_group = self.group.groups_as_dict[k]
            func_results_by_group = all([f(logs_in_group) for f in condition_on_logs_in_group])
            if func_results_by_group:
                good_logs.extend(logs_in_group)
            else:
                bad_logs.extend(logs_in_group)
        return good_logs, bad_logs


def main():
    print("HI! It is aggregator to analysis your logs")
    print("Define functions in ")

if __name__ == "__main__":
    main()