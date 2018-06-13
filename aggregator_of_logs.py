# !/usr/local/bin/python3
import csv
import os
from utils import *

be_in = lambda x, array: x in array
be_not_in = lambda x, array: not x in array
be_equal = lambda x, y: x == y
be_not_equal = lambda x, y: x != y
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


class GroupedData:
    def __init__(self, data, fields_for_group, func='', params=[]):

        indexes = data.set_of_fields.get_indexes_by_names(fields_for_group)
        logs = data.filtred_logs

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
                if all([log[ind] == value_el for ind, value_el in zip(indexes, value)])
            ]
            groups_as_dict[value] = group_of_logs

        self.groups = groups_as_dict

        if func:
            self.apply_func(func, params)

    def apply_func(self, func, params=[]):
        self.func_value_by_group_key = {
            k: func(v, params)
            for (k, v) in self.groups.items()
        }
        
    def reverse_func_value_by_keys(self):
        self.groups_key_by_func_value = {
            v: [k for k in self.func_value_by_group_key.keys() if self.func_value_by_group_key[k] == v]
            for v in self.func_value_by_group_key.values()
        }

    def get_logs_by_func_value(self):
        if not self.groups_key_by_func_value:
            self.reverse_func_value_by_keys()

        self.logs_by_func_value = {
            func_value: [self.groups[k] for k in list_of_key]
            for func_value, list_of_key in self.groups_key_by_func_value
        }
        return self.logs_by_func_value

    def get_logs_by_list_in_groups_with_keys(self, list_of_keys):
        collected_logs = []
        for k in list_of_keys:
            collected_logs.extend(self.groups[k])
        return collected_logs

    # ToDO: test it
    def filtred_logs_by_func_value_on_group(self,
                                            func_for_interested_value, func_for_bad_value,
                                            condition_on_logs_in_group):

        bad_keys = [k for k, v in self.func_value_by_group_key.items() if func_for_bad_value(v)]

        # remove it ? если предпологается такое использование,
        # то bad_keys должен быть возврщаемым значением ?
        if bad_keys:
            print("There are func_for_bad_value!")

        interested_keys = [k for k, v in self.func_value_by_group_key.items()
                           if func_for_interested_value(v)]
        good_logs = []
        bad_logs = []

        for k in interested_keys:
            logs_in_group = self.groups[k]
            func_results_by_group = all([f(logs_in_group) for f in condition_on_logs_in_group])
            if func_results_by_group:
                good_logs.extend(logs_in_group)
            else:
                bad_logs.extend(logs_in_group)
        return good_logs, bad_logs


class ReaderAndFilter:

    def __init__(self,
                 schema_file='', fields_for_schema=[],
                 simple_conditions=[], selected_fields=[]):

        if schema_file:
            self.schema = Schema(file_name=schema_file)
        elif fields_for_schema:
            self.schema = Schema(fields_for_schema=fields_for_schema)
        else:
            logging.error("for define Data need data to define schema")

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

        if selected_fields:
            self.set_of_fields = self.schema.get_set_of_fields_by_names(selected_fields)
        else:
            self.set_of_fields = self.schema.get_set_of_fields_by_names(self.schema.get_all_names())

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

    def apply_filter_to_file(self, file):
        fresh_logs = self.read_and_filter(file)
        logging.info("apply filter to file {} {}".format(file, len(fresh_logs)))
        return fresh_logs

    def apply_filter_to_dir(self, dir_path):
        files_logs = os.listdir(dir_path)
        interested_logs = []
        for file_name in files_logs:
            fresh_logs = self.apply_filter_to_file(dir_path + "/" + file_name)
            interested_logs.extend(fresh_logs)
        return interested_logs

    def apply_filter_to_files(self, files):
        interested_logs = []
        for file_name in files:
            fresh_logs = self.apply_filter_to_file(file_name)
            interested_logs.extend(fresh_logs)
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
            logging.info("for definition Schema need file_name or fields_for_schema")

        self.name_by_index = {field[1]: field[0] for field in self.fields}
        self.index_by_name = {field[0]: field[1] for field in self.fields}

    def get_field_by_name(self, name):
        return (name, self.index_by_name[name])

    def get_set_of_fields_by_names(self, names):
        return SetOfFields([self.get_field_by_name(name) for name in names])

    def get_all_names(self):
        return [f[0] for f in self.fields]


class Data:
    def __init__(self, raf,
                 logs_dir='', logs_file='', logs_files=[], logs=[]):

        self.set_of_fields = raf.set_of_fields

        # apply filter
        if logs_file:
            self.filtred_logs = raf.apply_filter_to_file(logs_file)
        elif logs_files:
            self.filtred_logs = raf.apply_filter_to_files(logs_files)
        elif logs_dir:
            self.filtred_logs = raf.apply_filter_to_dir(logs_dir)
        elif logs:
            self.filtred_logs = raf.apply_all(logs)
        else:
            logging.error("Provide logs to create Data: as dir, as file, as files or as list")


# @params: func_values_a, func_values_b - dicts - key: func_value
# @params: func_for_compare define on value of  func_values_a and func_values_b
# @return: res -- boolean, true if func_values_a == func_values_b
# @return: diff_a_b_values -- dict - contains difference of values by
# common keys in func_values_a and func_values_b
# @return: no_keys_in_a -- dict -- contains key and value
# if func_values_b contains key but func_values_a does not contain
# @return:  no_keys_in_b -- dict -- 
# eq_a_b_values
def func_values_compare(func_values_a, func_values_b,
                        descr_a='a', descr_b='b',
                        func_for_compare='', dump_all=False, prefix=''):
    if not func_for_compare:
        func_for_compare = lambda a, b=0: a - b

    if not func_values_a or not func_values_b:
        logging.error("Can not compare with empty func_values dict")
        return

    no_keys_in_b, diff_a_b_values, eq_a_b_values = compare_dicts(func_values_a,
                                                                 func_values_b,
                                                                 func_for_compare)

    no_keys_in_a, diff_b_a_values, eq_b_a_values = compare_dicts(func_values_b,
                                                                 func_values_a,
                                                                 func_for_compare)

    if len(diff_a_b_values) != len(diff_b_a_values):
        logging.debug("something is broken")
        res = False

    elif not (no_keys_in_b or no_keys_in_a or diff_a_b_values):
        res = True

    else:
        res = False

    print_description(res, diff_a_b_values,
                      no_keys_in_a, no_keys_in_b, eq_a_b_values,
                      descr_a, descr_b,
                      dump_all=dump_all, prefix=prefix)

    return res, diff_a_b_values, no_keys_in_a, no_keys_in_b, eq_a_b_values


def main():
    logging.info("HI! It is aggregator to analysis your logs")
    logging.info("Define functions in ")

if __name__ == "__main__":
    main()