from datetime import datetime


# param: dict, dict, fundtion
# return:dict, dict, dict
def compare_dicts(a, b, func_for_compare):
    no_keys_in_b = dict()
    diff_a_b_values = dict()
    eq_a_b_values = dict()

    for key, value in a.items():
        value_in_b = b.get(key, False)

        if not value_in_b:
            no_keys_in_b[key] = value
        elif value_in_b != value:
            diff_a_b_values[key] = func_for_compare(value, value_in_b)
        else:
            eq_a_b_values[key] = value

    return no_keys_in_b, diff_a_b_values, eq_a_b_values


def print_description(res, diff_a_b_values, no_keys_in_a, no_keys_in_b, eq_a_b_values, descr_a, descr_b, dump_all=False,
                      prefix=''):
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    print("{}_eq_{}".format(descr_a, descr_b), res)
    print("{}_diff_{}".format(descr_a, descr_b), sum_values(diff_a_b_values))
    print("no_in_{}".format(descr_a), sum_values(no_keys_in_a))
    print("no_in_{}".format(descr_b), sum_values(no_keys_in_b))
    print("eq_{}_{}".format(descr_a, descr_b), sum_values(eq_a_b_values))

    print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    if dump_all:
        dump_dict_to_file(diff_a_b_values, prefix + "{}_diff_{}.txt".format(descr_a, descr_b))
        dump_dict_to_file(no_keys_in_a, prefix + "in_{}_and_no_in_{}.txt".format(descr_b, descr_a))
        dump_dict_to_file(no_keys_in_b, prefix + "in_{}_and_no_in_{}.txt".format(descr_a, descr_b))
        dump_dict_to_file(eq_a_b_values, prefix + "eq_{}_{}.txt".format(descr_a, descr_b))


def dump_dict_to_file(d, file_name, sort_by_value=False):
    common = "result_admatic_70/"
    if sort_by_value:
        list_for_dump = sorted(d.items(), key=lambda x: x[1])
    else:
        list_for_dump = list(d.items())
    with open(common + file_name, 'w') as f:
        for key, value in list_for_dump:
            # print("key in compare_ordered_dicts (expect see tuple)",  key)
            # print(key)
            f.write('by keys: {} see value {} \n'.format(
                '....'.join(key), value
                )
            )


# "2017-11-08"
# "2018-01-13"
#
def days_in_work(logs, date_ind):

    date_from_logs = sorted((log[date_ind] for log in logs), key=lambda x: x[date_ind])
    a, b = date_from_logs[0], date_from_logs[-1]
    a_y, a_m, a_d = a.split("-")
    b_y, b_m, b_d = b.split("-")
    a_data = datetime(int(a_y), int(a_m), int(a_d))
    b_data = datetime(int(b_y), int(b_m), int(b_d))
    delta = b_data - a_data
    return delta.days + 1


def sum_of_time(logs, time_inds):
    start_time_ind, end_time_ind = time_inds
    time_deltas = [
        datetime.strptime(log[end_time_ind], '%H:%M') - datetime.strptime(log[start_time_ind], '%H:%M')
        for log in logs
    ]

    time_durations = [delta.seconds for delta in time_deltas]
    return sum(time_durations) // 60


