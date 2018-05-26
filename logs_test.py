from aggregator_of_logs import *
import unittest
from utils import *

class TestUtils(unittest.TestCase):
    def test_days_in_work(self):
        one_day_in_work = days_in_work(
            [['2017-11-30', '12:00', '15:00', 'RT:473812']],
            0
        )
        self.assertEqual(one_day_in_work, 1)

        three_days_in_work = days_in_work(
            [['2017-11-02', '13:30', '17:30', 'RT:467888'],
             ['2017-11-04', '11:30', '12:30', 'RT:467888']],
            0
        )
        self.assertEqual(three_days_in_work, 3)

        five_days_in_work = days_in_work(
            [['2017-11-28', '12:00', '16:00', 'RT:476222'],
             ['2017-11-29', '10:30', '12:00', 'RT:476222'],
             ['2017-12-02', '10:30', '12:00', 'RT:476222']],
            0
        )
        self.assertEqual(five_days_in_work, 5)


    def test_sum_of_time_test(self):
        m = sum_of_time(
            [['2017-11-30', '12:00', '15:00', 'RT:473812']],
            [1, 2]
        )
        # 3 hours + 0 minutes
        self.assertEqual(m, 3 * 60 + 0)

        m = sum_of_time(
            [['2017-11-02', '13:30', '17:30', 'RT:467888'],
             ['2017-11-04', '11:30', '12:45', 'RT:467888']],
            [1, 2]
        )
        # 5 hours and 15 minutes
        self.assertEqual(m, 5*60 + 15)

        m = sum_of_time(
            [['2017-11-14', '16:00', '19:00', 'RT:446048'], # 2
             ['2017-11-15', '10:00', '12:00', 'RT:446048'], # 3
             ['2017-11-16', '11:00', '16:00', 'RT:446048']],# 5
            [1, 2]
        )
        self.assertEqual(m, 10*60 + 0)


class TestFixture:
    log = ["i", "dislike", "my", "code"]

    log_with_epmty_value = ["", "dislike", "my", ""]

    logs_1 = [
        ["i", "dislike", "my", "code", "", ""],
        ["i", "like", "my", "code",  "very", "mush"],
        ["teamlead", "likes", "my", "code", "", ""],
        ["mum", "likes", "my", "code", "so-so", "much"],
    ]

    in_values = ["i", "like", "tested", "code"]
    eq_value = "code"
    no_eq_value = "tested"

    cond_1 = Condition(0, be_in, in_values)
    cond_2 = Condition(1, be_in, in_values)
    cond_3 = Condition(3, be_equal, eq_value)
    cond_4 = Condition(3, be_equal, no_eq_value)

    set_1 = SetOfFields([("second_field", 1), ("third_field", 2)])
    set_2 = SetOfFields([("third_field", 2), ("second_field", 1)])

    schema = ["first_field", "second_field", "third_field", "fourth_field", "fifth_field", "six_field"]

    # cond_1, cond_2
    filter_1 = ReaderAndFilter(
        fields_for_schema=schema,
        simple_conditions=[
            (be_in, in_values, "first_field"),
            (be_in, in_values, "second_field"),
        ],
        selected_fields=["second_field", "third_field"]
    )

    # cond_1, cond_3
    filter_2 = ReaderAndFilter(
        fields_for_schema=schema,
        simple_conditions=[
            (be_in, in_values, "first_field"),
            (be_equal, eq_value, "fourth_field"),
        ],
        selected_fields=["second_field", "third_field"]
    )

    # cond_2, cond_4
    filter_3 = ReaderAndFilter(
        fields_for_schema=schema,
        simple_conditions=[
            (be_in, in_values, "second_field"),
            (be_equal, no_eq_value, "fourth_field")
        ],
        selected_fields=["second_field", "third_field"]
    )

    # cond_3, cond_4
    filter_4 = ReaderAndFilter(
        fields_for_schema=schema,
        simple_conditions=[
            (be_equal, no_eq_value, "fourth_field"),
            (be_equal, eq_value, "fourth_field"),
        ],
        selected_fields=["second_field", "third_field"]
    )

    cond_1_to_logs_1 = [["i", "like", "my", "code",  "very", "mush"]]

    cond_2_to_logs_1 = [
        ["i", "dislike", "my", "code", "", ""],
        ["i", "like", "my", "code",  "very", "mush"]
    ]

    set_to_logs_1 = [
        ["dislike", "my"],
        ["like", "my"],
        ["likes", "my"],
        ["likes", "my"]
    ]

    filter_1_to_logs_1 = [["like", "my"]]

    filter_2_to_logs_1 = [["dislike", "my"], ["like", "my"]]

    group_1 = Group([0, 1], logs_1)
    group_2 = Group([2, 3], logs_1)
    group_3 = Group([1], logs_1)

    group_1.apply_func(count_in_group)
    group_2.apply_func(count_in_group)
    group_3.apply_func(count_in_group)
    grouped_log_1 = group_1.func_value_by_group_key
    grouped_log_2 = group_2.func_value_by_group_key
    grouped_log_3 = group_3.func_value_by_group_key


class TestCondition(unittest.TestCase, TestFixture):

    def test_satisfy(self):
        self.assertEqual(
            self.cond_1.satisfy(self.log),
            True
        )

        self.assertEqual(
            self.cond_2.satisfy(self.log),
            False
        )

        self.assertEqual(
            self.cond_3.satisfy(self.log),
            True
        )

        self.assertEqual(
            self.cond_4.satisfy(self.log),
            False
        )


    def test_cond_init(self):
        self.assertEqual(self.cond_3.params, self.eq_value)
        self.assertEqual(self.cond_3.index, 3)
        self.assertEqual(self.cond_3.func("code", self.eq_value), True)
        self.assertEqual(self.cond_3.func("no_code", self.eq_value), False)
        self.assertEqual(self.cond_3.func("", self.eq_value), False)

    def test_satisfy_with_empty_value(self):
        self.assertEqual(
            self.cond_1.satisfy(self.log_with_epmty_value),
            False
        )
        self.assertEqual(
            self.cond_3.satisfy(self.log_with_epmty_value),
            False
        )


class TestSetOfFields(unittest.TestCase, TestFixture):
    def test_eq_set(self):
        self.assertEqual(self.set_1.indexes, self.set_2.indexes)
        self.assertEqual(self.set_1.names, self.set_2.names)

    def test_filter_by_set(self):
        self.assertEqual(
            self.set_1.filter_by_set(self.log),
            [ "dislike", "my"]
        )


class TestReaderAndFilter(unittest.TestCase, TestFixture):
    def test_satisfy_conditions(self):
        # 1 && 0
        self.assertEqual(
            self.filter_1.satisfy_conditions(self.log),
            False
        )
        # 1 && 1
        self.assertEqual(
            self.filter_2.satisfy_conditions(self.log),
            True
        )
        # 0 && 0
        self.assertEqual(
            self.filter_3.satisfy_conditions(self.log),
            False
        )
        # 0 && 1
        self.assertEqual(
            self.filter_4.satisfy_conditions(self.log),
            False
        )

    def test_satisfy_conditions_with_empty_value(self):
        self.assertEqual(
            self.filter_1.satisfy_conditions(self.log_with_epmty_value),
            False
        )
        self.assertEqual(
            self.filter_2.satisfy_conditions(self.log_with_epmty_value),
            False
        )
        self.assertEqual(
            self.filter_3.satisfy_conditions(self.log_with_epmty_value),
            False
        )
        self.assertEqual(
            self.filter_4.satisfy_conditions(self.log_with_epmty_value),
            False
        )

    def test_apply_conditions(self):
        self.assertEqual(
            self.filter_1.apply_conditions(self.logs_1),
            self.cond_1_to_logs_1
        )

        self.assertEqual(
            self.filter_2.apply_conditions(self.logs_1),
            self.cond_2_to_logs_1
        )

        self.assertEqual(
            self.filter_3.apply_conditions(self.logs_1),
            []
        )

    def test_apply_conditions_with_empty_value_log(self):
        self.assertEqual(
            self.filter_1.apply_conditions([self.log_with_epmty_value]),
            []
        )
        self.assertEqual(
            self.filter_2.apply_conditions([self.log_with_epmty_value]),
            []
        )
        self.assertEqual(
            self.filter_3.apply_conditions([self.log_with_epmty_value]),
            []
        )
        self.assertEqual(
            self.filter_4.apply_conditions([self.log_with_epmty_value]),
            []
        )

    def test_apply_set_filter(self):
        self.assertEqual(
            self.filter_1.apply_set_filter(self.logs_1),
            self.set_to_logs_1
        )

    def test_apply_all(self):
        self.assertEqual(
            self.filter_1.apply_all(self.logs_1),
            self.filter_1_to_logs_1
        )

        self.assertEqual(
            self.filter_2.apply_all(self.logs_1),
            self.filter_2_to_logs_1
        )

        self.assertEqual(
            self.filter_3.apply_all(self.logs_1),
            []
        )

class TestSchema(unittest.TestCase):
    def test_ssp_schema(self):
        ssp_schema = Schema(file_name="schemas/timesheet")
        self.assertEqual(ssp_schema.index_by_name["number_of_ticket"], 4)
        self.assertEqual(ssp_schema.name_by_index[4], "number_of_ticket")

        set_of_fields_3 = ssp_schema.get_set_of_fields_by_names(["number_of_ticket"])
        self.assertEqual(set_of_fields_3.indexes, [4])
        self.assertEqual(set_of_fields_3.names, ["number_of_ticket"])

        set_of_fields_4 = ssp_schema.get_set_of_fields_by_names(["number_of_ticket", "date"])
        self.assertEqual(set_of_fields_4.indexes, [0, 4])
        self.assertEqual(set_of_fields_4.names, ["date", "number_of_ticket"])


class TestGroup(unittest.TestCase, TestFixture):
    def test_grouped_logs(self):

        self.assertEqual(
            self.group_1.groups_as_dict[("i", "like")],
            [["i", "like", "my", "code",  "very", "mush"]]
        )

        self.assertEqual(
            self.group_2.groups_as_dict[("my", "code")],
            self.logs_1
        )

        self.assertEqual(
            self.group_3.groups_as_dict[("like",)],
            [self.logs_1[1]]
        )

        self.assertEqual(
            self.group_3.groups_as_dict[("dislike",)],
            [self.logs_1[0]]
        )

        self.assertEqual(
            self.group_3.groups_as_dict[("likes",)],
            self.logs_1[2:]
        )

        self.assertEqual(
            self.grouped_log_3[("like",)],
            1
        )
        self.assertEqual(
            self.grouped_log_3[("dislike",)],
            1
        )

        self.assertEqual(
            self.grouped_log_3[("likes",)],
            2
        )


        self.assertEqual(
            self.grouped_log_2[("my", "code")], 4
        )


        for key, value in self.grouped_log_1.items():
            self.assertEqual(value, 1)

    def test_func_values_are(self):
        group_1_are_1 = {
                ("i", "dislike"): 1,
                ("i", "like"): 1,
                ("teamlead", "likes"): 1,
                ("mum", "likes"): 1
        }

        group_1_are_2 = {
            ("i", "dislike"): 1,
            ("mum", "likes"): 1
        }

        group_1_are_3 = {
                ("i", "dislike"): 1,
                ("i", "like"): 1,
                ("teamlead", "likes"): 1,
                ("mum", "likes"): 1,
                ("sister", ""): 2,
        }

        group_1_are_4 = {
                ("i", "dislike"): 2,
                ("teamlead", "likes"): 1,
                ("mum", "likes") : -3,
                ("sister", ""): 2,
        }

        group_1_is_eq_1, group_1_diff_1, no_in_gr_1, no_in_gr_1_are_1, eq_group_1_1 = self.group_1.func_values_are(group_1_are_1)

        self.assertEqual(group_1_is_eq_1, True)
        self.assertEqual(len(group_1_diff_1), 0)
        self.assertEqual(len(no_in_gr_1), 0)
        self.assertEqual(len(no_in_gr_1_are_1), 0)
        self.assertDictEqual(eq_group_1_1, group_1_are_1)

        group_1_is_eq_2, group_1_diff_2, no_in_gr_1, no_in_gr_1_are_2, eq_group_1_2 = self.group_1.func_values_are(group_1_are_2)

        self.assertEqual(group_1_is_eq_2, False)
        self.assertEqual(len(group_1_diff_2),0 )
        self.assertEqual(len(no_in_gr_1), 0)
        self.assertDictEqual(
            no_in_gr_1_are_2,
            {
                ("i", "like") : 1,
                ("teamlead", "likes"): 1,
            }
        )
        self.assertDictEqual(eq_group_1_2, group_1_are_2)


        group_1_is_eq_3, group_1_diff_3, no_in_gr_1, no_in_gr_1_are_3, eq_group_1_3 = self.group_1.func_values_are(group_1_are_3)

        self.assertEqual(group_1_is_eq_3, False)
        self.assertEqual(len(group_1_diff_3),0)
        self.assertDictEqual(
            no_in_gr_1,
            {
                ("sister", ""): 2,
            }
        )
        self.assertEqual(len(no_in_gr_1_are_3), 0)
        self.assertDictEqual(eq_group_1_3, group_1_are_1)

        group_1_is_eq_4, group_1_diff_4, no_in_gr_1, no_in_gr_1_are_4, eq_group_1_4 = self.group_1.func_values_are(group_1_are_4)
        self.assertEqual(group_1_is_eq_4, False)
        self.assertDictEqual(
            group_1_diff_4,
            {
                ("i", "dislike") : -1,
                ("mum", "likes") : 4,
            }
        )
        self.assertDictEqual(
            no_in_gr_1,
            {
                ("sister", ""): 2,
            }
        )
        self.assertDictEqual(
            no_in_gr_1_are_4,
            {
                ("i", "like") : 1,
            }
        )
        self.assertDictEqual(eq_group_1_4, {("teamlead", "likes"): 1})

    # не проходит
    # не обходимо пересмотреть создание папки для записи результатов
    # def test_dump_to_file(self):
    #     group_1_are_4 = {
    #             ("i", "dislike") : 2,
    #             ("teamlead", "likes"): 1,
    #             ("mum", "likes") : -3,
    #             ("sister", ""): 2,
    #     }
    #     group_1_is_eq_4, group_1_diff_4, no_in_gr_1, no_in_gr_1_are_4, eq_group_1_4 = self.group_1.func_values_are(group_1_are_4)
    #     dump_dict_to_file(group_1_diff_4, 'test_group_1_diff_4.txt')
    #     # dump_dict_to_file(no_in_gr_1_are_4, 'test_no_in_gr_1_are_4.txt')
    #     # dump_dict_to_file(eq_group_1_4, 'test_eq_group_1_4.txt')
    #     self.assertDictEqual(eq_group_1_4, {("teamlead", "likes"): 1})


class TestAllLogicOnMyTimesheet(unittest.TestCase):
    reader_and_filter = ReaderAndFilter(
        schema_file="schemas/timesheet",
        simple_conditions=[
            (lambda x, y: x >= y, "2017-11", "date"),
            (lambda x, y: x <= y, "2017-12", "date"),
        ],
        selected_fields=["date", "start_time", "end_time", "number_of_ticket"],
    )

    def test_count_of_ticket_in_work_from_october(self):

        data = Data(
            logs_dir="logs/timesheet/nkruglyak",
            raf=self.reader_and_filter,
            names_of_fields_for_group=["number_of_ticket"]
        )

        grouped_data_1 = data.create_groups(count_in_group)
        n = len(grouped_data_1.groups_as_dict.keys())
        self.assertEqual(n, 32)


    def test_maximum_days_in_work(self):

        data = Data(
            logs_dir="logs/timesheet/nkruglyak",
            raf=self.reader_and_filter,
            names_of_fields_for_group=["number_of_ticket"]
        )

        # максимальное время потраченное на задачу в октябре

        start_time_ind = data.set_of_fields.get_index_by_name("start_time")
        end_time_ind = data.set_of_fields.get_index_by_name("end_time")
        # переписать как функцию от параметров
        grouped_data = data.create_groups(sum_of_time, params=[end_time_ind, start_time_ind])

        inverse = [(value, key) for key, value in grouped_data.func_value_by_group_key.items()]
        # inverse
        sum_days_in_work, number_of_ticket = max(inverse)
        self.assertEqual(number_of_ticket[0],'RT:454239')

        days, hours, minutes = \
            sum_days_in_work // (8 * 60), \
            (sum_days_in_work % (8 * 60)) // 60,\
            (sum_days_in_work % (8 * 60)) % 60
        self.assertEqual(days, 58)
        self.assertEqual(hours, 3)
        self.assertEqual(minutes, 40)


if __name__ == "__main__":
    unittest.main()