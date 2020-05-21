from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import logging



# with open("/home/trang/working/projects/pratice/data_engin/hadoop/streaming/mrjob/Baltimore_City_Employee_Salaries_FY2014.csv", "r", newline="") as _file:
#     col_name = csv.DictReader(_file).fieldnames
#     data = csv.reader(_file).__next__()
# print(data)
col_name = ['Name', 'JobTitle', 'AgencyID', 'Agency', 'HireDate', 'AnnualSalary', 'GrossPay']
i = 0


# get top ten salary and gross
class topSalary(MRJob):
    def mapper(self, _, line):
        global i
        if (i > 0):
            data = dict(zip(col_name, [cell.strip() for cell in csv.reader([line]).__next__()]))
            yield 'salary', (float(data['AnnualSalary']), line, i)
        i += 1

    def reducer(self, key, values):
        for val in values:
            yield key, val

if __name__ == '__main__':
    topSalary.run()


