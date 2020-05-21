import csv

with open("streaming/mrjob/Baltimore_City_Employee_Salaries_FY2014 copy.csv", "r", newline="") as _file:
    col_name = csv.DictReader(_file).fieldnames
    data = csv.reader(_file).__next__()
print(data)