%default INPUT '/home/trang/working/projects/practice/data_engin/hadoop/pig/input/employee_salary.csv';
%default OUTPUT '/home/trang/working/projects/practice/data_engin/hadoop/pig/output';

employees = LOAD '$INPUT' USING PigStorage(',') AS (firstname:chararray, name:chararray, title:chararray, agencyid:chararray, agency:chararray, hiredate:datetime, annualsalary:float, grosspay:float);

data_filter = FOREACH employees GENERATE name, title, agencyid, agency, annualsalary, grosspay;
data_f = FILTER data_filter BY annualsalary >= 20000;

dump data_f;