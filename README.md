## Introduction
This repository contains my solution to the data engineering test case provided by Aanisah from Mekari.

## Problem

Suppose I have two tables in two csv file below

[employee.csv](https://drive.google.com/file/d/17MI0YTTQORDUJ6t-CobNfE_1RXwDy0Th/view?usp=sharing)

| employe_id | branch_id | salary    | join_date  | resign_date |
|------------|-----------|-----------|------------|-------------|
| 1          | 3         | 7500000   | 2018-08-23 |             |
| 7          | 1         | 7500000   | 2017-04-28 |             |
| 8          | 1         | 13000000  | 2017-04-28 |             |
| 9          | 1         | 13500000  | 2017-12-22 | 2020-10-14  |
| ......     | .......   | .......   | .......    | .......     

[timesheet.csv](https://drive.google.com/file/d/18NJQWs-K4sYzRYFGhAgp2ACs84FLrq25/view?usp=sharing)

| timesheet_id | employee_id | date       | checkin   | checkout   |
|--------------|-------------|------------|-----------|------------|
| 23907432     | 66          | 2019-08-21 | 08:13:31  | 17:05:02   |
| 23907433     | 22          | 2019-08-21 | 08:56:34  | 18:00:48   |
| 23907434     | 21          | 2019-08-21 | 09:45:08  | 18:24:06   |
| 23907435     | 63          | 2019-08-21 | 09:55:47  |            |
| 23907437     | 60          | 2019-08-21 | 09:56:05  | 17:31:08   |
| 23907443     | 66          | 2019-08-22 | 08:28:27  | 17:20:25   |
| ......     | .......   | .......   | .......    | .......     

I need to calculate monthly base salary per hour for each branch.

The output of this ETL process should be loaded to a table that will be read by a BI tool with a straightforward SQL query:
```postgres
SELECT year, month, branch_id, salary_per_hour FROM …
```

## Assumption

While working with the data, I encountered a few challenges. Checkin and checkout column contains null values, it is crucial since the total working hours for each employee and branch is depends on it. This missing values would gives an inaccuracy for the salary per hour calculation. In this case I make an assumption to remove the null values for these two columns.

Total working hours is found by calculate the time interval between checkin and checkout time. Because there's no detail about date in checkin and checkout time, I make following assumption while calculating the total working hours:
- If checkin time is earlier than checkout time, then the interval can be calculated by subtracting checkout with checkin.

```
total_working_hours = checkout - checkin
```

For example, if checkin time is 09.00 and checkout time is 17.00, then the total working hours will be 8 hours. 

- If checkin time is later than checkout time, I will assume that the **checkout time is on the next day of checkin time**. The total working hours then can be calculated by subtracting checkout with checkin and then add a day to this time interval.

```
total_working_hours = checkout - checkin + a day
```

For example, if checkin time is 15.00 and checkout time is 04.00, then the total working hours is 04.00 - 15.00 + a day, which is 13 hours.

To find the salary per hour, I need to find total working hours and total salary per month of each branch. Salary per hour then can be calculated by dividing total salary by total working hours.

## Approach

To solve the problem, I use the following approach:
1. Inner join the employee table with the timesheet table using employee_id as a key.
2. Remove null values from checkin and checkout column.
3. Calculate the working hours of each employee using the time interval between checkin and checkout. In this process, I need to change the checkin and checkout data type into timestamp. There are some following assumption I did here
	- If checkin time is smaller than the checkout, then I just find the working hours immediately by find the interval between checkin and checkout time by subtracting it.
	- If checkout time is smaller that the checkin, then I can't just find the interval by directly subtracting it. I need to make the adjustment by adding one day (integer with value 24) to the calculation.
4. Find the monthly salary for each branch. In this process, I need to remove duplicate on salary, branch_id, employee_id, and date columns. After removing the duplicates, I do aggregation by summing the salary and groupby it based on the branch_id and date column. I will call the result of this process as salary_table.
5. Find the monthly total working hours for each branch. In this process I just simply summing all the working hours by groupby it based on the branch_id and date column. This resulting a total_hours_table
6. Inner join the salary_table and the total_hours_table by using branch_id and date as the key.
7. Calculate the monthly base salary per hour, which is salary divided by total working hours.
8. Extract year and month from date column, and drop unnecessary column.
## Conclusion

Above Approach is done by using Python and PostgreSQL, which the full script can be found in ETL_python.py and ETL_postgres.sql provided in this repository.

I'm grateful for the opportunity provided by Aanisah from Mekari, and I look forward to further discussions as part of the next steps in the recruitment process.
