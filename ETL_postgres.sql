with main_table as (
SELECT employe_id, salary, branch_id, checkin, checkout, hours_difference, date, TO_CHAR(date,'mm') as month, TO_CHAR(date,'yyyy') as year,
       CASE
           WHEN hours_difference < 0 THEN hours_difference +24
           ELSE hours_difference
       END AS adjusted_hours_difference
FROM (
    SELECT 
    e.employe_id,
    e.salary,
    e.branch_id,
    NULLIF(t.checkin, '') as checkin,
    NULLIF(t.checkout, '') as checkout,
    TO_TIMESTAMP(NULLIF(t.date, ''), 'YYYY-MM-DD') as date,
    EXTRACT(EPOCH FROM (TO_TIMESTAMP(NULLIF(t.checkout, ''), 'HH24:MI:SS') - TO_TIMESTAMP(NULLIF(t.checkin, ''), 'HH24:MI:SS'))) / 3600 AS hours_difference
    FROM employees e
    INNER JOIN timesheets t ON e.employe_id = t.employee_id
	) subquery
WHERE checkin IS NOT NULL AND checkout IS NOT null
),
salary_table as (SELECT
    "month",
    "year",
    branch_id,
    SUM(salary) AS total_salary
FROM (
    SELECT DISTINCT ON ("month", "year", branch_id, employe_id)
        "month",
        "year",
        branch_id,
        salary
    FROM main_table
) distinct_rows
GROUP BY "month", "year", branch_id),
total_hours_table as (
SELECT
    "month",
    "year",
    branch_id,
    SUM(adjusted_hours_difference) AS total_hours
FROM main_table
GROUP BY "month", "year", branch_id)
SELECT h.month, h.year, h.branch_id, (s.total_salary/h.total_hours) AS salary_per_hour FROM total_hours_table h
INNER JOIN salary_table s ON  h.month = s.month AND h.year = s.year AND h.branch_id = s.branch_id