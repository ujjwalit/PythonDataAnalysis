--query to fetch gross salary of all employees
select emp_id, emp_name, emp_salary from emp_salary where emp_salary > (select avg(emp_salary) from emp_salary);

--query to fetch all employees whose salary is greater than average salary
select emp_id, emp_name, emp_salary from emp_salary where emp_salary > (select avg(emp_salary) from emp_salary);

--query to 