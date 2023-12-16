with calc_employees as 
(
    select 
    (DATE_PART(year, current_date) - DATE_PART(year, birth_date)) age,
    (DATE_PART(year, current_date) - DATE_PART(year, hire_date)) length_of_service,
    (first_name || ' ' || last_name) full_name,
    *
    from {{source('sources', 'employees')}}
)
select * from calc_employees