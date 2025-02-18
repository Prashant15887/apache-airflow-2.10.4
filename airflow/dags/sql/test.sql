select * from test_data
where 1=1
        and run_id = '{{ run_id }}'
        and something_else = '{{ params.foobar }}'