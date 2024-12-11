
-- select *
-- from `agile-tangent-427801-a6.dl_northwind.customer`





with source as (
    select * from {{source('northwind','date')}}
)

select * from source