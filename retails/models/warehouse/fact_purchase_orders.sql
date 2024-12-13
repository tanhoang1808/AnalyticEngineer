
with source as (
    select
    c.id as customer_id,
    e.id as employee_id,
    pod.purchase_order_id,
    p.id as product_id ,
    pod.quantity,
    pod.unit_cost,
    pod.date_received,
    pod.posted_to_inventory,
    pod.inventory_id,
    po.supplier_id,
    po.created_by,
    po.submitted_date,
    date(po.creation_date) as creation_date,
    po.status_id,
    po.expected_date,
    po.shipping_fee,
    po.taxes,
    po.payment_date,
    po.payment_amount,
    po.payment_method,
    po.notes,
    po.approved_by,
    po.approved_date,
    po.submitted_by,
    current_timestamp() as ingestion_timestamp
from {{ref('stg_purchase_orders')}} po
left join {{ref('stg_purchase_order_details')}} pod
on pod.purchase_order_id = po.id
left join {{ref('stg_products')}} p 
on pod.product_id = p.id
left join {{ref('stg_order_details')}} od
on p.id = od.product_id
left join {{ref('stg_orders')}} o 
on od.order_id = o.id
left join {{ref('stg_customer')}} c
on o.customer_id = c.id
left join {{ref('stg_employee')}} e 
on po.created_by = e.id
left join {{ref('stg_suppliers')}} s 
on s.id = po.supplier_id
),
unique_source as (
    select *,
    row_number() over ( 
        partition by 
        customer_id,
        employee_id,
        product_id,
        supplier_id,
        purchase_order_id,
        inventory_id,
        creation_date

    ) as row_number
    from source
        where
        customer_id is not null
        AND 
        employee_id is not null
        AND 
        product_id is not null
        AND 
        inventory_id is not null
        AND 
        creation_date is not null
    )

select *
except (row_number)
from unique_source
where (row_number) = 1