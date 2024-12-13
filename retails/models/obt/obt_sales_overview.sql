with source as (
    select
    dc.customer_id,
    de.employee_id,
    dc.company,
    dc.last_name,
    dc.first_name,
    dc.email_address,
    dc.job_title,
    dc.business_phone,
    dc.home_phone,
    dc.mobile_phone,
    dc.fax_number,
    dc.address,
    dc.city,
    dc.state_province,
    dc.zip_postal_code,
    dc.country_region,
    dc.web_page,
    dc.notes,
    -- dc.attachments,
    dp.product_id,
    dp.product_code,
    dp.product_name,
    dp.description,
    dp.supplier_company,
    dp.standard_cost,
    dp.list_price,
    dp.reorder_level,
    dp.target_level,
    dp.quantity_per_unit,
    dp.discontinued,
    dp.minimum_reorder_quantity,
    dp.category,
    -- dp.attachments,
    fs.order_id,
    fs.shipper_id,
    fs.quantity,
    fs.unit_price,
    fs.discount,
    fs.status_id,
    fs.date_allocated,
    fs.purchase_order_id,
    fs.inventory_id,
    fs.order_date,
    fs.shipped_date,
    fs.paid_date
    from {{ref('fact_sales')}} fs
    left join {{ref('dim_customer')}} dc
    on fs.customer_id = dc.customer_id
    left join {{ref('dim_products')}} dp
    on fs.product_id = dp.product_id
    left join {{ref('dim_employee')}} de
    on fs.employee_id = de.employee_id
    

)

select * from source