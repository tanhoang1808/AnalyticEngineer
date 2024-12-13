with source as (
    select 
    fpo.purchase_order_id,
    fpo.quantity,
    fpo.unit_cost,
    fpo.date_received,
    fpo.posted_to_inventory,
    fpo.inventory_id,
    fpo.supplier_id,
    fpo.created_by,
    fpo.submitted_date,
    fpo.creation_date,
    fpo.status_id,
   fpo.expected_date,
   fpo.shipping_fee,
    fpo.taxes,
    fpo.payment_date,
    fpo.payment_amount,
    fpo.payment_method,
    fpo.notes,
    fpo.approved_by,
    fpo.approved_date,
    fpo.submitted_by,
    dc.customer_id,
    dc.company as customer_company,
    dc.last_name as customer_last_name,
    dc.first_name as customer_first_name,
    dc.email_address as customer_email_address,
    dc.job_title as customer_job_title,
    dc.business_phone as customer_business_phone,
    dc.home_phone as customer_home_phone,
    dc.mobile_phone as customer_mobile_phone,
    dc.address as customer_address,
    dc.city as customer_city,
    dc.state_province as customer_state_province,
    dc.zip_postal_code as customer_zip_postal_code,
    dc.country_region as customer_country_region,
    dc.web_page as customer_web_page,
    de.employee_id as employee_employee_id,
    de.company as employee_company,
    de.last_name as employee_last_name,
    de.first_name as employee_first_name,
    de.email_address as employee_email_address,
    de.job_title as employee_job_title,
    de.business_phone as employee_business_phone,
    de.home_phone as employee_home_phone,
    de.mobile_phone as employee_mobile_phone,
    de.city as employee_city,
    de.state_province as employee_state_province,
    de.zip_postal_code as employee_zip_postal_code,
    de.country_region as employee_country_region,
    de.web_page as employee_web_page,
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
    dp.category
    from {{ref('fact_purchase_orders')}} fpo
    left join {{ref('dim_customer')}} dc
    on fpo.customer_id = dc.customer_id
    left join {{ref('dim_employee')}} de
    on fpo.employee_id = de.employee_id
    left join {{ref('dim_products')}} dp
    on fpo.product_id = dp.product_id
)


select * from source