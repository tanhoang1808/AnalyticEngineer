with source as (
    select
    fi.inventory_id,
    fi.transaction_type,
    fi.transaction_created_date,
    fi.transaction_modified_date,
    dp.product_id,
    fi.quantity,
    fi.purchase_order_id,
    fi.customer_order_id,
    fi.comments,
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
    dp.attachments
    from {{ref('fact_inventory')}} fi
    left join {{ref('dim_products')}} dp
    on fi.product_id = dp.product_id
)

select * from source