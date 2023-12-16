WITH prod as
(
    select
        ct.category_name,
        sp.company_name as suppliers,
        pr.product_name,
        pr.unit_price,
        pr.product_id
        from {{source("sources", "products")}} pr
        left join {{source("sources", "suppliers")}} sp on pr.supplier_id = sp.supplier_id
        left join {{source("sources", "categories")}} ct on pr.category_id = ct.category_id
), ord_details as
(
    select 
        pr.*,
        od.order_id,
        od.quantity,
        od.discount
    from {{ref("order_details")}} od 
    LEFT JOIN prod pr on od.product_id = pr.product_id

), orders as
(
    select 
    ord.order_date,
    ord.order_id,
    c.company_name as customer,
    e.full_name as employee_name,
    e.age,
    e.length_of_service
    from {{source("sources", "orders")}} ord
    left join {{ref("customers")}} c on ord.customer_id = c.customer_id
    left join {{ref("employees")}} e on ord.employee_id = e.employee_id
    left join  {{source("sources", "shippers")}} s on ord.ship_via = s.shipper_id
), final_join as
(
    select 
    od.*,
    o.order_date,
    o.customer,
    o.employee_name,
    o.age,
    o.length_of_service
    from ord_details od
    inner join orders o on od.order_id = o.order_id
)

select * from final_join