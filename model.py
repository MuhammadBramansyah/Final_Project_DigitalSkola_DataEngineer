

def get_distribution_centers():
    sql = """
            select * from public.dim_distribution_centers
    """
    return sql

def get_orders():
    sql = """
            select * from public.dim_orders
    """
    return sql 

def get_products():
    sql = """
            select * from public.dim_products
    """
    return sql 

def get_employees():
    sql = """
            select * from public.fact_employees
    """
    return sql

def get_events():
    sql = """
            select * from public.fact_events
    """
    return sql 

def get_inventory_items():
    sql = """
            select * from public.fact_inventory_items
    """
    return sql

def get_order_items():
    sql = """
            select * from public.fact_order_items
    """
    return sql

def get_users():
    sql = """
            select * from public.fact_users
    """
    return sql 

def list_tables():
    tables = [("distribution_centers", get_distribution_centers()),
              ("orders", get_orders()),
              ("products", get_products()),
              ("employees", get_employees()),
              ("events", get_events()),
              ("inventory_items", get_inventory_items()),
              ("order_items", get_order_items()),
              ("users", get_users())]
    return tables

def datamart_table():
    sql = """
            select A.name,
	               A.brand,
	               A.department,
	               B.gender,
	               C.state,
	               C.city,
	               C.country,
	               B.num_of_item as jumlah_item,
	               E.created_at
            from public.dim_products as A
            inner join public.fact_orders_item as D
            on A.product_id = D.product_id 
            inner join public.dim_orders as B 
            on D.order_id = B.order_id 
            inner join public.dim_users as C 
            on D.user_id = C.user_id 
            inner join public.dim_inventory_items as E 
            on D.inventory_item_id = E.inventory_item_id 
    """
    return sql