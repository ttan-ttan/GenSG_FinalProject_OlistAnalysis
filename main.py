import sys
from pyspark.sql import SparkSession

# Import cleaning + validation modules
from src.cleaning_customers import clean_customers
from src.validation_customers import validate_customers

# Future teammates will add:
# from src.cleaning_orders import clean_orders
# from src.validation_orders import validate_orders
# etc...

spark = SparkSession.builder.getOrCreate()


def run_customers():
    try:
        df = spark.read.csv("data/raw/olist_customers_dataset.csv", header=True)
        cleaned = clean_customers(df)
        validate_customers(cleaned)
        print("[OK] Customers dataset passed cleaning + validation")
    except Exception as e:
        print("[FAIL] Customers dataset error:", e)


def run_geolocation():
    print("[TODO] Geolocation cleaning not implemented yet")


def run_order_items():
    print("[TODO] Order items cleaning not implemented yet")


def run_order_payments():
    print("[TODO] Order payments cleaning not implemented yet")


def run_order_reviews():
    print("[TODO] Order reviews cleaning not implemented yet")


def run_orders():
    print("[TODO] Orders cleaning not implemented yet")


def run_products():
    print("[TODO] Products cleaning not implemented yet")


def run_sellers():
    print("[TODO] Sellers cleaning not implemented yet")


def run_category_translation():
    print("[TODO] Category translation cleaning not implemented yet")


def run_all():
    print("\n=== Running ALL datasets ===")
    run_customers()
    run_geolocation()
    run_order_items()
    run_order_payments()
    run_order_reviews()
    run_orders()
    run_products()
    run_sellers()
    run_category_translation()
    print("\n=== ALL tasks completed ===")


MENU = {
    "1": ("Customers", run_customers),
    "2": ("Geolocation", run_geolocation),
    "3": ("Order Items", run_order_items),
    "4": ("Order Payments", run_order_payments),
    "5": ("Order Reviews", run_order_reviews),
    "6": ("Orders", run_orders),
    "7": ("Products", run_products),
    "8": ("Sellers", run_sellers),
    "9": ("Category Translation", run_category_translation),
    "10": ("Run ALL", run_all),
}


def main():
    print("\n=== Local Olist Pipeline Runner ===")
    for key, (desc, _) in MENU.items():
        print(f"{key}. {desc}")

    choice = input("\nSelect an option: ").strip()

    if choice in MENU:
        print(f"\nRunning: {MENU[choice][0]}\n")
        MENU[choice][1]()
    else:
        print("Invalid option.")


if __name__ == "__main__":
    main()
