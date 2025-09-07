import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Ustawienie seed dla powtarzalności wyników
np.random.seed(42)
random.seed(42)

# Parametry generacji danych
NUM_RECORDS = 10000
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 12, 31)

# Definicje kategorii produktów z różnymi przedziałami cenowymi
PRODUCT_CATEGORIES = {
    'Electronics': {'min_price': 50, 'max_price': 2000, 'min_qty': 1, 'max_qty': 3},
    'Clothing': {'min_price': 15, 'max_price': 200, 'min_qty': 1, 'max_qty': 5},
    'Home & Garden': {'min_price': 20, 'max_price': 500, 'min_qty': 1, 'max_qty': 4},
    'Beauty & Personal Care': {'min_price': 5, 'max_price': 150, 'min_qty': 1, 'max_qty': 6},
    'Sports & Outdoors': {'min_price': 25, 'max_price': 800, 'min_qty': 1, 'max_qty': 3},
    'Books & Media': {'min_price': 8, 'max_price': 80, 'min_qty': 1, 'max_qty': 5},
    'Food & Beverages': {'min_price': 2, 'max_price': 50, 'min_qty': 1, 'max_qty': 10},
    'Automotive': {'min_price': 30, 'max_price': 1000, 'min_qty': 1, 'max_qty': 2},
    'Toys & Games': {'min_price': 10, 'max_price': 300, 'min_qty': 1, 'max_qty': 4},
    'Health & Wellness': {'min_price': 15, 'max_price': 200, 'min_qty': 1, 'max_qty': 3}
}

def generate_random_date(start_date, end_date):
    """Generuje losową datę między start_date a end_date"""
    time_between = end_date - start_date
    days_between = time_between.days
    random_days = random.randrange(days_between)
    return start_date + timedelta(days=random_days)

def generate_random_time():
    """Generuje losowy czas w godzinach pracy sklepu (8:00-22:00)"""
    hour = random.randint(8, 21)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return f"{hour:02d}:{minute:02d}:{second:02d}"

def generate_age_with_distribution():
    """Generuje wiek z realistyczną dystrybucją"""
    # Większość klientów w wieku 25-65
    age_groups = [
        (18, 24, 0.15),  # 15% młodzi dorośli
        (25, 34, 0.25),  # 25% młodzi profesjonaliści
        (35, 44, 0.25),  # 25% w średnim wieku
        (45, 54, 0.20),  # 20% w średnim wieku
        (55, 65, 0.10),  # 10% starsi
        (66, 80, 0.05)   # 5% seniorzy
    ]
    
    rand = random.random()
    cumulative = 0
    for min_age, max_age, prob in age_groups:
        cumulative += prob
        if rand <= cumulative:
            return random.randint(min_age, max_age)
    return random.randint(25, 45)  # fallback

def calculate_cogs(price_per_unit, category):
    """Oblicza koszt własny sprzedanych towarów (COGS)"""
    # Różne marże dla różnych kategorii
    margin_rates = {
        'Electronics': 0.25,      # 25% marża
        'Clothing': 0.60,         # 60% marża
        'Home & Garden': 0.40,    # 40% marża
        'Beauty & Personal Care': 0.70, # 70% marża
        'Sports & Outdoors': 0.35, # 35% marża
        'Books & Media': 0.20,    # 20% marża
        'Food & Beverages': 0.30, # 30% marża
        'Automotive': 0.25,       # 25% marża
        'Toys & Games': 0.45,     # 45% marża
        'Health & Wellness': 0.50 # 50% marża
    }
    
    margin = margin_rates.get(category, 0.35)
    cogs = price_per_unit * (1 - margin)
    return round(cogs, 2)

def generate_retail_sales_data(num_records):
    """Generuje dane sprzedaży detalicznej"""
    data = []
    
    for i in range(1, num_records + 1):
        # Podstawowe informacje
        sale_date = generate_random_date(START_DATE, END_DATE)
        sale_time = generate_random_time()
        customer_id = random.randint(1001, 9999)
        gender = random.choice(['Male', 'Female'])
        age = generate_age_with_distribution()
        
        # Kategoria produktu
        category = random.choice(list(PRODUCT_CATEGORIES.keys()))
        cat_info = PRODUCT_CATEGORIES[category]
        
        # Ilość i cena
        quantity = random.randint(cat_info['min_qty'], cat_info['max_qty'])
        price_per_unit = round(random.uniform(cat_info['min_price'], cat_info['max_price']), 2)
        
        # Obliczenia finansowe
        total_sale = round(quantity * price_per_unit, 2)
        cogs = calculate_cogs(price_per_unit, category)
        
        # Dodanie rekordu
        record = {
            'id': i,
            'sale_date': sale_date.strftime('%Y-%m-%d'),
            'sale_time': sale_time,
            'customer_id': customer_id,
            'gender': gender,
            'age': age,
            'category': category,
            'quantity': quantity,
            'price_per_unit': price_per_unit,
            'cogs': cogs,
            'total_sale': total_sale
        }
        
        data.append(record)
        
        # Postęp co 1000 rekordów
        if i % 1000 == 0:
            print(f"Wygenerowano {i} rekordów...")
    
    return data

# Generowanie danych
print("Rozpoczynam generowanie danych sprzedaży...")
retail_data = generate_retail_sales_data(NUM_RECORDS)

# Konwersja do DataFrame
df = pd.DataFrame(retail_data)

# Wyświetlenie podstawowych statystyk
print("\n" + "="*50)
print("PODSUMOWANIE WYGENEROWANYCH DANYCH")
print("="*50)
print(f"Liczba rekordów: {len(df):,}")
print(f"Zakres dat: {df['sale_date'].min()} - {df['sale_date'].max()}")
print(f"Liczba unikalnych klientów: {df['customer_id'].nunique():,}")
print(f"Łączna wartość sprzedaży: ${df['total_sale'].sum():,.2f}")

print("\nRozkład według kategorii:")
print(df['category'].value_counts())

print("\nRozkład według płci:")
print(df['gender'].value_counts())

print(f"\nŚredni wiek klientów: {df['age'].mean():.1f} lat")
print(f"Średnia wartość transakcji: ${df['total_sale'].mean():.2f}")

# Wyświetlenie pierwszych kilku rekordów
print("\nPierwsze 5 rekordów:")
print(df.head())

# Zapisanie do pliku CSV
output_filename = 'retail_sales_data.csv'
df.to_csv(output_filename, index=False, encoding='utf-8')
print(f"\nDane zostały zapisane do pliku: {output_filename}")

# Zapisanie dodatkowych formatów dla elastyczności
df.to_json('retail_sales_data.json', orient='records', date_format='iso')
print("Dane zostały również zapisane do pliku: retail_sales_data.json")

print("\n" + "="*50)
print("GENEROWANIE DANYCH ZAKOŃCZONE POMYŚLNIE!")
print("="*50)