#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DuckDB Analyzer - Główny moduł analizy danych sprzedaży
=====================================================

Ten moduł zapewnia kompletną funkcjonalność analizy danych przy użyciu DuckDB:
- Ładowanie danych CSV do DuckDB
- Wykonywanie złożonych zapytań SQL
- Analiza trendów sprzedaży
- Segmentacja klientów
- Analiza rentowności
- Generacja raportów i wizualizacji

Autor: [Twoje Imię]  
Data: 2025-09-07
"""

import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Union, Any
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Konfiguracja wizualizacji
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

class DuckDBAnalyzer:
    """
    Główna klasa do analizy danych sprzedaży z wykorzystaniem DuckDB
    """
    
    def __init__(self, database_path: Union[str, Path] = ":memory:", logger=None):
        """
        Inicjalizacja analyzera
        
        Args:
            database_path (str|Path): Ścieżka do bazy DuckDB lub ":memory:" dla bazy w pamięci
            logger: Logger do logowania operacji
        """
        self.database_path = str(database_path) if database_path != ":memory:" else database_path
        self.connection = duckdb.connect(self.database_path)
        self.logger = logger or logging.getLogger(__name__)
        self.table_name = None
        
        # Instaluj i załaduj rozszerzenia DuckDB
        self._setup_extensions()
        
        self.logger.info(f"🦆 Połączono z DuckDB: {self.database_path}")
    
    def _setup_extensions(self):
        """Instaluj i załaduj przydatne rozszerzenia DuckDB"""
        try:
            # Załaduj rozszerzenia statystyczne
            self.connection.execute("INSTALL httpfs;")  # dla pracy z plikami zdalnymi
            self.connection.execute("LOAD httpfs;")
            
            self.logger.debug("✅ Załadowano rozszerzenia DuckDB")
        except Exception as e:
            self.logger.warning(f"⚠️  Nie udało się załadować niektórych rozszerzeń: {e}")
    
    def load_csv_data(self, csv_file: Union[str, Path], table_name: str = "retail_sales") -> None:
        """
        Załaduj dane CSV do tabeli DuckDB
        
        Args:
            csv_file (str|Path): Ścieżka do pliku CSV
            table_name (str): Nazwa tabeli w DuckDB
        """
        csv_path = Path(csv_file)
        if not csv_path.exists():
            raise FileNotFoundError(f"Plik CSV nie istnieje: {csv_path}")
        
        self.table_name = table_name
        
        try:
            # Załaduj CSV bezpośrednio do DuckDB (bardzo szybkie!)
            create_table_sql = f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * FROM read_csv_auto('{csv_path}',
                header=true,
                sample_size=1000,
                auto_type_candidates=['BIGINT', 'DOUBLE', 'VARCHAR', 'DATE', 'TIME']
            );
            """
            
            self.connection.execute(create_table_sql)
            
            # Sprawdź liczbę załadowanych rekordów
            count_result = self.connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            record_count = count_result[0]
            
            self.logger.info(f"✅ Załadowano {record_count:,} rekordów do tabeli '{table_name}'")
            
            # Pokaż schemat tabeli
            self._display_table_schema(table_name)
            
        except Exception as e:
            self.logger.error(f"❌ Błąd podczas ładowania CSV: {e}")
            raise
    
    def _display_table_schema(self, table_name: str) -> None:
        """Wyświetl schemat tabeli"""
        try:
            schema_query = f"DESCRIBE {table_name};"
            schema_result = self.connection.execute(schema_query).fetchdf()
            self.logger.info(f"📋 Schemat tabeli '{table_name}':")
            for _, row in schema_result.iterrows():
                self.logger.info(f"   {row['column_name']:15} | {row['column_type']:10} | Nullable: {row['null']}")
        except Exception as e:
            self.logger.warning(f"⚠️  Nie udało się wyświetlić schematu: {e}")
    
    def get_data_overview(self) -> Dict[str, Any]:
        """
        Podstawowy przegląd danych
        
        Returns:
            Dict zawierający podstawowe statystyki
        """
        if not self.table_name:
            raise ValueError("Najpierw załaduj dane używając load_csv_data()")
        
        self.logger.info("📊 Generuję przegląd danych...")
        
        overview = {}
        
        try:
            # 1. Podstawowe statystyki
            stats_query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT customer_id) as unique_customers,
                COUNT(DISTINCT category) as unique_categories,
                MIN(sale_date) as earliest_sale,
                MAX(sale_date) as latest_sale,
                SUM(total_sale) as total_revenue,
                AVG(total_sale) as avg_transaction,
                SUM(quantity) as total_items_sold
            FROM {self.table_name};
            """
            
            stats = self.connection.execute(stats_query).fetchone()
            
            overview['basic_stats'] = {
                'total_records': stats[0],
                'unique_customers': stats[1], 
                'unique_categories': stats[2],
                'date_range': f"{stats[3]} - {stats[4]}",
                'total_revenue': round(stats[5], 2),
                'avg_transaction': round(stats[6], 2),
                'total_items_sold': stats[7]
            }
            
            # 2. Rozkład według kategorii
            category_query = f"""
            SELECT 
                category,
                COUNT(*) as transactions,
                SUM(total_sale) as revenue,
                AVG(total_sale) as avg_transaction,
                SUM(quantity) as items_sold
            FROM {self.table_name}
            GROUP BY category
            ORDER BY revenue DESC;
            """
            
            overview['categories'] = self.connection.execute(category_query).fetchdf().to_dict('records')
            
            # 3. Rozkład według płci
            gender_query = f"""
            SELECT 
                gender,
                COUNT(*) as transactions,
                SUM(total_sale) as revenue,
                AVG(age) as avg_age
            FROM {self.table_name}
            GROUP BY gender;
            """
            
            overview['gender_breakdown'] = self.connection.execute(gender_query).fetchdf().to_dict('records')
            
            # 4. Top klienci
            top_customers_query = f"""
            SELECT 
                customer_id,
                COUNT(*) as transaction_count,
                SUM(total_sale) as total_spent,
                AVG(total_sale) as avg_per_transaction,
                STRING_AGG(DISTINCT category, ', ') as categories_bought
            FROM {self.table_name}
            GROUP BY customer_id
            ORDER BY total_spent DESC
            LIMIT 10;
            """
            
            overview['top_customers'] = self.connection.execute(top_customers_query).fetchdf().to_dict('records')
            
            self.logger.info("✅ Przegląd danych wygenerowany")
            return overview
            
        except Exception as e:
            self.logger.error(f"❌ Błąd podczas generowania przeglądu: {e}")
            raise
    
    def analyze_sales_trends(self) -> Dict[str, Any]:
        """
        Analiza trendów sprzedaży w czasie
        
        Returns:
            Dict zawierający analizy czasowe
        """
        self.logger.info("📈 Analizuję trendy sprzedaży...")
        
        trends = {}
        
        try:
            # 1. Sprzedaż według miesięcy
            monthly_query = f"""
            SELECT 
                strftime(sale_date, '%Y-%m') as month,
                COUNT(*) as transactions,
                SUM(total_sale) as revenue,
                AVG(total_sale) as avg_transaction,
                SUM(quantity) as items_sold
            FROM {self.table_name}
            GROUP BY strftime(sale_date, '%Y-%m')
            ORDER BY month;
            """
            
            trends['monthly'] = self.connection.execute(monthly_query).fetchdf().to_dict('records')
            
            # 2. Sprzedaż według dni tygodnia
            weekday_query = f"""
            SELECT 
                strftime(sale_date, '%w') as weekday_num,
                CASE strftime(sale_date, '%w')
                    WHEN '0' THEN 'Niedziela'
                    WHEN '1' THEN 'Poniedziałek'
                    WHEN '2' THEN 'Wtorek'
                    WHEN '3' THEN 'Środa'
                    WHEN '4' THEN 'Czwartek'
                    WHEN '5' THEN 'Piątek'
                    WHEN '6' THEN 'Sobota'
                END as weekday_name,
                COUNT(*) as transactions,
                SUM(total_sale) as revenue,
                AVG(total_sale) as avg_transaction
            FROM {self.table_name}
            GROUP BY strftime(sale_date, '%w')
            ORDER BY weekday_num;
            """
            
            trends['weekdays'] = self.connection.execute(weekday_query).fetchdf().to_dict('records')
            
            # 3. Sprzedaż według godzin
            hourly_query = f"""
            SELECT 
                strftime(sale_time, '%H') as hour,
                COUNT(*) as transactions,
                SUM(total_sale) as revenue,
                AVG(total_sale) as avg_transaction
            FROM {self.table_name}
            GROUP BY strftime(sale_time, '%H')
            ORDER BY hour;
            """
            
            trends['hourly'] = self.connection.execute(hourly_query).fetchdf().to_dict('records')
            
            # 4. Trend wzrostu (czy sprzedaż rośnie w czasie?)
            growth_query = f"""
            WITH daily_sales AS (
                SELECT 
                    sale_date,
                    SUM(total_sale) as daily_revenue,
                    COUNT(*) as daily_transactions
                FROM {self.table_name}
                GROUP BY sale_date
                ORDER BY sale_date
            )
            SELECT 
                sale_date,
                daily_revenue,
                daily_transactions,
                SUM(daily_revenue) OVER (ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) / 7.0 as revenue_7day_avg,
                LAG(daily_revenue, 7) OVER (ORDER BY sale_date) as revenue_week_ago
            FROM daily_sales
            ORDER BY sale_date;
            """
            
            trends['daily_growth'] = self.connection.execute(growth_query).fetchdf().to_dict('records')
            
            self.logger.info("✅ Analiza trendów zakończona")
            return trends
            
        except Exception as e:
            self.logger.error(f"❌ Błąd podczas analizy trendów: {e}")
            raise
    
    def analyze_product_categories(self) -> Dict[str, Any]:
        """
        Analiza kategorii produktów
        
        Returns:
            Dict zawierający analizę produktów
        """
        self.logger.info("🛍️ Analizuję kategorie produktów...")
        
        categories = {}
        
        try:
            # 1. Szczegółowa analiza kategorii
            detailed_query = f"""
            SELECT 
                category,
                COUNT(*) as transaction_count,
                SUM(total_sale) as total_revenue,
                AVG(total_sale) as avg_transaction_value,
                SUM(quantity) as total_quantity,
                AVG(quantity) as avg_quantity_per_transaction,
                AVG(price_per_unit) as avg_price_per_unit,
                COUNT(DISTINCT customer_id) as unique_customers,
                SUM(total_sale - cogs * quantity) as total_profit,
                (SUM(total_sale - cogs * quantity) / SUM(total_sale)) * 100 as profit_margin_pct
            FROM {self.table_name}
            GROUP BY category
            ORDER BY total_revenue DESC;
            """
            
            categories['detailed'] = self.connection.execute(detailed_query).fetchdf().to_dict('records')
            
            # 2. Analiza cenowa według kategorii
            price_analysis_query = f"""
            SELECT 
                category,
                MIN(price_per_unit) as min_price,
                MAX(price_per_unit) as max_price,
                AVG(price_per_unit) as avg_price,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_per_unit) as price_p25,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_unit) as price_median,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_per_unit) as price_p75
            FROM {self.table_name}
            GROUP BY category
            ORDER BY avg_price DESC;
            """
            
            categories['price_analysis'] = self.connection.execute(price_analysis_query).fetchdf().to_dict('records')
            
            # 3. Cross-selling analysis (które kategorie kupowane razem)
            # Najpierw sprawdzimy klientów którzy kupowali więcej niż jedną kategorię
            cross_sell_query = f"""
            WITH customer_categories AS (
                SELECT 
                    customer_id,
                    STRING_AGG(DISTINCT category, ', ' ORDER BY category) as categories_bought,
                    COUNT(DISTINCT category) as category_count
                FROM {self.table_name}
                GROUP BY customer_id
                HAVING COUNT(DISTINCT category) > 1
            )
            SELECT 
                categories_bought,
                COUNT(*) as customer_count,
                category_count
            FROM customer_categories
            GROUP BY categories_bought, category_count
            ORDER BY customer_count DESC
            LIMIT 20;
            """
            
            categories['cross_selling'] = self.connection.execute(cross_sell_query).fetchdf().to_dict('records')
            
            self.logger.info("✅ Analiza kategorii zakończona")
            return categories
            
        except Exception as e:
            self.logger.error(f"❌ Błąd podczas analizy kategorii: {e}")
            raise
    
    def analyze_customer_segments(self) -> Dict[str, Any]:
        """
        Segmentacja i analiza klientów
        
        Returns:
            Dict zawierający segmentację klientów
        """
        self.logger.info("👥 Analizuję segmentację klientów...")
        
        segments = {}
        
        try:
            # 1. Podstawowa segmentacja demograficzna
            demographic_query = f"""
            WITH customer_stats AS (
                SELECT 
                    customer_id,
                    gender,
                    age,
                    CASE 
                        WHEN age < 25 THEN '18-24'
                        WHEN age < 35 THEN '25-34'
                        WHEN age < 45 THEN '35-44'
                        WHEN age < 55 THEN '45-54'
                        WHEN age < 65 THEN '55-64'
                        ELSE '65+'
                    END as age_group,
                    COUNT(*) as transaction_count,
                    SUM(total_sale) as total_spent,
                    AVG(total_sale) as avg_transaction,
                    SUM(quantity) as total_items,
                    COUNT(DISTINCT category) as categories_bought,
                    MIN(sale_date) as first_purchase,
                    MAX(sale_date) as last_purchase
                FROM {self.table_name}
                GROUP BY customer_id, gender, age
            )
            SELECT 
                age_group,
                gender,
                COUNT(*) as customer_count,
                AVG(total_spent) as avg_total_spent,
                AVG(transaction_count) as avg_transactions,
                AVG(avg_transaction) as avg_transaction_value,
                AVG(categories_bought) as avg_categories_per_customer
            FROM customer_stats
            GROUP BY age_group, gender
            ORDER BY age_group, gender;
            """
            
            segments['demographic'] = self.connection.execute(demographic_query).fetchdf().to_dict('records')
            
            # 2. Segmentacja RFM (Recency, Frequency, Monetary)
            rfm_query = f"""
            WITH customer_rfm AS (
                SELECT 
                    customer_id,
                    -- Recency (dni od ostatniego zakupu)
                    (CURRENT_DATE - MAX(sale_date)) as days_since_last_purchase,
                    -- Frequency (liczba transakcji)
                    COUNT(*) as transaction_frequency,
                    -- Monetary (łączna wartość zakupów)
                    SUM(total_sale) as monetary_value
                FROM {self.table_name}
                GROUP BY customer_id
            ),
            rfm_scored AS (
                SELECT *,
                    -- Scoring RFM (1-5, gdzie 5 = najlepsze)
                    CASE 
                        WHEN days_since_last_purchase <= 30 THEN 5
                        WHEN days_since_last_purchase <= 60 THEN 4
                        WHEN days_since_last_purchase <= 90 THEN 3
                        WHEN days_since_last_purchase <= 180 THEN 2
                        ELSE 1
                    END as recency_score,
                    
                    CASE 
                        WHEN transaction_frequency >= 10 THEN 5
                        WHEN transaction_frequency >= 7 THEN 4
                        WHEN transaction_frequency >= 5 THEN 3
                        WHEN transaction_frequency >= 3 THEN 2
                        ELSE 1
                    END as frequency_score,
                    
                    CASE 
                        WHEN monetary_value >= 1000 THEN 5
                        WHEN monetary_value >= 500 THEN 4
                        WHEN monetary_value >= 200 THEN 3
                        WHEN monetary_value >= 100 THEN 2
                        ELSE 1
                    END as monetary_score
                FROM customer_rfm
            ),
            rfm_segments AS (
                SELECT *,
                    (recency_score + frequency_score + monetary_score) as rfm_total,
                    CASE 
                        WHEN (recency_score + frequency_score + monetary_score) >= 13 THEN 'Champions'
                        WHEN (recency_score + frequency_score + monetary_score) >= 11 THEN 'Loyal Customers'
                        WHEN (recency_score + frequency_score + monetary_score) >= 9 THEN 'Potential Loyalists'
                        WHEN (recency_score + frequency_score + monetary_score) >= 7 THEN 'New Customers'
                        WHEN recency_score >= 3 AND monetary_score >= 3 THEN 'At Risk'
                        ELSE 'Lost Customers'
                    END as customer_segment
                FROM rfm_scored
            )
            SELECT 
                customer_segment,
                COUNT(*) as customer_count,
                AVG(days_since_last_purchase) as avg_recency,
                AVG(transaction_frequency) as avg_frequency,
                AVG(monetary_value) as avg_monetary,
                AVG(recency_score) as avg_recency_score,
                AVG(frequency_score) as avg_frequency_score,
                AVG(monetary_score) as avg_monetary_score
            FROM rfm_segments
            GROUP BY customer_segment
            ORDER BY avg_monetary DESC;
            """
            
            segments['rfm'] = self.connection.execute(rfm_query).fetchdf().to_dict('records')
            
            # 3. Analiza wartości życiowej klienta (Customer Lifetime Value)
            clv_query = f"""
            WITH customer_behavior AS (
                SELECT 
                    customer_id,
                    COUNT(*) as total_transactions,
                    SUM(total_sale) as total_revenue,
                    AVG(total_sale) as avg_transaction_value,
                    MIN(sale_date) as first_purchase,
                    MAX(sale_date) as last_purchase,
                    (MAX(sale_date) - MIN(sale_date)) as customer_lifespan_days,
                    CASE 
                        WHEN (MAX(sale_date) - MIN(sale_date)) > 0 
                        THEN CAST(COUNT(*) AS FLOAT) / ((MAX(sale_date) - MIN(sale_date)) + 1) 
                        ELSE COUNT(*)
                    END as purchase_frequency_per_day
                FROM {self.table_name}
                GROUP BY customer_id
            )
            SELECT 
                customer_id,
                total_revenue,
                avg_transaction_value,
                total_transactions,
                customer_lifespan_days,
                purchase_frequency_per_day,
                -- Szacunkowa wartość życiowa (uproszczona)
                (avg_transaction_value * purchase_frequency_per_day * 365) as estimated_annual_value,
                CASE 
                    WHEN total_revenue >= 1000 THEN 'High Value'
                    WHEN total_revenue >= 500 THEN 'Medium Value'
                    WHEN total_revenue >= 200 THEN 'Low Value'
                    ELSE 'Very Low Value'
                END as value_segment
            FROM customer_behavior
            ORDER BY total_revenue DESC;
            """
            
            segments['clv'] = self.connection.execute(clv_query).fetchdf().to_dict('records')
            
            # 4. Podsumowanie segmentów
            segment_summary_query = f"""
            WITH customer_behavior AS (
                SELECT 
                    customer_id,
                    SUM(total_sale) as total_revenue,
                    COUNT(*) as transaction_count
                FROM {self.table_name}
                GROUP BY customer_id
            )
            SELECT 
                CASE 
                    WHEN total_revenue >= 1000 THEN 'High Value'
                    WHEN total_revenue >= 500 THEN 'Medium Value'
                    WHEN total_revenue >= 200 THEN 'Low Value'
                    ELSE 'Very Low Value'
                END as value_segment,
                COUNT(*) as customer_count,
                SUM(total_revenue) as segment_revenue,
                AVG(total_revenue) as avg_customer_value,
                AVG(transaction_count) as avg_transactions_per_customer
            FROM customer_behavior
            GROUP BY 1
            ORDER BY avg_customer_value DESC;
            """
            
            segments['value_summary'] = self.connection.execute(segment_summary_query).fetchdf().to_dict('records')
            
            self.logger.info("✅ Segmentacja klientów zakończona")
            return segments
            
        except Exception as e:
            self.logger.error(f"❌ Błąd podczas segmentacji klientów: {e}")
            raise
    
    def analyze_profitability(self) -> Dict[str, Any]:
        """
        Analiza rentowności
        
        Returns:
            Dict zawierający analizę rentowności
        """
        self.logger.info("💰 Analizuję rentowność...")
        
        profitability = {}
        
        try:
            # 1. Ogólna rentowność
            overall_query = f"""
            SELECT 
                SUM(total_sale) as total_revenue,
                SUM(cogs * quantity) as total_cogs,
                SUM(total_sale - cogs * quantity) as total_profit,
                (SUM(total_sale - cogs * quantity) / SUM(total_sale)) * 100 as overall_margin_pct,
                AVG(total_sale - cogs * quantity) as avg_profit_per_transaction,
                COUNT(*) as total_transactions
            FROM {self.table_name};
            """
            
            profitability['overall'] = self.connection.execute(overall_query).fetchone()
            
            # 2. Rentowność według kategorii
            category_profit_query = f"""
            SELECT 
                category,
                SUM(total_sale) as revenue,
                SUM(cogs * quantity) as total_cogs,
                SUM(total_sale - cogs * quantity) as profit,
                (SUM(total_sale - cogs * quantity) / SUM(total_sale)) * 100 as margin_pct,
                AVG(total_sale - cogs * quantity) as avg_profit_per_transaction,
                COUNT(*) as transaction_count
            FROM {self.table_name}
            GROUP BY category
            ORDER BY profit DESC;
            """
            
            profitability['by_category'] = self.connection.execute(category_profit_query).fetchdf().to_dict('records')
            
            # 3. Rentowność według miesięcy
            monthly_profit_query = f"""
            SELECT 
                strftime(sale_date, '%Y-%m') as month,
                SUM(total_sale) as revenue,
                SUM(cogs * quantity) as total_cogs,
                SUM(total_sale - cogs * quantity) as profit,
                (SUM(total_sale - cogs * quantity) / SUM(total_sale)) * 100 as margin_pct,
                COUNT(*) as transaction_count
            FROM {self.table_name}
            GROUP BY strftime(sale_date, '%Y-%m')
            ORDER BY month;
            """
            
            profitability['monthly'] = self.connection.execute(monthly_profit_query).fetchdf().to_dict('records')
            
            # 4. Top najrentowniejsze produkty (według price_per_unit i marży)
            top_profitable_query = f"""
            WITH product_profitability AS (
                SELECT 
                    category,
                    price_per_unit,
                    cogs,
                    (price_per_unit - cogs) as unit_profit,
                    ((price_per_unit - cogs) / price_per_unit) * 100 as unit_margin_pct,
                    SUM(quantity) as total_quantity_sold,
                    SUM(total_sale - cogs * quantity) as total_profit_contribution,
                    COUNT(*) as transaction_count
                FROM {self.table_name}
                GROUP BY category, price_per_unit, cogs
                HAVING COUNT(*) >= 5  -- tylko produkty sprzedane co najmniej 5 razy
            )
            SELECT 
                category,
                price_per_unit,
                unit_profit,
                unit_margin_pct,
                total_quantity_sold,
                total_profit_contribution,
                transaction_count
            FROM product_profitability
            ORDER BY total_profit_contribution DESC
            LIMIT 20;
            """
            
            profitability['top_products'] = self.connection.execute(top_profitable_query).fetchdf().to_dict('records')
            
            # 5. Analiza rentowności według segmentów klientów
            customer_profitability_query = f"""
            WITH customer_profit AS (
                SELECT 
                    customer_id,
                    gender,
                    age,
                    SUM(total_sale) as total_revenue,
                    SUM(cogs * quantity) as total_cogs,
                    SUM(total_sale - cogs * quantity) as total_profit,
                    COUNT(*) as transaction_count,
                    CASE 
                        WHEN age < 25 THEN '18-24'
                        WHEN age < 35 THEN '25-34'
                        WHEN age < 45 THEN '35-44'
                        WHEN age < 55 THEN '45-54'
                        WHEN age < 65 THEN '55-64'
                        ELSE '65+'
                    END as age_group
                FROM {self.table_name}
                GROUP BY customer_id, gender, age
            )
            SELECT 
                age_group,
                gender,
                COUNT(*) as customer_count,
                AVG(total_profit) as avg_profit_per_customer,
                SUM(total_profit) as segment_total_profit,
                AVG(total_profit / transaction_count) as avg_profit_per_transaction
            FROM customer_profit
            GROUP BY age_group, gender
            ORDER BY avg_profit_per_customer DESC;
            """
            
            profitability['by_customer_segment'] = self.connection.execute(customer_profitability_query).fetchdf().to_dict('records')
            
            self.logger.info("✅ Analiza rentowności zakończona")
            return profitability
            
        except Exception as e:
            self.logger.error(f"❌ Błąd podczas analizy rentowności: {e}")
            raise
    
    def create_visualizations(self, analysis_results: Dict[str, Any], output_dir: Path) -> List[Path]:
        """
        Tworzenie wizualizacji na podstawie wyników analiz
        
        Args:
            analysis_results: Wyniki analiz
            output_dir: Katalog wyjściowy dla wykresów
            
        Returns:
            Lista ścieżek do utworzonych wykresów
        """
        self.logger.info("📊 Tworzę wizualizacje...")
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        created_files = []
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            # 1. Wykres sprzedaży według kategorii
            if 'overview' in analysis_results and 'categories' in analysis_results['overview']:
                categories_df = pd.DataFrame(analysis_results['overview']['categories'])
                
                plt.figure(figsize=(12, 6))
                plt.subplot(1, 2, 1)
                plt.bar(categories_df['category'], categories_df['revenue'])
                plt.title('Przychody według kategorii')
                plt.xticks(rotation=45)
                plt.ylabel('Przychody ($)')
                
                plt.subplot(1, 2, 2)
                plt.bar(categories_df['category'], categories_df['transactions'])
                plt.title('Liczba transakcji według kategorii')
                plt.xticks(rotation=45)
                plt.ylabel('Liczba transakcji')
                
                plt.tight_layout()
                file_path = output_dir / f'categories_analysis_{timestamp}.png'
                plt.savefig(file_path, dpi=300, bbox_inches='tight')
                plt.close()
                created_files.append(file_path)
                
            # 2. Trendy czasowe
            if 'time_analysis' in analysis_results and 'monthly' in analysis_results['time_analysis']:
                monthly_df = pd.DataFrame(analysis_results['time_analysis']['monthly'])
                
                fig = make_subplots(
                    rows=2, cols=1,
                    subplot_titles=('Przychody miesięczne', 'Liczba transakcji miesięcznych'),
                    vertical_spacing=0.1
                )
                
                fig.add_trace(
                    go.Scatter(x=monthly_df['month'], y=monthly_df['revenue'], 
                              mode='lines+markers', name='Przychody'),
                    row=1, col=1
                )
                
                fig.add_trace(
                    go.Scatter(x=monthly_df['month'], y=monthly_df['transactions'],
                              mode='lines+markers', name='Transakcje', line=dict(color='orange')),
                    row=2, col=1
                )
                
                fig.update_layout(height=600, title_text="Trendy sprzedaży w czasie")
                
                file_path = output_dir / f'sales_trends_{timestamp}.html'
                fig.write_html(file_path)
                created_files.append(file_path)
                
            # 3. Segmentacja klientów (jeśli dostępna)
            if 'customer_analysis' in analysis_results and 'rfm' in analysis_results['customer_analysis']:
                rfm_df = pd.DataFrame(analysis_results['customer_analysis']['rfm'])
                
                plt.figure(figsize=(10, 6))
                plt.pie(rfm_df['customer_count'], labels=rfm_df['customer_segment'], autopct='%1.1f%%')
                plt.title('Segmentacja klientów RFM')
                
                file_path = output_dir / f'customer_segments_{timestamp}.png'
                plt.savefig(file_path, dpi=300, bbox_inches='tight')
                plt.close()
                created_files.append(file_path)
                
            # 4. Rentowność według kategorii
            if 'profitability' in analysis_results and 'by_category' in analysis_results['profitability']:
                profit_df = pd.DataFrame(analysis_results['profitability']['by_category'])
                
                plt.figure(figsize=(12, 6))
                x_pos = range(len(profit_df))
                
                plt.subplot(1, 2, 1)
                bars1 = plt.bar(x_pos, profit_df['revenue'], alpha=0.7, label='Przychody')
                bars2 = plt.bar(x_pos, profit_df['total_cogs'], alpha=0.7, label='Koszty')
                plt.title('Przychody vs Koszty według kategorii')
                plt.xticks(x_pos, profit_df['category'], rotation=45)
                plt.legend()
                
                plt.subplot(1, 2, 2)
                plt.bar(x_pos, profit_df['margin_pct'])
                plt.title('Marża % według kategorii')
                plt.xticks(x_pos, profit_df['category'], rotation=45)
                plt.ylabel('Marża (%)')
                
                plt.tight_layout()
                file_path = output_dir / f'profitability_analysis_{timestamp}.png'
                plt.savefig(file_path, dpi=300, bbox_inches='tight')
                plt.close()
                created_files.append(file_path)
                
            self.logger.info(f"✅ Utworzono {len(created_files)} wykresów")
            return created_files
            
        except Exception as e:
            self.logger.error(f"❌ Błąd podczas tworzenia wizualizacji: {e}")
            return created_files
    
    def generate_text_report(self, analysis_results: Dict[str, Any]) -> str:
        """
        Generuje raport tekstowy z wyników analiz
        
        Args:
            analysis_results: Wyniki wszystkich analiz
            
        Returns:
            Sformatowany raport tekstowy
        """
        self.logger.info("📝 Generuję raport tekstowy...")
        
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("RAPORT ANALIZY DANYCH SPRZEDAŻY DETALICZNEJ")
        report_lines.append("=" * 80)
        report_lines.append(f"Data wygenerowania: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Źródło danych: {self.table_name}")
        report_lines.append("")
        
        # 1. Przegląd ogólny
        if 'overview' in analysis_results:
            overview = analysis_results['overview']
            if 'basic_stats' in overview:
                stats = overview['basic_stats']
                report_lines.append("1. PRZEGLĄD OGÓLNY")
                report_lines.append("-" * 40)
                report_lines.append(f"• Łączna liczba transakcji: {stats['total_records']:,}")
                report_lines.append(f"• Unikalni klienci: {stats['unique_customers']:,}")
                report_lines.append(f"• Kategorie produktów: {stats['unique_categories']}")
                report_lines.append(f"• Okres analizy: {stats['date_range']}")
                report_lines.append(f"• Łączne przychody: ${stats['total_revenue']:,.2f}")
                report_lines.append(f"• Średnia wartość transakcji: ${stats['avg_transaction']:.2f}")
                report_lines.append(f"• Łączna sprzedaż (szt.): {stats['total_items_sold']:,}")
                report_lines.append("")
        
        # 2. Top kategorie
        if 'overview' in analysis_results and 'categories' in analysis_results['overview']:
            report_lines.append("2. TOP KATEGORIE PRODUKTÓW")
            report_lines.append("-" * 40)
            categories = analysis_results['overview']['categories'][:5]  # top 5
            for i, cat in enumerate(categories, 1):
                report_lines.append(f"{i}. {cat['category']}: ${cat['revenue']:,.2f} "
                                  f"({cat['transactions']:,} transakcji)")
            report_lines.append("")
        
        # 3. Segmentacja klientów
        if 'customer_analysis' in analysis_results and 'rfm' in analysis_results['customer_analysis']:
            report_lines.append("3. SEGMENTACJA KLIENTÓW (RFM)")
            report_lines.append("-" * 40)
            rfm_segments = analysis_results['customer_analysis']['rfm']
            for segment in rfm_segments:
                report_lines.append(f"• {segment['customer_segment']}: "
                                  f"{segment['customer_count']} klientów "
                                  f"(śr. wartość: ${segment['avg_monetary']:.2f})")
            report_lines.append("")
        
        # 4. Rentowność
        if 'profitability' in analysis_results:
            prof = analysis_results['profitability']
            if 'overall' in prof:
                overall_prof = prof['overall']
                report_lines.append("4. ANALIZA RENTOWNOŚCI")
                report_lines.append("-" * 40)
                report_lines.append(f"• Łączne przychody: ${overall_prof[0]:,.2f}")
                report_lines.append(f"• Łączne koszty: ${overall_prof[1]:,.2f}")
                report_lines.append(f"• Zysk brutto: ${overall_prof[2]:,.2f}")
                report_lines.append(f"• Marża brutto: {overall_prof[3]:.2f}%")
                report_lines.append(f"• Średni zysk na transakcję: ${overall_prof[4]:.2f}")
                report_lines.append("")
        
        # 5. Rekomendacje
        report_lines.append("5. KLUCZOWE WNIOSKI I REKOMENDACJE")
        report_lines.append("-" * 40)
        
        # Automatyczne wnioski na podstawie danych
        if 'overview' in analysis_results and 'categories' in analysis_results['overview']:
            top_category = analysis_results['overview']['categories'][0]
            report_lines.append(f"• Najlepiej sprzedająca się kategoria to {top_category['category']} "
                              f"z przychodami ${top_category['revenue']:,.2f}")
        
        if 'customer_analysis' in analysis_results and 'rfm' in analysis_results['customer_analysis']:
            champions = [s for s in analysis_results['customer_analysis']['rfm'] 
                        if s['customer_segment'] == 'Champions']
            if champions:
                report_lines.append(f"• {champions[0]['customer_count']} klientów to 'Champions' "
                                  f"- najcenniejsi klienci wymagający szczególnej uwagi")
        
        report_lines.append("• Szczegółowe analizy i wizualizacje dostępne w plikach graficznych")
        report_lines.append("")
        report_lines.append("=" * 80)
        report_lines.append("Koniec raportu")
        report_lines.append("=" * 80)
        
        return "\n".join(report_lines)
    
    def close(self):
        """Zamknij połączenie z DuckDB"""
        if self.connection:
            self.connection.close()
            self.logger.info("🔌 Połączenie z DuckDB zostało zamknięte")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
    
    def execute_custom_query(self, query: str) -> pd.DataFrame:
        """
        Wykonaj niestandardowe zapytanie SQL
        
        Args:
            query (str): Zapytanie SQL
            
        Returns:
            DataFrame z wynikami
        """
        try:
            result = self.connection.execute(query).fetchdf()
            self.logger.info(f"✅ Wykonano zapytanie zwracające {len(result)} wierszy")
            return result
        except Exception as e:
            self.logger.error(f"❌ Błąd podczas wykonywania zapytania: {e}")
            raise