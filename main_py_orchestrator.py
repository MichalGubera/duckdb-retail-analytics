#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DuckDB Retail Analytics - Główny plik orchestrator
==================================================

Ten plik koordynuje cały workflow projektu:
1. Generacja danych
2. Analiza z DuckDB  
3. Tworzenie raportów
4. Eksport wyników

Autor: [Twoje Imię]
Data: 2025-09-07
"""

import os
import sys
from pathlib import Path
import argparse
import logging
from datetime import datetime

# Dodaj ścieżki do modułów projektu
project_root = Path(__file__).parent
sys.path.append(str(project_root / "src"))

# Import modułów projektu
try:
    from src.data_generation.retail_data_generator import generate_retail_sales_data
    from src.analysis.duckdb_analyzer import DuckDBAnalyzer
    from src.utils.logger_config import setup_logging
    from src.utils.config_manager import ConfigManager
except ImportError as e:
    print(f"❌ Błąd importu modułów: {e}")
    print("💡 Upewnij się, że wszystkie wymagane pliki są w odpowiednich katalogach")
    sys.exit(1)

class ProjectOrchestrator:
    """
    Główna klasa koordynująca cały projekt
    """
    
    def __init__(self, config_path=None):
        """
        Inicjalizacja orchestratora
        
        Args:
            config_path (str): Ścieżka do pliku konfiguracyjnego
        """
        self.project_root = Path(__file__).parent
        self.config = ConfigManager(config_path)
        self.logger = setup_logging()
        self.analyzer = None
        
        # Utwórz katalogi jeśli nie istnieją
        self.ensure_directories()
        
    def ensure_directories(self):
        """Upewnij się, że wszystkie wymagane katalogi istnieją"""
        dirs_to_create = [
            self.config.raw_data_dir,
            self.config.processed_data_dir,
            self.config.output_dir,
            self.config.figures_dir,
            self.config.reports_dir
        ]
        
        for directory in dirs_to_create:
            directory.mkdir(parents=True, exist_ok=True)
            
    def step_1_generate_data(self, num_records=None, force_regenerate=False):
        """
        Krok 1: Generacja danych sprzedaży
        
        Args:
            num_records (int): Liczba rekordów do wygenerowania
            force_regenerate (bool): Czy wymuszać regenerację danych
        """
        self.logger.info("🔄 KROK 1: Generacja danych sprzedaży")
        
        csv_file = self.config.raw_data_dir / "retail_sales_data.csv"
        
        # Sprawdź czy dane już istnieją
        if csv_file.exists() and not force_regenerate:
            self.logger.info(f"📄 Dane już istnieją w: {csv_file}")
            self.logger.info("💡 Użyj --regenerate żeby wygenerować nowe dane")
            return str(csv_file)
        
        try:
            num_records = num_records or self.config.default_num_records
            self.logger.info(f"📊 Generuję {num_records:,} rekordów danych...")
            
            # Generuj dane
            retail_data = generate_retail_sales_data(num_records)
            
            # Zapisz do CSV
            import pandas as pd
            df = pd.DataFrame(retail_data)
            df.to_csv(csv_file, index=False, encoding='utf-8')
            
            # Statystyki
            self.logger.info(f"✅ Wygenerowano {len(df):,} rekordów")
            self.logger.info(f"📁 Zapisano do: {csv_file}")
            self.logger.info(f"💰 Łączna wartość sprzedaży: ${df['total_sale'].sum():,.2f}")
            
            return str(csv_file)
            
        except Exception as e:
            self.logger.error(f"❌ Błąd podczas generacji danych: {e}")
            raise
    
    def step_2_analyze_data(self, csv_file):
        """
        Krok 2: Analiza danych z DuckDB
        
        Args:
            csv_file (str): Ścieżka do pliku CSV z danymi
        """
        self.logger.info("🔄 KROK 2: Analiza danych z DuckDB")
        
        try:
            # Inicjalizuj analyzer DuckDB
            self.analyzer = DuckDBAnalyzer(
                database_path=self.config.duckdb_file,
                logger=self.logger
            )
            
            # Załaduj dane do DuckDB
            self.analyzer.load_csv_data(csv_file, table_name="retail_sales")
            
            # Wykonaj podstawowe analizy
            results = {}
            
            # 1. Przegląd danych
            results['overview'] = self.analyzer.get_data_overview()
            
            # 2. Analiza sprzedaży w czasie
            results['time_analysis'] = self.analyzer.analyze_sales_trends()
            
            # 3. Analiza kategorii produktów
            results['category_analysis'] = self.analyzer.analyze_product_categories()
            
            # 4. Segmentacja klientów
            results['customer_analysis'] = self.analyzer.analyze_customer_segments()
            
            # 5. Analiza rentowności
            results['profitability'] = self.analyzer.analyze_profitability()
            
            self.logger.info("✅ Analiza danych zakończona pomyślnie")
            return results
            
        except Exception as e:
            self.logger.error(f"❌ Błąd podczas analizy danych: {e}")
            raise
    
    def step_3_generate_reports(self, analysis_results):
        """
        Krok 3: Generacja raportów i wizualizacji
        
        Args:
            analysis_results (dict): Wyniki analiz
        """
        self.logger.info("🔄 KROK 3: Generacja raportów")
        
        try:
            # Generuj raporty
            reports_generated = []
            
            # 1. Raport tekstowy
            text_report = self.analyzer.generate_text_report(analysis_results)
            report_file = self.config.reports_dir / f"retail_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(text_report)
            reports_generated.append(report_file)
            
            # 2. Wizualizacje
            charts_created = self.analyzer.create_visualizations(
                analysis_results, 
                output_dir=self.config.figures_dir
            )
            reports_generated.extend(charts_created)
            
            self.logger.info(f"✅ Wygenerowano {len(reports_generated)} plików:")
            for report in reports_generated:
                self.logger.info(f"   📄 {report}")
            
            return reports_generated
            
        except Exception as e:
            self.logger.error(f"❌ Błąd podczas generacji raportów: {e}")
            raise
    
    def run_full_pipeline(self, num_records=None, force_regenerate=False):
        """
        Uruchom cały pipeline projektu
        
        Args:
            num_records (int): Liczba rekordów do wygenerowania
            force_regenerate (bool): Czy wymuszać regenerację danych
        """
        start_time = datetime.now()
        self.logger.info("🚀 ROZPOCZYNAM PEŁNY PIPELINE PROJEKTU")
        self.logger.info("="*60)
        
        try:
            # Krok 1: Generacja danych
            csv_file = self.step_1_generate_data(num_records, force_regenerate)
            
            # Krok 2: Analiza z DuckDB
            analysis_results = self.step_2_analyze_data(csv_file)
            
            # Krok 3: Raporty i wizualizacje
            reports = self.step_3_generate_reports(analysis_results)
            
            # Podsumowanie
            end_time = datetime.now()
            duration = end_time - start_time
            
            self.logger.info("="*60)
            self.logger.info("🎉 PIPELINE ZAKOŃCZONY POMYŚLNIE!")
            self.logger.info(f"⏱️  Czas wykonania: {duration}")
            self.logger.info(f"📊 Plik danych: {csv_file}")
            self.logger.info(f"🗃️  Baza DuckDB: {self.config.duckdb_file}")
            self.logger.info(f"📈 Raporty: {len(reports)} plików w {self.config.reports_dir}")
            self.logger.info("="*60)
            
        except Exception as e:
            self.logger.error(f"💥 PIPELINE PRZERWANY: {e}")
            raise
        finally:
            # Zamknij połączenie z DuckDB
            if self.analyzer:
                self.analyzer.close()

def main():
    """Główna funkcja uruchamiająca"""
    
    # Argumenty linii poleceń
    parser = argparse.ArgumentParser(
        description="DuckDB Retail Analytics - System analizy danych sprzedaży",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Przykłady użycia:
  python main.py                          # Uruchom pełny pipeline
  python main.py --records 50000          # Wygeneruj 50k rekordów
  python main.py --regenerate             # Wymuszaj regenerację danych
  python main.py --step generate          # Tylko generacja danych
  python main.py --step analyze           # Tylko analiza danych
        """
    )
    
    parser.add_argument(
        '--records', '-r',
        type=int,
        default=10000,
        help='Liczba rekordów do wygenerowania (default: 10000)'
    )
    
    parser.add_argument(
        '--regenerate',
        action='store_true',
        help='Wymuszaj regenerację danych nawet jeśli już istnieją'
    )
    
    parser.add_argument(
        '--step', '-s',
        choices=['generate', 'analyze', 'reports', 'full'],
        default='full',
        help='Który krok uruchomić (default: full - wszystkie kroki)'
    )
    
    parser.add_argument(
        '--config', '-c',
        type=str,
        help='Ścieżka do pliku konfiguracyjnego'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Szczegółowe informacje debug'
    )
    
    args = parser.parse_args()
    
    try:
        # Inicjalizuj orchestrator
        orchestrator = ProjectOrchestrator(config_path=args.config)
        
        if args.verbose:
            orchestrator.logger.setLevel(logging.DEBUG)
        
        # Wykonaj odpowiedni krok
        if args.step == 'generate':
            orchestrator.step_1_generate_data(args.records, args.regenerate)
        elif args.step == 'analyze':
            csv_file = orchestrator.config.raw_data_dir / "retail_sales_data.csv"
            if not csv_file.exists():
                print("❌ Brak danych! Uruchom najpierw: python main.py --step generate")
                sys.exit(1)
            orchestrator.step_2_analyze_data(str(csv_file))
        elif args.step == 'reports':
            print("🔄 Funkcja raportów będzie dostępna po implementacji analizy")
        else:  # full
            orchestrator.run_full_pipeline(args.records, args.regenerate)
            
    except KeyboardInterrupt:
        print("\n⚠️  Pipeline przerwany przez użytkownika")
        sys.exit(1)
    except Exception as e:
        print(f"💥 Błąd: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()