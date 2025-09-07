#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Python Orchestrator - Główny orkestrator projektu
=====================================================

Centralne zarządzanie i koordynacja wszystkich komponentów projektu
analizy danych retailowych z wykorzystaniem DuckDB.

Autor: Michał Gubera
Data: 2025-09-07
"""

import sys
import argparse
from pathlib import Path
from typing import Optional, List, Dict, Any
import logging

# Dodaj src do PYTHONPATH
project_root = Path(__file__).parent
src_path = project_root / 'src'
sys.path.insert(0, str(src_path))

# Import zależny od struktury katalogów
try:
    from src.config_manager import ConfigManager, create_config_manager
except ImportError:
    try:
        from config_manager import ConfigManager, create_config_manager
    except ImportError:
        print("❌ Nie można znaleźć config_manager.py")
        print("Sprawdź czy plik istnieje w katalogu 'src/' lub głównym katalogu")
        sys.exit(1)

class RetailAnalyticsOrchestrator:
    """
    Główny orkestrator projektu analizy danych retailowych
    """
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Inicjalizacja orchestratora
        
        Args:
            config_file: Ścieżka do pliku konfiguracyjnego (domyślnie: config/config.yaml)
        """
        # Domyślny plik konfiguracyjny
        if config_file is None:
            config_file = Path(__file__).parent / "config" / "config.yaml"
            
        # Inicjalizacja ConfigManagera
        try:
            self.config = create_config_manager(str(config_file) if config_file.exists() else None)
            self.logger = self.config.setup_logging()
            self.logger.info("🚀 Retail Analytics Orchestrator zainicjalizowany")
        except Exception as e:
            print(f"❌ Błąd inicjalizacji konfiguracji: {e}")
            # Fallback do domyślnej konfiguracji
            self.config = create_config_manager()
            self.logger = self.config.setup_logging()
            self.logger.warning("⚠️  Używam domyślnej konfiguracji")
        
        # Status komponentów
        self.components_status = {
            'config': True,
            'database': False,
            'data_generator': False,
            'data_processor': False,
            'analyzer': False,
            'visualizer': False
        }
        
        # Cache dla importowanych modułów
        self._modules = {}
        
    def _import_module(self, module_name: str, class_name: str = None):
        """
        Bezpieczny import modułu z cache
        
        Args:
            module_name: Nazwa modułu do importu
            class_name: Nazwa klasy do importu (opcjonalne)
            
        Returns:
            Zaimportowany moduł lub klasa
        """
        cache_key = f"{module_name}.{class_name}" if class_name else module_name
        
        if cache_key in self._modules:
            return self._modules[cache_key]
        
        try:
            module = __import__(module_name, fromlist=[class_name] if class_name else [])
            result = getattr(module, class_name) if class_name else module
            self._modules[cache_key] = result
            return result
        except ImportError as e:
            self.logger.error(f"❌ Nie można zaimportować {cache_key}: {e}")
            return None
    
    def initialize_database(self) -> bool:
        """
        Inicjalizacja połączenia z bazą danych DuckDB
        
        Returns:
            True jeśli inicjalizacja się powiodła
        """
        try:
            # Import modułu bazy danych (gdy będzie gotowy)
            db_manager_class = self._import_module('src.database.db_manager', 'DatabaseManager')
            
            if db_manager_class:
                db_config = self.config.get_database_config()
                self.db_manager = db_manager_class(**db_config)
                self.components_status['database'] = True
                self.logger.info("✅ Baza danych zainicjalizowana")
                return True
            else:
                self.logger.warning("⚠️  Moduł DatabaseManager niedostępny")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Błąd inicjalizacji bazy danych: {e}")
            return False
    
    def initialize_data_generator(self) -> bool:
        """
        Inicjalizacja generatora danych
        
        Returns:
            True jeśli inicjalizacja się powiodła
        """
        try:
            # Import generatora danych (gdy będzie gotowy)
            generator_class = self._import_module('src.data_generation.data_generator', 'DataGenerator')
            
            if generator_class:
                gen_config = self.config.get_processing_config()
                self.data_generator = generator_class(self.config)
                self.components_status['data_generator'] = True
                self.logger.info("✅ Generator danych zainicjalizowany")
                return True
            else:
                self.logger.warning("⚠️  Moduł DataGenerator niedostępny")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Błąd inicjalizacji generatora danych: {e}")
            return False
    
    def initialize_data_processor(self) -> bool:
        """
        Inicjalizacja procesora danych
        
        Returns:
            True jeśli inicjalizacja się powiodła
        """
        try:
            processor_class = self._import_module('src.data_processing.data_processor', 'DataProcessor')
            
            if processor_class:
                self.data_processor = processor_class(self.config)
                self.components_status['data_processor'] = True
                self.logger.info("✅ Procesor danych zainicjalizowany")
                return True
            else:
                self.logger.warning("⚠️  Moduł DataProcessor niedostępny")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Błąd inicjalizacji procesora danych: {e}")
            return False
    
    def initialize_analyzer(self) -> bool:
        """
        Inicjalizacja analizatora danych
        
        Returns:
            True jeśli inicjalizacja się powiodła
        """
        try:
            analyzer_class = self._import_module('src.analysis.analyzer', 'DataAnalyzer')
            
            if analyzer_class:
                self.analyzer = analyzer_class(self.config)
                self.components_status['analyzer'] = True
                self.logger.info("✅ Analizator danych zainicjalizowany")
                return True
            else:
                self.logger.warning("⚠️  Moduł DataAnalyzer niedostępny")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Błąd inicjalizacji analizatora: {e}")
            return False
    
    def initialize_visualizer(self) -> bool:
        """
        Inicjalizacja modułu wizualizacji
        
        Returns:
            True jeśli inicjalizacja się powiodła
        """
        try:
            visualizer_class = self._import_module('src.visualization.visualizer', 'DataVisualizer')
            
            if visualizer_class:
                viz_config = self.config.get_visualization_config()
                self.visualizer = visualizer_class(self.config)
                self.components_status['visualizer'] = True
                self.logger.info("✅ Wizualizator zainicjalizowany")
                return True
            else:
                self.logger.warning("⚠️  Moduł DataVisualizer niedostępny")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Błąd inicjalizacji wizualizatora: {e}")
            return False
    
    def initialize_all_components(self) -> Dict[str, bool]:
        """
        Inicjalizuj wszystkie komponenty systemu
        
        Returns:
            Słownik ze statusem każdego komponentu
        """
        self.logger.info("🔄 Inicjalizacja wszystkich komponentów...")
        
        # Kolejność inicjalizacji jest ważna
        self.initialize_database()
        self.initialize_data_generator()
        self.initialize_data_processor()
        self.initialize_analyzer()
        self.initialize_visualizer()
        
        # Podsumowanie
        successful = sum(self.components_status.values())
        total = len(self.components_status)
        
        self.logger.info(f"📊 Zainicjalizowano {successful}/{total} komponentów")
        
        if successful == total:
            self.logger.info("🎉 Wszystkie komponenty gotowe!")
        else:
            failed = [name for name, status in self.components_status.items() if not status]
            self.logger.warning(f"⚠️  Nie udało się zainicjalizować: {', '.join(failed)}")
        
        return self.components_status
    
    def run_full_pipeline(self, num_records: int = None) -> bool:
        """
        Uruchom pełny pipeline analizy danych
        
        Args:
            num_records: Liczba rekordów do wygenerowania (opcjonalne)
            
        Returns:
            True jeśli pipeline zakończył się sukcesem
        """
        try:
            self.logger.info("🚀 Rozpoczynam pełny pipeline analizy danych")
            
            # 1. Generowanie danych
            if self.components_status.get('data_generator'):
                records = num_records or self.config.default_num_records
                self.logger.info(f"📊 Generowanie {records:,} rekordów danych")
                # self.data_generator.generate_sample_data(records)
            
            # 2. Przetwarzanie danych
            if self.components_status.get('data_processor'):
                self.logger.info("🔄 Przetwarzanie danych")
                # self.data_processor.process_data()
            
            # 3. Analiza danych
            if self.components_status.get('analyzer'):
                self.logger.info("🔍 Analiza danych")
                # results = self.analyzer.run_all_analyses()
            
            # 4. Wizualizacja wyników
            if self.components_status.get('visualizer'):
                self.logger.info("📈 Generowanie wizualizacji")
                # self.visualizer.create_all_charts()
            
            self.logger.info("✅ Pipeline zakończony pomyślnie!")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Błąd w pipeline: {e}")
            return False
    
    def run_analysis_only(self) -> bool:
        """
        Uruchom tylko analizę danych (bez generowania)
        
        Returns:
            True jeśli analiza się powiodła
        """
        try:
            self.logger.info("🔍 Uruchamiam analizę istniejących danych")
            
            if not self.components_status.get('analyzer'):
                self.logger.error("❌ Analizator nie jest zainicjalizowany")
                return False
            
            # Uruchom analizę
            # results = self.analyzer.run_all_analyses()
            
            # Generuj wizualizacje jeśli dostępne
            if self.components_status.get('visualizer'):
                # self.visualizer.create_all_charts()
                pass
            
            self.logger.info("✅ Analiza zakończona!")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Błąd w analizie: {e}")
            return False
    
    def generate_report(self, report_type: str = "full") -> str:
        """
        Wygeneruj raport z analizy
        
        Args:
            report_type: Typ raportu ('full', 'summary', 'charts')
            
        Returns:
            Ścieżka do wygenerowanego raportu
        """
        try:
            self.logger.info(f"📋 Generowanie raportu: {report_type}")
            
            report_filename = self.config.get_dated_filename(f"report_{report_type}", "html")
            report_path = self.config.reports_dir / report_filename
            
            # TODO: Implementacja generowania raportów
            # report_content = self._create_report_content(report_type)
            # with open(report_path, 'w', encoding='utf-8') as f:
            #     f.write(report_content)
            
            self.logger.info(f"📁 Raport zapisany: {report_path}")
            return str(report_path)
            
        except Exception as e:
            self.logger.error(f"❌ Błąd generowania raportu: {e}")
            return ""
    
    def show_status(self) -> None:
        """Wyświetl status wszystkich komponentów"""
        print("\n" + "="*50)
        print("📊 STATUS KOMPONENTÓW SYSTEMU")
        print("="*50)
        
        for component, status in self.components_status.items():
            emoji = "✅" if status else "❌"
            status_text = "AKTYWNY" if status else "NIEAKTYWNY"
            print(f"{emoji} {component.upper():<15} - {status_text}")
        
        print("\n📁 ŚCIEŻKI KONFIGURACJI:")
        print(f"   Projekt: {self.config.project_root}")
        print(f"   Baza danych: {self.config.duckdb_file}")
        print(f"   Wyniki: {self.config.output_dir}")
        print(f"   Logi: {self.config.log_file}")
        print("="*50 + "\n")


def main():
    """Główna funkcja programu"""
    
    # Argumenty linii poleceń
    parser = argparse.ArgumentParser(
        description="Retail Analytics Orchestrator - główny punkt kontroli projektu"
    )
    
    parser.add_argument(
        '--config', '-c',
        type=str,
        help='Ścieżka do pliku konfiguracyjnego'
    )
    
    parser.add_argument(
        '--command',
        choices=['init', 'pipeline', 'analysis', 'report', 'status'],
        default='status',
        help='Komenda do wykonania (domyślnie: status)'
    )
    
    parser.add_argument(
        '--records', '-r',
        type=int,
        help='Liczba rekordów do wygenerowania'
    )
    
    parser.add_argument(
        '--report-type',
        choices=['full', 'summary', 'charts'],
        default='full',
        help='Typ raportu do wygenerowania'
    )
    
    args = parser.parse_args()
    
    try:
        # Inicjalizacja orchestratora
        orchestrator = RetailAnalyticsOrchestrator(args.config)
        
        if args.command == 'init':
            print("🔄 Inicjalizacja komponentów...")
            orchestrator.initialize_all_components()
            orchestrator.show_status()
            
        elif args.command == 'pipeline':
            print("🚀 Uruchamianie pełnego pipeline...")
            orchestrator.initialize_all_components()
            success = orchestrator.run_full_pipeline(args.records)
            if success:
                print("✅ Pipeline zakończony pomyślnie!")
            else:
                print("❌ Pipeline zakończony błędem!")
                
        elif args.command == 'analysis':
            print("🔍 Uruchamianie analizy...")
            orchestrator.initialize_all_components()
            success = orchestrator.run_analysis_only()
            if success:
                print("✅ Analiza zakończona pomyślnie!")
            else:
                print("❌ Analiza zakończona błędem!")
                
        elif args.command == 'report':
            print(f"📋 Generowanie raportu ({args.report_type})...")
            orchestrator.initialize_all_components()
            report_path = orchestrator.generate_report(args.report_type)
            if report_path:
                print(f"✅ Raport zapisany: {report_path}")
            else:
                print("❌ Błąd generowania raportu!")
                
        elif args.command == 'status':
            orchestrator.show_status()
    
    except KeyboardInterrupt:
        print("\n⚠️  Przerwano przez użytkownika")
    except Exception as e:
        print(f"❌ Nieoczekiwany błąd: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()