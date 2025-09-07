#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Config Manager - Zarządzanie konfiguracją projektu
================================================

Centralne zarządzanie wszystkimi parametrami i ścieżkami projektu.

Autor: [Twoje Imię]
Data: 2025-09-07
"""

import os
from pathlib import Path
from datetime import datetime, date
from typing import Optional, Dict, Any, Union
import json
import yaml
import logging

class ConfigManager:
    """
    Menedżer konfiguracji projektu
    """
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Inicjalizacja menedżera konfiguracji
        
        Args:
            config_file: Opcjonalna ścieżka do pliku konfiguracyjnego JSON/YAML
        """
        self.project_root = Path(__file__).parent.parent.parent
        
        # Domyślne ścieżki
        self.data_dir = self.project_root / "data"
        self.raw_data_dir = self.data_dir / "raw"
        self.processed_data_dir = self.data_dir / "processed"
        self.sample_data_dir = self.data_dir / "sample"
        
        self.src_dir = self.project_root / "src"
        self.notebooks_dir = self.project_root / "notebooks"
        self.docs_dir = self.project_root / "docs"
        self.tests_dir = self.project_root / "tests"
        
        self.output_dir = self.project_root / "output"
        self.reports_dir = self.output_dir / "reports"
        self.figures_dir = self.output_dir / "figures"
        
        # Parametry domyślne
        self.default_num_records = 10000
        self.default_start_date = "2023-01-01"
        self.default_end_date = "2024-12-31"
        
        # Ustawienia DuckDB
        self.duckdb_file = self.data_dir / "retail_analytics.duckdb"
        self.duckdb_memory_limit = "2GB"
        self.duckdb_threads = 4
        
        # Ustawienia wizualizacji
        self.figure_dpi = 300
        self.figure_format = 'png'
        self.figure_size = (12, 8)
        
        # Ustawienia logowania
        self.log_level = "INFO"
        self.log_file = self.project_root / "logs" / "analytics.log"
        
        # Ustawienia przetwarzania danych
        self.chunk_size = 10000
        self.max_memory_usage = "1GB"
        
        # Załaduj konfigurację z pliku jeśli podano
        if config_file:
            self.load_config(config_file)
        
        # Upewnij się, że katalogi istnieją
        self.ensure_directories()
    
    def load_config(self, config_file: str) -> None:
        """
        Załaduj konfigurację z pliku JSON lub YAML
        
        Args:
            config_file: Ścieżka do pliku konfiguracyjnego
        """
        config_path = Path(config_file)
        if not config_path.exists():
            raise FileNotFoundError(f"Plik konfiguracyjny nie istnieje: {config_path}")
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                if config_path.suffix.lower() in ['.yaml', '.yml']:
                    config_data = yaml.safe_load(f)
                else:
                    config_data = json.load(f)
            
            # Aktualizuj parametry na podstawie pliku
            for key, value in config_data.items():
                if hasattr(self, key):
                    # Konwertuj ścieżki na obiekty Path
                    if 'dir' in key or 'file' in key:
                        value = self.project_root / value if not Path(value).is_absolute() else Path(value)
                    setattr(self, key, value)
                    
        except (json.JSONDecodeError, yaml.YAMLError) as e:
            raise ValueError(f"Błąd parsowania pliku konfiguracyjnego: {e}")
    
    def save_config(self, config_file: str) -> None:
        """
        Zapisz aktualną konfigurację do pliku
        
        Args:
            config_file: Ścieżka do pliku konfiguracyjnego
        """
        config_data = self.to_dict()
        config_path = Path(config_file)
        
        # Utwórz katalog jeśli nie istnieje
        config_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(config_path, 'w', encoding='utf-8') as f:
                if config_path.suffix.lower() in ['.yaml', '.yml']:
                    yaml.safe_dump(config_data, f, default_flow_style=False, allow_unicode=True)
                else:
                    json.dump(config_data, f, indent=2, ensure_ascii=False, default=str)
                    
        except (IOError, yaml.YAMLError) as e:
            raise ValueError(f"Błąd zapisywania pliku konfiguracyjnego: {e}")
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Konwertuj konfigurację do słownika
        
        Returns:
            Słownik z konfiguracją
        """
        config_dict = {}
        
        # Pobierz wszystkie atrybuty publiczne
        for key, value in self.__dict__.items():
            if not key.startswith('_'):
                # Konwertuj Path na string dla serializacji
                if isinstance(value, Path):
                    # Używaj ścieżek relatywnych do project_root
                    try:
                        value = value.relative_to(self.project_root)
                    except ValueError:
                        # Jeśli nie można zrobić relatywnej, użyj absolute
                        value = str(value)
                config_dict[key] = value
        
        return config_dict
    
    def ensure_directories(self) -> None:
        """
        Upewnij się, że wszystkie potrzebne katalogi istnieją
        """
        directories = [
            self.data_dir,
            self.raw_data_dir,
            self.processed_data_dir,
            self.sample_data_dir,
            self.output_dir,
            self.reports_dir,
            self.figures_dir,
            self.log_file.parent
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def get_dated_filename(self, base_name: str, extension: str = None) -> str:
        """
        Generuj nazwę pliku z datą
        
        Args:
            base_name: Podstawowa nazwa pliku
            extension: Rozszerzenie pliku (opcjonalne)
            
        Returns:
            Nazwa pliku z datą
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if extension:
            return f"{base_name}_{timestamp}.{extension}"
        return f"{base_name}_{timestamp}"
    
    def get_database_config(self) -> Dict[str, Any]:
        """
        Pobierz konfigurację bazy danych
        
        Returns:
            Słownik z konfiguracją DuckDB
        """
        return {
            'database_file': str(self.duckdb_file),
            'memory_limit': self.duckdb_memory_limit,
            'threads': self.duckdb_threads
        }
    
    def get_visualization_config(self) -> Dict[str, Any]:
        """
        Pobierz konfigurację wizualizacji
        
        Returns:
            Słownik z konfiguracją wykresów
        """
        return {
            'dpi': self.figure_dpi,
            'format': self.figure_format,
            'figsize': self.figure_size,
            'output_dir': str(self.figures_dir)
        }
    
    def get_processing_config(self) -> Dict[str, Any]:
        """
        Pobierz konfigurację przetwarzania danych
        
        Returns:
            Słownik z konfiguracją przetwarzania
        """
        return {
            'chunk_size': self.chunk_size,
            'max_memory_usage': self.max_memory_usage,
            'num_records': self.default_num_records,
            'start_date': self.default_start_date,
            'end_date': self.default_end_date
        }
    
    def setup_logging(self) -> logging.Logger:
        """
        Skonfiguruj system logowania
        
        Returns:
            Skonfigurowany logger
        """
        # Upewnij się, że katalog na logi istnieje
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Konfiguracja loggera
        logger = logging.getLogger('retail_analytics')
        logger.setLevel(getattr(logging, self.log_level.upper()))
        
        # Usuń istniejące handlery
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Handler dla pliku
        file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        
        # Handler dla konsoli
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            '%(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def validate_config(self) -> bool:
        """
        Sprawdź poprawność konfiguracji
        
        Returns:
            True jeśli konfiguracja jest poprawna
        """
        try:
            # Sprawdź czy project_root istnieje
            if not self.project_root.exists():
                raise ValueError(f"Katalog główny projektu nie istnieje: {self.project_root}")
            
            # Sprawdź daty
            datetime.strptime(self.default_start_date, "%Y-%m-%d")
            datetime.strptime(self.default_end_date, "%Y-%m-%d")
            
            # Sprawdź parametry numeryczne
            if self.default_num_records <= 0:
                raise ValueError("Liczba rekordów musi być większa od 0")
            
            if self.figure_dpi <= 0:
                raise ValueError("DPI musi być większe od 0")
            
            return True
            
        except Exception as e:
            logging.error(f"Błąd walidacji konfiguracji: {e}")
            return False
    
    def __str__(self) -> str:
        """String representation konfiguracji"""
        return f"ConfigManager(project_root='{self.project_root}')"
    
    def __repr__(self) -> str:
        """Detailed representation konfiguracji"""
        return f"ConfigManager(project_root='{self.project_root}', duckdb_file='{self.duckdb_file}')"


# Fabryka dla łatwego tworzenia instancji
def create_config_manager(config_file: Optional[str] = None) -> ConfigManager:
    """
    Fabryka do tworzenia ConfigManager
    
    Args:
        config_file: Opcjonalna ścieżka do pliku konfiguracyjnego
        
    Returns:
        Skonfigurowany ConfigManager
    """
    return ConfigManager(config_file)


# Przykład użycia
if __name__ == "__main__":
    # Tworzenie domyślnego config managera
    config = ConfigManager()
    
    # Walidacja konfiguracji
    if config.validate_config():
        print("✅ Konfiguracja jest poprawna")
        
        # Wyświetl podstawowe informacje
        print(f"Projekt: {config.project_root}")
        print(f"Baza danych: {config.duckdb_file}")
        print(f"Katalog wyników: {config.output_dir}")
        
        # Zapisz przykładową konfigurację
        config.save_config(config.project_root / "config" / "default_config.yaml")
        print("📁 Zapisano domyślną konfigurację")
        
        # Skonfiguruj logging
        logger = config.setup_logging()
        logger.info("Config Manager zainicjalizowany pomyślnie")
        
    else:
        print("❌ Błąd w konfiguracji")