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
from typing import Optional, Dict, Any
import json

class ConfigManager:
    """
    Menedżer konfiguracji projektu
    """
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Inicjalizacja menedżera konfiguracji
        
        Args:
            config_file: Opcjonalna ścieżka do pliku konfiguracyjnego JSON
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
        
        # Ustawienia wizualizacji
        self.figure_dpi = 300
        self.figure_format = 'png'
        
        # Załaduj konfigurację z pliku jeśli podano
        if config_file:
            self.load_config(config_file)
    
    def load_config(self, config_file: str) -> None:
        """
        Załaduj konfigurację z pliku JSON
        
        Args:
            config_file: Ścieżka do pliku konfiguracyjnego
        """
        config_path = Path(config_file)
        if not config_path.exists():
            raise FileNotFoundError(f"Plik konfiguracyjny nie istnieje: {config_path}")
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            # Aktualizuj parametry na podstawie pliku
            for key, value in config_data.items():
                if hasattr(self, key):
                    # Konwertuj ścieżki na obiekty Path
                    if 'dir' in key or 'file' in key:
                        value = self.project_root / value if not Path(value).is_absolute() else Path(value)