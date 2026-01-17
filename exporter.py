"""Enhanced data exporter supporting multiple formats (CSV, JSON, Excel, Parquet)."""
import csv
import json
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any
from enum import Enum

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    logging.warning("pandas not available. Excel and Parquet export disabled.")


class ExportFormat(Enum):
    """Supported export formats."""
    CSV = "csv"
    JSON = "json"
    EXCEL = "xlsx"
    PARQUET = "parquet"


class DataExporter:
    """Unified data exporter with multiple format support."""
    
    def __init__(self, output_dir: str = "output"):
        """Initialize exporter with output directory.
        
        Args:
            output_dir: Directory to save exported files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def export(self, data: List[Dict[str, Any]], filename: str, 
               format: ExportFormat = ExportFormat.CSV) -> Optional[str]:
        """Export data in the specified format.
        
        Args:
            data: List of dictionaries to export
            filename: Output filename (without extension)
            format: Export format (CSV, JSON, Excel, or Parquet)
            
        Returns:
            Path to exported file, or None if export failed
        """
        if not data:
            self.logger.warning("No data to export")
            return None
        
        try:
            if format == ExportFormat.CSV:
                return self._export_csv(data, filename)
            elif format == ExportFormat.JSON:
                return self._export_json(data, filename)
            elif format == ExportFormat.EXCEL:
                return self._export_excel(data, filename)
            elif format == ExportFormat.PARQUET:
                return self._export_parquet(data, filename)
            else:
                self.logger.error(f"Unsupported format: {format}")
                return None
        except Exception as e:
            self.logger.error(f"Export failed: {e}")
            return None
    
    def _export_csv(self, data: List[Dict], filename: str) -> str:
        """Export data to CSV format."""
        filepath = self.output_dir / f"{filename}.csv"
        
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            if data:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
        
        self.logger.info(f"Exported {len(data)} records to {filepath}")
        return str(filepath)
    
    def _export_json(self, data: List[Dict], filename: str) -> str:
        """Export data to JSON format."""
        filepath = self.output_dir / f"{filename}.json"
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Exported {len(data)} records to {filepath}")
        return str(filepath)
    
    def _export_excel(self, data: List[Dict], filename: str) -> Optional[str]:
        """Export data to Excel format."""
        if not PANDAS_AVAILABLE:
            self.logger.error("pandas required for Excel export")
            return None
        
        filepath = self.output_dir / f"{filename}.xlsx"
        df = pd.DataFrame(data)
        df.to_excel(filepath, index=False, engine="openpyxl")
        
        self.logger.info(f"Exported {len(data)} records to {filepath}")
        return str(filepath)
    
    def _export_parquet(self, data: List[Dict], filename: str) -> Optional[str]:
        """Export data to Parquet format."""
        if not PANDAS_AVAILABLE:
            self.logger.error("pandas required for Parquet export")
            return None
        
        filepath = self.output_dir / f"{filename}.parquet"
        df = pd.DataFrame(data)
        df.to_parquet(filepath, index=False, compression="snappy")
        
        self.logger.info(f"Exported {len(data)} records to {filepath}")
        return str(filepath)


# Legacy function for backward compatibility
def export_csv(data: list[dict], filepath: str) -> None:
    """Export data to CSV (legacy function).
    
    Args:
        data: List of dictionaries to export
        filepath: Output file path
    """
    if not data:
        return
    
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
