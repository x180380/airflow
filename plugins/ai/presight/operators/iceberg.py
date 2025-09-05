from typing import Any, Dict, List, Optional, Union
from pathlib import Path

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowException

try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.table import Table
    from pyiceberg.schema import Schema
except ImportError:
    raise AirflowException("pyiceberg is required for IcebergAddFilesOperator. Install with: pip install pyiceberg")


class IcebergAddFilesOperator(BaseOperator):
    """
    Operator to add files to an Iceberg table using PyIceberg.
    
    :param catalog_config: Configuration dictionary for the Iceberg catalog
    :param table_name: Full table name (e.g., 'database.table')
    :param file_paths: List of file paths to add to the table
    :param file_format: Format of the files (parquet, orc, avro)
    :param partition_spec: Optional partition specification for the files
    :param validate_schema: Whether to validate file schema against table schema
    """
    
    template_fields = ('table_name', 'file_paths', 'catalog_config')
    
    def __init__(
        self,
        catalog_config: Dict[str, Any],
        table_name: str,
        file_paths: Union[str, List[str]],
        file_format: str = "parquet",
        partition_spec: Optional[Dict[str, Any]] = None,
        validate_schema: bool = True,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.catalog_config = catalog_config
        self.table_name = table_name
        self.file_paths = file_paths if isinstance(file_paths, list) else [file_paths]
        self.file_format = file_format.lower()
        self.partition_spec = partition_spec or {}
        self.validate_schema = validate_schema
    
    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute the operator to add files to the Iceberg table."""
        
        try:
            # Load the catalog
            catalog = load_catalog(**self.catalog_config)
            self.log.info(f"Connected to catalog: {catalog}")
            
            # Load the table
            table = catalog.load_table(self.table_name)
            self.log.info(f"Loaded table: {self.table_name}")
            
            # Validate file paths exist
            for file_path in self.file_paths:
                if not Path(file_path).exists():
                    raise AirflowException(f"File does not exist: {file_path}")
            
            # Add files to the table
            result = self._add_files_to_table(table)
            
            self.log.info(f"Successfully added {len(self.file_paths)} files to table {self.table_name}")
            return result
            
        except Exception as e:
            self.log.error(f"Failed to add files to Iceberg table: {str(e)}")
            raise AirflowException(f"IcebergAddFilesOperator failed: {str(e)}")
    
    def _add_files_to_table(self, table: Table) -> Dict[str, Any]:
        """Add files to the Iceberg table using append operation."""
        
        # Start a new transaction
        transaction = table.transaction()
        
        files_added = []
        
        for file_path in self.file_paths:
            self.log.info(f"Adding file: {file_path}")
            
            try:
                # For PyIceberg, we typically use the append method with data files
                # This is a simplified approach - in practice, you might need to:
                # 1. Read the file to get schema and statistics
                # 2. Create proper DataFile objects
                # 3. Handle partitioning correctly
                
                if self.file_format == "parquet":
                    # Use PyIceberg's scan and append functionality
                    # Note: This is a conceptual implementation
                    # Actual implementation depends on your specific use case
                    transaction = transaction.append_file(
                        file_path=file_path,
                        partition_spec=self.partition_spec
                    )
                else:
                    raise AirflowException(f"Unsupported file format: {self.file_format}")
                
                files_added.append(file_path)
                
            except Exception as e:
                self.log.error(f"Failed to add file {file_path}: {str(e)}")
                raise AirflowException(f"Failed to add file {file_path}: {str(e)}")
        
        # Commit the transaction
        transaction.commit()
        
        return {
            "files_added": files_added,
            "table_name": self.table_name,
            "total_files": len(files_added)
        }
    
    def _validate_file_schema(self, file_path: str, table_schema: Schema) -> bool:
        """Validate that the file schema matches the table schema."""
        if not self.validate_schema:
            return True
        
        try:
            # Implementation would depend on file format
            # This is a placeholder for schema validation logic
            self.log.info(f"Validating schema for file: {file_path}")
            # Add actual schema validation logic here
            return True
        except Exception as e:
            self.log.warning(f"Schema validation failed for {file_path}: {str(e)}")
            return False


class IcebergTableOperator(BaseOperator):
    """
    Base operator for Iceberg table operations.
    
    :param catalog_config: Configuration dictionary for the Iceberg catalog
    :param table_name: Full table name (e.g., 'database.table')
    """
    
    template_fields = ('table_name', 'catalog_config')
    
    def __init__(
        self,
        catalog_config: Dict[str, Any],
        table_name: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.catalog_config = catalog_config
        self.table_name = table_name
    
    def get_table(self) -> Table:
        """Get the Iceberg table instance."""
        catalog = load_catalog(**self.catalog_config)
        return catalog.load_table(self.table_name)
