#data_normalizer.py:
from typing import Dict
from database import DatabaseManager

class DataNormalizer:
    def __init__(self, db: DatabaseManager):
        self.db = db
        self._location_cache = None
        self._service_cache = None
    
    async def normalize_filters(self, filters: Dict) -> Dict:
        normalized = {}
        
        for column, value in filters.items():
            if isinstance(value, str):
                if column == 'report_date':
                    normalized[column] = self._normalize_report_date(value)
                elif column == 'fullyears' and value.replace('.', '').isdigit():
                    normalized[column] = float(value)
                elif column == 'sex':
                    normalized[column] = value.upper()
                elif column == 'location_name':
                    normalized[column] = await self._find_location_match(value)
                elif column == 'service':
                    normalized[column] = await self._normalize_service_name(value)
                elif column == 'experience' and any(op in value for op in ['>', '<', '>=', '<=']):
                    normalized[column] = value
                elif column == 'fullyears' and any(op in value for op in ['>', '<', '>=', '<=']):
                    normalized[column] = value
                elif column == 'location_name' and '%' in value:
                    normalized[column] = value
                elif column == 'fte' and value.replace('.', '').isdigit():
                    normalized[column] = float(value)
                else:
                    normalized[column] = value
            else:
                normalized[column] = value
        
        return normalized
    
    def _normalize_report_date(self, date_str: str) -> str:
        """Нормализация дат отчета к реальным значениям из БД"""
        date_lower = date_str.lower()
        
        if 'август' in date_lower and '2025' in date_lower:
            return '2025-08-31'
        elif 'сентябрь' in date_lower and '2025' in date_lower:
            return '2025-09-03'
        elif 'июль' in date_lower and '2025' in date_lower:
            return '2025-07-31'
        elif len(date_str) == 7:  # '2025-08'
            if date_str == '2025-08':
                return '2025-08-31'
            elif date_str == '2025-09':
                return '2025-09-03'
            elif date_str == '2025-07':
                return '2025-07-31'
        
        return date_str
    
    async def _find_location_match(self, location_query: str) -> str:
        try:
            if self._location_cache is None:
                await self._load_location_cache()
            
            location_lower = location_query.lower()
            
            if '%' in location_query:
                return location_query
            
            if any(keyword in location_lower for keyword in ['дистан', 'удален', 'remote']):
                remote_location = await self._find_remote_location()
                if remote_location:
                    return remote_location
            
            if 'москв' in location_lower:
                moscow_location = await self._find_moscow_location()
                if moscow_location:
                    return moscow_location
            
            if any(keyword in location_lower for keyword in ['питер', 'петербург', 'спб']):
                spb_location = await self._find_spb_location()
                if spb_location:
                    return spb_location
            
            for location in self._location_cache:
                if location and location_lower == str(location).lower():
                    return location
            
            for location in self._location_cache:
                if location and location_lower in str(location).lower():
                    return location
            
            return location_query
            
        except Exception:
            return location_query
    
    async def _normalize_service_name(self, service_name: str) -> str:
        try:
            if self._service_cache is None:
                await self._load_service_cache()
            
            service_lower = service_name.lower()
            
            for service in self._service_cache:
                if service and service_lower == str(service).lower():
                    return service
            
            for service in self._service_cache:
                if service and service_lower in str(service).lower():
                    return service
            
            service_mapping = {
                'облако': 'Облако',
                'облаке': 'Облако',
                'cloud': 'Облако',
                'такси': 'Такси',
                'маркет': 'Маркет',
                'лавка': 'Лавка',
                'крауд': 'Крауд',
                'финтех': 'Финтех',
                'еда': 'Еда',
                'доставка': 'Доставка',
                'коммерческий': 'Коммерческий департамент',
                'коммерческий департамент': 'Коммерческий департамент',
                'общие': 'Общие подразделения',
                'общие подразделения': 'Общие подразделения'
            }
            
            for key, value in service_mapping.items():
                if key in service_lower:
                    return value
            
            return service_name
            
        except Exception:
            return service_name
    
    async def _load_location_cache(self) -> None:
        try:
            query = "SELECT DISTINCT location_name FROM hr_data_clean WHERE location_name IS NOT NULL"
            result = self.db.execute_query(query)
            self._location_cache = result['location_name'].tolist()
        except Exception:
            self._location_cache = []
    
    async def _load_service_cache(self) -> None:
        try:
            query = "SELECT DISTINCT service FROM hr_data_clean WHERE service IS NOT NULL"
            result = self.db.execute_query(query)
            self._service_cache = result['service'].tolist()
        except Exception:
            self._service_cache = []
    
    async def _find_remote_location(self) -> str:
        """Поиск локации для удаленных сотрудников"""
        try:
            if self._location_cache is None:
                await self._load_location_cache()
            
            return "%Дистанционщик%"
            
        except Exception:
            return "%Дистанционщик%"
    
    async def _find_moscow_location(self) -> str:
        """Поиск локации для Москвы"""
        try:
            if self._location_cache is None:
                await self._load_location_cache()
            
            return "%Москва%"
            
        except Exception:
            return "%Москва%"
    
    async def _find_spb_location(self) -> str:
        """Поиск локации для Санкт-Петербурга"""
        try:
            if self._location_cache is None:
                await self._load_location_cache()
            
            return "%Санкт-Петербург%"
            
        except Exception:
            return "%Санкт-Петербург%"
    
    async def find_remote_workers_location(self) -> str:
        """Метод для использования в других классах"""
        return await self._find_remote_location()
    
    async def normalize_age_filter(self, age_query: str) -> float:
        """Нормализация фильтров возраста"""
        try:
            import re
            if isinstance(age_query, str) and any(op in age_query for op in ['>', '<', '>=', '<=']):
                return age_query
            
            numbers = re.findall(r'\d+', age_query)
            if numbers:
                return float(numbers[0])
            return float(age_query)
        except:
            return 30.0
    
    async def normalize_experience_filter(self, experience_query: str) -> str:
        """Нормализация фильтров опыта работы"""
        try:
            if isinstance(experience_query, (int, float)):
                return str(experience_query)
            
            if isinstance(experience_query, str) and any(op in experience_query for op in ['>', '<', '>=', '<=']):
                return experience_query
            
            experience_lower = str(experience_query).lower()
            
            # Обработка текстовых описаний
            if 'месяц' in experience_lower:
                numbers = re.findall(r'\d+', experience_lower)
                if numbers:
                    return numbers[0]
            elif 'лет' in experience_lower:
                numbers = re.findall(r'\d+', experience_lower)
                if numbers:
                    return str(int(numbers[0]) * 12)
            elif 'год' in experience_lower:
                numbers = re.findall(r'\d+', experience_lower)
                if numbers:
                    return str(int(numbers[0]) * 12)
            
            return experience_query
        except:
            return experience_query
    
    async def normalize_fte_filter(self, fte_query: str) -> float:
        """Нормализация фильтров ставок"""
        try:
            if isinstance(fte_query, (int, float)):
                return float(fte_query)
            
            fte_lower = str(fte_query).lower()
            
            if '0.5' in fte_lower or 'половин' in fte_lower:
                return 0.5
            elif '0.2' in fte_lower:
                return 0.2
            elif '1.0' in fte_lower or 'полн' in fte_lower:
                return 1.0
            elif '0.0' in fte_lower or 'нулев' in fte_lower:
                return 0.0
            elif '0.37' in fte_lower:
                return 0.37
            elif '0.4' in fte_lower:
                return 0.4
            elif '0.75' in fte_lower:
                return 0.75
            
            # Попытка извлечь число
            numbers = re.findall(r'\d+\.?\d*', fte_lower)
            if numbers:
                fte_value = float(numbers[0])
                # Проверяем допустимые значения ставок
                if fte_value in [0.0, 0.2, 0.37, 0.4, 0.5, 0.75, 1.0]:
                    return fte_value
            
            return 0.5  # Значение по умолчанию
        except:
            return 0.5
    
    async def is_valid_fte_value(self, fte_value: float) -> bool:
        """Проверка что значение ставки допустимо"""
        valid_fte_values = [0.0, 0.2, 0.37, 0.4, 0.5, 0.75, 1.0]
        return fte_value in valid_fte_values
    
    async def get_unique_values(self, column_name: str) -> list:
        """Получение уникальных значений колонки"""
        try:
            query = f"SELECT DISTINCT {column_name} FROM hr_data_clean WHERE {column_name} IS NOT NULL ORDER BY {column_name}"
            result = self.db.execute_query(query)
            return result[column_name].tolist()
        except Exception:
            return []
    
    async def column_exists(self, column_name: str) -> bool:
        """Проверка существования колонки в таблице"""
        try:
            query = "PRAGMA table_info(hr_data_clean)"
            result = self.db.execute_query(query)
            columns = result['name'].tolist()
            return column_name in columns
        except Exception:
            return False
    
    async def get_column_type(self, column_name: str) -> str:
        """Получение типа колонки"""
        try:
            query = "PRAGMA table_info(hr_data_clean)"
            result = self.db.execute_query(query)
            column_info = result[result['name'] == column_name]
            return column_info['type'].iloc[0] if not column_info.empty else 'TEXT'
        except Exception:
            return 'TEXT'