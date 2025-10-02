#agent_tools.py:
import pandas as pd
from typing import Dict, List, Optional
from database import DatabaseManager
from .query_builder import QueryBuilder
from .data_normalizer import DataNormalizer
from .response_formatter import ResponseFormatter
from .constants import SUPPORTED_METRICS, TIME_SERIES_METRICS
from .agent_tools_complex import ComplexAgentTools

class AgentTools:
    def __init__(self, db_path: str):
        self.db = DatabaseManager(db_path)
        self.normalizer = DataNormalizer(self.db)
        self.query_builder = QueryBuilder()
        self.formatter = ResponseFormatter()
    
    async def get_column_statistics(self, column_name: str, filters: Optional[Dict] = None, user_query: str = "") -> str:
        try:
            valid_columns = await self._get_table_columns()
            if column_name not in valid_columns:
                return f"❌ Колонка '{column_name}' не найдена. Доступные: {', '.join(valid_columns)}"
            
            normalized_filters = await self.normalizer.normalize_filters(filters) if filters else {}
            
            where_conditions = ["report_date = '2025-08-31'"]
            params = []
            
            for column, value in normalized_filters.items():
                if isinstance(value, str) and any(op in value for op in ['>', '<', '>=', '<=']):
                    where_conditions.append(f"{column} {value}")
                elif column == 'location_name' and '%' in str(value):
                    where_conditions.append(f"{column} LIKE ?")
                    params.append(value)
                else:
                    where_conditions.append(f"{column} = ?")
                    params.append(value)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            column_type = await self._get_column_type(column_name)
            
            if column_type == 'REAL':
                query = f"""
                SELECT 
                    COUNT({column_name}) as count,
                    AVG({column_name}) as average,
                    MIN({column_name}) as min,
                    MAX({column_name}) as max,
                    SUM({column_name}) as sum
                FROM hr_data_clean 
                {where_clause}
                """
                result = self.db.execute_query(query, params)
                return self.formatter.format_numeric_statistics(result, column_name, user_query)
            else:
                query = f"""
                SELECT 
                    {column_name},
                    COUNT(*) as count
                FROM hr_data_clean 
                {where_clause}
                GROUP BY {column_name}
                ORDER BY count DESC
                LIMIT 20
                """
                result = self.db.execute_query(query, params)
                return self.formatter.format_categorical_statistics(result, column_name)
            
        except Exception as e:
            return f"❌ Ошибка при получении статистики: {str(e)}"
    
    async def calculate_metric(self, metric: str, filters: Optional[Dict] = None) -> str:
        metric = metric.lower() if metric else ""
        try:
            if metric == 'full_time_ratio':
                return await self._calculate_full_time_ratio(filters)
            
            complex_metrics = ['young_workers', 'experienced_workers']
            if metric in complex_metrics:
                complex_tools = ComplexAgentTools(self.db.db_path)
                return await complex_tools.calculate_complex_metric(metric, filters)
            
            if metric not in SUPPORTED_METRICS:
                return f"❌ Метрика '{metric}' не поддерживается. Доступные: {', '.join(SUPPORTED_METRICS)}"
            
            normalized_filters = await self.normalizer.normalize_filters(filters) if filters else {}
            
            if filters:
                for key, value in filters.items():
                    if isinstance(value, str) and any(op in value for op in ['>', '<', '>=', '<=']):
                        normalized_filters[key] = value
                    if key == 'location_name' and '%' in str(value):
                        normalized_filters[key] = value
            
            where_conditions = []
            params = []
            
            where_conditions.append("report_date = '2025-08-31'")
            
            for column, value in normalized_filters.items():
                if isinstance(value, str) and any(op in value for op in ['>', '<', '>=', '<=']):
                    where_conditions.append(f"{column} {value}")
                elif column == 'location_name' and '%' in str(value):
                    where_conditions.append(f"{column} LIKE ?")
                    params.append(value)
                else:
                    where_conditions.append(f"{column} = ?")
                    params.append(value)
            
            if metric in ['average_experience', 'average_age', 'average_fte']:
                where_conditions.append("fire_from_company = '1970-01-01'")
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            if metric == 'headcount':
                query = f"SELECT COUNT(*) as count FROM hr_data_clean {where_clause}"
            elif metric == 'turnover_rate':
                query = f"""
                SELECT 
                    COUNT(*) as total,
                    SUM(firecount) as fired,
                    CASE 
                        WHEN COUNT(*) > 0 THEN ROUND(CAST(SUM(firecount) AS FLOAT) / COUNT(*) * 100, 2)
                        ELSE 0 
                    END as turnover_rate
                FROM hr_data_clean 
                {where_clause}
                """
                print(f"🔍 TURNOVER SQL: {query}")
                print(f"🔍 TURNOVER PARAMS: {params}")
            elif metric == 'average_experience':
                query = f"SELECT AVG(experience) as avg_exp FROM hr_data_clean {where_clause}"
            elif metric == 'average_age':
                query = f"SELECT AVG(fullyears) as avg_age FROM hr_data_clean {where_clause}"
            elif metric == 'average_fte':
                query = f"SELECT AVG(fte) as avg_fte FROM hr_data_clean {where_clause}"
            elif metric == 'total_fired':
                query = f"SELECT SUM(firecount) as total_fired FROM hr_data_clean {where_clause}"
            elif metric == 'total_hired':
                query = f"SELECT SUM(hirecount) as total_hired FROM hr_data_clean {where_clause}"
            elif metric == 'fte_distribution':
                return await self._calculate_fte_distribution(normalized_filters)
            elif metric == 'remote_workers':
                return await self._calculate_remote_workers(normalized_filters)
            
            result = self.db.execute_query(query, params)
            return self._format_metric_result(metric, result)
                
        except Exception as e:
            return f"❌ Ошибка при расчете метрики '{metric}': {str(e)}"
    
    async def _calculate_full_time_ratio(self, filters: Optional[Dict] = None) -> str:
        try:
            where_conditions = ["report_date = '2025-08-31'"]
            params = []
            
            if filters:
                for column, value in filters.items():
                    if column == 'location_name' and '%' in str(value):
                        where_conditions.append(f"{column} LIKE ?")
                        params.append(value)
                    else:
                        where_conditions.append(f"{column} = ?")
                        params.append(value)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            total_query = f"SELECT COUNT(*) as total FROM hr_data_clean {where_clause}"
            total_result = self.db.execute_query(total_query, params)
            total_count = total_result['total'].iloc[0] if len(total_result) > 0 else 0
            
            full_time_query = f"SELECT COUNT(*) as full_time FROM hr_data_clean {where_clause} AND fte = 1.0"
            full_time_result = self.db.execute_query(full_time_query, params)
            full_time_count = full_time_result['full_time'].iloc[0] if len(full_time_result) > 0 else 0
            
            if total_count == 0:
                return "❌ Нет данных для расчета доли полных ставок"
            
            percentage = (full_time_count / total_count) * 100
            
            return f"💰 Доля полных ставок: {percentage:.1f}% ({full_time_count:,} из {total_count:,} сотрудников)".replace(',', ' ')
            
        except Exception as e:
            return f"❌ Ошибка при расчете доли полных ставок: {str(e)}"
    
    async def _calculate_fte_distribution(self, filters: Dict) -> str:
        try:
            where_conditions = ["report_date = '2025-08-31'"]
            params = []
            
            for column, value in filters.items():
                if column == 'location_name' and '%' in str(value):
                    where_conditions.append(f"{column} LIKE ?")
                    params.append(value)
                else:
                    where_conditions.append(f"{column} = ?")
                    params.append(value)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"""
            SELECT 
                fte,
                COUNT(*) as count
            FROM hr_data_clean 
            {where_clause}
            GROUP BY fte
            ORDER BY fte
            """
            
            result = self.db.execute_query(query, params)
            return self._format_fte_distribution_result(result)
            
        except Exception as e:
            return f"❌ Ошибка при расчете распределения ставок: {str(e)}"
    
    async def _calculate_remote_workers(self, filters: Dict) -> str:
        try:
            where_conditions = [
                "report_date = '2025-08-31'",
                "location_name LIKE '%Дистанционщик%'"
            ]
            params = []
            
            for column, value in filters.items():
                if column != 'location_name':
                    if column == 'location_name' and '%' in str(value):
                        where_conditions.append(f"{column} LIKE ?")
                        params.append(value)
                    else:
                        where_conditions.append(f"{column} = ?")
                        params.append(value)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"SELECT COUNT(*) as count FROM hr_data_clean {where_clause}"
            result = self.db.execute_query(query, params)
            
            total_query = "SELECT COUNT(*) as total FROM hr_data_clean WHERE report_date = '2025-08-31'"
            total_result = self.db.execute_query(total_query)
            
            remote_count = result['count'].iloc[0] if len(result) > 0 else 0
            total_count = total_result['total'].iloc[0] if len(total_result) > 0 else 0
            percentage = (remote_count / total_count * 100) if total_count > 0 else 0
            
            return f"👨‍💻 Удаленных сотрудников: {remote_count:,} ({percentage:.1f}% от общей численности)".replace(',', ' ')
            
        except Exception as e:
            return f"❌ Ошибка при расчете удаленных сотрудников: {str(e)}"
    
    async def time_series_analysis(self, metric: str, group_by: Optional[str] = None, filters: Optional[Dict] = None) -> str:
        try:
            if metric not in TIME_SERIES_METRICS:
                return f"❌ Метрика '{metric}' не поддерживается для временного анализа"
            
            if group_by:
                return "❌ Группировка по дополнительным параметрам пока не поддерживается. Используйте простой временной анализ."
            
            normalized_filters = await self.normalizer.normalize_filters(filters) if filters else {}
            
            query = self.query_builder.build_time_series_query(metric, None, normalized_filters)
            params = self.query_builder.build_params(normalized_filters)
            
            print(f"🔍 TIME SERIES SQL: {query}")
            print(f"🔍 TIME SERIES PARAMS: {params}")
            
            result = self.db.execute_query(query, params)
            return self.formatter.format_time_series(result, metric, None)
            
        except Exception as e:
            return f"❌ Ошибка при анализе временных рядов: {str(e)}"
    
    async def get_unique_values(self, column_name: str) -> str:
        try:
            valid_columns = await self._get_table_columns()
            if column_name not in valid_columns:
                return f"❌ Колонка '{column_name}' не найдена. Доступные: {', '.join(valid_columns)}"
            
            query = f"SELECT DISTINCT {column_name} FROM hr_data_clean WHERE {column_name} IS NOT NULL ORDER BY {column_name} LIMIT 20"
            result = self.db.execute_query(query)
            
            return self.formatter.format_unique_values(result, column_name)
                
        except Exception as e:
            return f"❌ Ошибка при получении уникальных значений: {str(e)}"
    
    async def get_top_values(self, column_name: str, n: int = 20, filters: Optional[Dict] = None) -> str:
        try:
            valid_columns = await self._get_table_columns()
            if column_name not in valid_columns:
                return f"❌ Колонка '{column_name}' не найдена"
            
            normalized_filters = await self.normalizer.normalize_filters(filters) if filters else {}
            
            where_conditions = ["report_date = '2025-08-31'"]
            params = []
            
            for filter_column, value in normalized_filters.items():
                if filter_column != column_name:
                    if isinstance(value, str) and any(op in value for op in ['>', '<', '>=', '<=']):
                        where_conditions.append(f"{filter_column} {value}")
                    elif filter_column == 'location_name' and '%' in str(value):
                        where_conditions.append(f"{filter_column} LIKE ?")
                        params.append(value)
                    else:
                        where_conditions.append(f"{filter_column} = ?")
                        params.append(value)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"""
            SELECT 
                {column_name},
                COUNT(*) as count
            FROM hr_data_clean 
            {where_clause}
            GROUP BY {column_name}
            ORDER BY count DESC
            LIMIT {n}
            """
            
            result = self.db.execute_query(query, params)
            return self._format_top_values_result(result, column_name, n, filters)
            
        except Exception as e:
            return f"❌ Ошибка при получении топ значений: {str(e)}"
    
    async def get_data_sample(self, limit: int = 5) -> str:
        try:
            query = f"SELECT * FROM hr_data_clean LIMIT {limit}"
            result = self.db.execute_query(query)
            return f"Пример данных ({limit} записей):\n{result.to_string(index=False)}"
        except Exception as e:
            return f"❌ Ошибка при получении sample данных: {str(e)}"
    
    async def get_column_info(self) -> str:
        try:
            query = "PRAGMA table_info(hr_data_clean)"
            result = self.db.execute_query(query)
            
            response = "📋 Структура таблица hr_data_clean:\n\n"
            for _, row in result.iterrows():
                response += f"• {row['name']}: {row['type']} {'(PK)' if row['pk'] else ''}\n"
            
            return response
        except Exception as e:
            return f"❌ Ошибка при получении информации о колонках: {str(e)}"

    def _format_fte_distribution_result(self, result: pd.DataFrame) -> str:
        if len(result) == 0:
            return "❌ Нет данных о распределении ставок"
        
        total = result['count'].sum()
        response = "💰 Распределение сотрудников по ставкам:\n\n"
        
        fte_names = {
            0.0: "0.0 (нулевая)",
            0.2: "0.2", 
            0.37: "0.37",
            0.4: "0.4",
            0.5: "0.5 (половинная)",
            0.75: "0.75",
            1.0: "1.0 (полная)"
        }
        
        for _, row in result.iterrows():
            fte_value = row['fte']
            count = row['count']
            percentage = (count / total) * 100
            
            fte_name = fte_names.get(fte_value, f"{fte_value}")
            response += f"• {fte_name} ставка: {count:,} ({percentage:.1f}%)\n".replace(',', ' ')
        
        return response

    def _format_metric_result(self, metric: str, result: pd.DataFrame) -> str:
        if len(result) == 0:
            return f"❌ Нет данных для метрики '{metric}'"
        
        formats = {
            'headcount': lambda r: f"Численность: {r['count'].iloc[0]:,.0f} сотрудников".replace(',', ' '),
            'turnover_rate': lambda r: f"Текучесть: {r['turnover_rate'].iloc[0]:.1f}% ({r['fired'].iloc[0]:,.0f} уволенных из {r['total'].iloc[0]:,.0f})".replace(',', ' '),
            'average_experience': lambda r: f"Средний опыт: {r['avg_exp'].iloc[0]:.1f} месяцев",
            'average_age': lambda r: f"Средний возраст: {r['avg_age'].iloc[0]:.1f} лет",
            'average_fte': lambda r: f"Средняя ставка: {r['avg_fte'].iloc[0]:.2f}",
            'total_fired': lambda r: f"Всего уволено: {r['total_fired'].iloc[0]:,.0f} сотрудников".replace(',', ' '),
            'total_hired': lambda r: f"Всего нанято: {r['total_hired'].iloc[0]:,.0f} сотрудников".replace(',', ' ')
        }
        
        return formats.get(metric, lambda r: f"Результат: {r.iloc[0,0]}")(result)
    
    def _format_top_values_result(self, result: pd.DataFrame, column_name: str, n: int, filters: Optional[Dict] = None) -> str:
        if len(result) == 0:
            return f"❌ Нет данных для анализа"
        
        column_names = {
            'service': 'сервисов',
            'location_name': 'локаций',
            'cluster': 'кластеров',
            'age_category': 'возрастных категорий',
            'experience_category': 'категорий опыта',
            'sex': 'пола'
        }
        
        display_name = column_names.get(column_name, column_name)
        
        header = f"🏆 Топ-{n} {display_name}"
        
        if filters and 'hirecount' in filters:
            header += " по найму:"
        else:
            header += " по численности:"
        
        response = f"{header}\n\n"
        
        for i, (_, row) in enumerate(result.iterrows(), 1):
            response += f"{i}. {row[column_name]}: {row['count']:,.0f}\n".replace(',', ' ')
        
        return response

    async def _get_table_columns(self) -> List[str]:
        query = "PRAGMA table_info(hr_data_clean)"
        result = self.db.execute_query(query)
        return result['name'].tolist()
    
    async def _get_column_type(self, column_name: str) -> str:
        query = "PRAGMA table_info(hr_data_clean)"
        result = self.db.execute_query(query)
        column_info = result[result['name'] == column_name]
        return column_info['type'].iloc[0] if not column_info.empty else 'TEXT'

    def _get_column_display_name(self, column_name: str) -> str:
        column_names = {
            'service': 'сервисов',
            'location_name': 'локаций',
            'cluster': 'кластеров',
            'age_category': 'возрастных категорий',
            'experience_category': 'категорий опыта',
            'sex': 'пола'
        }
        return column_names.get(column_name, column_name)