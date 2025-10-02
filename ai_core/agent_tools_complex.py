# agent_tools_complex.py
import pandas as pd
from typing import Dict, List, Optional
from database import DatabaseManager
from .query_builder import QueryBuilder
from .data_normalizer import DataNormalizer
from .response_formatter import ResponseFormatter
from .constants import SUPPORTED_METRICS, EXPERIENCE_THRESHOLDS, AGE_THRESHOLDS

class ComplexAgentTools:
    def __init__(self, db_path: str):
        self.db = DatabaseManager(db_path)
        self.normalizer = DataNormalizer(self.db)
        self.query_builder = QueryBuilder()
        self.formatter = ResponseFormatter()
    
    async def compare_metrics(self, metric: str, dimension: str, filters: Optional[Dict] = None) -> str:
        try:
            valid_dimensions = ['service', 'location_name', 'cluster', 'age_category', 'experience_category', 'sex']
            if dimension not in valid_dimensions:
                return f"❌ Измерение {dimension} не поддерживается. Доступные: {', '.join(valid_dimensions)}"
            
            normalized_filters = await self.normalizer.normalize_filters(filters) if filters else {}
            
            metric_calculation = self._get_metric_calculation(metric)
            
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
            
            if metric in ['headcount', 'average_experience', 'average_age', 'average_fte']:
                where_conditions.append("fire_from_company = '1970-01-01'")
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"""
            SELECT 
                {dimension},
                {metric_calculation} as value
            FROM hr_data_clean 
            {where_clause}
            GROUP BY {dimension}
            ORDER BY value DESC
            LIMIT 20
            """
            
            result = self.db.execute_query(query, params)
            return self._format_comparison_result(result, metric, dimension)
            
        except Exception as e:
            return f"❌ Ошибка сравнения: {str(e)}"

    async def compare_metrics_min(self, metric: str, dimension: str, filters: Optional[Dict] = None) -> str:
        try:
            valid_dimensions = ['service', 'location_name', 'cluster', 'age_category', 'experience_category', 'sex']
            if dimension not in valid_dimensions:
                return f"❌ Измерение {dimension} не поддерживается. Доступные: {', '.join(valid_dimensions)}"
            
            normalized_filters = await self.normalizer.normalize_filters(filters) if filters else {}
            
            metric_calculation = self._get_metric_calculation(metric)
            
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
            
            if metric in ['headcount', 'average_experience', 'average_age', 'average_fte']:
                where_conditions.append("fire_from_company = '1970-01-01'")
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"""
            SELECT 
                {dimension},
                {metric_calculation} as value
            FROM hr_data_clean 
            {where_clause}
            GROUP BY {dimension}
            ORDER BY value ASC
            LIMIT 20
            """
            
            result = self.db.execute_query(query, params)
            return self._format_comparison_result(result, metric, dimension)
            
        except Exception as e:
            return f"❌ Ошибка сравнения: {str(e)}"

    async def trend_analysis(self, metric: str, period: str = "3month", filters: Optional[Dict] = None) -> str:
        try:
            normalized_filters = await self.normalizer.normalize_filters(filters) if filters else {}
            
            metric_calculation = self._get_metric_calculation(metric)
            
            where_conditions = []
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
            
            if metric in ['headcount', 'average_experience', 'average_age', 'average_fte']:
                where_conditions.append("fire_from_company = '1970-01-01'")
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"""
            SELECT 
                report_date,
                {metric_calculation} as value
            FROM hr_data_clean 
            {where_clause}
            GROUP BY report_date
            ORDER BY report_date
            """
            
            result = self.db.execute_query(query, params)
            return self._format_trend_result(result, metric, period)
            
        except Exception as e:
            return f"❌ Ошибка анализа трендов: {str(e)}"

    async def calculate_complex_metric(self, metric: str, filters: Optional[Dict] = None) -> str:
        metric = metric.lower() if metric else ""
        try:
            if metric not in SUPPORTED_METRICS:
                return f"❌ Метрика {metric} не поддерживается. Доступные: {', '.join(SUPPORTED_METRICS)}"
            
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
            
            if metric in ['headcount', 'average_experience', 'average_age', 'average_fte', 'young_workers', 'experienced_workers']:
                where_conditions.append("fire_from_company = '1970-01-01'")
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            if metric == 'young_workers':
                return await self._calculate_young_workers(normalized_filters)
            elif metric == 'experienced_workers':
                return await self._calculate_experienced_workers(normalized_filters)
            else:
                return await self._calculate_basic_metric(metric, normalized_filters, where_clause, params)
                
        except Exception as e:
            return f"❌ Ошибка при расчете метрики {metric}: {str(e)}"

    async def _calculate_young_workers(self, filters: Dict) -> str:
        try:
            where_conditions = [
                "report_date = '2025-08-31'", 
                "fire_from_company = '1970-01-01'",
                "fullyears < 25"
            ]
            params = []
            
            for column, value in filters.items():
                if isinstance(value, str) and any(op in value for op in ['>', '<', '>=', '<=']):
                    where_conditions.append(f"{column} {value}")
                elif column == 'location_name' and '%' in str(value):
                    where_conditions.append(f"{column} LIKE ?")
                    params.append(value)
                else:
                    where_conditions.append(f"{column} = ?")
                    params.append(value)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"SELECT COUNT(*) as count FROM hr_data_clean {where_clause}"
            result = self.db.execute_query(query, params)
            
            total_query = "SELECT COUNT(*) as total FROM hr_data_clean WHERE report_date = '2025-08-31' AND fire_from_company = '1970-01-01'"
            total_result = self.db.execute_query(total_query)
            
            young_count = result['count'].iloc[0] if len(result) > 0 else 0
            total_count = total_result['total'].iloc[0] if len(total_result) > 0 else 0
            percentage = (young_count / total_count * 100) if total_count > 0 else 0
            
            return f"👦 Молодых сотрудников (<25 лет): {young_count:,} ({percentage:.1f}% от общей численности)".replace(',', ' ')
            
        except Exception as e:
            return f"❌ Ошибка при расчете молодых сотрудников: {str(e)}"

    async def _calculate_experienced_workers(self, filters: Dict) -> str:
        try:
            where_conditions = [
                "report_date = '2025-08-31'", 
                "fire_from_company = '1970-01-01'",
                "experience > 60"
            ]
            params = []
            
            for column, value in filters.items():
                if isinstance(value, str) and any(op in value for op in ['>', '<', '>=', '<=']):
                    where_conditions.append(f"{column} {value}")
                elif column == 'location_name' and '%' in str(value):
                    where_conditions.append(f"{column} LIKE ?")
                    params.append(value)
                else:
                    where_conditions.append(f"{column} = ?")
                    params.append(value)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"SELECT COUNT(*) as count FROM hr_data_clean {where_clause}"
            result = self.db.execute_query(query, params)
            
            total_query = "SELECT COUNT(*) as total FROM hr_data_clean WHERE report_date = '2025-08-31' AND fire_from_company = '1970-01-01'"
            total_result = self.db.execute_query(total_query)
            
            exp_count = result['count'].iloc[0] if len(result) > 0 else 0
            total_count = total_result['total'].iloc[0] if len(total_result) > 0 else 0
            percentage = (exp_count / total_count * 100) if total_count > 0 else 0
            
            return f"👴 Опытных сотрудников (>5 лет): {exp_count:,} ({percentage:.1f}% от общей численности)".replace(',', ' ')
            
        except Exception as e:
            return f"❌ Ошибка при расчете опытных сотрудников: {str(e)}"

    async def _calculate_basic_metric(self, metric: str, filters: Dict, where_clause: str, params: List) -> str:
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
        else:
            return f"❌ Метрика {metric} не поддерживается в сложном модуле"
        
        result = self.db.execute_query(query, params)
        return self._format_metric_result(metric, result)

    async def analyze_attrition_by_demography(self, service: str, dimension: str, filters: Optional[Dict] = None) -> str:
        try:
            valid_dimensions = ['age_category', 'experience_category', 'sex', 'cluster']
            if dimension not in valid_dimensions:
                return f"❌ Измерение {dimension} не поддерживается. Доступные: {', '.join(valid_dimensions)}"
            
            normalized_filters = await self.normalizer.normalize_filters(filters) if filters else {}
            normalized_filters['service'] = service
            
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
            
            query = f"""
            SELECT 
                {dimension},
                COUNT(*) as total_employees,
                SUM(firecount) as fired_count,
                CASE 
                    WHEN COUNT(*) > 0 THEN ROUND(CAST(SUM(firecount) AS FLOAT) / COUNT(*) * 100, 2)
                    ELSE 0 
                END as attrition_rate
            FROM hr_data_clean 
            {where_clause}
            GROUP BY {dimension}
            ORDER BY attrition_rate DESC
            """
            
            result = self.db.execute_query(query, params)
            return self._format_attrition_by_demography_result(result, service, dimension)
            
        except Exception as e:
            return f"❌ Ошибка анализа оттока по демографии: {str(e)}"

    async def deep_segmentation_analysis(self, segment_by: List[str], metrics: List[str], filters: Optional[Dict] = None) -> str:
        try:
            if len(segment_by) == 0:
                return "❌ Укажите хотя бы одно измерение для сегментации"
            
            if len(segment_by) > 2:
                return "❌ Слишком много измерений для сегментации. Максимум 2."
            
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
            
            metric_selects = []
            for metric in metrics:
                if metric == 'headcount':
                    metric_selects.append("COUNT(*) as headcount")
                elif metric == 'attrition_rate':
                    metric_selects.append("CASE WHEN COUNT(*) > 0 THEN ROUND(CAST(SUM(firecount) AS FLOAT) / COUNT(*) * 100, 2) ELSE 0 END as attrition_rate")
                elif metric == 'avg_experience':
                    metric_selects.append("AVG(experience) as avg_experience")
                elif metric == 'avg_age':
                    metric_selects.append("AVG(fullyears) as avg_age")
                elif metric == 'avg_fte':
                    metric_selects.append("AVG(fte) as avg_fte")
            
            select_clause = ", ".join([segment_by[0]] + ([] if len(segment_by) == 1 else [segment_by[1]]) + metric_selects)
            group_clause = "GROUP BY " + ", ".join(segment_by)
            order_clause = f"ORDER BY {segment_by[0]}" + (f", {segment_by[1]}" if len(segment_by) > 1 else "")
            
            query = f"""
            SELECT 
                {select_clause}
            FROM hr_data_clean 
            {where_clause}
            {group_clause}
            {order_clause}
            LIMIT 50
            """
            
            result = self.db.execute_query(query, params)
            return self._format_deep_segmentation_result(result, segment_by, metrics)
            
        except Exception as e:
            return f"❌ Ошибка глубокой сегментации: {str(e)}"

    async def attrition_risk_analysis(self, service: str = None, risk_factors: List[str] = None, filters: Optional[Dict] = None) -> str:
        try:
            normalized_filters = await self.normalizer.normalize_filters(filters) if filters else {}
            if service:
                normalized_filters['service'] = service
            
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
            
            risk_dimensions = risk_factors if risk_factors else ['age_category', 'experience_category', 'sex', 'fte']
            
            all_results = {}
            
            for dimension in risk_dimensions:
                query = f"""
                SELECT 
                    {dimension},
                    COUNT(*) as total,
                    SUM(firecount) as fired,
                    CASE 
                        WHEN COUNT(*) > 0 THEN ROUND(CAST(SUM(firecount) AS FLOAT) / COUNT(*) * 100, 2)
                        ELSE 0 
                    END as attrition_rate
                FROM hr_data_clean 
                {where_clause}
                GROUP BY {dimension}
                HAVING COUNT(*) >= 10
                ORDER BY attrition_rate DESC
                LIMIT 10
                """
                
                result = self.db.execute_query(query, params)
                all_results[dimension] = result
            
            return self._format_risk_analysis_result(all_results, service)
            
        except Exception as e:
            return f"❌ Ошибка анализа рисков увольнения: {str(e)}"

    async def calculate_hiring_needs(self, service: str = None, period: str = "month", filters: Optional[Dict] = None) -> str:
        try:
            if not service or service in ['all', 'service']:
                return await self._calculate_company_hiring_needs(period, filters)
            
            normalized_filters = await self.normalizer.normalize_filters(filters) if filters else {}
            normalized_filters['service'] = service
            
            attrition_query = """
            SELECT 
                SUM(firecount) as monthly_attrition,
                COUNT(*) as total_employees
            FROM hr_data_clean 
            WHERE service = ? AND report_date = '2025-08-31'
            """
            
            result = self.db.execute_query(attrition_query, [service])
            
            if len(result) == 0 or result['total_employees'].iloc[0] == 0:
                return f"❌ Нет данных для сервиса {service}"
            
            monthly_attrition = result['monthly_attrition'].iloc[0]
            total_employees = result['total_employees'].iloc[0]
            attrition_rate = (monthly_attrition / total_employees * 100) if total_employees > 0 else 0
            
            hiring_query = """
            SELECT SUM(hirecount) as monthly_hiring
            FROM hr_data_clean 
            WHERE service = ? AND report_date = '2025-08-31'
            """
            
            hiring_result = self.db.execute_query(hiring_query, [service])
            monthly_hiring = hiring_result['monthly_hiring'].iloc[0] if len(hiring_result) > 0 else 0
            
            hiring_gap = monthly_attrition - monthly_hiring
            needed_hiring = max(0, hiring_gap)
            
            return self._format_hiring_needs_result(
                service, monthly_attrition, monthly_hiring, 
                needed_hiring, attrition_rate, total_employees
            )
            
        except Exception as e:
            return f"❌ Ошибка расчета потребности в найме: {str(e)}"

    async def _calculate_company_hiring_needs(self, period: str = "month", filters: Optional[Dict] = None) -> str:
        try:
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
            
            attrition_query = f"""
            SELECT 
                SUM(firecount) as monthly_attrition,
                COUNT(*) as total_employees
            FROM hr_data_clean 
            {where_clause}
            """
            
            result = self.db.execute_query(attrition_query, params)
            
            if len(result) == 0 or result['total_employees'].iloc[0] == 0:
                return "❌ Нет данных для анализа по компании"
            
            monthly_attrition = result['monthly_attrition'].iloc[0]
            total_employees = result['total_employees'].iloc[0]
            attrition_rate = (monthly_attrition / total_employees * 100) if total_employees > 0 else 0
            
            hiring_query = f"""
            SELECT SUM(hirecount) as monthly_hiring
            FROM hr_data_clean 
            {where_clause}
            """
            
            hiring_result = self.db.execute_query(hiring_query, params)
            monthly_hiring = hiring_result['monthly_hiring'].iloc[0] if len(hiring_result) > 0 else 0
            
            hiring_gap = monthly_attrition - monthly_hiring
            needed_hiring = max(0, hiring_gap)
            
            return self._format_company_hiring_needs_result(
                monthly_attrition, monthly_hiring, 
                needed_hiring, attrition_rate, total_employees
            )
            
        except Exception as e:
            return f"❌ Ошибка расчета потребности в найме по компании: {str(e)}"

    async def correlation_analysis(self, metric1: str, metric2: str, filters: Optional[Dict] = None) -> str:
        return "❌ Корреляционный анализ временно недоступен. Используйте сравнение метрик."

    async def segmentation_analysis(self, segment_by: str, metrics: List[str], filters: Optional[Dict] = None) -> str:
        return "❌ Сегментационный анализ временно недоступен. Используйте сравнение по измерениям."

    async def risk_assessment(self, filters: Optional[Dict] = None) -> str:
        return "❌ Анализ рисков временно недоступен. Используйте сравнение текучести по сервисам."

    def _format_attrition_by_demography_result(self, result: pd.DataFrame, service: str, dimension: str) -> str:
        if len(result) == 0:
            return f"❌ Нет данных для анализа оттока в сервисе {service}"
        
        dimension_names = {
            'age_category': 'возрастным категориям',
            'experience_category': 'категориям опыта',
            'sex': 'полу',
            'cluster': 'кластерам'
        }
        
        dimension_name = dimension_names.get(dimension, dimension)
        
        response = f"📊 Анализ текучести в сервисе {service} по {dimension_name}:\n\n"
        
        for i, (_, row) in enumerate(result.iterrows(), 1):
            dimension_value = row[dimension]
            total = row['total_employees']
            fired = row['fired_count']
            attrition_rate = row['attrition_rate']
            
            if dimension == 'sex':
                dimension_value = 'Женщины' if dimension_value == 'F' else 'Мужчины' if dimension_value == 'M' else dimension_value
            
            if attrition_rate > 15:
                risk_emoji = "🔴"
            elif attrition_rate > 8:
                risk_emoji = "🟡"
            else:
                risk_emoji = "🟢"
            
            response += f"{i}. {dimension_value}:\n"
            response += f"   • Сотрудников: {total:,}\n".replace(',', ' ')
            response += f"   • Уволено: {fired:,}\n".replace(',', ' ')
            response += f"   • Текучесть: {attrition_rate:.1f}% {risk_emoji}\n\n"
        
        return response

    def _format_deep_segmentation_result(self, result: pd.DataFrame, segment_by: List[str], metrics: List[str]) -> str:
        if len(result) == 0:
            return "❌ Нет данных для сегментационного анализа"
        
        dimension_names = {
            'service': 'сервис',
            'age_category': 'возраст',
            'experience_category': 'опыт',
            'sex': 'пол',
            'cluster': 'кластер',
            'location_name': 'локация',
            'fte': 'ставка'
        }
        
        metric_names = {
            'headcount': 'сотрудников',
            'attrition_rate': 'текучесть',
            'avg_experience': 'средний опыт',
            'avg_age': 'средний возраст', 
            'avg_fte': 'средняя ставка'
        }
        
        response = f"🔍 Глубокая сегментация по {dimension_names.get(segment_by[0], segment_by[0])}"
        if len(segment_by) > 1:
            response += f" и {dimension_names.get(segment_by[1], segment_by[1])}"
        response += ":\n\n"
        
        total_count = result['headcount'].sum() if 'headcount' in result.columns else 0
        
        current_segment = None
        for _, row in result.iterrows():
            segment_value = row[segment_by[0]]
            
            if segment_value != current_segment:
                if current_segment is not None:
                    response += "\n"
                response += f"📋 {dimension_names.get(segment_by[0], segment_by[0]).capitalize()} {segment_value}:\n"
                current_segment = segment_value
            
            sub_segment = row[segment_by[1]] if len(segment_by) > 1 else "Все"
            
            if segment_by[1] == 'sex' if len(segment_by) > 1 else False:
                sub_segment = 'Женщины' if sub_segment == 'F' else 'Мужчины' if sub_segment == 'M' else sub_segment
            elif segment_by[1] == 'fte' if len(segment_by) > 1 else False:
                fte_names = {
                    0.0: "0.0 (нулевая)",
                    0.2: "0.2", 
                    0.37: "0.37",
                    0.4: "0.4",
                    0.5: "0.5 (половинная)",
                    0.75: "0.75",
                    1.0: "1.0 (полная)"
                }
                sub_segment = fte_names.get(sub_segment, f"{sub_segment}")
            
            segment_line = f"   • {sub_segment}: "
            metric_values = []
            
            for metric in metrics:
                if metric in row:
                    value = row[metric]
                    if metric == 'headcount':
                        percentage = (value / total_count * 100) if total_count > 0 else 0
                        metric_values.append(f"{value:,} сотрудников ({percentage:.1f}%)".replace(',', ' '))
                    elif metric == 'attrition_rate':
                        metric_values.append(f"{value:.1f}% текучести")
                    elif metric == 'avg_experience':
                        metric_values.append(f"{value:.1f} мес опыта")
                    elif metric == 'avg_age':
                        metric_values.append(f"{value:.1f} лет")
                    elif metric == 'avg_fte':
                        metric_values.append(f"{value:.2f} ставки")
                    else:
                        metric_values.append(f"{value:,}".replace(',', ' '))
            
            segment_line += " | ".join(metric_values)
            response += segment_line + "\n"
        
        return response

    def _format_risk_analysis_result(self, all_results: Dict, service: str) -> str:
        response = f"⚠️ Анализ рисков увольнения"
        if service:
            response += f" в сервисе {service}"
        response += ":\n\n"
        
        has_data = False
        
        for dimension, result in all_results.items():
            if len(result) > 0:
                has_data = True
                dimension_names = {
                    'age_category': 'Возрастные группы',
                    'experience_category': 'Группы опыта', 
                    'sex': 'Пол',
                    'fte': 'Типы ставок'
                }
                
                response += f"📊 {dimension_names.get(dimension, dimension)}:\n"
                
                high_risk_groups = result[result['attrition_rate'] > 10]
                if len(high_risk_groups) > 0:
                    for _, row in high_risk_groups.iterrows():
                        dimension_value = row[dimension]
                        
                        if dimension == 'sex':
                            dimension_value = 'Женщины' if dimension_value == 'F' else 'Мужчины'
                        elif dimension == 'fte':
                            fte_names = {
                                0.0: "0.0 (нулевая)",
                                0.2: "0.2", 
                                0.37: "0.37",
                                0.4: "0.4", 
                                0.5: "0.5 (половинная)",
                                0.75: "0.75",
                                1.0: "1.0 (полная)"
                            }
                            dimension_value = fte_names.get(dimension_value, f"{dimension_value}")
                        
                        response += f"   • {dimension_value}: {row['attrition_rate']:.1f}% текучести ({row['fired']}/{row['total']} чел.) 🔴\n"
                else:
                    response += f"   • Высокорисковых групп не обнаружено 🟢\n"
                
                response += "\n"
        
        if not has_data:
            return "❌ Недостаточно данных для анализа рисков"
        
        return response

    def _format_hiring_needs_result(self, service: str, monthly_attrition: int, monthly_hiring: int, 
                                  needed_hiring: int, attrition_rate: float, total_employees: int) -> str:
        response = f"📈 Анализ потребности в найме для сервиса {service}:\n\n"
        
        response += f"• Всего сотрудников: {total_employees:,}\n".replace(',', ' ')
        response += f"• Текущая текучесть: {attrition_rate:.1f}% ({monthly_attrition:,} чел./мес)\n".replace(',', ' ')
        response += f"• Текущий найм: {monthly_hiring:,} чел./мес\n".replace(',', ' ')
        
        if needed_hiring > 0:
            response += f"• 🔴 Потребность в найме: {needed_hiring:,} чел./мес\n".replace(',', ' ')
            response += f"• 💡 Рекомендация: увеличить найм на {needed_hiring:,} сотрудников в месяц".replace(',', ' ')
        else:
            response += f"• 🟢 Найм покрывает отток: +{abs(needed_hiring):,} чел.\n".replace(',', ' ')
            response += f"• 💡 Текущий уровень найма достаточен"
        
        return response

    def _format_company_hiring_needs_result(self, monthly_attrition: int, monthly_hiring: int, 
                                          needed_hiring: int, attrition_rate: float, total_employees: int) -> str:
        response = f"🏢 Анализ потребности в найме по всей компании:\n\n"
        
        response += f"• Всего сотрудников: {total_employees:,}\n".replace(',', ' ')
        response += f"• Текущая текучесть: {attrition_rate:.1f}% ({monthly_attrition:,} чел./мес)\n".replace(',', ' ')
        response += f"• Текущий найм: {monthly_hiring:,} чел./мес\n".replace(',', ' ')
        
        if needed_hiring > 0:
            response += f"• 🔴 Общая потребность в найме: {needed_hiring:,} чел./мес\n".replace(',', ' ')
            response += f"• 💡 Рекомендация: увеличить найм на {needed_hiring:,} сотрудников в месяц".replace(',', ' ')
        else:
            response += f"• 🟢 Найм покрывает отток: +{abs(needed_hiring):,} чел.\n".replace(',', ' ')
            response += f"• 💡 Текущий уровень найма достаточен"
        
        return response

    def _get_metric_calculation(self, metric: str) -> str:
        calculations = {
            'headcount': "COUNT(*)",
            'turnover_rate': "CASE WHEN COUNT(*) > 0 THEN ROUND(CAST(SUM(firecount) AS FLOAT) / COUNT(*) * 100, 2) ELSE 0 END",
            'average_experience': "AVG(experience)",
            'average_age': "AVG(fullyears)", 
            'average_fte': "AVG(fte)",
            'total_fired': "SUM(firecount)",
            'total_hired': "SUM(hirecount)"
        }
        return calculations.get(metric, "COUNT(*)")

    def _format_comparison_result(self, result: pd.DataFrame, metric: str, dimension: str) -> str:
        if len(result) == 0:
            return f"❌ Нет данных для сравнения {metric} по {dimension}"
        
        metric_names = {
            'headcount': 'Численность',
            'turnover_rate': 'Текучесть',
            'average_experience': 'Средний опыт',
            'average_age': 'Средний возраст',
            'average_fte': 'Средняя ставка',
            'total_fired': 'Уволено',
            'total_hired': 'Нанято'
        }
        
        dimension_names = {
            'service': 'сервисам',
            'location_name': 'локациям', 
            'cluster': 'кластерам',
            'age_category': 'возрастным категориям',
            'experience_category': 'категориям опыта',
            'sex': 'полу'
        }
        
        metric_name = metric_names.get(metric, metric)
        dimension_name = dimension_names.get(dimension, dimension)
        
        response = f"📊 {metric_name} по {dimension_name}:\n\n"
        
        for i, (_, row) in enumerate(result.iterrows(), 1):
            value = row['value']
            dimension_value = row[dimension]
            
            if metric in ['turnover_rate']:
                value_str = f"{value:.1f}%"
            elif metric in ['average_experience', 'average_age']:
                value_str = f"{value:.1f}"
            elif metric in ['average_fte']:
                value_str = f"{value:.2f}"
            else:
                value_str = f"{value:,.0f}".replace(',', ' ')
            
            if metric == 'turnover_rate':
                if value > 10:
                    emoji = "🔴"
                elif value > 5:
                    emoji = "🟡" 
                else:
                    emoji = "🟢"
                response += f"{i}. {dimension_value}: {value_str} {emoji}\n"
            else:
                response += f"{i}. {dimension_value}: {value_str}\n"
        
        return response

    def _format_trend_result(self, result: pd.DataFrame, metric: str, period: str) -> str:
        if len(result) == 0:
            return f"❌ Нет данных для анализа тренда {metric}"
        
        metric_names = {
            'headcount': 'Численности',
            'turnover_rate': 'Текучести',
            'total_fired': 'Увольнений', 
            'total_hired': 'Наймов'
        }
        
        metric_name = metric_names.get(metric, metric)
        
        response = f"📈 Динамика {metric_name}:\n\n"
        
        previous_value = None
        for _, row in result.iterrows():
            date_str = str(row['report_date'])[:10]
            value = row['value']
            
            if metric in ['turnover_rate']:
                value_str = f"{value:.1f}%"
            else:
                value_str = f"{value:,.0f}".replace(',', ' ')
            
            trend_emoji = "📊"
            if previous_value is not None and previous_value != 0:
                change = ((value - previous_value) / previous_value) * 100
                if change > 5:
                    trend_emoji = "📈"
                elif change < -5:
                    trend_emoji = "📉"
            
            response += f"• {date_str}: {value_str} {trend_emoji}\n"
            previous_value = value
        
        return response

    def _format_metric_result(self, metric: str, result: pd.DataFrame) -> str:
        if len(result) == 0:
            return f"❌ Нет данных для метрики {metric}"
        
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