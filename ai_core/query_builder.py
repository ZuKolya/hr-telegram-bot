#query_builder.py:
from typing import Dict, List, Optional

class QueryBuilder:
    @staticmethod
    def build_where_clause(filters: Optional[Dict], use_last_date: bool = True) -> str:
        """Построение WHERE условия с безопасной обработкой дат"""
        conditions = []
        
        if use_last_date:
            conditions.append("report_date = '2025-08-31'")
        
        if filters:
            for column, value in filters.items():
                if value is not None:
                    if isinstance(value, str) and any(op in value for op in ['>', '<', '>=', '<=']):
                        conditions.append(f"{column} {value}")
                    elif column == 'location_name' and isinstance(value, str) and '%' in value:
                        conditions.append(f"{column} LIKE ?")
                    elif column == 'report_date' and isinstance(value, str):
                        if value == '2025-08-31' or value == '2025-09-03' or value == '2025-07-31':
                            conditions.append(f"{column} = ?")
                        elif value == '2025-08':
                            conditions.append("report_date = '2025-08-31'")
                        elif value == '2025-09':
                            conditions.append("report_date = '2025-09-03'")
                        elif value == '2025-07':
                            conditions.append("report_date = '2025-07-31'")
                        else:
                            conditions.append(f"{column} = ?")
                    else:
                        conditions.append(f"{column} = ?")
        
        return "WHERE " + " AND ".join(conditions) if conditions else ""

    @staticmethod
    def build_params(filters: Optional[Dict]) -> List:
        """Построение списка параметров для SQL запроса"""
        if not filters:
            return []
        params = []
        for value in filters.values():
            if value is not None:
                if (isinstance(value, str) and 
                    (len(value) == 7 and value in ['2025-08', '2025-09', '2025-07'] or
                     any(op in value for op in ['>', '<', '>=', '<=']) or
                     '%' in value)):
                    continue
                params.append(value)
        return params

    @staticmethod
    def build_headcount_query(filters: Optional[Dict]) -> str:
        """Построение запроса для численности сотрудников"""
        where_clause = QueryBuilder.build_where_clause(filters)
        return f"""
        SELECT COUNT(*) as count 
        FROM hr_data_clean 
        {where_clause}
        AND fire_from_company = '1970-01-01'
        """

    @staticmethod
    def build_turnover_rate_query(filters: Optional[Dict]) -> str:
        """Построение запроса для текучести кадров"""
        where_clause = QueryBuilder.build_where_clause(filters)
        return f"""
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

    @staticmethod
    def build_numeric_stats_query(column_name: str, filters: Optional[Dict]) -> str:
        """Построение запроса для числовой статистики"""
        where_clause = QueryBuilder.build_where_clause(filters)
        return f"""
        SELECT 
            COUNT({column_name}) as count,
            AVG({column_name}) as average,
            MIN({column_name}) as min,
            MAX({column_name}) as max,
            SUM({column_name}) as sum
        FROM hr_data_clean 
        {where_clause}
        """

    @staticmethod
    def build_categorical_stats_query(column_name: str, filters: Optional[Dict]) -> str:
        """Построение запроса для категориальной статистики"""
        where_clause = QueryBuilder.build_where_clause(filters)
        return f"""
        SELECT 
            {column_name},
            COUNT(*) as count
        FROM hr_data_clean 
        {where_clause}
        GROUP BY {column_name}
        ORDER BY count DESC
        LIMIT 20
        """

    @staticmethod
    def build_time_series_query(metric: str, group_by: Optional[str], filters: Optional[Dict]) -> str:
        """Построение запроса для временных рядов"""
        where_clause = QueryBuilder.build_where_clause(filters, use_last_date=False)
        
        metric_selects = {
            'headcount': "report_date, COUNT(*) as value",
            'total_hired': "report_date, SUM(hirecount) as value",
            'total_fired': "report_date, SUM(firecount) as value"
        }
        
        select_clause = metric_selects.get(metric, "report_date, COUNT(*) as value")
        
        if group_by:
            select_clause += f", {group_by}"
            group_clause = f"GROUP BY report_date, {group_by}"
            order_clause = f"ORDER BY report_date, {group_by}"
        else:
            group_clause = "GROUP BY report_date"
            order_clause = "ORDER BY report_date"
        
        return f"""
        SELECT {select_clause}
        FROM hr_data_clean 
        {where_clause}
        {group_clause}
        {order_clause}
        """

    @staticmethod
    def build_service_comparison_query(metric: str, filters: Optional[Dict] = None) -> str:
        """Построение запроса для сравнения сервисов"""
        where_clause = QueryBuilder.build_where_clause(filters)
        
        metric_calculations = {
            'headcount': "COUNT(*)",
            'total_hired': "SUM(hirecount)",
            'total_fired': "SUM(firecount)",
            'turnover_rate': "CASE WHEN COUNT(*) > 0 THEN ROUND(CAST(SUM(firecount) AS FLOAT) / COUNT(*) * 100, 2) ELSE 0 END"
        }
        
        calculation = metric_calculations.get(metric, "COUNT(*)")
        
        return f"""
        SELECT 
            service,
            {calculation} as value
        FROM hr_data_clean 
        {where_clause}
        GROUP BY service
        ORDER BY value DESC
        """

    @staticmethod
    def build_demographic_query(demographic_field: str, filters: Optional[Dict] = None) -> str:
        """Построение запроса для демографического анализа"""
        where_clause = QueryBuilder.build_where_clause(filters)
        
        return f"""
        SELECT 
            {demographic_field},
            COUNT(*) as count
        FROM hr_data_clean 
        {where_clause}
        AND fire_from_company = '1970-01-01'
        GROUP BY {demographic_field}
        ORDER BY count DESC
        """

    
    @staticmethod
    def build_fte_distribution_query(filters: Optional[Dict] = None) -> str:
        """Построение запроса для распределения ставок"""
        where_clause = QueryBuilder.build_where_clause(filters)
        return f"""
        SELECT 
            fte,
            COUNT(*) as count
        FROM hr_data_clean 
        {where_clause}
        AND fire_from_company = '1970-01-01'
        GROUP BY fte
        ORDER BY fte
        """

    @staticmethod
    def build_remote_workers_query(filters: Optional[Dict] = None) -> str:
        """Построение запроса для удаленных сотрудников"""
        where_clause = QueryBuilder.build_where_clause(filters)
        remote_condition = "AND location_name LIKE '%Дистанционщик%'"
        return f"""
        SELECT COUNT(*) as count
        FROM hr_data_clean 
        {where_clause}
        AND fire_from_company = '1970-01-01'
        {remote_condition}
        """

    @staticmethod
    def build_age_filter_query(age_condition: str, filters: Optional[Dict] = None) -> str:
        """Построение запроса для фильтрации по возрасту"""
        where_clause = QueryBuilder.build_where_clause(filters)
        age_filter = f"AND fullyears {age_condition}"
        return f"""
        SELECT COUNT(*) as count
        FROM hr_data_clean 
        {where_clause}
        AND fire_from_company = '1970-01-01'
        {age_filter}
        """

    @staticmethod
    def build_experience_filter_query(experience_condition: str, filters: Optional[Dict] = None) -> str:
        """Построение запроса для фильтрации по опыту"""
        where_clause = QueryBuilder.build_where_clause(filters)
        experience_filter = f"AND experience {experience_condition}"
        return f"""
        SELECT COUNT(*) as count
        FROM hr_data_clean 
        {where_clause}
        AND fire_from_company = '1970-01-01'
        {experience_filter}
        """

    @staticmethod
    def build_location_like_query(location_pattern: str, filters: Optional[Dict] = None) -> str:
        """Построение запроса для поиска локаций по шаблону"""
        where_clause = QueryBuilder.build_where_clause(filters)
        location_condition = f"AND location_name LIKE '{location_pattern}'"
        return f"""
        SELECT COUNT(*) as count
        FROM hr_data_clean 
        {where_clause}
        AND fire_from_company = '1970-01-01'
        {location_condition}
        """